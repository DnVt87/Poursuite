"""Aggregate endpoints (UI Phase 2.5, Patch 2).

Three views over the snapshot store, all sharing the same `where` /
`snapshot` / `flagged_only` / `unflagged_only` surface as POST /api/query
via `synth_where_only` in esaj_query.py.

  POST /api/aggregates/group_by   — count rows by a categorical column
  POST /api/aggregates/histogram  — bucket counts on value_centavos
  POST /api/aggregates/stats      — count/sum/mean/median/min/max on value_centavos

All operate on `process_snapshot ps` with the snapshot predicate baked in.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request

from poursuite.api.auth import require_api_key
from poursuite.db.esaj_query import QueryError, synth_where_only

router = APIRouter(prefix="/api/aggregates", tags=["aggregates"])


# Whitelisted group_by columns. last_movement_bucket is the time-bucket
# virtual field, computed from last_movement_iso.
GROUP_BY_FIELDS = (
    "class_type", "foro_code", "foro_name", "vara", "juiz",
    "distribution_year", "last_movement_bucket",
)
STAT_FIELDS = ("value",)  # alias -> value_centavos


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


def _connection(request: Request) -> sqlite3.Connection:
    return _store(request)._conn  # private but stable; same handle build_query uses


def _scoped_where(body: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """Return (where_sql, params) honouring `where`, `snapshot`, flag filters."""
    where_sql, params, _joins = synth_where_only(body)
    return where_sql, params


# ──────────────────────────────────────────────────────────────────────
# group_by
# ──────────────────────────────────────────────────────────────────────

# Time bucket boundaries in days; emit labels in PT-BR style.
_LAST_MOVEMENT_BUCKETS = (
    ("≤30d",      0,   30),
    ("30-90d",   30,   90),
    ("90-180d",  90,  180),
    ("180-365d", 180, 365),
    (">365d",    365, None),  # None upper bound = open-ended
)


@router.post("/group_by")
def group_by(
    body: Dict[str, Any],
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")
    field = body.get("group_by")
    if field not in GROUP_BY_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"group_by must be one of {list(GROUP_BY_FIELDS)}",
        )
    try:
        where_sql, params = _scoped_where(body)
    except QueryError as e:
        raise HTTPException(status_code=400, detail=str(e))

    conn = _connection(request)
    cur = conn.cursor()

    if field == "last_movement_bucket":
        # One SELECT per bucket, UNIONed by Python. Simpler than a CASE+GROUP BY
        # for read-once aggregates and easier to read in profiling.
        results = []
        for label, low, high in _LAST_MOVEMENT_BUCKETS:
            bucket_sql, bucket_params = _bucket_clause(low, high)
            sql = (
                f"SELECT COUNT(*) FROM process_snapshot ps "
                f"WHERE ({where_sql}) AND ps.last_movement_iso IS NOT NULL "
                f"AND ({bucket_sql})"
            )
            try:
                cur.execute(sql, params + bucket_params)
            except sqlite3.OperationalError as e:
                raise HTTPException(status_code=400, detail=f"SQL error: {e}")
            results.append({"value": label, "count": int(cur.fetchone()[0])})
        return {"results": results}

    # Regular categorical group_by.
    sql = (
        f"SELECT ps.{field} AS value, COUNT(*) AS count "
        f"FROM process_snapshot ps WHERE {where_sql} "
        f"GROUP BY ps.{field} ORDER BY count DESC"
    )
    try:
        cur.execute(sql, params)
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=400, detail=f"SQL error: {e}")
    return {"results": [{"value": r["value"], "count": int(r["count"])} for r in cur.fetchall()]}


def _bucket_clause(low_days: int, high_days: Optional[int]) -> Tuple[str, List[Any]]:
    """SQL fragment for `last_movement_iso` falling within [low_days, high_days)
    days before today. high_days=None means open-ended."""
    if high_days is None:
        return (
            "julianday('now') - julianday(ps.last_movement_iso) >= ?",
            [low_days],
        )
    return (
        "julianday('now') - julianday(ps.last_movement_iso) >= ? "
        "AND julianday('now') - julianday(ps.last_movement_iso) < ?",
        [low_days, high_days],
    )


# ──────────────────────────────────────────────────────────────────────
# histogram
# ──────────────────────────────────────────────────────────────────────

_DEFAULT_HISTOGRAM_BUCKETS = [0, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000]


@router.post("/histogram")
def histogram(
    body: Dict[str, Any],
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")
    field = body.get("field", "value")
    if field not in STAT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"histogram field must be one of {list(STAT_FIELDS)}",
        )
    buckets = body.get("buckets") or _DEFAULT_HISTOGRAM_BUCKETS
    if not isinstance(buckets, list) or not buckets or not all(
        isinstance(b, (int, float)) for b in buckets
    ):
        raise HTTPException(
            status_code=400, detail="buckets must be a non-empty list of numbers"
        )
    edges = sorted(set(buckets))
    try:
        where_sql, params = _scoped_where(body)
    except QueryError as e:
        raise HTTPException(status_code=400, detail=str(e))

    conn = _connection(request)
    cur = conn.cursor()

    results: List[Dict[str, Any]] = []
    for i, lo in enumerate(edges):
        hi: Optional[float] = edges[i + 1] if i + 1 < len(edges) else None
        # Operate on value_centavos (cents); buckets are BRL units.
        lo_cents = int(lo * 100)
        if hi is None:
            sql = (
                f"SELECT COUNT(*) FROM process_snapshot ps WHERE ({where_sql}) "
                f"AND ps.value_centavos IS NOT NULL AND ps.value_centavos >= ?"
            )
            bucket_params = params + [lo_cents]
        else:
            hi_cents = int(hi * 100)
            sql = (
                f"SELECT COUNT(*) FROM process_snapshot ps WHERE ({where_sql}) "
                f"AND ps.value_centavos IS NOT NULL "
                f"AND ps.value_centavos >= ? AND ps.value_centavos < ?"
            )
            bucket_params = params + [lo_cents, hi_cents]
        try:
            cur.execute(sql, bucket_params)
        except sqlite3.OperationalError as e:
            raise HTTPException(status_code=400, detail=f"SQL error: {e}")
        results.append({
            "range_low": lo,
            "range_high": hi,
            "count": int(cur.fetchone()[0]),
        })
    return {"results": results}


# ──────────────────────────────────────────────────────────────────────
# stats
# ──────────────────────────────────────────────────────────────────────


@router.post("/stats")
def stats(
    body: Dict[str, Any],
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")
    field = body.get("field", "value")
    if field not in STAT_FIELDS:
        raise HTTPException(
            status_code=400, detail=f"stats field must be one of {list(STAT_FIELDS)}"
        )
    try:
        where_sql, params = _scoped_where(body)
    except QueryError as e:
        raise HTTPException(status_code=400, detail=str(e))

    conn = _connection(request)
    cur = conn.cursor()
    sql = (
        f"SELECT COUNT(ps.value_centavos) AS count, "
        f"COALESCE(SUM(ps.value_centavos), 0) AS sum, "
        f"AVG(ps.value_centavos) AS mean, "
        f"MIN(ps.value_centavos) AS min, "
        f"MAX(ps.value_centavos) AS max "
        f"FROM process_snapshot ps WHERE ({where_sql}) "
        f"AND ps.value_centavos IS NOT NULL"
    )
    try:
        cur.execute(sql, params)
    except sqlite3.OperationalError as e:
        raise HTTPException(status_code=400, detail=f"SQL error: {e}")
    row = cur.fetchone()
    count = int(row["count"] or 0)
    if count == 0:
        return {
            "count": 0, "sum": 0.0, "mean": None, "median": None,
            "min": None, "max": None,
        }

    # Median: SQLite has no PERCENTILE_DISC, so do the OFFSET trick.
    median_offset = count // 2
    median_sql = (
        f"SELECT ps.value_centavos FROM process_snapshot ps WHERE ({where_sql}) "
        f"AND ps.value_centavos IS NOT NULL "
        f"ORDER BY ps.value_centavos LIMIT 1 OFFSET ?"
    )
    cur.execute(median_sql, params + [median_offset])
    median_row = cur.fetchone()
    median_cents = median_row[0] if median_row else None

    def _to_brl(cents: Optional[float]) -> Optional[float]:
        return None if cents is None else cents / 100.0

    return {
        "count": count,
        "sum": _to_brl(row["sum"]),
        "mean": _to_brl(row["mean"]),
        "median": _to_brl(median_cents),
        "min": _to_brl(row["min"]),
        "max": _to_brl(row["max"]),
    }
