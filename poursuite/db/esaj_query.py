"""eSAJ snapshot-store query builder.

Translates the documented JSON body shape (see POST /api/query) into
parameterized SQL against `esaj_snapshots.db`. The builder enforces:

  - Whitelisted field names (no arbitrary SQL identifiers)
  - Whitelisted operators
  - FTS5 MATCH only on movimento.nome / complementos_text
  - All values bound as parameters (no string interpolation)

The output is a tuple of (sql, params, count_sql, count_params) the caller
runs against an open SQLite connection. `count_sql` runs the same WHERE
without SELECT/ORDER/LIMIT, returning the total match count.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


# ──────────────────────────────────────────────────────────────────────
# Whitelists
# ──────────────────────────────────────────────────────────────────────

HEADER_FIELDS: Tuple[str, ...] = (
    "process_number", "snapshot_ts",
    "initial_date", "class_type", "subject", "value",
    "last_movement", "status", "plaintiff", "defendant", "other_processes",
    "foro", "vara", "juiz", "controle", "outros_assuntos", "outros_numeros",
    "local_fisico", "area", "foro_code", "tribunal_code", "distribution_year",
    "scrape_outcome", "scrape_error",
    # UI Phase 2.5: normalized / promoted derived columns (schema v4).
    "foro_name", "last_movement_iso", "value_centavos",
)

MOVIMENTO_FIELDS: Tuple[str, ...] = (
    "ordem", "data_hora", "codigo", "nome", "complementos_text", "cd_documento",
)
MOVIMENTO_FTS_FIELDS: Tuple[str, ...] = ("nome", "complementos_text")

LINKED_FIELDS: Tuple[str, ...] = ("linked_number", "relationship_type")
PETICAO_FIELDS: Tuple[str, ...] = ("ordem", "data", "tipo", "cd_documento")

# DataJud enrichment (Layer 3-lite). Filtered via EXISTS against the process's
# CURRENT enrichment row — see _synth_enrichment / _synth_complemento_any.
# v1 filterable enrichment field is `grau` only; assuntos / codigo_municipio_ibge
# / dataHoraUltimaAtualizacao are display-only (rendered in Detalhe, not filtered).
ENRICHMENT_FIELDS: Tuple[str, ...] = ("grau",)
COMPLEMENTO_FIELDS: Tuple[str, ...] = (
    "complemento_codigo", "complemento_valor",
    "complemento_nome", "complemento_descricao",
    "movimento_codigo", "movimento_nome",
)

SCALAR_OPS: Tuple[str, ...] = ("=", "!=", "<", "<=", ">", ">=")
LIST_OPS: Tuple[str, ...] = ("in", "not_in")
NULL_OPS: Tuple[str, ...] = ("is_null", "is_not_null")
FTS_OPS: Tuple[str, ...] = ("match",)
COUNT_OPS: Tuple[str, ...] = SCALAR_OPS  # count comparisons reuse scalar ops

ALL_OPS: Tuple[str, ...] = SCALAR_OPS + LIST_OPS + NULL_OPS + FTS_OPS


class QueryError(ValueError):
    """Raised on malformed or disallowed query bodies. Caller maps to HTTP 400."""


# ──────────────────────────────────────────────────────────────────────
# Synthesis context (mutable state passed through recursive walks)
# ──────────────────────────────────────────────────────────────────────

@dataclass
class _Ctx:
    """Mutable parameter-binding accumulator for one query synthesis pass."""
    params: List[Any]

    def bind(self, v: Any) -> str:
        self.params.append(v)
        return "?"


# ──────────────────────────────────────────────────────────────────────
# Leaf-clause synthesis
# ──────────────────────────────────────────────────────────────────────

def _validate_leaf(clause: Dict[str, Any], allowed_fields: Sequence[str]) -> None:
    if "field" not in clause or "op" not in clause:
        raise QueryError(f"leaf clause missing 'field' or 'op': {clause!r}")
    field = clause["field"]
    op = clause["op"]
    if field not in allowed_fields:
        raise QueryError(f"unknown field {field!r} in this scope (allowed: {list(allowed_fields)})")
    if op not in ALL_OPS:
        raise QueryError(f"unknown op {op!r} (allowed: {list(ALL_OPS)})")
    if op in NULL_OPS:
        if "value" in clause and clause["value"] is not None:
            raise QueryError(f"{op} takes no value")
    elif op in LIST_OPS:
        if not isinstance(clause.get("value"), (list, tuple)) or len(clause["value"]) == 0:
            raise QueryError(f"{op} requires a non-empty list value")
    else:
        if "value" not in clause:
            raise QueryError(f"{op} requires a value")


def _synth_header_leaf(clause: Dict[str, Any], ctx: _Ctx, alias: str = "ps") -> str:
    """SQL for a leaf clause targeting process_snapshot columns (header)."""
    _validate_leaf(clause, HEADER_FIELDS)
    field = clause["field"]
    op = clause["op"]
    col = f"{alias}.{field}"
    if op in NULL_OPS:
        return f"{col} IS NULL" if op == "is_null" else f"{col} IS NOT NULL"
    if op in LIST_OPS:
        placeholders = ",".join(ctx.bind(v) for v in clause["value"])
        return f"{col} {'NOT IN' if op == 'not_in' else 'IN'} ({placeholders})"
    if op == "match":
        raise QueryError("`match` is only valid on movimento.nome / complementos_text")
    # scalar comparisons
    return f"{col} {op} {ctx.bind(clause['value'])}"


def _synth_child_leaf(
    clause: Dict[str, Any],
    allowed: Sequence[str],
    alias: str,
    ctx: _Ctx,
    fts_table: Optional[str] = None,
    fts_fields: Sequence[str] = (),
) -> str:
    """SQL for a leaf clause inside a *_any sub-clause.

    `fts_table` enables FTS5 MATCH against the named FTS5 virtual table. The
    canonical SQLite pattern is `{alias}.rowid IN (SELECT rowid FROM {fts_table}
    WHERE {fts_table} MATCH ?)` — MATCH must reference the FTS5 table name
    itself, not an aliased column. Column-specific match is expressed by the
    caller via FTS5 column-prefix syntax in the value (e.g., `"nome:penhora"`).
    """
    _validate_leaf(clause, allowed)
    field = clause["field"]
    op = clause["op"]
    if op == "match":
        if field not in fts_fields:
            raise QueryError(
                f"`match` not supported on {field!r}; FTS-eligible: {list(fts_fields)}"
            )
        if fts_table is None:
            raise QueryError("FTS not available in this scope")
        # If the user provides a bare term and meant a column-specific search,
        # they can pass "nome:penhora" or "complementos_text:bloqueio". We
        # don't auto-prefix; treat the value as a full FTS5 expression.
        return (
            f"{alias}.rowid IN ("
            f"SELECT rowid FROM {fts_table} WHERE {fts_table} MATCH {ctx.bind(clause['value'])}"
            f")"
        )
    col = f"{alias}.{field}"
    if op in NULL_OPS:
        return f"{col} IS NULL" if op == "is_null" else f"{col} IS NOT NULL"
    if op in LIST_OPS:
        placeholders = ",".join(ctx.bind(v) for v in clause["value"])
        return f"{col} {'NOT IN' if op == 'not_in' else 'IN'} ({placeholders})"
    return f"{col} {op} {ctx.bind(clause['value'])}"


# ──────────────────────────────────────────────────────────────────────
# Clause-tree walker
# ──────────────────────────────────────────────────────────────────────

def _synth_clause(clause: Dict[str, Any], ctx: _Ctx) -> str:
    """Synthesize SQL for a clause in the top-level (header) scope."""
    if not isinstance(clause, dict) or not clause:
        raise QueryError(f"clause must be a non-empty dict, got {type(clause).__name__}")

    # Logical operators
    if "and" in clause:
        inner = clause["and"]
        if not isinstance(inner, list) or not inner:
            raise QueryError("`and` requires a non-empty list")
        parts = [_synth_clause(c, ctx) for c in inner]
        return "(" + " AND ".join(parts) + ")"
    if "or" in clause:
        inner = clause["or"]
        if not isinstance(inner, list) or not inner:
            raise QueryError("`or` requires a non-empty list")
        parts = [_synth_clause(c, ctx) for c in inner]
        return "(" + " OR ".join(parts) + ")"
    if "not" in clause:
        return "(NOT " + _synth_clause(clause["not"], ctx) + ")"

    # Child-table existential sub-clauses
    if "movimento_any" in clause:
        return _synth_movimento_any(clause["movimento_any"], ctx)
    if "linked_any" in clause:
        return _synth_linked_any(clause["linked_any"], ctx)
    if "peticao_any" in clause:
        return _synth_peticao_any(clause["peticao_any"], ctx)

    # Count sub-clauses
    if "movimento_count" in clause:
        return _synth_child_count("movimento", clause["movimento_count"], ctx)
    if "linked_count" in clause:
        return _synth_child_count("linked_process", clause["linked_count"], ctx)
    if "peticao_count" in clause:
        return _synth_child_count("peticao", clause["peticao_count"], ctx)

    # DataJud enrichment sub-clauses (scoped to the current enrichment row)
    if "enrichment" in clause:
        return _synth_enrichment(clause["enrichment"], ctx)
    if "complemento_any" in clause:
        return _synth_complemento_any(clause["complemento_any"], ctx)
    if "complemento_count" in clause:
        return _synth_complemento_count(clause["complemento_count"], ctx)

    # Leaf — header field condition
    if "field" in clause:
        return _synth_header_leaf(clause, ctx)

    raise QueryError(f"unrecognized clause shape: keys={list(clause.keys())}")


def _synth_child_clause(
    clause: Dict[str, Any],
    allowed: Sequence[str],
    alias: str,
    ctx: _Ctx,
    fts_table: Optional[str] = None,
    fts_fields: Sequence[str] = (),
) -> str:
    """Synthesize SQL for a clause inside a *_any sub-clause.

    Supports the same and/or/not composition as top-level, but every leaf
    must reference the child table's columns.
    """
    if not isinstance(clause, dict) or not clause:
        raise QueryError(f"child clause must be a non-empty dict, got {type(clause).__name__}")
    if "and" in clause:
        parts = [_synth_child_clause(c, allowed, alias, ctx, fts_table, fts_fields)
                 for c in clause["and"]]
        return "(" + " AND ".join(parts) + ")"
    if "or" in clause:
        parts = [_synth_child_clause(c, allowed, alias, ctx, fts_table, fts_fields)
                 for c in clause["or"]]
        return "(" + " OR ".join(parts) + ")"
    if "not" in clause:
        return "(NOT " + _synth_child_clause(clause["not"], allowed, alias, ctx,
                                              fts_table, fts_fields) + ")"
    if "field" in clause:
        return _synth_child_leaf(clause, allowed, alias, ctx, fts_table, fts_fields)
    raise QueryError(f"child clause unrecognized: keys={list(clause.keys())}")


def _synth_movimento_any(inner: Dict[str, Any], ctx: _Ctx) -> str:
    """EXISTS subquery on movimento, scoped to one row.

    FTS5 MATCH is expressed as `m.rowid IN (SELECT rowid FROM movimento_fts
    WHERE movimento_fts MATCH ?)` — see _synth_child_leaf for why.
    """
    inner_sql = _synth_child_clause(
        inner,
        allowed=MOVIMENTO_FIELDS,
        alias="m",
        ctx=ctx,
        fts_table="movimento_fts",
        fts_fields=MOVIMENTO_FTS_FIELDS,
    )
    return (
        "EXISTS (SELECT 1 FROM movimento m "
        "WHERE m.process_number = ps.process_number "
        "AND m.snapshot_ts = ps.snapshot_ts "
        f"AND {inner_sql})"
    )


def _synth_linked_any(inner: Dict[str, Any], ctx: _Ctx) -> str:
    inner_sql = _synth_child_clause(
        inner, allowed=LINKED_FIELDS, alias="lp", ctx=ctx,
    )
    return (
        "EXISTS (SELECT 1 FROM linked_process lp "
        "WHERE lp.process_number = ps.process_number "
        "AND lp.snapshot_ts = ps.snapshot_ts "
        f"AND {inner_sql})"
    )


def _synth_peticao_any(inner: Dict[str, Any], ctx: _Ctx) -> str:
    inner_sql = _synth_child_clause(
        inner, allowed=PETICAO_FIELDS, alias="p", ctx=ctx,
    )
    return (
        "EXISTS (SELECT 1 FROM peticao p "
        "WHERE p.process_number = ps.process_number "
        "AND p.snapshot_ts = ps.snapshot_ts "
        f"AND {inner_sql})"
    )


def _synth_child_count(table: str, spec: Dict[str, Any], ctx: _Ctx) -> str:
    if "op" not in spec or "value" not in spec:
        raise QueryError(f"{table}_count needs `op` and `value`")
    op = spec["op"]
    if op not in COUNT_OPS:
        raise QueryError(f"{table}_count op must be one of {list(COUNT_OPS)}")
    val = spec["value"]
    if not isinstance(val, int):
        raise QueryError(f"{table}_count value must be an integer, got {type(val).__name__}")
    return (
        f"((SELECT COUNT(*) FROM {table} t "
        f"WHERE t.process_number = ps.process_number "
        f"AND t.snapshot_ts = ps.snapshot_ts) {op} {ctx.bind(val)})"
    )


# ──────────────────────────────────────────────────────────────────────
# DataJud enrichment sub-clauses (Layer 3-lite)
# ──────────────────────────────────────────────────────────────────────

# Correlated subquery selecting the process's CURRENT enrichment timestamp.
# Enrichment is append-on-change keyed by (process_number, fetched_at) on a
# cadence independent of snapshots, so "current" is the latest fetched_at — NOT
# ps.snapshot_ts. datajud_complemento rows carry their parent enrichment's
# fetched_at, so the same predicate scopes complementos to the current row.
_CURRENT_ENRICHMENT_FETCHED_AT = (
    "(SELECT MAX(de2.fetched_at) FROM datajud_enrichment de2 "
    "WHERE de2.process_number = ps.process_number)"
)


def _synth_enrichment(inner: Dict[str, Any], ctx: _Ctx) -> str:
    """EXISTS against the process's current datajud_enrichment row.

    A process with no enrichment fails EXISTS, so enrichment filters match only
    enriched processes (v1 semantic — `grau != "G1"` excludes un-enriched; see
    UI_DESIGN_NOTES.md).

    `inner` goes through `_synth_child_clause`, which already accepts BOTH a
    single `{field, op, value}` leaf AND a full `and`/`or`/`not` tree. In v1
    `grau` is the only filterable enrichment field, so callers (and the visual
    builder, EU-d) pass a single predicate — but that's a UI simplification, not
    a parser limit. If `codigo_municipio_ibge` / freshness graduate from
    display-only to filterable, add them to ENRICHMENT_FIELDS and let the builder
    emit a tree; nothing changes here. Don't bake single-predicate into the
    builder as if it were permanent."""
    inner_sql = _synth_child_clause(inner, allowed=ENRICHMENT_FIELDS, alias="de", ctx=ctx)
    return (
        "EXISTS (SELECT 1 FROM datajud_enrichment de "
        "WHERE de.process_number = ps.process_number "
        f"AND de.fetched_at = {_CURRENT_ENRICHMENT_FETCHED_AT} "
        f"AND {inner_sql})"
    )


def _synth_complemento_any(inner: Dict[str, Any], ctx: _Ctx) -> str:
    """EXISTS against the current enrichment's complementosTabelados — the
    success/failure-axis filter (e.g. complemento_codigo + complemento_valor)."""
    inner_sql = _synth_child_clause(inner, allowed=COMPLEMENTO_FIELDS, alias="dc", ctx=ctx)
    return (
        "EXISTS (SELECT 1 FROM datajud_complemento dc "
        "WHERE dc.process_number = ps.process_number "
        f"AND dc.fetched_at = {_CURRENT_ENRICHMENT_FETCHED_AT} "
        f"AND {inner_sql})"
    )


def _synth_complemento_count(spec: Dict[str, Any], ctx: _Ctx) -> str:
    if "op" not in spec or "value" not in spec:
        raise QueryError("complemento_count needs `op` and `value`")
    op = spec["op"]
    if op not in COUNT_OPS:
        raise QueryError(f"complemento_count op must be one of {list(COUNT_OPS)}")
    val = spec["value"]
    if not isinstance(val, int):
        raise QueryError(f"complemento_count value must be an integer, got {type(val).__name__}")
    return (
        "((SELECT COUNT(*) FROM datajud_complemento dc "
        "WHERE dc.process_number = ps.process_number "
        f"AND dc.fetched_at = {_CURRENT_ENRICHMENT_FETCHED_AT}) {op} {ctx.bind(val)})"
    )


# ──────────────────────────────────────────────────────────────────────
# Snapshot modifier
# ──────────────────────────────────────────────────────────────────────

def _snapshot_predicate(snapshot: Any, ctx: _Ctx) -> str:
    """Return the SQL fragment that scopes process_snapshot rows to one
    snapshot per process per the body's `snapshot` modifier.

    "latest" (default) → most-recent snapshot per process.
    "any"              → no filter; all snapshots match.
    {"at": TS}         → most-recent snapshot at or before TS.
    """
    if snapshot is None or snapshot == "latest":
        return (
            "ps.snapshot_ts = ("
            " SELECT MAX(s2.snapshot_ts) FROM process_snapshot s2 "
            " WHERE s2.process_number = ps.process_number"
            ")"
        )
    if snapshot == "any":
        return "1=1"
    if isinstance(snapshot, dict) and "at" in snapshot:
        ts = snapshot["at"]
        if not isinstance(ts, str):
            raise QueryError("snapshot.at must be an ISO 8601 string")
        return (
            "ps.snapshot_ts = ("
            " SELECT MAX(s2.snapshot_ts) FROM process_snapshot s2 "
            " WHERE s2.process_number = ps.process_number "
            f" AND s2.snapshot_ts <= {ctx.bind(ts)}"
            ")"
        )
    raise QueryError(f"unrecognized snapshot modifier: {snapshot!r}")


# ──────────────────────────────────────────────────────────────────────
# Select / order-by / pagination validation
# ──────────────────────────────────────────────────────────────────────

def _validate_select(select: Any) -> List[str]:
    if select is None:
        return ["process_number"]
    if not isinstance(select, list) or not select:
        raise QueryError("`select` must be a non-empty list of field names")
    for f in select:
        if f not in HEADER_FIELDS:
            raise QueryError(f"select field {f!r} not allowed (must be a process_snapshot column)")
    return list(select)


def _validate_order_by(order_by: Any) -> List[Tuple[str, str]]:
    if order_by is None:
        return [("snapshot_ts", "desc")]
    if not isinstance(order_by, list):
        raise QueryError("`order_by` must be a list")
    out: List[Tuple[str, str]] = []
    for item in order_by:
        if not isinstance(item, dict) or "field" not in item:
            raise QueryError(f"order_by item malformed: {item!r}")
        field = item["field"]
        if field not in HEADER_FIELDS:
            raise QueryError(f"order_by field {field!r} not allowed")
        direction = (item.get("dir") or "asc").lower()
        if direction not in ("asc", "desc"):
            raise QueryError(f"order_by dir must be asc/desc, got {direction!r}")
        out.append((field, direction))
    return out


# ──────────────────────────────────────────────────────────────────────
# Top-level entry point
# ──────────────────────────────────────────────────────────────────────

def _apply_flag_filter(
    where_sql: str, params: List[Any], body: Dict[str, Any]
) -> Tuple[str, List[Any]]:
    """Append `flagged_only` / `unflagged_only` top-level filters to where_sql.

    These are top-level on the request body (not inside `where`) so that the
    visual builder doesn't need to know about a virtual `flagged` field. The
    EXISTS/NOT EXISTS clause adds no bound parameters; params is returned
    unchanged but kept in the signature for caller symmetry.

    Limitation: cannot compose with OR/NOT inside `where` (the filter is an
    outer AND). Acceptable for v1; the proper LEFT JOIN refactor is the
    additive path if it becomes a real need.
    """
    flagged_only = bool(body.get("flagged_only"))
    unflagged_only = bool(body.get("unflagged_only"))
    if flagged_only and unflagged_only:
        raise QueryError("flagged_only and unflagged_only are mutually exclusive")
    if flagged_only:
        where_sql = (
            f"({where_sql}) AND EXISTS ("
            "SELECT 1 FROM process_flags pf "
            "WHERE pf.process_number = ps.process_number)"
        )
    elif unflagged_only:
        where_sql = (
            f"({where_sql}) AND NOT EXISTS ("
            "SELECT 1 FROM process_flags pf "
            "WHERE pf.process_number = ps.process_number)"
        )
    return where_sql, params


def synth_where_only(body: Dict[str, Any]) -> Tuple[str, List[Any], List[str]]:
    """Synthesize the WHERE fragment + params for a query body.

    Returns `(where_sql, params, joins)` where:
      - `where_sql` is the user's WHERE tree AND-merged with the snapshot
        predicate AND-merged with the optional flagged_only/unflagged_only
        filter. Callers don't need to know snapshot semantics or how flag
        filtering is wired.
      - `params` is the bound-parameter list in the order `where_sql` consumes
        them: WHERE-tree params first, then snapshot-predicate params.
      - `joins` is the slot for `LEFT JOIN ...` fragments. In v1 this is
        always an empty list — kept in the signature so a future refactor
        (e.g. promoting `flagged` to a first-class field) is additive.

    The function operates on `ps`-aliased `process_snapshot` rows. Callers
    are responsible for assembling the full SELECT/ORDER/LIMIT envelope.
    """
    if not isinstance(body, dict):
        raise QueryError(f"body must be a dict, got {type(body).__name__}")

    where = body.get("where")
    where_ctx = _Ctx(params=[])
    snapshot_ctx = _Ctx(params=[])

    snapshot_sql = _snapshot_predicate(body.get("snapshot"), snapshot_ctx)
    if where is None:
        where_sql = "1=1"
    else:
        where_sql = _synth_clause(where, where_ctx)

    combined_where = f"({where_sql}) AND ({snapshot_sql})"
    combined_params = where_ctx.params + snapshot_ctx.params

    combined_where, combined_params = _apply_flag_filter(
        combined_where, combined_params, body
    )
    return combined_where, combined_params, []


@dataclass
class BuiltQuery:
    select_sql: str
    select_params: List[Any]
    count_sql: str
    count_params: List[Any]
    fields: List[str]      # the fields the caller asked for in `select`
    limit: int
    offset: int
    count_only: bool


def build_query(body: Dict[str, Any]) -> BuiltQuery:
    """Validate the body and produce parameterized SQL for results + count.

    Raises QueryError on any malformed input. The caller maps QueryError to
    HTTP 400 with the message.
    """
    if not isinstance(body, dict):
        raise QueryError(f"body must be a dict, got {type(body).__name__}")

    fields = _validate_select(body.get("select"))
    order_by = _validate_order_by(body.get("order_by"))

    limit = body.get("limit", 100)
    if not isinstance(limit, int) or limit < 0 or limit > 10000:
        raise QueryError("limit must be an integer in [0, 10000]")
    offset = body.get("offset", 0)
    if not isinstance(offset, int) or offset < 0:
        raise QueryError("offset must be a non-negative integer")

    count_only = bool(body.get("count_only", False))

    combined_where, combined_params, _joins = synth_where_only(body)

    select_cols = ", ".join(f"ps.{f}" for f in fields)
    select_cols_with_ts = select_cols
    if "snapshot_ts" not in fields:
        select_cols_with_ts = "ps.snapshot_ts, " + select_cols
    order_clause = ", ".join(f"ps.{f} {d.upper()}" for f, d in order_by)

    select_sql = (
        f"SELECT {select_cols_with_ts} "
        f"FROM process_snapshot ps "
        f"WHERE {combined_where} "
        f"ORDER BY {order_clause} "
        f"LIMIT ? OFFSET ?"
    )
    select_params = combined_params + [limit, offset]

    count_sql = (
        f"SELECT COUNT(*) FROM process_snapshot ps "
        f"WHERE {combined_where}"
    )
    count_params = list(combined_params)

    return BuiltQuery(
        select_sql=select_sql,
        select_params=select_params,
        count_sql=count_sql,
        count_params=count_params,
        fields=fields,
        limit=limit,
        offset=offset,
        count_only=count_only,
    )
