"""'Por que zero?' — clause-by-clause decomposition of a zero-result query.

Lawyer flow: a query returns 0 results, and they want to know which clause
killed everything. Algorithm: re-run each top-level AND child in isolation
(plus one level into *_any sub-clauses, since FTS5 misses inside movimento_any
are the most common zero-result cause); report each clause's standalone count.

Out of scope for v1:
  - Top-level OR/NOT: the card returns a "consulta complexa" message.
  - More than one level of *_any recursion (and-inside-and-inside-*_any).
"""
from __future__ import annotations

import copy
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request

from poursuite.api.auth import require_api_key
from poursuite.db.esaj_query import QueryError

router = APIRouter(prefix="/api/query", tags=["query"])

_ANY_KEYS = ("movimento_any", "linked_any", "peticao_any")


def _make_subbody(body: Dict[str, Any], where: Dict[str, Any]) -> Dict[str, Any]:
    """Clone body, replacing where; preserve snapshot + flag filters."""
    sub = {"where": where, "count_only": True, "limit": 0}
    for k in ("snapshot", "flagged_only", "unflagged_only"):
        if k in body:
            sub[k] = body[k]
    return sub


def _decompose(body: Dict[str, Any]) -> List[Tuple[Dict[str, Any], str]]:
    """Return (sub_body, decomposition_path) pairs.

    Empty list means the query shape isn't decomposable (top-level isn't AND,
    or is a bare leaf, etc.) and the caller should return the "complex" msg.
    """
    where = body.get("where")
    if not isinstance(where, dict) or "and" not in where:
        return []
    children = where["and"]
    if not isinstance(children, list) or not children:
        return []

    out: List[Tuple[Dict[str, Any], str]] = []
    for i, child in enumerate(children):
        path = f"and[{i}]"
        # Recurse one level into *_any if it wraps an inner AND.
        any_key = next((k for k in _ANY_KEYS if k in child), None)
        if any_key is not None:
            inner = child[any_key]
            if isinstance(inner, dict) and isinstance(inner.get("and"), list):
                for j, grand in enumerate(inner["and"]):
                    out.append((
                        _make_subbody(body, {any_key: grand}),
                        f"{path}.{any_key}.and[{j}]",
                    ))
                continue
        out.append((_make_subbody(body, copy.deepcopy(child)), path))
    return out


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


@router.post("/explain_zero")
def explain_zero(
    body: Dict[str, Any],
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    store = _store(request)
    parts = _decompose(body)
    if not parts:
        return {
            "clauses": [],
            "message": "consulta complexa — explicação automática indisponível, tente simplificar.",
        }
    results: List[Dict[str, Any]] = []
    for sub_body, path in parts:
        try:
            r = store.query(sub_body)
        except QueryError as e:
            raise HTTPException(status_code=400, detail=str(e))
        # `where` was set above and is the only meaningful field for display.
        results.append({
            "clause": sub_body["where"],
            "count_alone": int(r.get("total", 0)),
            "decomposition_path": path,
        })
    return {"clauses": results}
