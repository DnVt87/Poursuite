"""Saved-query library — six endpoints backing Workflow 5 (UI Phase 2.5).

Shared library: global namespace, anyone with the API key can create, edit,
delete, or re-run anyone's query. query_body is the JSON body POST /api/query
accepts, stored verbatim so re-running is just sending it back.

Endpoints:
  GET    /api/saved_queries              — list all (no body)
  GET    /api/saved_queries/{id}         — full record including query_body
  POST   /api/saved_queries              — create
  PUT    /api/saved_queries/{id}         — partial update
  DELETE /api/saved_queries/{id}         — delete with soft confirm in UI
  POST   /api/saved_queries/{id}/touch   — record a re-run (UI calls after /api/query)
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from poursuite.api.auth import require_api_key

router = APIRouter(prefix="/api/saved_queries", tags=["saved_queries"])


class CreateSavedQueryRequest(BaseModel):
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    query_body: Dict[str, Any]


class UpdateSavedQueryRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    query_body: Optional[Dict[str, Any]] = None


class TouchRequest(BaseModel):
    result_count: int = Field(..., ge=0)


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


def _row_for_wire(row: Dict[str, Any]) -> Dict[str, Any]:
    """Decode query_body JSON for clients."""
    out = dict(row)
    if "query_body" in out and isinstance(out["query_body"], str):
        try:
            out["query_body"] = json.loads(out["query_body"])
        except json.JSONDecodeError:
            pass
    return out


@router.get("")
def list_saved(request: Request, _key: str = Depends(require_api_key)) -> Dict[str, Any]:
    return {"results": _store(request).list_saved_queries()}


@router.get("/{qid}")
def get_saved(
    qid: int, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    row = _store(request).get_saved_query(qid)
    if row is None:
        raise HTTPException(status_code=404, detail="Saved query not found.")
    return _row_for_wire(row)


@router.post("")
def create_saved(
    body: CreateSavedQueryRequest,
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    return _store(request).create_saved_query(
        name=body.name,
        description=body.description,
        query_body=json.dumps(body.query_body, ensure_ascii=False),
    )


@router.put("/{qid}")
def update_saved(
    qid: int,
    body: UpdateSavedQueryRequest,
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    qb = json.dumps(body.query_body, ensure_ascii=False) if body.query_body is not None else None
    updated = _store(request).update_saved_query(
        qid, name=body.name, description=body.description, query_body=qb
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Saved query not found.")
    return {"updated": True, "id": qid}


@router.delete("/{qid}")
def delete_saved(
    qid: int, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    if not _store(request).delete_saved_query(qid):
        raise HTTPException(status_code=404, detail="Saved query not found.")
    return {"deleted": True, "id": qid}


@router.post("/{qid}/touch")
def touch_saved(
    qid: int,
    body: TouchRequest,
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    if not _store(request).touch_saved_query(qid, body.result_count):
        raise HTTPException(status_code=404, detail="Saved query not found.")
    return {"touched": True, "id": qid}
