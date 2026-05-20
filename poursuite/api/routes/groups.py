"""Process-group endpoints (UI Phase 2.5).

Carteiras are upload provenance: a named bag of process_numbers the lawyer
uploaded together and may want to reference later. The query API stays
unaware of groups — scoping a query to a group is a UI-side operation that
fetches the group's process_numbers and adds them as an `in` clause.
"""
from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from poursuite.api.auth import require_api_key

router = APIRouter(prefix="/api/groups", tags=["groups"])


class CreateGroupRequest(BaseModel):
    name: str = Field(..., min_length=1)
    process_numbers: List[str] = Field(..., min_length=1)


def _store(request: Request):
    store = getattr(request.app.state, "process_groups", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Process-group store not available.")
    return store


@router.get("")
def list_groups(request: Request, _key: str = Depends(require_api_key)) -> Dict[str, Any]:
    return {"results": _store(request).list_groups()}


@router.get("/{group_id}")
def get_group(
    group_id: str, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    g = _store(request).get_group(group_id)
    if g is None:
        raise HTTPException(status_code=404, detail="Group not found.")
    return g


@router.post("")
def create_group(
    body: CreateGroupRequest, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    try:
        return _store(request).create_group(body.name, body.process_numbers)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{group_id}")
def delete_group(
    group_id: str, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    if not _store(request).delete_group(group_id):
        raise HTTPException(status_code=404, detail="Group not found.")
    return {"deleted": True, "group_id": group_id}
