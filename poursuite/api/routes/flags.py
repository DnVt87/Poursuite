"""Single-state ★ flag endpoints (UI Phase 2.5, Patch 4).

Global namespace: anyone with the API key flags, everyone sees. No author.
Multi-state is a future additive change — rename to process_labels, add a
label column, default to 'starred'.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request

from poursuite.api.auth import require_api_key

router = APIRouter(prefix="/api/flags", tags=["flags"])


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


@router.get("")
def list_flags(request: Request, _key: str = Depends(require_api_key)) -> Dict[str, Any]:
    return {"flagged": _store(request).list_flagged()}


@router.post("/{process_number}")
def flag_process(
    process_number: str, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    return _store(request).flag(process_number)


@router.delete("/{process_number}")
def unflag_process(
    process_number: str, request: Request, _key: str = Depends(require_api_key)
) -> Dict[str, Any]:
    return _store(request).unflag(process_number)
