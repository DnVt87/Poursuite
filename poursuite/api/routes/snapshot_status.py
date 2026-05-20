"""Bulk snapshot-freshness lookup (UI Phase 2.5).

Used by Workflow 2 step 3 — the carteira upload screen needs to tell the
lawyer "X of N already have a recent snapshot; Y need fresh scraping" so
he doesn't waste eSAJ requests on data that's already current.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from poursuite.api.auth import require_api_key

router = APIRouter(prefix="/api", tags=["snapshot_status"])


class SnapshotStatusRequest(BaseModel):
    process_numbers: List[str] = Field(..., min_length=1)
    # max_age_days=None means "no cutoff — every existing snapshot is fresh,
    # missing ones still missing." Maps to the UI's "never" option.
    max_age_days: Optional[int] = 7


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


@router.post("/snapshot_status")
def snapshot_status(
    body: SnapshotStatusRequest,
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    results = _store(request).snapshot_status(
        body.process_numbers, max_age_days=body.max_age_days
    )
    return {"results": results}
