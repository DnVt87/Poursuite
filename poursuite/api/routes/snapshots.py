"""eSAJ snapshot-store query endpoints (Phase 2e).

Five endpoints expose the data extracted in 2c/2d:

  GET  /api/process/{number}/snapshots
       List all stored snapshots for a process, newest first.

  GET  /api/process/{number}/movimentos[?since=YYYY-MM-DD][&snapshot_ts=...]
       Movimentos for the latest snapshot (or a specific one) optionally
       filtered by data_hora >= since.

  GET  /api/process/{number}/links[?snapshot_ts=...]
       Linked processes (apensos + incidentes) for the snapshot.

  GET  /api/process/{number}/peticoes[?snapshot_ts=...]
       Petições for the snapshot.

  POST /api/query
       Structured search over the snapshot store. Body shape documented
       in poursuite/db/esaj_query.py (see ARCHITECTURE.md §13).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from poursuite.api.auth import require_api_key
from poursuite.db.esaj_query import QueryError

router = APIRouter(prefix="/api", tags=["snapshots"])


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


@router.get("/process/{process_number}/snapshots")
def list_snapshots(
    process_number: str,
    request: Request,
    _key: str = Depends(require_api_key),
):
    return {"results": _store(request).list_snapshots(process_number)}


@router.get("/process/{process_number}/movimentos")
def list_movimentos(
    process_number: str,
    request: Request,
    snapshot_ts: Optional[str] = None,
    since: Optional[str] = None,
    _key: str = Depends(require_api_key),
):
    movs = _store(request).get_movimentos(
        process_number, snapshot_ts=snapshot_ts, since=since,
    )
    return {"process_number": process_number, "count": len(movs), "results": movs}


@router.get("/process/{process_number}/links")
def list_links(
    process_number: str,
    request: Request,
    snapshot_ts: Optional[str] = None,
    _key: str = Depends(require_api_key),
):
    links = _store(request).get_linked(process_number, snapshot_ts=snapshot_ts)
    return {"process_number": process_number, "count": len(links), "results": links}


@router.get("/process/{process_number}/peticoes")
def list_peticoes(
    process_number: str,
    request: Request,
    snapshot_ts: Optional[str] = None,
    _key: str = Depends(require_api_key),
):
    peti = _store(request).get_peticoes(process_number, snapshot_ts=snapshot_ts)
    return {"process_number": process_number, "count": len(peti), "results": peti}


@router.post("/query")
def query_snapshots(
    body: Dict[str, Any],
    request: Request,
    _key: str = Depends(require_api_key),
):
    """Structured query over the snapshot store.

    Body shape: see poursuite/db/esaj_query.py. Malformed bodies return
    HTTP 400 with the validation message; FTS5 syntax errors in `match`
    values likewise surface as 400.
    """
    try:
        return _store(request).query(body)
    except QueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
