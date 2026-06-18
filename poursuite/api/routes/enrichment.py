"""DataJud enrichment read endpoints (Layer 3-lite UI — EU-b/EU-c).

  GET  /api/process/{number}/enrichment
       The process's current enrichment row + its complementos (for Detalhe).

  POST /api/enrichment_status
       Bulk "which of these process_numbers have a current enrichment" — the
       Resultados "enriquecido" indicator. Body: {"process_numbers": [...]}.

  GET  /api/datajud/complemento_catalog
       Distinct complemento tuples across current enrichments, grouped by
       movement (the Esquema catalog for harvesting outcome-tuple semantics).

All read-only, behind the existing X-API-Key. Filtering/aggregation of these
fields lives in /api/query (esaj_query.py); these just surface the data.
"""
from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request

from poursuite.api.auth import require_api_key

router = APIRouter(prefix="/api", tags=["enrichment"])


def _store(request: Request):
    store = getattr(request.app.state, "snapshot_store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="Snapshot store not available.")
    return store


@router.get("/process/{process_number}/enrichment")
def get_enrichment(
    process_number: str,
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    """Current enrichment + complementos for one process. `enrichment` is None
    if the process has never been enriched."""
    store = _store(request)
    enrichment = store.get_latest_enrichment(process_number)
    complementos = store.get_complementos(process_number) if enrichment else []
    return {
        "process_number": process_number,
        "enrichment": enrichment,
        "complementos": complementos,
    }


@router.post("/enrichment_status")
def enrichment_status(
    body: Dict[str, Any],
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    process_numbers = body.get("process_numbers")
    if not isinstance(process_numbers, list):
        raise HTTPException(status_code=400, detail="process_numbers must be a list")
    results = _store(request).enrichment_status([str(p) for p in process_numbers])
    return {"results": results}


@router.get("/datajud/complemento_catalog")
def complemento_catalog(
    request: Request,
    _key: str = Depends(require_api_key),
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = _store(request).complemento_catalog()
    return {"count": len(results), "results": results}
