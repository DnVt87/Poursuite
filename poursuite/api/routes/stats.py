from fastapi import APIRouter, Depends, Request

from poursuite.api.auth import require_api_key
from poursuite.api.schemas import StatsResponse

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("", response_model=StatsResponse)
def get_stats(
    request: Request,
    _key: str = Depends(require_api_key),
):
    """Return metadata about all available databases. No COUNT(*) queries — fast."""
    db_manager = request.app.state.db_manager
    stats = db_manager.get_database_stats()
    return StatsResponse(**stats)
