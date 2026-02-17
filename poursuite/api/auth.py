from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from poursuite.config import API_KEY

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(key: str = Security(_api_key_header)) -> str:
    """
    FastAPI dependency that validates the X-API-Key header.
    Raises 500 if the server has no API key configured (forces operator to set env var).
    Raises 403 if the key is missing or wrong.
    """
    if not API_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API key not configured on server. Set POURSUITE_API_KEY environment variable.",
        )
    if key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API key.",
        )
    return key
