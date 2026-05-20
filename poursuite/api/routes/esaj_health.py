"""eSAJ reachability probe (UI Phase 2.5).

Lightweight HEAD/GET against the eSAJ entry URL so the carteira-upload screen
can warn the lawyer before kicking off a scrape that's doomed by an upstream
503. Cheap to call (single HTTP request, 5s timeout); not cached server-side
since TJSP outages can be transient — fresh signal beats stale cache.
"""
from __future__ import annotations

import urllib.error
import urllib.request
from typing import Any, Dict

from fastapi import APIRouter, Depends

from poursuite.api.auth import require_api_key
from poursuite.config import ESAJ_URL

router = APIRouter(prefix="/api", tags=["esaj_health"])

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@router.get("/esaj_health")
def esaj_health(_key: str = Depends(require_api_key)) -> Dict[str, Any]:
    """Probe ESAJ_URL once; return up/degraded/down classification.

    - `up`         — HTTP 200, response looks like the expected consulta page.
    - `degraded`   — non-200 status but reachable (e.g. 3xx redirect, 4xx).
    - `down`       — 5xx, connection failure, or timeout.

    No caching — the cost of one HTTP HEAD is tiny vs. the cost of a doomed
    scrape, and TJSP outages can flip back to "up" in minutes.
    """
    req = urllib.request.Request(ESAJ_URL, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            code = resp.status
            if code == 200:
                # Trust 200. We previously peeked at a 2 KB body slice for a
                # form-field marker, but `numeroDigitoAnoUnificado` lives
                # deeper than 2 KB in the real page, producing false-positive
                # "degraded" verdicts on a perfectly healthy eSAJ. The 503
                # case is captured by the HTTPError branch below; non-503
                # weirdness is rare and the scraper will surface it with a
                # proper traceback.
                return {"status": "up", "code": 200}
            return {"status": "degraded", "code": code}
    except urllib.error.HTTPError as e:
        # TJSP serves a 503 with a body when the app server is down.
        kind = "down" if e.code >= 500 else "degraded"
        return {"status": kind, "code": e.code, "reason": str(e.reason)}
    except urllib.error.URLError as e:
        return {"status": "down", "code": None, "reason": str(e.reason)}
    except Exception as e:
        return {"status": "down", "code": None, "reason": f"{type(e).__name__}: {e}"}
