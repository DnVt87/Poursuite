"""Low-level DataJud public-API client.

Batched `_msearch` over `api-publica.datajud.cnj.jus.br`. Lifted from
`poursuite.probes.datajud` (the investigation code) so production paths have a
package-level dependency, not a `probes/` import. Defensive: HTTP wrappers
return a result dict, never raise.

Constraints baked in from the DataJud re-inventory (docs/DATAJUD_CAPABILITY_INVENTORY.md):
  - Single-threaded throughput ≈ 0.25 req/sec; latency dominates. We batch via
    `_msearch` so N process lookups cost one round-trip, and run batches
    SEQUENTIALLY — no concurrent connections in v1 (parallel behavior untested).
  - `_source` includes cut payload ~90%; we project only the target fields.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

API_BASE = "https://api-publica.datajud.cnj.jus.br"
MSEARCH_URL = f"{API_BASE}/_msearch"
DEFAULT_INDEX = "api_publica_tjsp"

# Wiki-published public key (PROBE_FINDINGS §1.2). Override via env var. This is
# the read-only public-tier key (`dpj_api_publica`); not a secret.
DEFAULT_API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="


def resolve_api_key(logger: Optional[logging.Logger] = None) -> str:
    """API key from POURSUITE_DATAJUD_API_KEY, else the public wiki key."""
    env_key = os.environ.get("POURSUITE_DATAJUD_API_KEY", "").strip()
    if env_key:
        if logger:
            logger.info("DataJud API key: from POURSUITE_DATAJUD_API_KEY env var.")
        return env_key
    if logger:
        logger.info("DataJud API key: public wiki value (override via "
                    "POURSUITE_DATAJUD_API_KEY).")
    return DEFAULT_API_KEY


def digits_only(process: str) -> str:
    """CNJ process number → 20 bare digits (DataJud's numeroProcesso match shape)."""
    return re.sub(r"\D", "", process)


def format_cnj(process_digits: str) -> str:
    """20 bare digits → canonical NNNNNNN-DD.AAAA.J.TR.OOOO. Pass-through if not 20 digits."""
    d = process_digits
    if len(d) != 20 or not d.isdigit():
        return process_digits
    return f"{d[0:7]}-{d[7:9]}.{d[9:13]}.{d[13:14]}.{d[14:16]}.{d[16:20]}"


def msearch_by_process(
    process_numbers: Sequence[str],
    *,
    index: str = DEFAULT_INDEX,
    source_includes: Optional[Sequence[str]] = None,
    api_key: Optional[str] = None,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """One `_msearch` round-trip: a numeroProcesso subquery per process,
    `_source`-projected to `source_includes`.

    Returns a result dict:
      {"ok": bool, "status": int|None, "latency_ms": int,
       "responses": [subresponse, ...]  # aligned to process_numbers order,
       "error": str|None}

    The `responses` list is empty on a batch-level failure (HTTP/transport
    error) — callers map that to per-process errors. Each subresponse is a
    normal ES search response; 0 hits means the process isn't in the index.
    """
    api_key = api_key or resolve_api_key(logger)
    lines: List[str] = []
    for pn in process_numbers:
        body: Dict[str, Any] = {
            "query": {"match": {"numeroProcesso": digits_only(pn)}},
            "size": 1,
        }
        if source_includes:
            body["_source"] = {"includes": list(source_includes)}
        lines.append(json.dumps({"index": index}))
        lines.append(json.dumps(body))
    raw_body = ("\n".join(lines) + "\n").encode("utf-8")

    headers = {
        "Authorization": f"APIKey {api_key}",
        "User-Agent": "poursuite-datajud/1.0",
        "Content-Type": "application/x-ndjson",
    }
    req = urllib.request.Request(MSEARCH_URL, data=raw_body, headers=headers, method="POST")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            latency = int((time.perf_counter() - t0) * 1000)
            body = json.loads(raw)
            return {
                "ok": True,
                "status": resp.status,
                "latency_ms": latency,
                "responses": body.get("responses") or [],
                "error": None,
            }
    except urllib.error.HTTPError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            pass
        if logger:
            logger.warning("DataJud _msearch HTTP %s: %s", e.code, raw[:200])
        return {
            "ok": False,
            "status": e.code,
            "latency_ms": latency,
            "responses": [],
            "error": f"HTTPError {e.code} {e.reason}: {raw[:200]}",
        }
    except Exception as e:  # noqa: BLE001
        latency = int((time.perf_counter() - t0) * 1000)
        if logger:
            logger.warning("DataJud _msearch failed: %s", e)
        return {
            "ok": False,
            "status": None,
            "latency_ms": latency,
            "responses": [],
            "error": f"{type(e).__name__}: {e}",
        }
