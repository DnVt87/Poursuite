"""
eSAJ extraction API.

Jobs run in background daemon threads. Results accumulate in an in-memory
store polled by the frontend every 2 seconds. The store is never persisted —
a server restart clears all jobs (acceptable for a local single-user setup).
"""

import csv
import io
import threading
import uuid
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from poursuite.api.auth import require_api_key
from poursuite.config import DEFAULT_MAX_BROWSERS
from poursuite.models import ProcessData
from poursuite.scraper.esaj import ProcessValueScraper

router = APIRouter(prefix="/extract", tags=["extract"])

# ── In-memory job store ───────────────────────────────────────────────

_jobs: Dict[str, dict] = {}
_jobs_lock = threading.Lock()


# ── Request schema ────────────────────────────────────────────────────

class ExtractStartRequest(BaseModel):
    process_numbers: List[str] = Field(..., min_length=1)
    concurrent: int = Field(default=DEFAULT_MAX_BROWSERS, ge=1, le=8)
    include_other_processes: bool = False


# ── Background job runner ─────────────────────────────────────────────

def _run_extraction(
    job_id: str,
    process_numbers: List[str],
    concurrent: int,
    include_other_processes: bool,
) -> None:
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"

    def on_result(result: ProcessData) -> None:
        with _jobs_lock:
            _jobs[job_id]["results"].append(result.to_dict())
            _jobs[job_id]["done"] += 1

    scraper = ProcessValueScraper(max_concurrent_browsers=concurrent)
    try:
        scraper.process_batch(
            process_numbers,
            include_other_processes=include_other_processes,
            progress_callback=on_result,
        )
        with _jobs_lock:
            _jobs[job_id]["status"] = "done"
    except Exception as e:
        with _jobs_lock:
            _jobs[job_id]["status"] = "error"
            _jobs[job_id]["error"] = str(e)
    finally:
        del scraper


# ── Routes ────────────────────────────────────────────────────────────

@router.post("/start")
def start_extraction(
    body: ExtractStartRequest,
    _key: str = Depends(require_api_key),
):
    """Start a background eSAJ extraction job. Returns a job_id for polling."""
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "status": "pending",
            "total": len(body.process_numbers),
            "done": 0,
            "results": [],
            "error": None,
        }
    thread = threading.Thread(
        target=_run_extraction,
        args=(job_id, body.process_numbers, body.concurrent, body.include_other_processes),
        daemon=True,
    )
    thread.start()
    return {"job_id": job_id}


@router.get("/status/{job_id}")
def get_status(
    job_id: str,
    _key: str = Depends(require_api_key),
):
    """Poll extraction job status and accumulated results so far."""
    with _jobs_lock:
        if job_id not in _jobs:
            raise HTTPException(status_code=404, detail="Job not found.")
        job = _jobs[job_id]
        # Snapshot inside the lock to avoid races with the background thread
        snapshot = {
            "status": job["status"],
            "total": job["total"],
            "done": job["done"],
            "results": list(job["results"]),
            "error": job["error"],
        }
    return snapshot


@router.get("/export/{job_id}")
def export_csv(
    job_id: str,
    _key: str = Depends(require_api_key),
):
    """Download current extraction results as CSV (works on partial results too)."""
    with _jobs_lock:
        if job_id not in _jobs:
            raise HTTPException(status_code=404, detail="Job not found.")
        results = list(_jobs[job_id]["results"])

    if not results:
        raise HTTPException(status_code=404, detail="No results available yet.")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=esaj_results.csv"},
    )
