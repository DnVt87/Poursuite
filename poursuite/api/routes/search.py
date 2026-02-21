import io
import csv
import time
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import StreamingResponse

from poursuite.api.auth import require_api_key
from poursuite.api.schemas import MentionResult, ProcessResult, SearchResponse
from poursuite.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, SEARCH_TIMEOUT_SECONDS

router = APIRouter(prefix="/search", tags=["search"])


def _build_process_results(page_result) -> list[ProcessResult]:
    return [
        ProcessResult(
            process_number=pnum,
            mention_count=len(mentions),
            mentions=[
                MentionResult(
                    document_date=m.document_date,
                    db_id=m.db_id,
                    file_path=m.file_path,
                    content=m.content,
                )
                for m in mentions
            ],
        )
        for pnum, mentions in page_result.results.items()
    ]


@router.get("", response_model=SearchResponse)
def search(
    request: Request,
    keywords: Optional[str] = Query(default=None),
    process_number: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    exclusion_terms: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    _key: str = Depends(require_api_key),
):
    """
    Search across all databases. Returns paginated JSON results.

    If the 30-second timeout is hit, results are partial and the response includes
    X-Truncated: true header and truncated=true in the body.
    """
    engine = request.app.state.search_engine
    deadline = time.time() + SEARCH_TIMEOUT_SECONDS

    page_result = engine.search(
        keywords=keywords,
        process_number=process_number,
        start_date=start_date,
        end_date=end_date,
        exclusion_terms=exclusion_terms,
        page=page,
        page_size=page_size,
        deadline=deadline,
    )

    response_body = SearchResponse(
        total_processes=page_result.total_processes,
        page=page_result.page,
        page_size=page_result.page_size,
        truncated=page_result.truncated,
        results=_build_process_results(page_result),
    )

    headers = {"X-Truncated": "true"} if page_result.truncated else {}
    return Response(
        content=response_body.model_dump_json(),
        media_type="application/json",
        headers=headers,
    )


@router.get("/export")
def export_csv(
    request: Request,
    keywords: Optional[str] = Query(default=None),
    process_number: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    exclusion_terms: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    _key: str = Depends(require_api_key),
):
    """
    Same as GET /search but returns results as a downloadable CSV file.
    Subject to the same 30-second timeout; X-Truncated header is set if partial.
    """
    engine = request.app.state.search_engine
    deadline = time.time() + SEARCH_TIMEOUT_SECONDS

    page_result = engine.search(
        keywords=keywords,
        process_number=process_number,
        start_date=start_date,
        end_date=end_date,
        exclusion_terms=exclusion_terms,
        page=page,
        page_size=page_size,
        deadline=deadline,
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Process Number', 'Mention Count', 'Document Date',
        'Database', 'File Path', 'Content'
    ])
    for pnum, mentions in page_result.results.items():
        for idx, m in enumerate(mentions):
            writer.writerow([
                pnum, f"{idx + 1}/{len(mentions)}",
                m.document_date, m.db_id, m.file_path, m.content,
            ])

    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=search_results.csv"}
    if page_result.truncated:
        headers["X-Truncated"] = "true"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers=headers,
    )
