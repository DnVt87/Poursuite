from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from poursuite.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE


class MentionResult(BaseModel):
    document_date: str
    db_id: str
    file_path: str
    content: str


class ProcessResult(BaseModel):
    process_number: str
    mention_count: int
    mentions: List[MentionResult]


class SearchResponse(BaseModel):
    total_processes: int
    page: int
    page_size: int
    truncated: bool
    results: List[ProcessResult]


class StatsDatabase(BaseModel):
    size_mb: float
    date_range: str


class StatsResponse(BaseModel):
    total_databases: int
    total_size_mb: float
    date_range: Dict
    databases: Dict[str, StatsDatabase]
