from __future__ import annotations

from dataclasses import dataclass, fields, asdict
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class SearchResult:
    """Container for search results with metadata."""
    process_number: str
    content: str
    document_date: str
    file_path: str
    db_id: str


@dataclass
class DatabaseInfo:
    """Information about a database."""
    path: Path
    start_date: str
    end_date: str
    size_mb: float = 0.0


@dataclass
class ProcessData:
    """Data class to store process information.

    Field ordering matters: existing fields keep their positions so CSV
    consumers that read by column index remain stable. Phase 1 additions
    (Foro through distribution_year) are appended at the end; only `error`
    is allowed to remain as a trailing field for legacy reasons.
    """
    number: str
    initial_date: Optional[str] = None
    class_type: Optional[str] = None
    subject: Optional[str] = None
    value: Optional[str] = None
    last_movement: Optional[str] = None
    status: Optional[str] = None
    plaintiff: Optional[str] = None
    defendant: Optional[str] = None
    other_processes: Optional[int] = None
    # ── Phase 1 additions (see CLAUDE_CODE_BRIEF_VIEWPORT_AND_RERUN.md §7) ──
    # Scraped from the eSAJ header (Foro/Vara/Juiz in the primary panel;
    # the rest inside #maisDetalhes after the expand click).
    foro: Optional[str] = None
    vara: Optional[str] = None
    juiz: Optional[str] = None
    controle: Optional[str] = None
    outros_assuntos: Optional[str] = None
    outros_numeros: Optional[str] = None
    local_fisico: Optional[str] = None
    area: Optional[str] = None
    # Derived from the CNJ process number itself (no scraping needed).
    foro_code: Optional[str] = None
    tribunal_code: Optional[str] = None
    distribution_year: Optional[str] = None
    # ── Legacy trailing field ──
    error: Optional[str] = None

    @classmethod
    def get_headers(cls) -> List[str]:
        """Get formatted headers for display and CSV."""
        return [field.name.replace('_', ' ').title() for field in fields(cls)]

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame creation."""
        return asdict(self)


@dataclass
class SearchPage:
    """Paginated search result container."""
    results: Dict[str, List[SearchResult]]
    total_processes: int
    page: int
    page_size: int
    truncated: bool = False
