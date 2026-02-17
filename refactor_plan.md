# Poursuite Refactor Plan

## Phase 1: Project Structure & Shared Foundation

### 1.1 Create the package structure

```
poursuite/
├── __init__.py
├── config.py
├── models.py
├── utils.py
├── db/
│   ├── __init__.py
│   ├── connection.py
│   └── search.py
├── scraper/
│   ├── __init__.py
│   ├── esaj.py
│   └── csv_extractor.py
├── api/
│   ├── __init__.py
│   ├── main.py        # FastAPI app, lifespan startup/shutdown
│   ├── auth.py        # API key dependency
│   ├── schemas.py     # Pydantic request/response models
│   └── routes/
│       ├── __init__.py
│       ├── search.py  # GET /search, GET /search/export
│       └── stats.py   # GET /stats
└── cli.py
maintenance/           # archive one-off scripts here, untouched
```

### 1.2 `config.py` — Single source for all configuration

- All paths (`DB_DIR`, `OUTPUT_DIR`, `LOG_FILE`) with defaults, overridable via environment variables
- Constants: `DEFAULT_BATCH_SIZE`, `DEFAULT_MAX_WORKERS`, `DEFAULT_MAX_BROWSERS`, `PROCESS_NUMBER_PATTERN`
- Chrome options configuration
- API settings: `API_KEY`, `SEARCH_TIMEOUT_SECONDS = 30`, `DEFAULT_PAGE_SIZE = 100`, `MAX_PAGE_SIZE = 500`

### 1.3 `models.py` — Consolidate all dataclasses

- Move `SearchResult`, `DatabaseInfo` from NewSearchEngine
- Move `ProcessData` from ExtractDataBatch
- Keep existing fields and methods (`get_headers`, `to_dict`)

### 1.4 `utils.py` — Shared helpers

- `setup_logging(name)` — single logging config used everywhere
- `decompress_content(content)` — moved from search engine
- `format_currency(value)` — moved from scraper
- `sanitize_fts_query(query)` — new, escapes FTS special characters to prevent crashes

---

## Phase 2: Core Module Extraction

### 2.1 `db/connection.py` — Database connection management

- `DatabaseManager` class: discovery, connection pooling, lifecycle (open/close)
- Move `_discover_databases`, `_get_db_connection`, `close_connections` here
- **Fix thread-safety**: add a `threading.Lock` protecting `db_cache` reads/writes; open connections with `check_same_thread=False` so worker threads can reuse them safely
- Connections stay open for the session, closed only on explicit shutdown
- **Fix `close_connections` placement**: in the current code, connections are closed inside the search loop after every query. The manager must own connection lifetime — callers never close them.
- `get_database_stats()` lives here too (it's about DB metadata, not search). Make it lazy: skip `COUNT(*)` queries on startup, run them only when explicitly requested.

### 2.2 `db/search.py` — Pure search logic

- `SearchEngine` class, takes a `DatabaseManager` instance
- Move `_build_search_query`, `_identify_relevant_databases`, `_search_database`, `search`, `filter_processes` here
- `_build_search_query`: use `sanitize_fts_query` from utils
- `filter_processes`: add option to push exclusion into SQL (`NOT` in FTS or `WHERE content NOT LIKE`) instead of post-fetch filtering
- `get_results_summary`, `export_results_to_csv` stay here
- **Fix logging level**: the current `IOError` catch in `export_results_to_csv` logs at `INFO` — change to `ERROR`
- **Add pagination support**: `search()` accepts `page: int` and `page_size: int` parameters. Pagination is applied after merging and sorting cross-database results. Return a `SearchPage` result object that includes `total_processes`, `page`, `page_size`, `truncated: bool`.
- **Add timeout support**: `search()` accepts a `deadline: float` (Unix timestamp). Each `_search_database` call checks the deadline before executing and skips if already past it. After all futures complete, set `truncated=True` if any databases were skipped. The CLI passes `deadline=None` (no limit); the API passes `deadline=time.time() + 30`.

### 2.3 `scraper/esaj.py` — eSAJ scraper

- Move `ProcessValueScraper` class, stripped of CLI
- Chrome options come from `config.py`
- **Fix error handling**: log real exceptions, only present `"Segredo de justiça"` when the page actually indicates sealed access (detect by specific element on the eSAJ page — e.g. presence of `id="labelSituacaoProcesso"` with the sealed-case text, or a redirect to the sealed-case URL). Use a distinct `error` string for real failures (e.g. `"Scraping error"` or `str(e)`). Scraping is CLI-only — not exposed via API.
- `ProcessData` imported from `models.py`

### 2.4 `scraper/csv_extractor.py` — Keep `CSVProcessExtractor`

- Still useful for standalone CSV files the user has from previous runs
- Move as-is, import pattern from `config.py`

---

## Phase 3: Entry Points

### 3.1 `cli.py` — Single CLI replacing all `__main__` blocks

- Menu: Search → Filter → Export CSV → (optional) Scrape eSAJ → Export
- When going from search to scrape, pass process numbers directly from search results dict keys — no CSV round-trip
- CSV export remains available as user-facing output at any step
- Handle all user input/output here, zero `input()` calls in any other module
- No timeout on searches run from CLI

### 3.2 Archive maintenance scripts

Move `DownloadDJE.py`, `PDFtoDatabase v3.py`, `SplitDatabase.py`, `Static Database Optimizer v3.py`, `DatabaseVacuum.py`, `TEST.py` into `maintenance/`. No modifications.

---

## Phase 4: Web API

Scraping (eSAJ / Selenium) is **not exposed via the API**. The API handles search and CSV export only. Deployment uses a Cloudflare Tunnel — TLS terminates at Cloudflare's edge and forwards plaintext to the local FastAPI server. No nginx, no certificate management.

### 4.1 `api/auth.py` — API key authentication

- Single `API_KEY` read from environment variable (set in `config.py`)
- FastAPI `Security` dependency injected on every route
- Returns HTTP 403 if key is missing or wrong
- Key is passed by the caller as a header: `X-API-Key: <key>`

### 4.2 `api/schemas.py` — Pydantic models

Request / response contracts:

```python
# Request (query parameters on GET /search)
class SearchRequest:
    keywords: Optional[str]
    process_number: Optional[str]
    start_date: Optional[str]   # YYYY-MM-DD
    end_date: Optional[str]     # YYYY-MM-DD
    exclusion_terms: Optional[str]
    page: int = 1
    page_size: int = 100        # capped at MAX_PAGE_SIZE

# Per-process result
class ProcessResult:
    process_number: str
    mention_count: int
    mentions: List[MentionResult]

class MentionResult:
    document_date: str
    db_id: str
    file_path: str
    content: str

# Top-level response
class SearchResponse:
    total_processes: int        # total matching (across all pages)
    page: int
    page_size: int
    truncated: bool             # True if 30-second timeout was hit
    results: List[ProcessResult]
```

CSV export (`GET /search/export`) accepts the same query parameters, streams a CSV file response, subject to the same 30-second timeout.

### 4.3 `api/routes/search.py` — Search endpoints

- `GET /search` — paginated JSON search, auth required
- `GET /search/export` — same parameters, returns a `text/csv` streaming response
- Both pass `deadline=time.time() + SEARCH_TIMEOUT_SECONDS` to `SearchEngine.search()`
- If `truncated=True`, the response (JSON or CSV header) includes a `X-Truncated: true` header so the caller knows results are partial

### 4.4 `api/routes/stats.py` — Stats endpoint

- `GET /stats` — returns database count, total size, overall date range; auth required
- Calls `DatabaseManager.get_database_stats()` (the lazy version — no COUNT queries, just metadata from discovery)

### 4.5 `api/main.py` — App entry point

```python
@asynccontextmanager
async def lifespan(app):
    app.state.db_manager = DatabaseManager()   # discovers DBs on startup
    app.state.search_engine = SearchEngine(app.state.db_manager)
    yield
    app.state.db_manager.close_connections()   # clean shutdown
```

- Run with: `uvicorn poursuite.api.main:app --host 0.0.0.0 --port 8000`
- Cloudflare Tunnel points to `localhost:8000`

---

## Implementation Notes

- **Do not change behavior** — this is a refactor. All search logic, scraping logic, and output formats stay the same.
- **Imports**: every module imports from `config`, `models`, `utils` — never hardcoded values inline.
- **Type hints**: preserve existing ones, add where missing (especially function signatures).
- **`PROCESS_NUMBER_PATTERN` deduplication**: currently defined separately in `ExtractDataBatch.py` (line 48) and `CSVProcessExtractor.__init__` (line 487). Move the single definition to `config.py`.
- **Order of phases**: Phases 1 and 2 must be complete and working before touching Phase 3 or 4. The CLI and API are both thin layers over the same `DatabaseManager` + `SearchEngine` core.
