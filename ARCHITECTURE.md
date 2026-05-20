# Poursuite — Architecture & Functionality Reference

A development reference describing the current state of the Poursuite codebase. Poursuite is a search and data-extraction system for Brazilian court documents (TJSP — Tribunal de Justiça de São Paulo). It indexes the Diário de Justiça Eletrônico (DJE) into time-partitioned SQLite databases, exposes full-text search via FastAPI + a single-page web frontend, and provides a Selenium-based scraper for fetching live process metadata from the eSAJ system.

> Last verified May 2026. When code drifts from this document, treat the code as authoritative and update this file.

---

## 1. High-Level Overview

```
                           ┌─────────────────────────────┐
                           │  Browser (vanilla SPA)      │
                           │  served from GET /          │
                           └──────────────┬──────────────┘
                                          │ X-API-Key
                                          ▼
                       ┌────────────────────────────────────┐
                       │  FastAPI app  (uvicorn --workers 1)│
                       │   /search   /search/export         │
                       │   /extract/start  /status  /export │
                       │   /stats    (currently unmounted)  │
                       └─────┬─────────────────────────┬────┘
                             │                         │
                             ▼                         ▼
            ┌────────────────────────┐   ┌────────────────────────────┐
            │  SearchEngine          │   │  ProcessValueScraper       │
            │  (db/search.py)        │   │  (scraper/esaj.py)         │
            │  Threadpool fanout     │   │  Selenium thread pool      │
            │  over multiple SQLite  │   │  against esaj.tjsp.jus.br  │
            │  shards (FTS5)         │   │                            │
            └─────────┬──────────────┘   └────────────────────────────┘
                      │
                      ▼
            ┌────────────────────────┐
            │  DatabaseManager       │
            │  (db/connection.py)    │
            │  Discovers *.db in     │
            │  D:/Poursuite/Databases│
            └────────────────────────┘
```

Two complementary data flows:

1. **Offline ingest pipeline** (`maintenance/`): downloads gazette PDFs → parses & compresses → writes to SQLite shards → optimizes / vacuums / splits.
2. **Online query/extract pipeline** (`poursuite/`): the FastAPI app and CLI read those shards, plus drive a live Selenium scraper for current process metadata.

**Single-worker constraint.** The server must run with `uvicorn --workers 1`; SQLite connections are reused across threads but cannot be shared across OS workers. Internal parallelism is provided by `ThreadPoolExecutor`.

---

## 2. Repository Layout

```
poursuite/                        ← installed package
├── __init__.py                   ← __version__ = "1.0.0"
├── cli.py                        ← interactive menu CLI (entry point: `poursuite`)
├── config.py                     ← single source of truth for env vars / paths / constants
├── models.py                     ← dataclasses: SearchResult, DatabaseInfo, ProcessData, SearchPage
├── utils.py                      ← setup_logging, decompress_content, format_currency, sanitize_fts_query
├── api/
│   ├── main.py                   ← FastAPI app + lifespan
│   ├── auth.py                   ← X-API-Key dependency
│   ├── schemas.py                ← Pydantic response models
│   └── routes/
│       ├── frontend.py           ← serves SPAs: new at /, legacy at /legacy
│       ├── spa_v2.html           ← new query-builder SPA (vanilla JS, 10 screens; UI Phase 2.5)
│       ├── search.py             ← /search, /search/export
│       ├── extract.py            ← /extract/start, /status/{id}, /export/{id}
│       ├── stats.py              ← /stats   (defined but not currently included in main.py)
│       ├── snapshots.py          ← /api/process/{n}/{snapshots,movimentos,links,peticoes}; /api/query
│       ├── groups.py             ← /api/groups (carteira CRUD; backed by JSON file)
│       ├── flags.py              ← /api/flags (★ toggle; backed by process_flags table)
│       ├── snapshot_status.py    ← /api/snapshot_status (bulk freshness lookup)
│       ├── aggregates.py         ← /api/aggregates/{group_by,histogram,stats}
│       ├── explain_zero.py       ← /api/query/explain_zero
│       └── saved_queries.py      ← /api/saved_queries (shared library; schema v3)
├── db/
│   ├── connection.py             ← DatabaseManager (multi-shard discovery, conn pool)
│   ├── search.py                 ← SearchEngine (FTS5 fanout, pagination, exclusion)
│   ├── esaj_schema.sql           ← v1 baseline DDL for esaj_snapshots.db
│   ├── esaj_snapshots.py         ← SnapshotStore + migrations v2 (cd_documento) / v3 (flags + saved_queries) / v4 (normalized columns)
│   ├── esaj_query.py             ← build_query + synth_where_only (WHERE-tree synthesis)
│   └── process_groups.py         ← ProcessGroupStore (JSON file, write-rename atomicity)
└── scraper/
    ├── esaj.py                   ← ProcessValueScraper (Selenium pool against eSAJ)
    └── csv_extractor.py          ← CSVProcessExtractor (extract process numbers from CSV)

maintenance/                      ← offline / operational scripts (orchestrated by update_database.py)
├── DownloadDJE.py                ← scrape DJE PDFs from TJSP
├── pdf_to_database.py            ← parse PDFs → write to SQLite shards (uses sidecar dedup DB)
├── SplitDatabase.py              ← extract a date range out of a shard into a new one
├── static_database_optimizer.py  ← deduplicate + recompress + rebuild FTS for a frozen shard
├── DatabaseVacuum.py             ← VACUUM (manual, ad-hoc)
├── legacy/                       ← retained-for-reference precursors (not used)
│   ├── NewSearchEngine.py        ← legacy precursor to db/search.py
│   ├── ExtractDataBatch.py       ← legacy precursor to scraper/esaj.py
│   └── TEST.py                   ← ad-hoc
└── *.log                         ← historical logs (gitignored)

update_database.py                ← end-to-end pipeline orchestrator (top-level entry point)
pyproject.toml                    ← build config & deps (Python ≥3.13)
refactor_plan.md                  ← original 4-phase plan (largely executed; see §11)
```

External directories (configured, not in repo): `D:/Poursuite/Databases` (live shards), `C:/Poursuite/Staging` (orchestrator working dir), `C:/Poursuite/CourtDocs` (downloaded PDFs), `C:/Poursuite/SearchResults`, `C:/Poursuite/eSAJ`, `C:/Poursuite/Logs`.

---

## 3. Configuration ([poursuite/config.py](poursuite/config.py))

Every tunable is centralized here. Every path / constant accepts an env-var override.

| Env variable | Default | Used by |
|---|---|---|
| `POURSUITE_DB_DIR` | `D:/Poursuite/Databases` | DatabaseManager discovery; orchestrator publish target |
| `POURSUITE_STAGING_DB_DIR` | `C:/Poursuite/Staging` | Orchestrator: parse output, split output, optimizer working dir, sidecar dedup DB |
| `POURSUITE_COURT_DOCS_DIR` | `C:/Poursuite/CourtDocs` | DownloadDJE output; pdf_to_database input root |
| `POURSUITE_OUTPUT_DIR` | `C:/Poursuite/SearchResults` | CSV exports from CLI / SearchEngine |
| `POURSUITE_ESAJ_OUTPUT_DIR` | `C:/Poursuite/eSAJ` | Scraper outputs |
| `POURSUITE_LOG_DIR` | `C:/Poursuite/Logs` | All logging |
| `POURSUITE_MAX_WORKERS` | `16` | SearchEngine threadpool size |
| `POURSUITE_BATCH_SIZE` | `50` | Scraper batching default |
| `POURSUITE_MAX_BROWSERS` | `4` | Scraper concurrent Chromes |
| `POURSUITE_API_KEY` | `""` | API auth (must be set or every protected route returns 500) |
| `POURSUITE_SEARCH_TIMEOUT` | `30` | API search deadline (seconds) |

Constants of note:

- `DEFAULT_PAGE_SIZE = 100`, `MAX_PAGE_SIZE = 500` — API pagination cap.
- `PROCESS_NUMBER_PATTERN = r'\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}'` and a `^…$`-anchored strict variant. **This is the single source of truth** for the process-number format used everywhere (CLI, scraper validation, CSV extraction).
- `ESAJ_URL = "https://esaj.tjsp.jus.br/cpopg/open.do"`.
- `ESAJ_SEALED_ELEMENT_ID = "labelSituacaoProcesso"`, `ESAJ_SEALED_TEXT = "Segredo de Justiça"` — markers used to detect sealed cases that must be skipped.
- Logging targets: `SEARCH_LOG_FILE` and `SCRAPER_LOG_FILE` under `LOG_DIR`.

---

## 4. Domain Models ([poursuite/models.py](poursuite/models.py))

All cross-module data exchange goes through these dataclasses.

### `SearchResult`
A single mention found in a SQLite shard.

| Field | Type | Notes |
|---|---|---|
| `process_number` | str | TJSP number, format `NNNNNNN-DD.AAAA.J.TR.OOOO` |
| `content` | str | Decompressed paragraph text |
| `document_date` | str | `YYYY-MM-DD` |
| `file_path` | str | Source PDF path within the shard's domain |
| `db_id` | str | Filename stem of the originating `.db` |

### `DatabaseInfo`
Metadata about a discovered shard.

| Field | Type | Notes |
|---|---|---|
| `path` | Path | Absolute path to the `.db` |
| `start_date` / `end_date` | str | Min/max `document_date` in the shard, used for date-range pruning |
| `size_mb` | float | From `Path.stat().st_size` |

### `ProcessData`
The shape returned by the eSAJ scraper. All fields beyond `number` are `Optional`.

`number, initial_date, class_type, subject, value, last_movement, status, plaintiff, defendant, other_processes, error`

- `get_headers()` → title-cased column labels for CSV exports.
- `to_dict()` → `dataclasses.asdict()`; the API stores these dicts directly in the in-memory job state.

### `SearchPage`
Result envelope returned by `SearchEngine.search`.

| Field | Type | Notes |
|---|---|---|
| `results` | `Dict[str, List[SearchResult]]` | Keyed by process_number, values are mention lists |
| `total_processes` | int | Count of distinct processes matched (across the full result set, before pagination) |
| `page`, `page_size` | int | Pagination is by **process**, not mention |
| `truncated` | bool | True if any shard was skipped because `deadline` was exceeded |

---

## 5. Shared Utilities ([poursuite/utils.py](poursuite/utils.py))

- **`setup_logging(name, log_file=None)`** — named logger with rotating file + console handler at INFO; idempotent (guards against duplicate handlers).
- **`decompress_content(content)`** — handles zlib-compressed bytes (the on-disk format) **or** plain strings; falls back to `decode('utf-8', errors='replace')` if zlib fails.
- **`format_currency(value)`** — normalizes "R$" Brazilian-Real strings (single space after symbol).
- **`sanitize_fts_query(query)`** — minimal FTS5 sanitizer. **Be careful changing this.**

  Tokenizes via regex `(?:"[^"]*"|\S)+`. For each token:
  - Uppercase `AND` / `OR` / `NOT` → preserved as boolean operators.
  - `"…"` quoted phrases → preserved.
  - Otherwise, escape `\` and `^` only via `_FTS_UNSAFE = re.compile(r'([\\^])')`.

  This deliberately preserves parentheses, `*` (prefix wildcard), `.`, `|`, braces, slashes — all valid FTS5 syntax. The previous over-aggressive version (escaping all of these) broke real queries; see commit `9ab59bb`.

---

## 6. Database Layer ([poursuite/db/](poursuite/db/))

### 6.1 Storage Model

- Backend: **SQLite 3** with **FTS5** virtual tables.
- Sharding: many `.db` files in `DB_DIR`, time-partitioned. Each shard self-describes its date range via `MIN/MAX(document_date)`.
- Required schema per shard:
  - Table `paragraphs` with at least `id`, `process_number`, `content` (zlib-compressed bytes), `document_date` (text, `YYYY-MM-DD`), `file_path`.
  - Optional virtual table `paragraphs_fts` (FTS5). When present, `paragraphs_fts MATCH ?` is the search backend; without it, keyword search returns nothing.
  - An index on `document_date` is implied (used for ORDER BY and range pruning).
- Database files are listed in `.gitignore` and live entirely outside the repo. No `.db` exists in the project tree.

### 6.2 [`db/connection.py`](poursuite/db/connection.py) — `DatabaseManager`

- **Discovery** (`_discover_databases`): on init, globs `DB_DIR/*.db`, opens each, validates that `paragraphs` exists, queries date range, captures size. Failures are logged and silently skipped (so a corrupt shard doesn't break the whole app).
- **Connection cache**: `Dict[db_id, sqlite3.Connection]` protected by `threading.Lock`. Connections opened with `check_same_thread=False` and `row_factory = sqlite3.Row` so threads in `ThreadPoolExecutor` can share them.
- `get_connection(db_id)` is lazy — opens on first use, reuses thereafter.
- `close_connections()` — called from the FastAPI lifespan shutdown handler.
- `get_database_stats()` — returns `{total_databases, total_size_mb, date_range, databases}` purely from cached metadata; no `COUNT(*)` queries (would be expensive against ~677 GB of data).

### 6.3 [`db/search.py`](poursuite/db/search.py) — `SearchEngine`

`search(...)` is the single entry point used by both the CLI and the API.

**Algorithm**

1. Cap `page_size` at `MAX_PAGE_SIZE`.
2. `_identify_relevant_databases(start_date, end_date)` — interval-overlap filter against each shard's `start_date/end_date`. Skips entire shards that can't contribute.
3. Submit `_search_database(...)` for each relevant shard to `ThreadPoolExecutor(max_workers=min(len(shards), max_workers))`.
4. Inside each worker:
   - If `time.time() > deadline`, return `None` immediately (signals truncation).
   - Build SQL via `_build_search_query`; decompress each row's `content` via `decompress_content`; group into `defaultdict[process_number] -> [SearchResult, ...]`.
5. Merge worker outputs. If any worker returned `None`, set `truncated = True`.
6. Apply `filter_processes(...)` if `exclusion_terms` provided (post-filter, not in SQL).
7. Sort each process's mentions by `document_date DESC`; sort processes by their **most recent mention** DESC.
8. Slice `[(page-1)*page_size : page*page_size]`. Note: `total_processes` reflects the **full** matched set (pre-pagination, post-exclusion).

**SQL construction** (`_build_search_query`)

```sql
SELECT process_number, content, document_date, file_path
FROM paragraphs
WHERE {conditions joined by AND}
ORDER BY document_date DESC
```

Conditions, added only when supplied:
- `id IN (SELECT rowid FROM paragraphs_fts WHERE paragraphs_fts MATCH ?)` — keyword via FTS5; param goes through `sanitize_fts_query`.
- `process_number LIKE ?` — partial process-number match (`%…%`).
- `document_date >= ?` and/or `document_date <= ?` — text comparison works because of `YYYY-MM-DD` format.

**Exclusion filtering** (`filter_processes`) is a Python-side substring check, case-insensitive, applied after the merge. It tokenizes exclusion strings with the same `(?:"[^"]*"|\S+)` regex, so phrases can be quoted. **A process is dropped entirely if any of its mentions matches any exclusion term** — this is intentional (the goal is to suppress noisy boilerplate processes wholesale).

**CSV export** (`export_results_to_csv`) writes to `OUTPUT_DIR / output_path`. Optional summary header (search params + per-shard mention counts), then rows: `Process Number, Mention Count (i/N), Document Date, Database, File Path, Content`. The API's `/search/export` builds essentially the same payload but streams it back as `Content-Disposition: attachment`.

**Truncation behavior.** `deadline` is a Unix timestamp (`time.time() + SEARCH_TIMEOUT_SECONDS`). The API always sets it; the CLI passes `None` (no timeout). When the API hits the deadline mid-fanout, the response carries `SearchPage.truncated = True` **and** the HTTP header `X-Truncated: true`. The frontend renders a yellow warning bar telling the user to narrow the query.

---

## 7. Scraper Layer ([poursuite/scraper/](poursuite/scraper/))

### 7.1 [`scraper/esaj.py`](poursuite/scraper/esaj.py) — `ProcessValueScraper`

Headless-Selenium scraper against `https://esaj.tjsp.jus.br/cpopg/open.do`. Each thread holds its own Chrome driver; results from `process_batch` may arrive via callback as soon as each process finishes (not in submission order), then are re-ordered into the input order before returning.

**Driver pool**

- `_drivers: Dict[thread_id, WebDriver]`, lock-protected.
- `_get_driver()` — lazy per-thread Chrome with: `--headless --disable-gpu --no-sandbox --disable-dev-shm-usage --log-level=3 --disable-logging` and `excludeSwitches=["enable-logging"]` to silence noise.
- `_cleanup_thread_driver()` runs in the worker's `finally` so each batch task quits its driver cleanly.

**Per-process flow** (`get_process_data`)

1. Validate against `PROCESS_NUMBER_PATTERN_STRICT`; raise `ValueError` if malformed.
2. `driver.get(ESAJ_URL)`.
3. Fill the search form: `numeroDigitoAnoUnificado` ← `process_number[:15]`; `foroNumeroUnificado` ← `process_number[-4:]`. Click `botaoConsultarProcessos`.
4. Wait (15s) for **either** `classeProcesso` (normal) or `labelSituacaoProcesso` (sealed).
5. **Sealed case detection**: if `labelSituacaoProcesso` text contains `"Segredo de Justiça"`, return `ProcessData(number=..., error="Segredo de justiça")` — no further extraction.
6. Try to expand the "Mais" link via JS click and re-wait for `dataHoraDistribuicaoProcesso`.
7. Parse fields by `FIELD_MAPPINGS`:
   - `initial_date`: div `dataHoraDistribuicaoProcesso`, sliced `[0:10]` (the date portion of a datetime string).
   - `class_type` ← span `classeProcesso`; `subject` ← span `assuntoProcesso`.
   - `value` ← div `valorAcaoProcesso`, normalized by `format_currency`.
   - `last_movement` ← td `dataMovimentacao`.
   - `status` ← span `labelSituacaoProcesso.unj-tag`.
   - Parties: all `td.nomeParteEAdvogado`, split on first newline; `[0]` plaintiff, `[1]` defendant.
8. If `include_other_processes=True` **and** a defendant was extracted, run a secondary search by defendant name on the same driver (clears cookies after) — selects `cbPesquisa = "NMPARTE"`, ticks "pesquisar por nome completo", submits, reads `contadorDeProcessos`. Returns 0 on any failure.

**Batch flow** (`process_batch`)

- `ThreadPoolExecutor(max_workers=max_concurrent_browsers)` (default 4, capped 1–8 by the API schema).
- `progress_callback(ProcessData)` fires as each future completes — used by the API to push partial results into the in-memory job state.
- Default for `include_other_processes` is **False** in batch mode (it doubles the number of eSAJ requests); the per-process default is True.
- Errors at any layer (validation, extraction, worker) become `ProcessData(error=..., number=...)` rather than exceptions — the batch never aborts on a single bad input.

### 7.2 [`scraper/csv_extractor.py`](poursuite/scraper/csv_extractor.py) — `CSVProcessExtractor`

Pulls process numbers out of arbitrary CSVs (e.g. exports from external search engines).

- Sets `csv.field_size_limit(2_000_000_000)` to handle huge cells.
- Two-pass read: scans for a line containing the literal substring `"Process Number"` to locate the actual header row (skips arbitrary preambles), then reads CSV from there.
- Applies `re.findall(PROCESS_NUMBER_PATTERN, cell)` to every cell in the matched column. Returns a `Set[str]` (deduplicated).
- Encoding: `utf-8-sig` (strips BOM) with `errors='replace'`.
- Fallback: if structured parsing throws, treats the whole file as a single string and runs the regex globally — useful when the input is malformed.

---

## 8. FastAPI Application ([poursuite/api/](poursuite/api/))

### 8.1 [`api/main.py`](poursuite/api/main.py) — App Wiring

- `lifespan` async context manager creates a single `DatabaseManager` and `SearchEngine` at startup, attaches them to `app.state`, and calls `db_manager.close_connections()` on shutdown.
- All built-in OpenAPI/docs endpoints are disabled (`docs_url=None, redoc_url=None, openapi_url=None`). Documentation **is** the SPA at `/`.
- Routers included: `frontend_router`, `search_router`, `extract_router`. Note: `stats.py` exists with a working `/stats` endpoint but is **not currently included** in `main.py`.
- No CORS middleware, no rate limiting, no custom exception handlers — same-origin only by default.
- Run: `uvicorn poursuite.api.main:app --host 0.0.0.0 --port 8000 --workers 1`.

### 8.2 [`api/auth.py`](poursuite/api/auth.py) — Authentication

`require_api_key` is a FastAPI dependency wired into every protected route as `_key: str = Depends(require_api_key)`.

- Header: `X-API-Key`. `auto_error=False` so we can return our own messages.
- If `config.API_KEY` is empty → **HTTP 500** "API key not configured on server. Set POURSUITE_API_KEY environment variable." (server misconfiguration, not a client error).
- If header is missing or doesn't match → **HTTP 403** "Invalid or missing API key."
- Comparison is plain string equality (no constant-time compare).

### 8.3 [`api/schemas.py`](poursuite/api/schemas.py) — Pydantic Models

`MentionResult`, `ProcessResult`, `SearchResponse`, `StatsDatabase`, `StatsResponse`. These are the wire formats; they map 1:1 from the dataclasses in `models.py` for search, and contain the dictionary form of `ProcessData` for extraction.

### 8.4 [`api/routes/search.py`](poursuite/api/routes/search.py)

#### `GET /search` → `SearchResponse`

| Query param | Type | Notes |
|---|---|---|
| `keywords` | str? | FTS5 syntax allowed |
| `process_number` | str? | Partial OK, matches via `LIKE %…%` |
| `start_date`, `end_date` | str? | `YYYY-MM-DD` |
| `exclusion_terms` | str? | Space-separated; quoted phrases supported |
| `page` | int ≥1, default 1 | |
| `page_size` | int ≤`MAX_PAGE_SIZE`, default `DEFAULT_PAGE_SIZE` | |

Auth required. Sets `deadline = time.time() + SEARCH_TIMEOUT_SECONDS`, calls `engine.search(...)`, builds `SearchResponse`. Adds `X-Truncated: true` header if the engine truncated.

#### `GET /search/export` → CSV

Same query params. Streams back a CSV via `StreamingResponse(iter([buffer.getvalue()]))` with `Content-Disposition: attachment; filename=search_results.csv`. The CSV is fully built in memory before being sent — fine for typical result sizes, but not a true streaming export.

### 8.5 [`api/routes/extract.py`](poursuite/api/routes/extract.py) — Background Jobs

Implements the live scraper as a fire-and-poll job system. **All state is process-local in-memory; nothing survives a restart.**

```python
_jobs: Dict[str, dict] = {}           # job_id -> {status, total, done, results, error}
_jobs_lock = threading.Lock()         # protects every read/write
```

#### `POST /extract/start`

Body schema (`ExtractStartRequest`):

```python
process_numbers: List[str] = Field(..., min_length=1)
concurrent: int = Field(default=DEFAULT_MAX_BROWSERS, ge=1, le=8)
include_other_processes: bool = False
```

- Generates a UUID; initializes `_jobs[job_id] = {status:"pending", total:N, done:0, results:[], error:None}`.
- Spawns a **daemon** `threading.Thread` running `_run_extraction(...)`, which:
  1. Sets status `"running"`.
  2. Builds `ProcessValueScraper(max_concurrent_browsers=concurrent)`.
  3. `scraper.process_batch(process_numbers, include_other_processes=..., progress_callback=on_result)`.
  4. `on_result` appends `result.to_dict()` and increments `done` under lock.
  5. On success: status `"done"`. On exception: status `"error"`, `error=str(e)`.
- Returns `{job_id}` immediately. Validation errors → 422.

#### `GET /extract/status/{job_id}`

Returns a snapshot taken under the lock. 404 if unknown. The frontend polls this every 2 s.

#### `GET /extract/export/{job_id}`

Returns CSV built from `csv.DictWriter` over the current results snapshot — works even mid-job. 404 if unknown or no results yet.

**Caveats and gotchas**

- The job dict grows unbounded: nothing cleans up completed jobs. Acceptable for a single-user local deployment; would need TTL/eviction in a multi-tenant context.
- Daemon threads die with the server. A restart loses every in-flight extraction.
- Errors inside the batch are **per-process** (stored in each `ProcessData.error`), not job-fatal. The job status flips to `"error"` only when the scraper itself raises.

### 8.6 [`api/routes/stats.py`](poursuite/api/routes/stats.py)

`GET /stats` returns `StatsResponse` from `db_manager.get_database_stats()`. Currently **defined but not mounted in `main.py`** — this is dead code from the developer's perspective until the router is wired.

### 8.7 [`api/routes/frontend.py`](poursuite/api/routes/frontend.py) — The SPA

A single self-contained HTML document served at `GET /`, with inline CSS and JS. `include_in_schema=False`. No external assets; deployment is just the Python package.

**UI structure**

- Sticky header with title, two tab buttons (Search / Extract eSAJ), and an `X-API-Key` input persisted to `sessionStorage` (cleared on tab close).
- **Search tab**: form (keywords, process number, start/end date, exclusion terms, page size), result section with collapsible per-process cards showing each mention's date / db_id / file_path / content (truncated to 400 chars with Show more / Show less), pagination (Prev, current ±2, "of N", Next), and a "Send N to Extract →" button that pre-fills the Extract textarea.
- **Extract tab**: textarea for process numbers (one per line), concurrent-browsers select (2/4/6/8), "Include defendant's process count" checkbox, progress bar, sortable results table.

**Sortable extract table**

11 columns defined by `EXTRACT_COLS`. Click a header to sort asc/desc; clicking the same column toggles direction. Empty values always sort last. Special parsing:
- `value` column: BR-formatted Real (`R$ 1.234,56`) → strips non-digits/commas, swaps `,`→`.`, parses float.
- `other_processes`: numeric coercion.
- Everything else: case-insensitive string compare.

**State (Search)**: `currentPage`, `searching`, `lastSearchNumbers`.
**State (Extract)**: `extractJobId`, `extractPollTimer`, `extractResultCount`, `extractResults`, `extractSortCol`, `extractSortDir`.

**Network behavior**

- Every API call attaches `X-API-Key` from `apiKey()`; missing key short-circuits to a status banner.
- Polling uses `setInterval(pollExtract, 2000)`; transient network blips are silently swallowed and retried next tick. Polling stops when status becomes `"done"` or `"error"`.

---

## 9. CLI ([poursuite/cli.py](poursuite/cli.py))

Entry point declared in `pyproject.toml`: `poursuite = "poursuite.cli:main"`.

Interactive menu (no argparse):

1. Search by keywords (with optional dates).
2. Search by process number.
3. Show database statistics.
4. Scrape eSAJ from CSV (uses `CSVProcessExtractor` to seed `ProcessValueScraper.process_batch`).
5. Exit.

Search results from 1/2 share a follow-up flow: print a summary, optionally export to CSV (under `OUTPUT_DIR`), optionally apply second-layer exclusion-term filtering, optionally feed the matched process numbers directly into the eSAJ scraper. The CLI never enforces a search timeout (`deadline=None`) — long queries just run to completion.

`main()` wraps everything in `try/finally` to ensure `DatabaseManager.close_connections()` runs.

---

## 10. Maintenance Pipeline

### 10.1 The orchestrator: [`update_database.py`](update_database.py)

A single top-level entry point that runs the full update pipeline end-to-end. Designed for a 6-month cadence (after each half-year closes); zero-arg invocation derives sensible defaults from what's already on D:.

```
python update_database.py                       # full auto-derived run
python update_database.py --start 01/01/2025    # override start date
python update_database.py --label 2025_Jan-Jun  # override published filename suffix
python update_database.py --dry-run             # plan only, no side effects
python update_database.py cleanup-staging --year 2024
```

**Stages** (each skippable via `--skip-stage <name>`, repeatable; re-runnable via `--force-stage <name>`):

```
download → C:/Poursuite/CourtDocs/<year>/<MM>/                       (Selenium / DownloadDJE)
parse    → C:/Poursuite/Staging/legal_documents_<year>.db            (pdf_to_database)
split    → C:/Poursuite/Staging/legal_documents_<label>.db           (SplitDatabase)
optimize → C:/Poursuite/Staging/Optimized/legal_documents_<label>.db (static_database_optimizer)
publish  → D:/Poursuite/Databases/legal_documents_<label>.db          (atomic move)
cleanup  → delete the per-half intermediate split shard
```

**Auto-derived defaults**

- `--start`: day after `MAX(document_date)` across every shard in `DB_DIR`.
- `--end`: last day of the half-year containing `--start` (Jun 30 or Dec 31).
- `--label`: e.g. `2025_Jan-Jun` (derived from `--end`).
- The orchestrator refuses to run if today is on or before `--end` (the half isn't yet complete).

**Idempotency.** Each stage skips if its expected output already exists. `download` and `parse` deduplicate at finer granularity: `download` skips PDFs whose final filenames already exist in CourtDocs; `parse` consults a sidecar `processed_files.db` and skips PDFs already ingested into a year shard.

**Pre-flight.** Before any stage runs (unless `--skip-disk-check`), the orchestrator refuses to start if `C:` has < 100 GB free or `D:` has < 50 GB free. Half-year shards are large (~50 GB optimized; staging + temp + final coexist during optimize).

**Publish safety.** The publish stage refuses to overwrite an existing target on D: unless `--force-stage publish` is passed.

**`cleanup-staging` subcommand.** Deletes a year's staging shard (`C:/Poursuite/Staging/legal_documents_<year>.db`) once both halves are published. Sidecar dedup state (`processed_files.db`) is retained.

### 10.2 Stage-implementation scripts ([maintenance/](maintenance/))

These are **not** imported at runtime; they are imported by `update_database.py` and remain runnable as standalone CLIs.

| Script | Role |
|---|---|
| [`DownloadDJE.py`](maintenance/DownloadDJE.py) | Selenium scraper: downloads 5 cadernos per business day from `https://dje.tjsp.jus.br/cdje`, output to `COURT_DOCS_DIR/<year>/<MM>/<YYYYMMDD>_<caderno>.pdf`. Chrome init is lazy (only fires on `download_documents()`). |
| [`pdf_to_database.py`](maintenance/pdf_to_database.py) | Parses PDFs via PyMuPDF, extracts process numbers and paragraphs, zlib-compresses content, writes to `STAGING_DB_DIR/legal_documents_<year>.db`. Dedup state lives in `STAGING_DB_DIR/processed_files.db` (sidecar) so the year shard can be deleted after publish without losing the record of which PDFs have already been ingested. |
| [`SplitDatabase.py`](maintenance/SplitDatabase.py) | Extracts a date range from a source shard into a new one; copies schema (table, FTS5 virtual table, triggers, indices) via `sqlite_master`. Bulk-write pragmas: `journal_mode=OFF`, `synchronous=OFF`, `mmap_size=8GiB`, `page_size=64KiB`. |
| [`static_database_optimizer.py`](maintenance/static_database_optimizer.py) | Deduplicates rows by content hash, recompresses, rebuilds FTS5, and produces a read-optimized archive. Year extraction uses regex (`legal_documents_(\d{4})`) — works for both full-year and half-year filenames. Outputs `archive_<year>.db`; the orchestrator renames to the label-based name post-hoc. |
| [`DatabaseVacuum.py`](maintenance/DatabaseVacuum.py) | `VACUUM`. Manual, ad-hoc — not part of the orchestrated pipeline. |
| [`maintenance/legacy/`](maintenance/legacy/) | Retained-for-reference precursors: `NewSearchEngine.py`, `ExtractDataBatch.py`, `TEST.py`. |

### 10.3 Known issues in the optimizer (deferred)

- **Windows file-lock retention**: after `optimize()` returns, sqlite handles may briefly keep the archive file locked. The orchestrator works around this with `gc.collect()` + retry-with-backoff before the post-hoc rename.
- **VACUUM may fail with "database is locked"** under certain timings (the script has a fallback that logs the warning and continues without VACUUM).
- **Specialized indices (`idx_process_content`, `idx_document_process`, `idx_file_path`) are inconsistently persisted.** The runtime doesn't require them — search uses FTS5 — but they would speed up some queries. Pre-existing behavior; not addressed in this work.

---

## 11. Packaging & Tooling

### `pyproject.toml`

- Build system: `setuptools>=68`, `wheel`. Backend `setuptools.build_meta`.
- Python: `>=3.13`.
- Console script: `poursuite = "poursuite.cli:main"`.
- Dependencies:
  - `fastapi>=0.111`
  - `uvicorn[standard]>=0.30`
  - `pydantic>=2.0`
  - `python-multipart>=0.0.9`
  - `selenium>=4.0`
  - `beautifulsoup4>=4.0`
  - `pandas>=2.0`
  - `tabulate>=0.9`
- No dev-deps section; no formal test suite is checked in.

### `.gitignore`

Excludes: `__pycache__/`, `*.py[cod]`, `*.egg-info/`, `dist/`, `build/`, `.venv/`, `venv/`, `.idea/`, `.vscode/`, `*.log`, `*.db`, `*.sqlite`, `*.sqlite3`, `.claude/`, `.DS_Store`, `Thumbs.db`. Real data and IDE artifacts stay out.

### `refactor_plan.md`

The codebase is largely the executed form of this plan. Phases 1–3 (package layout, config consolidation, unified CLI, archived legacy scripts) match the current state. Phase 4 (FastAPI + auth + pagination + timeout + streaming export) is also implemented — the plan is essentially executed. The document is now historical context; treat the code as authoritative when they disagree.

---

## 12. Cross-Cutting Concerns

### Authentication & secrets
- One static API key in `POURSUITE_API_KEY`. No user accounts, no rotation. Ship the key out-of-band.
- Frontend stores it in `sessionStorage` — survives reload, dies on tab close.

### Concurrency model
- **Server**: 1 OS process, 1 uvicorn worker. Internal parallelism via `ThreadPoolExecutor` in `SearchEngine` (default 16) and a separate `ThreadPoolExecutor` per extraction job in `ProcessValueScraper` (1–8).
- **DB connections**: one per shard, shared across threads (`check_same_thread=False`), guarded by a lock at the `DatabaseManager` level.
- **Job state**: one global `_jobs` dict, every access under `_jobs_lock`.

### Timeouts
- Search: hard deadline (`SEARCH_TIMEOUT_SECONDS`, default 30 s). Partial results are a feature, not an error — `truncated=true` flag and `X-Truncated` header notify the client.
- eSAJ scraper: per-element WebDriverWait — 10 s for form fields, 15 s for results page, 5 s for secondary searches and the "Mais" link.

### Error handling philosophy
- **Search** errors are mostly best-effort: a corrupt/inaccessible shard logs and skips, doesn't 500.
- **Scraper** errors are **per-process**: failures land in `ProcessData.error` rather than aborting the batch.
- **API auth** misconfiguration intentionally returns 500 (operator problem, not a client problem) so it's visible in monitoring.

### Logging
- All loggers via `setup_logging`, INFO level, format `'%(asctime)s - %(levelname)s - %(message)s'`.
- File outputs under `LOG_DIR`: `search_engine.log`, `tjsp_scraper.log`. Plus per-script logs in `maintenance/`.

### Performance notes
- Date-range pruning (`_identify_relevant_databases`) is the single biggest knob: a 1-month query touches a tiny fraction of shards, a 5-year query touches most of them.
- `paragraphs.content` is zlib-compressed — `decompress_content` runs per-row inside each shard worker.
- CSV exports build the full payload in memory before streaming. Fine at current scale; revisit if result sets grow.
- `get_database_stats()` is intentionally cheap (cached metadata). Don't replace it with `COUNT(*)` over `paragraphs` — that's minutes of I/O per shard.

---

## 13. UI Phase 2.5 — Query-builder additions (May 2026)

The query-builder UI ships against six new endpoint groups, two schema-migration
versions, a JSON-backed group store, and a vanilla-JS SPA replacement at `/`.
The original SPA stays available at `/legacy` for the transition month.

### 13.1 Schema v3 + v4

- **v3** — additive tables, no risk of breaking existing rows:
  - `process_flags(process_number TEXT PRIMARY KEY, flagged_at TEXT NOT NULL)`
  - `saved_queries(id INTEGER PK AUTOINCREMENT, name, description, query_body TEXT, created_at, last_run_at, last_run_count)`
- **v4** — column adds on `process_snapshot` (idempotent via `PRAGMA table_info` guard):
  - `foro_name` — `derive_from_cnj(...)["foro_name"]`, previously computed and discarded.
  - `last_movement_iso` — ISO-8601 date parsed from the raw `DD/MM/YYYY` `last_movement`.
  - `value_centavos` — BRL × 100 integer parsed from the raw `R$ N.NNN,NN` `value`.

Indices: `idx_process_snapshot_foro_name`, `..._last_movement_iso`, `..._value_centavos`
(WHERE-clause partial indices, NULL-skipping).

Scraper populates the three v4 fields on write. Existing rows are backfilled
by [`scripts/backfill_normalized_columns.py`](scripts/backfill_normalized_columns.py)
(idempotent, `--dry-run` available).

### 13.2 `synth_where_only` refactor

`build_query` in [`poursuite/db/esaj_query.py`](poursuite/db/esaj_query.py) was decomposed:
`synth_where_only(body) → (where_sql, params, joins)` returns the
WHERE fragment with the snapshot predicate and `flagged_only` / `unflagged_only`
already merged. `build_query` now wraps this; the aggregate and explain-zero
endpoints share the same primitive. `joins` is `[]` in v1 — the hook for a
future LEFT-JOIN promotion of `flagged` to a regular field.

### 13.3 New endpoint groups (six)

All under `poursuite/api/routes/`, all using the existing `X-API-Key` middleware.

| Module | Routes |
|---|---|
| `groups.py` | `GET /api/groups`, `GET /api/groups/{id}`, `POST /api/groups`, `DELETE /api/groups/{id}` |
| `flags.py` | `GET /api/flags`, `POST /api/flags/{pn}`, `DELETE /api/flags/{pn}` |
| `snapshot_status.py` | `POST /api/snapshot_status` (bulk freshness lookup; chunked `IN`) |
| `aggregates.py` | `POST /api/aggregates/group_by`, `.../histogram`, `.../stats` |
| `explain_zero.py` | `POST /api/query/explain_zero` |
| `saved_queries.py` | `GET /api/saved_queries`, `GET /api/saved_queries/{id}`, `POST`, `PUT /{id}`, `DELETE /{id}`, `POST /{id}/touch` |

Group storage is a **JSON file** at `$POURSUITE_SNAPSHOT_DIR/process_groups.json`,
managed by `poursuite/db/process_groups.py` (write-rename atomicity, per-instance
`threading.Lock`). Per Patch 1 of `CLAUDE_CODE_BRIEF_UI_IMPL_v2.md`, the carteira
is upload provenance — not a workspace — so a table would be over-built.

### 13.4 Aggregate semantics

- `group_by` accepts the same `where` / `snapshot` / `flagged_only` /
  `unflagged_only` surface as `/api/query`. Whitelisted fields: `class_type`,
  `foro_code`, `foro_name`, `vara`, `juiz`, `distribution_year`, plus the
  virtual `last_movement_bucket` (SQL `julianday('now') - julianday(last_movement_iso)`,
  five buckets ≤30d / 30-90d / 90-180d / 180-365d / >365d).
- `histogram` operates on `value_centavos`. Default bucket edges in BRL units:
  `[0, 10k, 50k, 100k, 500k, 1M, 5M]`. The unbounded top bucket emits
  `range_high: null`.
- `stats` returns `count`, `sum`, `mean`, `median`, `min`, `max` in BRL.
  Median via `LIMIT 1 OFFSET count/2` (SQLite has no `PERCENTILE_DISC`).

### 13.5 Frontend

- `GET /` serves the new SPA from [`poursuite/api/routes/spa_v2.html`](poursuite/api/routes/spa_v2.html)
  (file read once at module load). Vanilla HTML/CSS/JS, 10 screens, hash router.
- `GET /legacy` serves the original parameter-form SPA from the embedded
  `_LEGACY_HTML` in [`frontend.py`](poursuite/api/routes/frontend.py).
- The visual builder ↔ JSON textarea sync triggers on blur (per Patch 3);
  builder parity audited in [`docs/ui_design/BUILDER_PARITY.md`](docs/ui_design/BUILDER_PARITY.md).
- Cancellable queries are UI-side only (`AbortController.abort()`); the server
  keeps running. Adequate at current query times.

### 13.6 Env var

`POURSUITE_SNAPSHOT_DIR` (default: `POURSUITE_DB_DIR`) — holds
`esaj_snapshots.db` and `process_groups.json`. Lets the operator separate
snapshot data from the DJE shards if disk pressure shifts.

---

## 14. Quick Reference — Where Things Live

| Need to… | Look in |
|---|---|
| Add a new search filter | `db/search.py::_build_search_query` (SQL) + `api/routes/search.py` (query param) + frontend form in `routes/frontend.py` |
| Tighten/relax FTS5 sanitization | `utils.py::sanitize_fts_query` and `_FTS_UNSAFE` |
| Change the in-memory job model | `api/routes/extract.py` (`_jobs`, `_run_extraction`, all three handlers) |
| Add a scraped field | `models.py::ProcessData` + `scraper/esaj.py::FIELD_MAPPINGS` and `get_process_data` + frontend `EXTRACT_COLS` |
| Wire `/stats` back in | `api/main.py` → `app.include_router(stats.router)` |
| Tune search concurrency / timeout | `config.py` — `DEFAULT_MAX_WORKERS`, `SEARCH_TIMEOUT_SECONDS` (or env vars) |
| Add a new shard | Drop the `.db` into `DB_DIR` and restart the server (discovery runs in `lifespan`) |
| Change the API key | Set `POURSUITE_API_KEY` env var and restart |
| Run the offline pipeline | `python update_database.py` (full auto-run) — see §10.1 for overrides |
| Free up staging disk space | `python update_database.py cleanup-staging --year YYYY` after both halves of `YYYY` are published |
