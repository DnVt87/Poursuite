# Running Poursuite

How to (1) run the search API and expose it via Cloudflare Tunnel, and (2) run the database update pipeline. For architecture/internals, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Prerequisites

- Windows + Python 3.13 (the `.venv/` in this repo is the canonical interpreter).
- The shards on `D:\Poursuite\Databases\` (read by the search API; written to by the orchestrator's publish stage).
- Chrome installed — only needed for the **eSAJ scraper** at `/extract/start`. Downloads no longer use Selenium.
- `cloudflared` installed — only needed if you want to expose the API outside `localhost`.

Install once:

```bash
.venv/Scripts/python.exe -m pip install -e .
```

Verify the install:

```bash
.venv/Scripts/python.exe -c "import poursuite, maintenance.pdf_to_database, maintenance.static_database_optimizer; print('ok')"
```

## Environment variables

All have defaults; override only when needed.

| Variable | Default | What it controls |
|---|---|---|
| `POURSUITE_API_KEY` | *(empty — protected routes return HTTP 500 until set)* | Required for any API request via `X-API-Key` header |
| `POURSUITE_DB_DIR` | `D:/Poursuite/Databases` | Live shards (read by API; written by orchestrator publish) |
| `POURSUITE_STAGING_DB_DIR` | `C:/Poursuite/Staging` | Orchestrator working dir (parse output, sidecar dedup DB, optimizer temp) |
| `POURSUITE_COURT_DOCS_DIR` | `C:/Poursuite/CourtDocs` | Downloaded DJE PDFs (`<year>/<MM>/<YYYYMMDD>_<caderno>.pdf`) |
| `POURSUITE_LOG_DIR` | `C:/Poursuite/Logs` | All log output |
| `POURSUITE_OUTPUT_DIR` | `C:/Poursuite/SearchResults` | CSV exports from the CLI / API |
| `POURSUITE_ESAJ_OUTPUT_DIR` | `C:/Poursuite/eSAJ` | eSAJ scraper outputs |
| `POURSUITE_SEARCH_TIMEOUT` | `30` | API search deadline (seconds) |

Set the API key in the current shell (bash):

```bash
export POURSUITE_API_KEY="<the-secret>"
```

To persist across reboots, add the same line to `~/.bashrc` or set it via Windows *System Properties → Environment Variables*.

---

## 1. Running the search API

Two terminals: one for the server, one for the tunnel.

### Terminal 1 — start the API

```bash
.venv/Scripts/python.exe -m uvicorn poursuite.api.main:app --host 0.0.0.0 --port 8000 --workers 1
```

`--workers 1` is **mandatory**. SQLite connections aren't shareable across OS workers; internal parallelism is via `ThreadPoolExecutor` inside the single worker.

The SPA is served at `GET /`. There is no `/docs` endpoint — OpenAPI is disabled.

### Terminal 2 — start the Cloudflare Tunnel

TLS terminates at Cloudflare's edge, so cloudflared just forwards plaintext to `localhost:8000`. No nginx or local certs needed.

**Named tunnel** (the standard path — config lives at `%USERPROFILE%\.cloudflared\config.yml`):

```bash
cloudflared tunnel run <tunnel-name>
```

Replace `<tunnel-name>` with whatever your tunnel is registered as. You can list registered tunnels with `cloudflared tunnel list`.

**Quick throwaway** (random `*.trycloudflare.com` URL, no auth or config needed):

```bash
cloudflared tunnel --url http://localhost:8000
```

### Use it

Open the tunnel hostname (or `http://localhost:8000` locally). Paste your `POURSUITE_API_KEY` into the API Key field in the page header — it persists in `sessionStorage` until the tab closes.

If a search times out, the response carries `X-Truncated: true` and the SPA shows a yellow warning bar — narrow the query (date range or stricter keywords).

Stop both processes with Ctrl-C when done.

---

## 2. Running the database update pipeline

The orchestrator [`update_database.py`](update_database.py) runs the full Download → Parse → Split → Optimize → Publish flow in one command. Designed for a 6-month cadence (after each half-year closes); zero-arg invocation derives sensible defaults from what's already on `D:`.

### Routine update (every six months)

```bash
.venv/Scripts/python.exe update_database.py
```

That's it. The orchestrator will:

1. Read `MAX(document_date)` across every shard in `DB_DIR`.
2. Compute `start = day after that`, `end = end of the half-year containing start`, `label = e.g. 2025_Jan-Jun`.
3. Auto-advance past any half that's already published (logs `Half X already published; advancing to Y`).
4. Refuse if today is on or before `end` — i.e. the half isn't complete yet.
5. Pre-flight: refuse if `C:` < 100 GB free or `D:` < 50 GB free (override with `--skip-disk-check`).
6. Run all six stages. Idempotent — each stage skips if its output already exists.

A typical 6-month run takes hours (most of which is downloading and parsing PDFs). All progress is logged to console and to `POURSUITE_LOG_DIR/update_database.log`.

### Useful flags

```bash
# Plan only — touches nothing, prints what would happen
.venv/Scripts/python.exe update_database.py --dry-run

# Override one or more inputs (any override disables auto-advance)
.venv/Scripts/python.exe update_database.py --start 01/01/2025 --end 30/06/2025 --label 2025_Jan-Jun

# Skip a stage (repeatable). Stages: download, parse, split, optimize, publish, cleanup
.venv/Scripts/python.exe update_database.py --skip-stage download --skip-stage parse

# Re-run a stage even if its output exists (repeatable)
.venv/Scripts/python.exe update_database.py --force-stage optimize

# Bypass the disk-space pre-flight (use with caution)
.venv/Scripts/python.exe update_database.py --skip-disk-check
```

### Reclaim staging disk space

After both halves of a year are published, the staging year shard at `C:/Poursuite/Staging/legal_documents_<year>.db` can be ~100 GB. Delete it with:

```bash
.venv/Scripts/python.exe update_database.py cleanup-staging --year 2024
```

Add `--dry-run` to see what would be deleted without doing it. The sidecar dedup DB (`processed_files.db`) is retained so future runs still skip already-ingested PDFs.

### Restart picks up where it left off

If a run is interrupted, just re-run the same command. Each stage will skip work whose output already exists; download skips PDFs already on disk; parse skips PDFs already in the sidecar `processed_files.db`. To force redo of a specific stage, use `--force-stage <name>`.

---

## Restarts and recovery

- **API restart**: drop the `.db` into `DB_DIR` and restart `uvicorn` — discovery runs at startup. Restarting loses any in-flight `/extract/start` jobs (they live in-memory in daemon threads).
- **Tunnel restart**: just `cloudflared tunnel run <tunnel-name>` again. Hostname is stable for named tunnels.
- **Pipeline interrupted mid-run**: re-run; idempotent stage-skipping picks it back up.
