"""
FastAPI application entry point.

Run with:
    uvicorn poursuite.api.main:app --host 0.0.0.0 --port 8000 --workers 1

--workers 1 is intentional: SQLite connections are not safe to share across
OS processes, and the ThreadPoolExecutor inside SearchEngine handles
intra-process parallelism already.

Cloudflare Tunnel handles TLS termination — no nginx or certificate management needed.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from poursuite.db.connection import DatabaseManager
from poursuite.db.esaj_snapshots import SnapshotStore
from poursuite.db.process_groups import ProcessGroupStore
from poursuite.db.search import SearchEngine
from poursuite.api.routes import aggregates as aggregates_router
from poursuite.api.routes import enrichment as enrichment_router
from poursuite.api.routes import esaj_health as esaj_health_router
from poursuite.api.routes import explain_zero as explain_zero_router
from poursuite.api.routes import extract as extract_router
from poursuite.api.routes import flags as flags_router
from poursuite.api.routes import frontend as frontend_router
from poursuite.api.routes import groups as groups_router
from poursuite.api.routes import saved_queries as saved_queries_router
from poursuite.api.routes import search as search_router
from poursuite.api.routes import snapshot_status as snapshot_status_router
from poursuite.api.routes import snapshots as snapshots_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: discover databases once, hold connections for the session
    app.state.db_manager = DatabaseManager()
    app.state.search_engine = SearchEngine(app.state.db_manager)
    app.state.snapshot_store = SnapshotStore()
    app.state.process_groups = ProcessGroupStore()
    yield
    # Shutdown: close all open SQLite connections cleanly
    app.state.db_manager.close_connections()
    app.state.snapshot_store.close()


app = FastAPI(
    title="Poursuite API",
    description="Search Brazilian court documents across 677GB of SQLite databases.",
    version="1.0.0",
    lifespan=lifespan,
    # Disable built-in docs — the HTML frontend at / replaces them.
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.include_router(frontend_router.router)
app.include_router(search_router.router)
app.include_router(extract_router.router)
app.include_router(snapshots_router.router)
# UI Phase 2.5 — six new endpoint groups backing the query-builder UI.
app.include_router(groups_router.router)
app.include_router(flags_router.router)
app.include_router(snapshot_status_router.router)
app.include_router(aggregates_router.router)
app.include_router(explain_zero_router.router)
app.include_router(saved_queries_router.router)
app.include_router(esaj_health_router.router)
# Layer 3-lite enrichment surfacing (EU-b/EU-c).
app.include_router(enrichment_router.router)
