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
from poursuite.db.search import SearchEngine
from poursuite.api.routes import search as search_router
from poursuite.api.routes import stats as stats_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: discover databases once, hold connections for the session
    app.state.db_manager = DatabaseManager()
    app.state.search_engine = SearchEngine(app.state.db_manager)
    yield
    # Shutdown: close all open SQLite connections cleanly
    app.state.db_manager.close_connections()


app = FastAPI(
    title="Poursuite API",
    description="Search Brazilian court documents across 677GB of SQLite databases.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(search_router.router)
app.include_router(stats_router.router)
