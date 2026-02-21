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
from fastapi.openapi.utils import get_openapi

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
    # Disable Swagger UI syntax highlighting — responses containing full court document
    # text can be several MB of JSON, which causes the Swagger highlighter to recurse
    # until it hits the JavaScript call stack limit.
    swagger_ui_parameters={"syntaxHighlight": False},
)

app.include_router(search_router.router)
app.include_router(stats_router.router)


def _custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Register X-API-Key as the global security scheme so Swagger UI shows
    # an "Authorize" button and automatically sends the header on every request.
    schema.setdefault("components", {})["securitySchemes"] = {
        "ApiKeyAuth": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
        }
    }
    schema["security"] = [{"ApiKeyAuth": []}]

    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = _custom_openapi
