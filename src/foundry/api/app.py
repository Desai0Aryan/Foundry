from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response

from .. import __version__
from ..config import Settings, get_settings
from ..db import create_pool, run_migrations
from ..errors import ApiError
from ..logging_config import configure_logging
from .routes import admin, health, jobs

log = logging.getLogger("foundry.api")
PROBLEM_JSON = "application/problem+json"


def _problem(
    status: int,
    code: str,
    title: str,
    detail: str | None,
    request: Request,
    errors: list | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    body: dict = {
        "type": f"https://foundry.dev/problems/{code}",
        "title": title,
        "status": status,
        "code": code,
        "request_id": getattr(request.state, "request_id", None),
    }
    if detail:
        body["detail"] = detail
    if errors:
        body["errors"] = errors
    return JSONResponse(body, status_code=status, media_type=PROBLEM_JSON, headers=headers)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level, settings.log_json)
        app.state.pool = await create_pool(settings)
        if settings.auto_migrate:
            await run_migrations(app.state.pool)
        log.info("api started", extra={"version": __version__, "env": settings.environment})
        try:
            yield
        finally:
            await app.state.pool.close()

    app = FastAPI(
        title="Foundry",
        version=__version__,
        summary="Distributed job processing platform",
        description=(
            "Submit long-running jobs and let a fleet of workers execute them reliably: "
            "idempotent submission, retries with backoff, dead-letter queues, quotas, metrics."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings

    @app.middleware("http")
    async def request_context(request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
        request.state.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        started = time.perf_counter()
        try:
            response: Response = await call_next(request)
        except Exception:
            log.exception("unhandled error", extra={"request_id": request.state.request_id})
            response = _problem(500, "internal_error", "Internal server error", None, request)
        elapsed_ms = (time.perf_counter() - started) * 1000
        response.headers["X-Request-ID"] = request.state.request_id
        user = getattr(request.state, "user", None)
        log.info(
            "request",
            extra={
                "request_id": request.state.request_id,
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "ms": round(elapsed_ms, 2),
                "user_id": str(user.id) if user else None,
            },
        )
        return response

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> JSONResponse:
        return _problem(
            exc.status, exc.code, exc.title, exc.detail, request, exc.errors, exc.headers
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [{"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
        return _problem(422, "validation_error", "Request validation failed", None, request, errors)

    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(admin.router)
    return app


def app_factory() -> FastAPI:
    """uvicorn --factory foundry.api.app:app_factory"""
    return create_app()
