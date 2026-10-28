from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis import asyncio as aioredis

from .. import __version__, metrics
from ..config import Settings, get_settings
from ..db import create_pool, run_migrations
from ..errors import ApiError
from ..logging_config import configure_logging
from ..quota import QuotaManager
from ..ratelimit import RateLimiter
from ..security import constant_time_equals
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
        redis = (
            aioredis.from_url(
                settings.redis_url,
                socket_timeout=0.25,  # a slow Redis must never make the API slow
                socket_connect_timeout=0.5,
                decode_responses=True,
            )
            if settings.redis_url
            else None
        )
        app.state.redis = redis
        app.state.limiter = RateLimiter(redis, settings)
        app.state.quotas = QuotaManager(redis, app.state.pool, settings)
        log.info("api started", extra={"version": __version__, "env": settings.environment})
        try:
            yield
        finally:
            if redis is not None:
                await redis.aclose()
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
        elapsed = time.perf_counter() - started
        elapsed_ms = elapsed * 1000
        response.headers["X-Request-ID"] = request.state.request_id
        decision = getattr(request.state, "ratelimit", None)
        if decision is not None:
            for key, value in decision.headers().items():
                response.headers.setdefault(key, value)
        route = request.scope.get("route")
        route_path = getattr(route, "path", None) or "unmatched"  # template, not raw URL
        if route_path != "/metrics":
            metrics.HTTP_REQUESTS.labels(
                request.method, route_path, str(response.status_code)
            ).inc()
            metrics.HTTP_LATENCY.labels(request.method, route_path).observe(elapsed)
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

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics(request: Request) -> Response:
        token = settings.metrics_token
        if token is not None:
            supplied = request.headers.get("authorization", "").removeprefix("Bearer ")
            if not constant_time_equals(supplied, token.get_secret_value()):
                return _problem(401, "unauthorized", "Unauthorized", None, request)
        await metrics.refresh_system_gauges(app.state.pool, settings.system_metrics_cache_seconds)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(admin.router)
    return app


def app_factory() -> FastAPI:
    """uvicorn --factory foundry.api.app:app_factory"""
    return create_app()
