"""REST surface for gov.business.registration.v1.

    GET /v1/gov/business/registration?country=GB&company_name=Monzo%20Bank%20Limited
    GET /v1/gov/business/registration?country=GB&registration_number=09446231
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from app.config import settings
from app.errors import ErrorCode, PipelineError
from app.ingestion import IngestionScheduler, parse_watchlist
from app.lifecycle import LifecycleWorker
from app.models import Envelope
from app.pipeline import RegistrationPipeline, new_request_id
from app.ratelimit import RateLimiter
from app.sources.companies_house import CompaniesHouseSource
from app.sources.gleif import GleifSource
from app.sources.opencorporates import OpenCorporatesSource
from app.storage import build_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")


def build_sources(client: httpx.AsyncClient):
    return [
        CompaniesHouseSource(
            client, settings.upstream_timeout_seconds,
            base=settings.companies_house_base,
            api_key=settings.companies_house_key,
        ),
        GleifSource(client, settings.upstream_timeout_seconds, base=settings.gleif_base),
        OpenCorporatesSource(
            client, settings.upstream_timeout_seconds,
            base=settings.opencorporates_base,
            api_key=settings.opencorporates_key,
        ),
    ]


def create_app(source_factory=build_sources) -> FastAPI:
    """Application factory.

    `source_factory` is injectable so the test suite can substitute scripted
    providers without touching module-level state — every app instance owns its
    own store, rate limiter and purge worker.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(
            headers={"User-Agent": "AI4012-registration-agent/1.0"},
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        store = build_store(database_url=settings.database_url, app_env=settings.app_env)
        await store.init()

        app.state.client = client
        app.state.store = store
        pipeline = RegistrationPipeline(source_factory(client), store, settings)
        app.state.pipeline = pipeline
        app.state.limiter = RateLimiter(settings.rate_limit, settings.rate_limit_window_seconds)

        app.state.worker = LifecycleWorker(store, settings.purge_interval_seconds)
        app.state.worker.start()

        # Continuous live ingestion (Section 1.1), independent of HTTP traffic.
        app.state.ingest = IngestionScheduler(
            pipeline,
            settings.refresh_interval_seconds,
            max_entries=settings.watchlist_max,
            seeds=parse_watchlist(settings.watchlist_seed),
        )
        pipeline.scheduler = app.state.ingest
        app.state.ingest.start()

        log.info("ready: product=%s sources=%s watching=%d", settings.product_id,
                 [s.source_id for s in pipeline.sources],
                 len(app.state.ingest.watchlist))
        try:
            yield
        finally:
            await app.state.ingest.stop()
            await app.state.worker.stop()
            await store.close()
            await client.aclose()

    application = FastAPI(
        title="Business Registration Lookup",
        version=settings.product_version,
        description=(
            "Multi-source, consensus-verified company registration facts "
            "for agent runtimes."
        ),
        lifespan=lifespan,
    )
    _register_routes(application)
    return application


def _register_routes(application: FastAPI) -> None:
    @application.exception_handler(PipelineError)
    async def pipeline_error_handler(request: Request, exc: PipelineError):
        return JSONResponse(
            status_code=exc.http_status,
            content=exc.to_envelope(new_request_id(), settings.product_id),
        )

    @application.get("/healthz")
    async def healthz():
        """Liveness: the process is up."""
        return {"status": "ok", "product_id": settings.product_id}

    @application.get("/readyz")
    async def readyz(request: Request):
        """Readiness: storage reachable and the purge loop is alive."""
        worker: LifecycleWorker = request.app.state.worker
        ingest: IngestionScheduler = request.app.state.ingest
        return {
            "status": "ok",
            "sources": [s.source_id for s in request.app.state.pipeline.sources],
            "purge_runs": worker.runs,
            "purge_failures": worker.failures,
            "last_purge": worker.last_result,
            "ingest_cycles": ingest.cycles,
            "ingest_failures": ingest.failed,
            "last_ingest": ingest.last_cycle,
            "watching": [str(q) for q in ingest.watchlist],
        }

    @application.get(settings.api_path, response_model=Envelope, response_model_exclude_none=False)
    async def get_business_registration(
        request: Request,
        country: str | None = Query(None, min_length=2, max_length=2,
                                    description="ISO-3166 alpha-2, e.g. GB"),
        company_name: str | None = Query(None, min_length=2, max_length=200),
        registration_number: str | None = Query(None, min_length=1, max_length=40),
        force_refresh: bool = Query(False, description="Bypass the consensus store"),
    ):
        if not company_name and not registration_number:
            raise PipelineError(
                ErrorCode.INVALID_REQUEST,
                "Provide either company_name or registration_number",
                http_status=422,
            )

        limiter: RateLimiter = request.app.state.limiter
        client_key = request.client.host if request.client else "anonymous"
        allowed, remaining, retry_after = limiter.check(client_key)
        if not allowed:
            raise PipelineError(
                ErrorCode.RATE_LIMITED,
                f"Rate limit of {settings.rate_limit} requests per "
                f"{settings.rate_limit_window_seconds}s exceeded",
                http_status=429,
                detail={"retry_after_seconds": retry_after},
            )

        pipeline: RegistrationPipeline = request.app.state.pipeline
        envelope = await pipeline.serve(
            company_name=company_name,
            registration_number=registration_number,
            country=country,
            force_refresh=force_refresh,
        )
        return JSONResponse(
            content=envelope.model_dump(),
            headers={
                "X-RateLimit-Limit": str(settings.rate_limit),
                "X-RateLimit-Remaining": str(remaining),
                "X-Request-Id": envelope.meta.request_id,
            },
        )

    @application.post("/internal/purge")
    async def trigger_purge(request: Request):
        """Manual trigger for the TTL job — used by the lifecycle test suite."""
        return await request.app.state.worker.run_once()

    @application.post("/internal/refresh")
    async def trigger_refresh(request: Request):
        """Manual trigger for one continuous-ingestion sweep."""
        return await request.app.state.ingest.run_once()


app = create_app()
