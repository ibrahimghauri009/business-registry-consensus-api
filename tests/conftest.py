"""Shared fixtures.

APP_ENV is forced to `test` *before* app modules import, so the in-memory store
is selected legitimately rather than by accident, and no test ever needs a live
network call or a live database.
"""
from __future__ import annotations

import os

os.environ.setdefault("APP_ENV", "test")
os.environ.pop("DATABASE_URL", None)
os.environ.setdefault("CONSENSUS_TTL_SECONDS", "3600")
os.environ.setdefault("RATE_LIMIT", "10000")

from datetime import timedelta                                    # noqa: E402

import pytest                                                     # noqa: E402
import pytest_asyncio                                             # noqa: E402
from httpx import ASGITransport, AsyncClient                       # noqa: E402

from app.errors import ErrorCode, SourceError                      # noqa: E402
from app.models import SourceRecord, utcnow                        # noqa: E402
from app.sources.base import BaseSource                            # noqa: E402


# ---------------------------------------------------------------------------
# Fake providers — deterministic stand-ins with the real adapters' contract
# ---------------------------------------------------------------------------


class FakeSource(BaseSource):
    """Returns a scripted record, or raises a scripted SourceError."""

    supports_jurisdictions = None

    def __init__(self, source_id: str, publisher: str, record=None, error=None,
                 delay=0.0, jurisdictions=None):
        self.source_id = source_id
        self.publisher = publisher
        self._record = record
        self._error = error
        self._delay = delay
        self.supports_jurisdictions = jurisdictions
        self.calls = 0

    async def fetch(self, *, company_name, registration_number, country):
        import asyncio

        self.calls += 1
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._error is not None:
            raise self._error
        rec = self._record
        return SourceRecord(
            name_match_exact=rec.name_match_exact,
            registry_last_modified=rec.registry_last_modified,
            source_id=self.source_id,
            publisher=self.publisher,
            retrieved_at=rec.retrieved_at,
            source_last_updated_at=rec.source_last_updated_at,
            legal_name=rec.legal_name,
            status=rec.status,
            registered_date=rec.registered_date,
            jurisdiction=rec.jurisdiction,
            registration_number=rec.registration_number,
            raw_payload=dict(rec.raw_payload),
            latency_ms=rec.latency_ms,
        )


def make_record(
    source_id="SRC-CH-001",
    publisher="UK Companies House",
    legal_name="MONZO BANK LIMITED",
    status="active",
    registered_date="2015-02-06",
    jurisdiction="GB",
    registration_number="09446231",
    age_seconds=60,
    name_match_exact=True,
    registry_last_modified=None,
) -> SourceRecord:
    now = utcnow()
    return SourceRecord(
        name_match_exact=name_match_exact,
        registry_last_modified=registry_last_modified,
        source_id=source_id,
        publisher=publisher,
        retrieved_at=now,
        source_last_updated_at=now - timedelta(seconds=age_seconds),
        legal_name=legal_name,
        status=status,
        registered_date=registered_date,
        jurisdiction=jurisdiction,
        registration_number=registration_number,
        raw_payload={"stub": True},
        latency_ms=12,
    )


def ch_source(**kw) -> FakeSource:
    return FakeSource("SRC-CH-001", "UK Companies House", record=make_record(**kw))


def gleif_source(**kw) -> FakeSource:
    kw.setdefault("source_id", "SRC-GLEIF-001")
    kw.setdefault("publisher", "GLEIF Global LEI Index")
    return FakeSource("SRC-GLEIF-001", "GLEIF Global LEI Index", record=make_record(**kw))


def oc_source(**kw) -> FakeSource:
    kw.setdefault("source_id", "SRC-OC-001")
    kw.setdefault("publisher", "OpenCorporates")
    return FakeSource("SRC-OC-001", "OpenCorporates", record=make_record(**kw))


def failing_source(source_id="SRC-GLEIF-001", publisher="GLEIF Global LEI Index",
                   code=ErrorCode.UPSTREAM_TIMEOUT) -> FakeSource:
    return FakeSource(
        source_id, publisher,
        error=SourceError(source_id, code, f"{publisher} simulated failure"),
    )


# ---------------------------------------------------------------------------
# App fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def make_app():
    """Factory: build a real, fully isolated FastAPI app wired to fake providers.

    Each call produces its own application instance — own store, own rate
    limiter, own purge worker — so tests never leak cached consensus into one
    another. The genuine lifespan runs, so the background TTL worker is live.
    """
    from app.main import create_app

    created = []

    async def _factory(sources):
        application = create_app(source_factory=lambda _client: sources)
        cm = application.router.lifespan_context(application)
        await cm.__aenter__()
        client = AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        )
        created.append((cm, client))
        return application, client

    yield _factory

    for cm, client in created:
        await client.aclose()
        await cm.__aexit__(None, None, None)


@pytest.fixture
def anyio_backend():
    return "asyncio"
