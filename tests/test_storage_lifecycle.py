"""Requirement 1.3 — storage engine and lifecycle management.

The in-memory store mirrors the SQL semantics exactly (summarise, then purge),
so these run anywhere. The final test exercises the real PostgreSQL functions
and skips itself when no DATABASE_URL is exported.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pytest

from app.lifecycle import LifecycleWorker
from app.models import utcnow
from app.storage import MemoryStore, PostgresStore, RawEntry, build_store
from app.errors import PipelineError
from tests.conftest import ch_source, gleif_source

pytestmark = pytest.mark.asyncio

PRODUCT = "gov.business.registration.v1"
PATH = "/v1/gov/business/registration"
QUERY = {"country": "GB", "company_name": "Monzo Bank Limited"}


async def _seed(store, *, age_seconds: int, ttl: int, key="GB||monzo bank limited"):
    at = utcnow() - timedelta(seconds=age_seconds)
    await store.record_consensus(
        product_id=PRODUCT, query_key=key, request_id="req_TEST",
        data={"legal_name": "MONZO BANK LIMITED", "status": "active"},
        meta={"warnings": []}, confidence=0.98, quality_score=0.95,
        verified=True, contributing_sources=["SRC-CH-001", "SRC-GLEIF-001"],
        source_last_updated_at=at, computed_at=at, ttl_seconds=ttl,
    )
    await store.record_raw(
        product_id=PRODUCT, query_key=key, request_id="req_TEST",
        entries=[
            RawEntry(
                source_id="SRC-CH-001", publisher="UK Companies House",
                fetch_status="ok", error_code=None, payload={"stub": True},
                normalised={"status": "active"}, latency_ms=10, retrieved_at=at,
            )
        ],
        ttl_seconds=ttl,
    )


async def test_records_inside_ttl_survive_a_purge():
    store = MemoryStore()
    await _seed(store, age_seconds=10, ttl=3600)

    result = await store.purge_expired()

    assert result["consensus_purged"] == 0
    assert len(store.consensus) == 1


async def test_expired_records_are_purged():
    store = MemoryStore()
    await _seed(store, age_seconds=7200, ttl=3600)

    result = await store.purge_expired()

    assert result["consensus_purged"] == 1
    assert result["raw_purged"] == 1
    assert store.consensus == [] and store.raw == []


async def test_expired_records_are_summarised_before_deletion():
    """History must outlive the fine-grained rows — that is the whole point
    of summarisation rather than a plain DELETE."""
    store = MemoryStore()
    await _seed(store, age_seconds=7200, ttl=3600)
    await _seed(store, age_seconds=7300, ttl=3600)

    await store.purge_expired()

    assert store.consensus == []
    assert len(store.rollups) >= 1
    bucket = next(iter(store.rollups.values()))
    assert bucket["observations"] == 2
    assert bucket["modal_status"] == "active"
    assert 0.0 < bucket["avg_confidence"] <= 1.0


async def test_purge_is_idempotent():
    store = MemoryStore()
    await _seed(store, age_seconds=7200, ttl=3600)

    first = await store.purge_expired()
    second = await store.purge_expired()

    assert first["consensus_purged"] == 1
    assert second["consensus_purged"] == 0


async def test_every_purge_run_is_audited():
    store = MemoryStore()
    await _seed(store, age_seconds=7200, ttl=3600)
    await store.purge_expired()

    assert len(store.audit) == 1
    assert store.audit[0]["consensus_purged"] == 1


async def test_background_worker_runs_the_purge_on_its_own():
    store = MemoryStore()
    await _seed(store, age_seconds=7200, ttl=3600)
    worker = LifecycleWorker(store, interval_seconds=60)

    await worker.run_once()

    assert worker.runs == 1
    assert worker.last_result["consensus_purged"] == 1
    assert store.consensus == []


async def test_worker_counts_failures_and_keeps_running():
    """A purge that throws must be counted and retried, not kill the loop."""
    import asyncio

    class BrokenStore(MemoryStore):
        async def purge_expired(self):
            raise RuntimeError("database went away")

    worker = LifecycleWorker(BrokenStore(), interval_seconds=0.05)

    # run_once itself propagates — callers see the real error.
    with pytest.raises(RuntimeError):
        await worker.run_once()

    # The background loop records the failure and survives to try again.
    worker.start()
    await asyncio.sleep(0.2)
    await worker.stop()

    assert worker.failures >= 2


async def test_serving_writes_both_raw_evidence_and_consensus(make_app):
    application, client = await make_app([ch_source(), gleif_source()])
    await client.get(PATH, params=QUERY)
    store = application.state.store

    assert len(store.consensus) == 1
    assert {r["source_id"] for r in store.raw} == {"SRC-CH-001", "SRC-GLEIF-001"}
    assert all(r["fetch_status"] == "ok" for r in store.raw)


async def test_raw_evidence_is_retained_longer_than_consensus(make_app):
    """The audit trail must not vanish with the serving cache."""
    application, client = await make_app([ch_source(), gleif_source()])
    await client.get(PATH, params=QUERY)
    store = application.state.store

    assert store.raw[0]["ttl_seconds"] > store.consensus[0]["ttl_seconds"]


async def test_missing_database_url_is_fatal_outside_tests():
    with pytest.raises(PipelineError):
        build_store(database_url=None, app_env="prod")


async def test_test_environment_selects_the_in_memory_double():
    assert isinstance(build_store(database_url=None, app_env="test"), MemoryStore)


@pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="set DATABASE_URL to run the real PostgreSQL JSONB integration test",
)
async def test_postgres_jsonb_roundtrip_and_purge():
    store = PostgresStore(os.environ["DATABASE_URL"])
    await store.init()
    try:
        key = f"GB||pg-integration-{utcnow().timestamp()}"
        await _seed(store, age_seconds=7200, ttl=3600, key=key)

        latest = await store.get_latest_consensus(product_id=PRODUCT, query_key=key)
        assert latest is not None
        assert latest["data"]["status"] == "active"

        result = await store.purge_expired()
        assert result["consensus_purged"] >= 1
        assert await store.get_latest_consensus(product_id=PRODUCT, query_key=key) is None
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# .env loading
# ---------------------------------------------------------------------------


async def test_dotenv_file_is_loaded_into_the_environment(tmp_path, monkeypatch):
    from app.config import _load_dotenv

    env = tmp_path / ".env"
    env.write_text(
        "# a comment\n"
        "\n"
        "COMPANIES_HOUSE_API_KEY=abc123\n"
        'QUOTED_VALUE="hello world"\n'
        "SINGLE_QUOTED='single'\n"
        "  SPACED = padded  \n"
        "MALFORMED_NO_EQUALS\n",
        encoding="utf-8",
    )
    for key in ("COMPANIES_HOUSE_API_KEY", "QUOTED_VALUE", "SINGLE_QUOTED", "SPACED"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)

    _load_dotenv(env)

    assert os.environ["COMPANIES_HOUSE_API_KEY"] == "abc123"
    assert os.environ["QUOTED_VALUE"] == "hello world"
    assert os.environ["SINGLE_QUOTED"] == "single"
    assert os.environ["SPACED"] == "padded"
    assert "MALFORMED_NO_EQUALS" not in os.environ


async def test_a_real_environment_variable_beats_the_dotenv_file(tmp_path, monkeypatch):
    """An operator's export or a CI secret must win over a checked-out file."""
    from app.config import _load_dotenv

    env = tmp_path / ".env"
    env.write_text("COMPANIES_HOUSE_API_KEY=from_file\n", encoding="utf-8")
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "from_real_env")

    _load_dotenv(env)

    assert os.environ["COMPANIES_HOUSE_API_KEY"] == "from_real_env"


async def test_dotenv_is_ignored_under_app_env_test(tmp_path, monkeypatch):
    """Otherwise the suite would pass or fail depending on whose laptop it is."""
    from app.config import _load_dotenv

    env = tmp_path / ".env"
    env.write_text("SHOULD_NOT_APPEAR=1\n", encoding="utf-8")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.delenv("SHOULD_NOT_APPEAR", raising=False)

    _load_dotenv(env)

    assert "SHOULD_NOT_APPEAR" not in os.environ


async def test_a_missing_dotenv_is_not_an_error(tmp_path, monkeypatch):
    from app.config import _load_dotenv

    monkeypatch.delenv("APP_ENV", raising=False)
    _load_dotenv(tmp_path / "definitely-absent.env")     # must not raise
