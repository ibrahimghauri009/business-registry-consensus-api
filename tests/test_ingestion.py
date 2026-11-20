"""Requirement 1.1 — *continuous* live ingestion.

These cover the half of the pipeline that runs without any HTTP traffic: the
background poller that keeps watched queries fresh on a fixed interval.
"""
from __future__ import annotations

import asyncio

import pytest

from app.ingestion import IngestionScheduler, parse_watchlist
from app.models import QuerySpec
from tests.conftest import ch_source, failing_source, gleif_source

pytestmark = pytest.mark.asyncio

PATH = "/v1/gov/business/registration"
QUERY = {"country": "GB", "company_name": "Monzo Bank Limited"}


# ---------------------------------------------------------------------------
# Watchlist seeding
# ---------------------------------------------------------------------------


async def test_watchlist_seed_is_parsed():
    specs = parse_watchlist("GB|Monzo Bank Limited|;GB||09446231")

    assert specs == [
        QuerySpec(country="GB", company_name="Monzo Bank Limited", registration_number=None),
        QuerySpec(country="GB", company_name=None, registration_number="09446231"),
    ]


@pytest.mark.parametrize("raw", ["", None, "   ", ";;", "GB||"])
async def test_empty_or_useless_seeds_are_ignored(raw):
    assert parse_watchlist(raw) == []


async def test_seeded_queries_are_polled_without_any_http_traffic(make_app):
    """The poller must work from boot with zero requests — that is what makes
    ingestion continuous rather than merely on-demand."""
    ch, gleif = ch_source(), gleif_source()
    application, _ = await make_app([ch, gleif])

    seeded = IngestionScheduler(
        application.state.pipeline, interval_seconds=60,
        seeds=parse_watchlist("GB|Monzo Bank Limited|"),
    )
    assert ch.calls == 0

    result = await seeded.run_once()

    assert result == {"watched": 1, "refreshed": 1, "failed": 0}
    assert ch.calls == 1 and gleif.calls == 1


# ---------------------------------------------------------------------------
# Auto-subscription from served traffic
# ---------------------------------------------------------------------------


async def test_serving_a_query_subscribes_it_for_continuous_refresh(make_app):
    application, client = await make_app([ch_source(), gleif_source()])
    assert application.state.ingest.watchlist == []

    await client.get(PATH, params=QUERY)

    assert application.state.ingest.watchlist == [
        QuerySpec(country="GB", company_name="Monzo Bank Limited", registration_number=None)
    ]


async def test_repeat_queries_do_not_duplicate_the_watchlist(make_app):
    application, client = await make_app([ch_source(), gleif_source()])

    for _ in range(4):
        await client.get(PATH, params=QUERY)

    assert len(application.state.ingest.watchlist) == 1


async def test_watchlist_is_bounded_and_evicts_least_recent(make_app):
    """An agent probing thousands of names must not turn the poller into an
    unbounded crawler against a rate-limited provider."""
    application, _ = await make_app([ch_source(), gleif_source()])
    sched = IngestionScheduler(application.state.pipeline, 60, max_entries=3)

    for i in range(5):
        sched.subscribe(QuerySpec("GB", f"Company {i}", None))

    names = [q.company_name for q in sched.watchlist]
    assert len(names) == 3
    assert names == ["Company 2", "Company 3", "Company 4"]


async def test_resubscribing_refreshes_recency(make_app):
    application, _ = await make_app([ch_source(), gleif_source()])
    sched = IngestionScheduler(application.state.pipeline, 60, max_entries=2)

    sched.subscribe(QuerySpec("GB", "A", None))
    sched.subscribe(QuerySpec("GB", "B", None))
    sched.subscribe(QuerySpec("GB", "A", None))      # A becomes most recent
    sched.subscribe(QuerySpec("GB", "C", None))      # evicts B, not A

    assert [q.company_name for q in sched.watchlist] == ["A", "C"]


# ---------------------------------------------------------------------------
# Refresh behaviour
# ---------------------------------------------------------------------------


async def test_refresh_bypasses_the_cache_and_re_fetches(make_app):
    ch, gleif = ch_source(), gleif_source()
    application, client = await make_app([ch, gleif])

    await client.get(PATH, params=QUERY)             # 1 fetch, now cached
    calls_after_serve = ch.calls

    await application.state.ingest.run_once()

    assert ch.calls == calls_after_serve + 1, "poller must force a live re-fetch"


async def test_polling_does_not_resubscribe_and_cannot_grow_the_watchlist(make_app):
    application, client = await make_app([ch_source(), gleif_source()])
    await client.get(PATH, params=QUERY)

    for _ in range(3):
        await application.state.ingest.run_once()

    assert len(application.state.ingest.watchlist) == 1


async def test_a_failing_refresh_is_counted_not_swallowed(make_app):
    """Both providers down: the sweep records the failure and the previous
    record is left to expire on its own TTL rather than being re-stamped."""
    application, _ = await make_app([
        failing_source("SRC-CH-001", "UK Companies House"),
        failing_source("SRC-GLEIF-001", "GLEIF Global LEI Index"),
    ])
    sched = IngestionScheduler(
        application.state.pipeline, 60, seeds=parse_watchlist("GB|Monzo Bank Limited|")
    )

    result = await sched.run_once()

    assert result == {"watched": 1, "refreshed": 0, "failed": 1}
    assert sched.failed == 1
    assert application.state.store.consensus == [], "no record written on total failure"


async def test_one_bad_query_does_not_abort_the_sweep(make_app):
    """A query no provider covers fails alone; the rest of the sweep continues."""
    from tests.conftest import FakeSource, make_record

    uk_only = FakeSource("SRC-CH-001", "UK Companies House",
                         record=make_record(), jurisdictions={"GB"})
    application, _ = await make_app([uk_only])
    sched = IngestionScheduler(application.state.pipeline, 60)
    sched.subscribe(QuerySpec("ZZ", "Unreachable Jurisdiction Ltd", None))   # no provider
    sched.subscribe(QuerySpec("GB", "Monzo Bank Limited", None))

    result = await sched.run_once()

    assert result["failed"] == 1 and result["refreshed"] == 1


async def test_background_loop_polls_on_its_interval(make_app):
    ch, gleif = ch_source(), gleif_source()
    application, _ = await make_app([ch, gleif])
    sched = IngestionScheduler(
        application.state.pipeline, interval_seconds=0.05,
        seeds=parse_watchlist("GB|Monzo Bank Limited|"),
    )

    sched.start()
    await asyncio.sleep(0.22)
    await sched.stop()

    assert sched.cycles >= 2, "the poller must run repeatedly, not once"
    assert ch.calls >= 2


async def test_readyz_reports_ingestion_health(make_app):
    _, client = await make_app([ch_source(), gleif_source()])
    await client.get(PATH, params=QUERY)
    await client.post("/internal/refresh")

    ready = (await client.get("/readyz")).json()

    assert ready["ingest_cycles"] == 1
    assert ready["ingest_failures"] == 0
    assert ready["last_ingest"]["refreshed"] == 1
    assert ready["watching"] == ["GB:Monzo Bank Limited"]
