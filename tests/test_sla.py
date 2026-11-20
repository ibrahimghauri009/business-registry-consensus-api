"""Requirement 1.4 — SLA and reliability.

Availability under sustained polling, p95 < 200 ms, and the freshness contract
(`age_seconds <= ttl_seconds`, with anything older flagged, never disguised).

Latency is measured end-to-end through the ASGI stack with scripted providers,
so what is being asserted is *our* serving cost, not the public internet's.
"""
from __future__ import annotations

import asyncio
import statistics
import time

import pytest

from app.errors import ErrorCode
from tests.conftest import ch_source, gleif_source, make_record

pytestmark = pytest.mark.asyncio

PATH = "/v1/gov/business/registration"
QUERY = {"country": "GB", "company_name": "Monzo Bank Limited"}

# The brief's p95 < 200 ms applies to what the service SERVES. Two paths exist
# and they have honestly different costs, so they are budgeted separately:
#
#   warm path  — consensus record inside its TTL, no upstream call. This is the
#                steady state under sustained polling, because the continuous
#                ingestion poller keeps watched queries fresh in the background.
#                This is the path the 200 ms SLA is measured against.
#   cold path  — force_refresh or an expired record, so two live providers are
#                called. Real Companies House + GLEIF round-trips run roughly
#                200-700 ms, so no serving budget under 200 ms is achievable
#                here and claiming one would be dishonest. What IS asserted is
#                that our own overhead on top of the providers stays small.
P95_BUDGET_MS = 200
COLD_PATH_OVERHEAD_BUDGET_MS = 60
SIMULATED_PROVIDER_LATENCY_S = 0.15
SUSTAINED_REQUESTS = 200
CONCURRENCY = 20


async def _timed_get(client, params):
    started = time.perf_counter()
    resp = await client.get(PATH, params=params)
    return resp, (time.perf_counter() - started) * 1000


async def test_availability_under_sustained_polling(make_app):
    _, client = await make_app([ch_source(), gleif_source()])
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one():
        async with sem:
            return await _timed_get(client, QUERY)

    results = await asyncio.gather(*(one() for _ in range(SUSTAINED_REQUESTS)))
    statuses = [r.status_code for r, _ in results]

    assert all(s == 200 for s in statuses), f"non-200 responses: {set(statuses)}"


async def test_warm_path_p95_latency_under_200ms(make_app):
    """The headline SLA, measured on the path sustained polling actually takes."""
    _, client = await make_app([ch_source(), gleif_source()])
    await client.get(PATH, params=QUERY)                      # warm the path

    latencies = [(await _timed_get(client, QUERY))[1] for _ in range(SUSTAINED_REQUESTS)]
    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95) - 1]

    assert p95 < P95_BUDGET_MS, (
        f"p95={p95:.1f}ms exceeds {P95_BUDGET_MS}ms "
        f"(p50={statistics.median(latencies):.1f}ms, max={latencies[-1]:.1f}ms)"
    )


async def test_cold_path_overhead_stays_small_above_provider_latency(make_app):
    """Cold requests are dominated by the providers, which we do not control.

    Providers are simulated at 150 ms each and called concurrently, so the floor
    is ~150 ms. What this asserts is that ingestion, consensus, persistence and
    serialisation add only a small, bounded amount on top — the part that is
    actually our responsibility.
    """
    from tests.conftest import FakeSource, make_record

    slow = [
        FakeSource("SRC-CH-001", "UK Companies House",
                   record=make_record(), delay=SIMULATED_PROVIDER_LATENCY_S),
        FakeSource("SRC-GLEIF-001", "GLEIF Global LEI Index",
                   record=make_record(source_id="SRC-GLEIF-001"),
                   delay=SIMULATED_PROVIDER_LATENCY_S),
    ]
    _, client = await make_app(slow)
    cold = {**QUERY, "force_refresh": "true"}

    await client.get(PATH, params=cold)                       # warm code paths
    latencies = sorted([(await _timed_get(client, cold))[1] for _ in range(10)])
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    provider_floor_ms = SIMULATED_PROVIDER_LATENCY_S * 1000
    overhead = p95 - provider_floor_ms

    assert overhead < COLD_PATH_OVERHEAD_BUDGET_MS, (
        f"our overhead {overhead:.1f}ms above the {provider_floor_ms:.0f}ms provider "
        f"floor exceeds {COLD_PATH_OVERHEAD_BUDGET_MS}ms (p95={p95:.1f}ms)"
    )


async def test_providers_are_called_concurrently_not_serially(make_app):
    """Two 150 ms providers must cost ~150 ms, not ~300 ms."""
    from tests.conftest import FakeSource, make_record

    _, client = await make_app([
        FakeSource("SRC-CH-001", "UK Companies House",
                   record=make_record(), delay=SIMULATED_PROVIDER_LATENCY_S),
        FakeSource("SRC-GLEIF-001", "GLEIF Global LEI Index",
                   record=make_record(source_id="SRC-GLEIF-001"),
                   delay=SIMULATED_PROVIDER_LATENCY_S),
    ])

    _, elapsed = await _timed_get(client, {**QUERY, "force_refresh": "true"})

    assert elapsed < SIMULATED_PROVIDER_LATENCY_S * 1000 * 1.8


async def test_reported_latency_is_self_consistent(make_app):
    """meta.api.latency_ms must describe this request, not a cached one."""
    _, client = await make_app([ch_source(), gleif_source()])
    resp, wall_ms = await _timed_get(client, QUERY)
    reported = resp.json()["meta"]["api"]["latency_ms"]

    assert 0 <= reported <= wall_ms + 50


async def test_freshness_within_ttl_is_not_stale(make_app):
    _, client = await make_app([ch_source(age_seconds=30), gleif_source(age_seconds=30)])
    f = (await client.get(PATH, params=QUERY)).json()["meta"]["freshness"]

    assert f["age_seconds"] <= f["ttl_seconds"]
    assert f["stale"] is False


async def test_data_older_than_ttl_is_flagged_stale_not_hidden(make_app):
    """TTL here is 3600s (see conftest); a two-hour-old observation must say so."""
    old = 7_200
    _, client = await make_app([ch_source(age_seconds=old), gleif_source(age_seconds=old)])
    body = (await client.get(PATH, params=QUERY)).json()
    f = body["meta"]["freshness"]

    assert f["age_seconds"] > f["ttl_seconds"]
    assert f["stale"] is True
    assert ErrorCode.STALE_DATA_WITHHELD.value in {w["code"] for w in body["meta"]["warnings"]}


async def test_expired_cache_is_refetched_rather_than_reserved(make_app):
    """A stored record past its TTL must trigger a new upstream call."""
    ch, gleif = ch_source(age_seconds=7_200), gleif_source(age_seconds=7_200)
    _, client = await make_app([ch, gleif])

    await client.get(PATH, params=QUERY)
    first_calls = ch.calls
    await client.get(PATH, params=QUERY)

    assert ch.calls == first_calls + 1, "expired record was re-served instead of refreshed"


async def test_fresh_cache_is_reused_and_labelled(make_app):
    ch, gleif = ch_source(age_seconds=30), gleif_source(age_seconds=30)
    _, client = await make_app([ch, gleif])

    await client.get(PATH, params=QUERY)
    calls_after_first = ch.calls
    body = (await client.get(PATH, params=QUERY)).json()

    assert ch.calls == calls_after_first, "cache within TTL should not re-fetch"
    assert ErrorCode.SERVED_FROM_STORE.value in {w["code"] for w in body["meta"]["warnings"]}


async def test_force_refresh_bypasses_the_store(make_app):
    ch, gleif = ch_source(age_seconds=30), gleif_source(age_seconds=30)
    _, client = await make_app([ch, gleif])

    await client.get(PATH, params=QUERY)
    before = ch.calls
    await client.get(PATH, params={**QUERY, "force_refresh": "true"})

    assert ch.calls == before + 1


async def test_rate_limit_is_enforced_as_advertised(make_app):
    from app.ratelimit import RateLimiter

    application, client = await make_app([ch_source(), gleif_source()])
    application.state.limiter = RateLimiter(limit=3, window_seconds=60)

    statuses = [(await client.get(PATH, params=QUERY)).status_code for _ in range(5)]

    assert statuses[:3] == [200, 200, 200]
    assert statuses[3:] == [429, 429]


async def test_health_and_readiness_endpoints(make_app):
    _, client = await make_app([ch_source(), gleif_source()])

    assert (await client.get("/healthz")).json()["status"] == "ok"
    ready = (await client.get("/readyz")).json()
    assert ready["status"] == "ok"
    assert ready["purge_failures"] == 0
    assert set(ready["sources"]) == {"SRC-CH-001", "SRC-GLEIF-001"}


async def test_concurrent_requests_do_not_corrupt_each_others_identity(make_app):
    _, client = await make_app([ch_source(), gleif_source()])
    bodies = await asyncio.gather(
        *(client.get(PATH, params={**QUERY, "force_refresh": "true"}) for _ in range(25))
    )
    ids = [b.json()["meta"]["request_id"] for b in bodies]

    assert len(set(ids)) == len(ids)


async def test_make_record_helper_is_sane():
    """Guards the fixture itself, so an SLA failure is never a fixture artefact."""
    r = make_record()
    assert r.completeness() == 1.0
