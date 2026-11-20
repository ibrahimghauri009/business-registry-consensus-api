"""Requirement 1.1 — explicit error propagation, and requirement 1.4 — failover.

The contract being tested: when a provider dies the system degrades *visibly*.
It recalculates from whoever is left, says so in warnings and provenance, drops
`verified` to false, and lowers confidence. When everyone dies it returns an
error — it does not reach for expired data and call it fresh.
"""
from __future__ import annotations

import pytest

from app.errors import ErrorCode
from tests.conftest import ch_source, failing_source, gleif_source

pytestmark = pytest.mark.asyncio

PATH = "/v1/gov/business/registration"
QUERY = {"country": "GB", "company_name": "Monzo Bank Limited", "force_refresh": "true"}


async def test_one_provider_down_still_serves_but_flags_degradation(make_app):
    _, client = await make_app([ch_source(), failing_source()])
    resp = await client.get(PATH, params=QUERY)
    body = resp.json()

    assert resp.status_code == 200
    assert body["data"]["status"] == "active"
    assert body["meta"]["trust"]["verified"] is False
    assert body["data"]["source"] == "consensus:SRC-CH-001"

    codes = {w["code"] for w in body["meta"]["warnings"]}
    assert ErrorCode.UPSTREAM_TIMEOUT.value in codes
    assert ErrorCode.SINGLE_SOURCE_DEGRADED.value in codes


async def test_failed_provider_appears_in_provenance_with_its_error_code(make_app):
    _, client = await make_app([ch_source(), failing_source()])
    prov = (await client.get(PATH, params=QUERY)).json()["meta"]["provenance"]

    by_id = {p["source_id"]: p for p in prov}
    assert by_id["SRC-CH-001"]["status"] == "ok"
    assert by_id["SRC-GLEIF-001"]["status"] == "failed"
    assert by_id["SRC-GLEIF-001"]["error_code"] == ErrorCode.UPSTREAM_TIMEOUT.value


async def test_confidence_drops_when_a_provider_is_lost(make_app):
    _, full = await make_app([ch_source(), gleif_source()])
    _, degraded = await make_app([ch_source(), failing_source()])

    full_conf = (await full.get(PATH, params=QUERY)).json()["meta"]["trust"]["confidence"]
    deg_conf = (await degraded.get(PATH, params=QUERY)).json()["meta"]["trust"]["confidence"]

    assert deg_conf < full_conf
    assert full_conf == 1.0


async def test_total_outage_returns_503_and_no_data(make_app):
    _, client = await make_app([
        failing_source("SRC-CH-001", "UK Companies House", ErrorCode.UPSTREAM_UNAVAILABLE),
        failing_source("SRC-GLEIF-001", "GLEIF Global LEI Index", ErrorCode.UPSTREAM_TIMEOUT),
    ])
    resp = await client.get(PATH, params=QUERY)
    body = resp.json()

    assert resp.status_code == 503
    assert "data" not in body
    assert body["error"]["code"] == ErrorCode.NO_SOURCES_AVAILABLE.value
    # Both underlying causes are named, not collapsed into one vague message.
    assert {f["code"] for f in body["error"]["detail"]["failures"]} == {
        ErrorCode.UPSTREAM_UNAVAILABLE.value,
        ErrorCode.UPSTREAM_TIMEOUT.value,
    }


@pytest.mark.parametrize(
    "code",
    [
        ErrorCode.UPSTREAM_RATE_LIMITED,
        ErrorCode.UPSTREAM_BAD_SCHEMA,
        ErrorCode.UPSTREAM_AUTH_FAILED,
        ErrorCode.SOURCE_NOT_CONFIGURED,
        ErrorCode.UPSTREAM_NOT_FOUND,
    ],
)
async def test_every_failure_class_surfaces_by_name(make_app, code):
    _, client = await make_app([ch_source(), failing_source(code=code)])
    body = (await client.get(PATH, params=QUERY)).json()

    assert code.value in {w["code"] for w in body["meta"]["warnings"]}


async def test_recovery_recomputes_full_consensus(make_app):
    """Once the failed provider returns, verification comes back on its own."""
    _, degraded = await make_app([ch_source(), failing_source()])
    assert (await degraded.get(PATH, params=QUERY)).json()["meta"]["trust"]["verified"] is False

    _, healthy = await make_app([ch_source(), gleif_source()])
    restored = (await healthy.get(PATH, params=QUERY)).json()
    assert restored["meta"]["trust"]["verified"] is True
    assert len(restored["data"]["source"].split("+")) == 2


async def test_failed_source_contributes_no_values(make_app):
    """A dead provider must not vote. Its absence changes the answer's provenance,
    never its content."""
    _, client = await make_app([
        failing_source("SRC-CH-001", "UK Companies House"),
        gleif_source(legal_name="MONZO BANK LTD", status="active"),
    ])
    body = (await client.get(PATH, params=QUERY)).json()

    assert body["data"]["source"] == "consensus:SRC-GLEIF-001"
    assert body["data"]["legal_name"] == "MONZO BANK LTD"
