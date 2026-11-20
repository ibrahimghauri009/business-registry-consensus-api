"""Requirement 1.2 — the served payload must match Section 3 of the brief exactly."""
from __future__ import annotations

import re

import pytest

from tests.conftest import ch_source, gleif_source

pytestmark = pytest.mark.asyncio

PATH = "/v1/gov/business/registration"
QUERY = {"country": "GB", "company_name": "Monzo Bank Limited"}

RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


@pytest.fixture
async def client(make_app):
    _, c = await make_app([ch_source(), gleif_source()])
    return c


async def test_top_level_shape(client):
    body = (await client.get(PATH, params=QUERY)).json()
    assert set(body) == {"data", "meta"}


async def test_meta_contains_every_required_key(client):
    meta = (await client.get(PATH, params=QUERY)).json()["meta"]
    required = {
        "request_id", "product_id", "version", "served_at",
        "source_last_updated_at", "freshness", "provenance", "trust",
        "license", "api", "warnings",
    }
    assert required <= set(meta)


async def test_freshness_block_is_well_formed(client):
    f = (await client.get(PATH, params=QUERY)).json()["meta"]["freshness"]
    assert set(f) == {"age_seconds", "ttl_seconds", "stale"}
    assert isinstance(f["age_seconds"], int) and f["age_seconds"] >= 0
    assert isinstance(f["ttl_seconds"], int) and f["ttl_seconds"] > 0
    assert isinstance(f["stale"], bool)


async def test_trust_block_is_well_formed(client):
    t = (await client.get(PATH, params=QUERY)).json()["meta"]["trust"]
    assert set(t) == {"confidence", "quality_score", "verified"}
    assert 0.0 <= t["confidence"] <= 1.0
    assert 0.0 <= t["quality_score"] <= 1.0
    assert isinstance(t["verified"], bool)


async def test_provenance_lists_every_configured_source(client):
    prov = (await client.get(PATH, params=QUERY)).json()["meta"]["provenance"]
    assert {p["source_id"] for p in prov} == {"SRC-CH-001", "SRC-GLEIF-001"}
    for p in prov:
        assert {"source_id", "publisher", "retrieved_at"} <= set(p)
        assert RFC3339.match(p["retrieved_at"])


async def test_api_block_reports_latency_and_rate_limit(client):
    api = (await client.get(PATH, params=QUERY)).json()["meta"]["api"]
    assert isinstance(api["latency_ms"], int) and api["latency_ms"] >= 0
    assert set(api["rate_limit"]) == {"limit", "window_seconds"}


async def test_timestamps_are_rfc3339_zulu(client):
    meta = (await client.get(PATH, params=QUERY)).json()["meta"]
    assert RFC3339.match(meta["served_at"])
    assert RFC3339.match(meta["source_last_updated_at"])


async def test_request_id_is_unique_per_request(client):
    a = (await client.get(PATH, params=QUERY)).json()["meta"]["request_id"]
    b = (await client.get(PATH, params=QUERY)).json()["meta"]["request_id"]
    assert a != b and a.startswith("req_")


async def test_product_identity_is_stable(client):
    meta = (await client.get(PATH, params=QUERY)).json()["meta"]
    assert meta["product_id"] == "gov.business.registration.v1"
    assert meta["version"] == "1.0.0"


async def test_data_block_carries_the_business_fields(client):
    data = (await client.get(PATH, params=QUERY)).json()["data"]
    assert set(data) == {
        "legal_name", "registration_number", "status", "registered_date",
        "jurisdiction", "as_of", "source",
    }
    assert data["status"] in {"active", "inactive", "dissolved", "unknown"}
    assert data["source"].startswith("consensus:")


async def test_response_validates_against_the_pydantic_contract(client):
    from app.models import Envelope

    Envelope.model_validate((await client.get(PATH, params=QUERY)).json())


async def test_missing_query_parameters_are_rejected_with_a_structured_error(client):
    resp = await client.get(PATH, params={"country": "GB"})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "INVALID_REQUEST"
    assert "request_id" in body["meta"]


async def test_request_id_matches_the_ulid_shape_in_the_brief(client):
    """The sample in Section 3 is `req_01J8FX7K3M2Q9P4R6T8V` — a ULID."""
    rid = (await client.get(PATH, params=QUERY)).json()["meta"]["request_id"]
    body = rid.removeprefix("req_")

    assert rid.startswith("req_")
    assert len(body) == 26
    assert set(body) <= set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")


async def test_request_ids_sort_chronologically_across_milliseconds(client):
    import asyncio

    ids = []
    for _ in range(3):
        ids.append((await client.get(PATH, params=QUERY)).json()["meta"]["request_id"])
        await asyncio.sleep(0.002)

    assert ids == sorted(ids)


async def test_provenance_reports_the_registry_edit_date_separately(make_app):
    """The registry's own last-modified date is informational metadata, not the
    freshness clock — both must be visible and distinct."""
    from datetime import timedelta
    from tests.conftest import FakeSource, make_record
    from app.models import utcnow

    edited = utcnow() - timedelta(days=16)
    _, c = await make_app([
        FakeSource("SRC-CH-001", "UK Companies House", record=make_record()),
        FakeSource("SRC-GLEIF-001", "GLEIF Global LEI Index",
                   record=make_record(source_id="SRC-GLEIF-001",
                                      registry_last_modified=edited)),
    ])

    body = (await c.get(PATH, params={**QUERY, "force_refresh": "true"})).json()
    by_id = {p["source_id"]: p for p in body["meta"]["provenance"]}

    assert by_id["SRC-GLEIF-001"]["source_modified_at"].startswith(
        edited.strftime("%Y-%m-%d")
    )
    # A 16-day-old registry edit must NOT make a just-fetched fact stale.
    assert body["meta"]["freshness"]["stale"] is False
    assert body["meta"]["freshness"]["age_seconds"] < 300


async def test_provenance_exposes_whether_the_name_matched(client):
    prov = (await client.get(PATH, params=QUERY)).json()["meta"]["provenance"]

    assert all(p["name_match_exact"] is True for p in prov if p["status"] == "ok")
