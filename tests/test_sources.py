"""Provider adapter tests.

Real payload shapes from Companies House and GLEIF are replayed through an
httpx MockTransport, so normalisation and error translation are verified
without touching the network (and without needing API keys in CI).
"""
from __future__ import annotations

import httpx
import pytest

from app.errors import ErrorCode, SourceError
from app.sources.base import normalise_name, normalise_status
from app.sources.companies_house import CompaniesHouseSource
from app.sources.gleif import GleifSource

pytestmark = pytest.mark.asyncio

CH_SEARCH = {
    "items": [
        {
            "title": "MONZO BANK LIMITED",
            "company_number": "09446231",
            "company_status": "active",
            "company_type": "ltd",
            "date_of_creation": "2015-02-06",
            "address_snippet": "Broadwalk House, 5 Appold Street, London",
        }
    ]
}

CH_PROFILE = {
    "company_name": "MONZO BANK LIMITED",
    "company_number": "09446231",
    "company_status": "active",
    "type": "ltd",
    "date_of_creation": "2015-02-06",
    "jurisdiction": "england-wales",
}

GLEIF_RECORD = {
    "data": [
        {
            "type": "lei-records",
            "id": "213800ZK7Z7TFDHQ4F12",
            "attributes": {
                "lei": "213800ZK7Z7TFDHQ4F12",
                "entity": {
                    "legalName": {"name": "Monzo Bank Limited", "language": "en"},
                    "legalAddress": {"country": "GB", "city": "London"},
                    "registeredAs": "09446231",
                    "jurisdiction": "GB",
                    "status": "ACTIVE",
                },
                "registration": {
                    "initialRegistrationDate": "2016-06-20T00:00:00Z",
                    "lastUpdateDate": "2026-05-14T09:12:00Z",
                    "status": "ISSUED",
                },
            },
        }
    ]
}


def mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Companies House
# ---------------------------------------------------------------------------


async def test_companies_house_normalises_a_search_result():
    client = mock_client(lambda r: httpx.Response(200, json=CH_SEARCH))
    src = CompaniesHouseSource(client, 4.0, base="https://ch.test", api_key="k")

    rec = await src.fetch(company_name="Monzo", registration_number=None, country="GB")

    assert rec.legal_name == "MONZO BANK LIMITED"
    assert rec.status == "active"
    assert rec.registered_date == "2015-02-06"
    assert rec.registration_number == "09446231"
    assert rec.jurisdiction == "GB"
    assert rec.completeness() == 1.0


async def test_companies_house_uses_the_profile_endpoint_for_a_number():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json=CH_PROFILE)

    src = CompaniesHouseSource(mock_client(handler), 4.0, base="https://ch.test", api_key="k")
    rec = await src.fetch(company_name=None, registration_number="09446231", country="GB")

    assert "/company/09446231" in seen["url"]
    assert rec.legal_name == "MONZO BANK LIMITED"


async def test_companies_house_without_a_key_fails_loudly():
    src = CompaniesHouseSource(mock_client(lambda r: httpx.Response(200, json=CH_SEARCH)),
                               4.0, base="https://ch.test", api_key=None)

    with pytest.raises(SourceError) as exc:
        await src.fetch(company_name="Monzo", registration_number=None, country="GB")

    assert exc.value.code is ErrorCode.SOURCE_NOT_CONFIGURED


async def test_companies_house_only_covers_the_uk():
    src = CompaniesHouseSource(mock_client(lambda r: httpx.Response(200, json={})),
                               4.0, base="https://ch.test", api_key="k")

    assert src.supports("GB") is True
    assert src.supports("PK") is False


async def test_empty_search_is_not_found_not_an_empty_record():
    src = CompaniesHouseSource(mock_client(lambda r: httpx.Response(200, json={"items": []})),
                               4.0, base="https://ch.test", api_key="k")

    with pytest.raises(SourceError) as exc:
        await src.fetch(company_name="Nope Ltd", registration_number=None, country="GB")

    assert exc.value.code is ErrorCode.UPSTREAM_NOT_FOUND


# ---------------------------------------------------------------------------
# GLEIF
# ---------------------------------------------------------------------------


async def test_gleif_normalises_a_lei_record():
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json=GLEIF_RECORD)),
                      4.0, base="https://gleif.test/v1")

    rec = await src.fetch(company_name="Monzo Bank Limited", registration_number=None,
                          country="GB")

    assert rec.legal_name == "Monzo Bank Limited"
    assert rec.status == "active"                      # ACTIVE -> active
    assert rec.registered_date == "2016-06-20"         # timestamp -> bare ISO date
    assert rec.jurisdiction == "GB"
    assert rec.registration_number == "09446231"
    assert rec.source_last_updated_at is not None


async def test_gleif_needs_no_api_key():
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json=GLEIF_RECORD)),
                      4.0, base="https://gleif.test/v1")
    assert src.supports("PK") is True                  # global coverage


async def test_gleif_malformed_payload_is_a_schema_error():
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json={"unexpected": 1})),
                      4.0, base="https://gleif.test/v1")

    with pytest.raises(SourceError) as exc:
        await src.fetch(company_name="x", registration_number=None, country="GB")

    assert exc.value.code is ErrorCode.UPSTREAM_BAD_SCHEMA


# ---------------------------------------------------------------------------
# Shared HTTP error translation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (429, ErrorCode.UPSTREAM_RATE_LIMITED),
        (401, ErrorCode.UPSTREAM_AUTH_FAILED),
        (403, ErrorCode.UPSTREAM_AUTH_FAILED),
        (404, ErrorCode.UPSTREAM_NOT_FOUND),
        (500, ErrorCode.UPSTREAM_UNAVAILABLE),
        (503, ErrorCode.UPSTREAM_UNAVAILABLE),
        (400, ErrorCode.UPSTREAM_BAD_SCHEMA),
    ],
)
async def test_http_status_codes_map_to_named_errors(status, expected):
    src = GleifSource(mock_client(lambda r: httpx.Response(status, json={})),
                      4.0, base="https://gleif.test/v1")

    with pytest.raises(SourceError) as exc:
        await src.fetch(company_name="x", registration_number=None, country="GB")

    assert exc.value.code is expected


async def test_timeout_is_translated_not_swallowed():
    def handler(request):
        raise httpx.ReadTimeout("too slow", request=request)

    src = GleifSource(mock_client(handler), 0.1, base="https://gleif.test/v1")

    with pytest.raises(SourceError) as exc:
        await src.fetch(company_name="x", registration_number=None, country="GB")

    assert exc.value.code is ErrorCode.UPSTREAM_TIMEOUT


async def test_non_json_body_is_a_schema_error():
    src = GleifSource(mock_client(lambda r: httpx.Response(200, text="<html>oops</html>")),
                      4.0, base="https://gleif.test/v1")

    with pytest.raises(SourceError) as exc:
        await src.fetch(company_name="x", registration_number=None, country="GB")

    assert exc.value.code is ErrorCode.UPSTREAM_BAD_SCHEMA


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ("MONZO BANK LIMITED", "Monzo Bank Ltd."),
        ("Acme Corp.", "ACME CORPORATION"),
        ("Example Co", "EXAMPLE COMPANY"),
    ],
)
async def test_legal_form_suffixes_do_not_break_name_matching(a, b):
    assert normalise_name(a) == normalise_name(b)


async def test_different_companies_still_compare_unequal():
    assert normalise_name("Monzo Bank Ltd") != normalise_name("Starling Bank Ltd")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("active", "active"), ("ACTIVE", "active"), ("ISSUED", "active"),
        ("dissolved", "dissolved"), ("converted-closed", "dissolved"),
        ("liquidation", "inactive"), ("LAPSED", "inactive"),
        (None, "unknown"), ("something-new", "unknown"),
    ],
)
async def test_status_vocabularies_map_onto_one_enum(raw, expected):
    assert normalise_status(raw) == expected


# ---------------------------------------------------------------------------
# Name matching — picking the right entity out of a relevance-ranked list
# ---------------------------------------------------------------------------

# The real GLEIF response for "Monzo Bank Limited": the parent company ranks
# first, the bank itself second.
GLEIF_MONZO_BOTH = {
    "data": [
        {
            "attributes": {
                "lei": "PARENT00000000000000",
                "entity": {
                    "legalName": {"name": "MONZO BANK HOLDING GROUP LIMITED"},
                    "legalAddress": {"country": "GB"},
                    "registeredAs": "15070039",
                    "status": "ACTIVE",
                },
                "registration": {
                    "initialRegistrationDate": "2023-10-13T00:00:00Z",
                    "lastUpdateDate": "2026-09-01T11:48:36Z",
                    "status": "ISSUED",
                },
            }
        },
        {
            "attributes": {
                "lei": "213800ZK7Z7TFDHQ4F12",
                "entity": {
                    "legalName": {"name": "Monzo Bank Limited"},
                    "legalAddress": {"country": "GB"},
                    "registeredAs": "09446231",
                    "status": "ACTIVE",
                },
                "registration": {
                    "initialRegistrationDate": "2016-06-20T00:00:00Z",
                    "lastUpdateDate": "2026-09-01T11:48:36Z",
                    "status": "ISSUED",
                },
            }
        },
    ]
}


async def test_gleif_picks_the_exact_entity_not_the_top_hit():
    """Regression for a real failure: GLEIF ranked the parent holding company
    first, so taking data[0] silently answered about a different company."""
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json=GLEIF_MONZO_BOTH)),
                      4.0, base="https://gleif.test/v1")

    rec = await src.fetch(company_name="Monzo Bank Limited",
                          registration_number=None, country="GB")

    assert rec.legal_name == "Monzo Bank Limited"
    assert rec.registration_number == "09446231"
    assert rec.name_match_exact is True


async def test_gleif_flags_an_inexact_match_instead_of_pretending():
    """When the exact entity genuinely is not in the results, the record is
    still returned — hiding it would destroy evidence — but marked inexact."""
    only_parent = {"data": [GLEIF_MONZO_BOTH["data"][0]]}
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json=only_parent)),
                      4.0, base="https://gleif.test/v1")

    rec = await src.fetch(company_name="Monzo Bank Limited",
                          registration_number=None, country="GB")

    assert rec.legal_name == "MONZO BANK HOLDING GROUP LIMITED"
    assert rec.name_match_exact is False


async def test_companies_house_also_prefers_the_exact_name():
    payload = {
        "items": [
            {"title": "MONZO BANK HOLDING GROUP LIMITED", "company_number": "15070039",
             "company_status": "active", "date_of_creation": "2023-10-13"},
            {"title": "MONZO BANK LIMITED", "company_number": "09446231",
             "company_status": "active", "date_of_creation": "2015-02-18"},
        ]
    }
    src = CompaniesHouseSource(mock_client(lambda r: httpx.Response(200, json=payload)),
                               4.0, base="https://ch.test", api_key="k")

    rec = await src.fetch(company_name="Monzo Bank Limited",
                          registration_number=None, country="GB")

    assert rec.registration_number == "09446231"
    assert rec.name_match_exact is True


async def test_a_number_search_is_always_an_exact_match():
    """Looking up by registration number is unambiguous — no name to compare."""
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json=GLEIF_RECORD)),
                      4.0, base="https://gleif.test/v1")

    rec = await src.fetch(company_name=None, registration_number="09446231",
                          country="GB")

    assert rec.name_match_exact is True


# ---------------------------------------------------------------------------
# Freshness anchoring
# ---------------------------------------------------------------------------


async def test_gleif_freshness_is_when_we_checked_not_when_the_registry_edited():
    """An LEI row is typically touched once a year at renewal. Anchoring
    freshness to lastUpdateDate marked every healthy company permanently stale
    and pinned quality_score near its floor."""
    src = GleifSource(mock_client(lambda r: httpx.Response(200, json=GLEIF_RECORD)),
                      4.0, base="https://gleif.test/v1")

    rec = await src.fetch(company_name="Monzo Bank Limited",
                          registration_number=None, country="GB")

    assert rec.source_last_updated_at == rec.retrieved_at
    # The registry's own edit date is preserved, just not used as the clock.
    assert rec.registry_last_modified is not None
    assert rec.registry_last_modified.year == 2026
    assert rec.registry_last_modified < rec.retrieved_at


# ---------------------------------------------------------------------------
# OpenCorporates
# ---------------------------------------------------------------------------


OC_SEARCH_PK = {
    "results": {
        "companies": [
            {
                "company": {
                    "name": "HABIB BANK LIMITED",
                    "company_number": "0000001",
                    "jurisdiction_code": "pk",
                    "incorporation_date": "1947-08-25",
                    "current_status": "Active",
                    "updated_at": "2026-05-10T12:00:00Z",
                }
            }
        ]
    }
}

OC_PROFILE = {
    "results": {
        "company": {
            "name": "HABIB BANK LIMITED",
            "company_number": "0000001",
            "jurisdiction_code": "pk",
            "incorporation_date": "1947-08-25",
            "current_status": "Active",
            "updated_at": "2026-05-10T12:00:00Z",
        }
    }
}


async def test_opencorporates_normalises_a_search_result():
    from app.sources.opencorporates import OpenCorporatesSource

    client = mock_client(lambda r: httpx.Response(200, json=OC_SEARCH_PK))
    src = OpenCorporatesSource(client, 4.0, base="https://oc.test/v0.4", api_key="k")

    rec = await src.fetch(company_name="Habib Bank Limited", registration_number=None, country="PK")

    assert rec.legal_name == "HABIB BANK LIMITED"
    assert rec.status == "active"
    assert rec.registered_date == "1947-08-25"
    assert rec.registration_number == "0000001"
    assert rec.jurisdiction == "PK"
    assert rec.source_id == "SRC-OC-001"


async def test_opencorporates_uses_company_number_endpoint():
    from app.sources.opencorporates import OpenCorporatesSource

    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, json=OC_PROFILE)

    src = OpenCorporatesSource(mock_client(handler), 4.0, base="https://oc.test/v0.4", api_key="k")
    rec = await src.fetch(company_name=None, registration_number="0000001", country="PK")

    assert "/companies/pk/0000001" in seen["url"]
    assert rec.legal_name == "HABIB BANK LIMITED"
    assert rec.name_match_exact is True


async def test_opencorporates_passes_api_key_when_configured():
    from app.sources.opencorporates import OpenCorporatesSource

    seen = {}

    def handler(request):
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=OC_SEARCH_PK)

    src = OpenCorporatesSource(mock_client(handler), 4.0, base="https://oc.test/v0.4", api_key="secret-token")
    await src.fetch(company_name="Habib Bank Limited", registration_number=None, country="PK")

    assert seen["params"].get("api_token") == "secret-token"


async def test_opencorporates_skips_when_api_key_missing():
    from app.sources.opencorporates import OpenCorporatesSource

    src = OpenCorporatesSource(mock_client(lambda r: None), 4.0, base="https://oc.test/v0.4", api_key=None)
    with pytest.raises(SourceError) as exc_info:
        await src.fetch(company_name="Habib Bank Limited", registration_number=None, country="PK")
    assert exc_info.value.code == ErrorCode.SOURCE_NOT_CONFIGURED

