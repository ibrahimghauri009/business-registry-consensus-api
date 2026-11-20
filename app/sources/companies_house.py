"""Provider A — UK Companies House Public Data API.

The statutory UK register. Authenticated with HTTP Basic where the API key is
the username and the password is empty (the service's documented convention).
Docs: https://developer-specs.company-information.service.gov.uk
Rate limit: 600 requests / 5 minutes per key.
"""
from __future__ import annotations

import httpx

from app.errors import ErrorCode, SourceError
from app.models import SourceRecord, utcnow
from app.sources.base import (
    BaseSource, normalise_date, normalise_status, pick_best_name_match,
)


class CompaniesHouseSource(BaseSource):
    source_id = "SRC-CH-001"
    publisher = "UK Companies House"
    supports_jurisdictions = {"GB", "UK"}

    def __init__(self, client: httpx.AsyncClient, timeout: float, *, base: str, api_key: str | None):
        super().__init__(client, timeout)
        self._base = base.rstrip("/")
        self._api_key = api_key

    async def fetch(
        self, *, company_name: str | None, registration_number: str | None, country: str | None
    ) -> SourceRecord:
        if not self._api_key:
            # Explicit, not silent: a missing key is a configuration error that
            # the caller gets to see in provenance + warnings.
            raise SourceError(
                self.source_id,
                ErrorCode.SOURCE_NOT_CONFIGURED,
                "COMPANIES_HOUSE_API_KEY is not set; this source was skipped",
            )

        auth = (self._api_key, "")
        retrieved_at = utcnow()

        if registration_number:
            body, latency = await self._get(
                f"{self._base}/company/{registration_number}", auth=auth
            )
            return self._from_profile(body, retrieved_at, latency)

        body, latency = await self._get(
            f"{self._base}/search/companies",
            auth=auth,
            params={"q": company_name, "items_per_page": 5},
        )
        items = body.get("items") or []
        if not items:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_NOT_FOUND,
                f"No Companies House record matched {company_name!r}",
            )
        # Search ranks by relevance, not identity, so the top hit for a short or
        # common name can be a different company. Prefer an exact name match.
        item, exact = pick_best_name_match(items, company_name, lambda i: i.get("title"))
        return self._from_search_item(item, retrieved_at, latency, exact)

    # -- normalisation -------------------------------------------------------

    def _from_profile(self, body: dict, retrieved_at, latency: int) -> SourceRecord:
        if not isinstance(body, dict) or "company_number" not in body:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                "Company profile payload is missing company_number",
            )
        return SourceRecord(
            source_id=self.source_id,
            publisher=self.publisher,
            # Companies House exposes no record-modified timestamp (`etag` changes
            # when data changes but carries no time), so the honest freshness
            # anchor is the moment we observed it. Deliberately NOT
            # `last_full_members_list_date` — that is a filing date, unrelated to
            # record freshness, and using it would report every company as stale.
            retrieved_at=retrieved_at,
            source_last_updated_at=retrieved_at,
            legal_name=body.get("company_name"),
            status=normalise_status(body.get("company_status")),
            registered_date=normalise_date(body.get("date_of_creation")),
            jurisdiction="GB",
            registration_number=body.get("company_number"),
            raw_payload=body,
            latency_ms=latency,
        )

    def _from_search_item(
        self, item: dict, retrieved_at, latency: int, name_match_exact: bool = True
    ) -> SourceRecord:
        if not isinstance(item, dict) or "company_number" not in item:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                "Search item payload is missing company_number",
            )
        return SourceRecord(
            source_id=self.source_id,
            publisher=self.publisher,
            retrieved_at=retrieved_at,
            source_last_updated_at=retrieved_at,
            name_match_exact=name_match_exact,
            legal_name=item.get("title"),
            status=normalise_status(item.get("company_status")),
            registered_date=normalise_date(item.get("date_of_creation")),
            jurisdiction="GB",
            registration_number=item.get("company_number"),
            raw_payload=item,
            latency_ms=latency,
        )
