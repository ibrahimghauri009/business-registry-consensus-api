"""Provider C — OpenCorporates Public Company API.

An independent global open data registry covering company registers worldwide,
including Pakistan (Securities and Exchange Commission of Pakistan - SECP) and
the UK (Companies House).

Docs: https://api.opencorporates.com/documentation/API-Reference
"""
from __future__ import annotations

import httpx

from app.errors import ErrorCode, SourceError
from app.models import SourceRecord, utcnow
from app.sources.base import (
    BaseSource, normalise_date, normalise_status, parse_dt, pick_best_name_match,
)


class OpenCorporatesSource(BaseSource):
    source_id = "SRC-OC-001"
    publisher = "OpenCorporates"
    supports_jurisdictions = None  # global coverage (including PK, GB, US, etc.)

    def __init__(
        self,
        client: httpx.AsyncClient,
        timeout: float,
        *,
        base: str = "https://api.opencorporates.com/v0.4",
        api_key: str | None = None,
    ):
        super().__init__(client, timeout)
        self._base = base.rstrip("/")
        self._api_key = api_key

    async def fetch(
        self, *, company_name: str | None, registration_number: str | None, country: str | None
    ) -> SourceRecord:
        if not self._api_key:
            raise SourceError(
                self.source_id,
                ErrorCode.SOURCE_NOT_CONFIGURED,
                "OPENCORPORATES_API_KEY is not set; this source was skipped",
            )

        retrieved_at = utcnow()
        params: dict[str, str | int] = {"api_token": self._api_key}

        jurisdiction = country.lower() if country else None

        if registration_number and jurisdiction:
            url = f"{self._base}/companies/{jurisdiction}/{registration_number}"
            body, latency = await self._get(url, params=params)
            company = (body.get("results") or {}).get("company")
            if not company:
                raise SourceError(
                    self.source_id,
                    ErrorCode.UPSTREAM_NOT_FOUND,
                    f"No OpenCorporates record matched number {registration_number!r} in {jurisdiction}",
                )
            return self._from_company(company, retrieved_at, latency, exact=True)

        # Search by name or registration number
        query = company_name or registration_number or ""
        params["q"] = query
        params["per_page"] = 5
        if jurisdiction:
            params["jurisdiction_code"] = jurisdiction

        body, latency = await self._get(f"{self._base}/companies/search", params=params)
        results = body.get("results") if isinstance(body, dict) else None
        if not isinstance(results, dict) or "companies" not in results:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                "OpenCorporates response missing 'results.companies'",
            )

        items = [wrapper.get("company") for wrapper in results.get("companies", []) if wrapper.get("company")]
        if not items:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_NOT_FOUND,
                f"No OpenCorporates record matched {query!r}",
            )

        best_item, exact = pick_best_name_match(
            items, company_name, lambda c: c.get("name")
        )
        return self._from_company(best_item, retrieved_at, latency, exact=exact)

    def _from_company(
        self, company: dict, retrieved_at, latency: int, exact: bool = True
    ) -> SourceRecord:
        if not isinstance(company, dict) or "name" not in company:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                "OpenCorporates company payload missing 'name'",
            )

        jurisdiction = (company.get("jurisdiction_code") or "").upper() or None
        updated_at = parse_dt(company.get("updated_at")) or retrieved_at

        return SourceRecord(
            source_id=self.source_id,
            publisher=self.publisher,
            retrieved_at=retrieved_at,
            source_last_updated_at=retrieved_at,
            registry_last_modified=updated_at,
            name_match_exact=exact,
            legal_name=company.get("name"),
            status=normalise_status(company.get("current_status")),
            registered_date=normalise_date(company.get("incorporation_date")),
            jurisdiction=jurisdiction,
            registration_number=company.get("company_number"),
            raw_payload=company,
            latency_ms=latency,
        )
