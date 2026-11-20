"""Provider B — GLEIF Global LEI Index.

An independent, keyless, global register of legal entities. Because GLEIF data
is supplied by Local Operating Units mirroring national registers, it is a
genuinely *independent* observation of the same fact, which is what makes
cross-source consensus meaningful rather than decorative.

Docs: https://api.gleif.org/api/v1  (JSON:API; values live under
data[].attributes.entity / .registration)
"""
from __future__ import annotations

import httpx

from app.errors import ErrorCode, SourceError
from app.models import SourceRecord, utcnow
from app.sources.base import (
    BaseSource, normalise_date, normalise_status, parse_dt, pick_best_name_match,
)


class GleifSource(BaseSource):
    source_id = "SRC-GLEIF-001"
    publisher = "GLEIF Global LEI Index"
    supports_jurisdictions = None   # global coverage

    def __init__(self, client: httpx.AsyncClient, timeout: float, *, base: str):
        super().__init__(client, timeout)
        self._base = base.rstrip("/")

    async def fetch(
        self, *, company_name: str | None, registration_number: str | None, country: str | None
    ) -> SourceRecord:
        retrieved_at = utcnow()
        params: dict[str, str | int] = {"page[size]": 5, "page[number]": 1}

        if registration_number:
            params["filter[entity.registeredAs]"] = registration_number
        else:
            params["filter[entity.legalName]"] = company_name or ""
        if country:
            params["filter[entity.legalAddress.country]"] = country.upper()

        body, latency = await self._get(f"{self._base}/lei-records", params=params)

        records = body.get("data") if isinstance(body, dict) else None
        if records is None:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                "GLEIF response has no 'data' array",
            )
        if not records:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_NOT_FOUND,
                f"No LEI record matched {company_name or registration_number!r}",
            )

        # GLEIF's legalName filter matches loosely: searching "Monzo Bank
        # Limited" returns the parent "MONZO BANK HOLDING GROUP LIMITED" first.
        # Pick the record that actually matches, and flag it when none does.
        record, exact = pick_best_name_match(
            records,
            company_name,
            lambda r: (((r.get("attributes") or {}).get("entity") or {})
                       .get("legalName") or {}).get("name"),
        )
        return self._normalise(record, retrieved_at, latency, exact)

    def _normalise(
        self, record: dict, retrieved_at, latency: int, name_match_exact: bool = True
    ) -> SourceRecord:
        attrs = (record or {}).get("attributes")
        if not isinstance(attrs, dict):
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                "LEI record is missing 'attributes'",
            )

        entity = attrs.get("entity") or {}
        registration = attrs.get("registration") or {}
        legal_name = ((entity.get("legalName") or {}).get("name")) or None
        country = ((entity.get("legalAddress") or {}).get("country")) or entity.get(
            "jurisdiction"
        )

        # Prefer entity status; fall back to LEI registration status only when the
        # entity status is absent — and that substitution is visible in raw_payload.
        status = normalise_status(entity.get("status") or registration.get("status"))

        return SourceRecord(
            source_id=self.source_id,
            publisher=self.publisher,
            retrieved_at=retrieved_at,
            # The freshness clock is when WE confirmed the fact, not when GLEIF
            # last edited the row. An LEI record is typically touched once a
            # year at renewal, so anchoring freshness to `lastUpdateDate` would
            # mark every healthy company permanently stale. That date is still
            # reported, as `registry_last_modified`.
            source_last_updated_at=retrieved_at,
            registry_last_modified=parse_dt(registration.get("lastUpdateDate")),
            name_match_exact=name_match_exact,
            legal_name=legal_name,
            status=status,
            registered_date=normalise_date(registration.get("initialRegistrationDate")),
            jurisdiction=(country or None),
            registration_number=entity.get("registeredAs") or attrs.get("lei"),
            raw_payload=record,
            latency_ms=latency,
        )
