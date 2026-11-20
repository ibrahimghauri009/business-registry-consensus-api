"""Canonical wire contract + internal record types.

The `Envelope` model below is a literal transcription of the schema in Section 3
of the assignment. Serving through a Pydantic model (rather than hand-built
dicts) means a field that drifts from spec fails loudly at serialisation time.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


# --------------------------------------------------------------------------
# Internal types (never serialised directly to the client)
# --------------------------------------------------------------------------

RegistrationStatus = Literal["active", "inactive", "dissolved", "unknown"]


@dataclass(frozen=True)
class QuerySpec:
    """One watched question. Hashable, so it can key the ingestion watchlist."""

    country: str | None
    company_name: str | None
    registration_number: str | None

    def __str__(self) -> str:
        return (f"{self.country or '*'}:"
                f"{self.registration_number or self.company_name}")


@dataclass
class SourceRecord:
    """One provider's normalised answer for one query."""

    source_id: str
    publisher: str
    retrieved_at: datetime
    source_last_updated_at: datetime | None
    legal_name: str | None
    status: RegistrationStatus
    registered_date: str | None          # ISO date, YYYY-MM-DD
    jurisdiction: str | None             # ISO-3166 alpha-2 where available
    registration_number: str | None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    latency_ms: int = 0

    # When the registry itself last edited its row. Reported for transparency,
    # but deliberately NOT the freshness clock: a company whose details have not
    # changed since 2023 is not stale data, it is stable data. `freshness` must
    # answer "how long since we confirmed this", which is `retrieved_at`.
    registry_last_modified: datetime | None = None

    # False when the registry's best hit was not the entity we asked for.
    name_match_exact: bool = True

    def completeness(self) -> float:
        """Fraction of the five business fields this source actually filled."""
        fields = [
            self.legal_name,
            self.status if self.status != "unknown" else None,
            self.registered_date,
            self.jurisdiction,
            self.registration_number,
        ]
        return sum(1 for f in fields if f) / len(fields)


@dataclass
class ConsensusResult:
    data: dict[str, Any]
    confidence: float
    quality_score: float
    verified: bool
    contributing_sources: list[str]
    field_agreement: dict[str, float]
    warnings: list[Any] = field(default_factory=list)
    source_last_updated_at: datetime | None = None


# --------------------------------------------------------------------------
# Wire contract (Section 3 of the brief)
# --------------------------------------------------------------------------


class RegistrationData(BaseModel):
    legal_name: str | None
    registration_number: str | None
    status: str
    registered_date: str | None
    jurisdiction: str | None
    as_of: str
    source: str


class Freshness(BaseModel):
    age_seconds: int
    ttl_seconds: int
    stale: bool


class Provenance(BaseModel):
    source_id: str
    publisher: str
    retrieved_at: str
    status: Literal["ok", "failed"] = "ok"
    weight: float | None = None
    error_code: str | None = None
    # When the registry itself last edited the row, where it publishes that.
    # Informational: `freshness` is measured from `retrieved_at`, not this.
    source_modified_at: str | None = None
    # False when this registry's best hit was not the entity that was asked for.
    name_match_exact: bool | None = None


class Trust(BaseModel):
    confidence: float
    quality_score: float
    verified: bool


class License(BaseModel):
    type: str
    usage: str


class RateLimit(BaseModel):
    limit: int
    window_seconds: int


class ApiMeta(BaseModel):
    latency_ms: int
    rate_limit: RateLimit


class Meta(BaseModel):
    request_id: str
    product_id: str
    version: str
    served_at: str
    source_last_updated_at: str | None
    freshness: Freshness
    provenance: list[Provenance]
    trust: Trust
    license: License
    api: ApiMeta
    warnings: list[dict[str, Any]] = Field(default_factory=list)


class Envelope(BaseModel):
    data: RegistrationData
    meta: Meta


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None) -> str | None:
    """RFC-3339 with a trailing Z, exactly as the spec samples are written."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
