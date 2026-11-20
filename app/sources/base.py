"""Provider adapter interface.

Every upstream provider implements `fetch()` and either returns a normalised
`SourceRecord` or raises `SourceError`. Adapters never return `None`, never
return a partially-invented record, and never catch their own errors — that is
what makes "no silent fallbacks" enforceable at the orchestrator level.
"""
from __future__ import annotations

import abc
import re
import time
from datetime import datetime, timezone

import httpx

from app.errors import ErrorCode, SourceError
from app.models import RegistrationStatus, SourceRecord

# Legal-form suffixes stripped before comparing names across registers.
_SUFFIXES = {
    "ltd", "limited", "plc", "llp", "llc", "inc", "incorporated", "corp",
    "corporation", "co", "company", "gmbh", "ag", "sa", "nv", "bv", "pvt",
    "private", "pte", "oy", "ab", "as", "aps", "spa", "srl",
}


def normalise_name(name: str | None) -> str:
    """Canonical form used for cross-source name comparison.

    'Monzo Bank Limited' and 'MONZO BANK LTD.' both collapse to 'monzo bank',
    so two registers spelling the same entity differently still agree.
    """
    if not name:
        return ""
    cleaned = re.sub(r"[^\w\s]", " ", name.lower())
    tokens = [t for t in cleaned.split() if t and t not in _SUFFIXES]
    return " ".join(tokens)


def pick_best_name_match(
    candidates: list, query_name: str | None, name_of
) -> tuple[object, bool]:
    """Choose the candidate whose legal name best matches what was asked for.

    A registry name search returns *near* matches, and the top hit is often a
    different legal entity — searching "Monzo Bank Limited" on GLEIF returns
    "MONZO BANK HOLDING GROUP LIMITED", the parent company, incorporated eight
    years later. Taking `candidates[0]` blindly therefore silently answers a
    question nobody asked.

    Preference order:
      1. exact match on the normalised name  (the entity that was asked for)
      2. a candidate whose normalised name starts with the query's
      3. the registry's own top hit, reported as *not* an exact match

    Returns `(candidate, exact)`. When `exact` is False the caller still gets a
    usable record — suppressing it entirely would hide evidence — but the
    consensus engine refuses to mark such an answer `verified`.
    """
    if not candidates:
        raise ValueError("pick_best_name_match requires at least one candidate")

    target = normalise_name(query_name)
    if not target:
        return candidates[0], True          # searched by number, not by name

    scored = [(c, normalise_name(name_of(c))) for c in candidates]

    for candidate, name in scored:
        if name and name == target:
            return candidate, True

    for candidate, name in scored:
        if name and (name.startswith(target) or target.startswith(name)):
            return candidate, False

    return candidates[0], False


def normalise_status(raw: str | None) -> RegistrationStatus:
    """Map each provider's status vocabulary onto one shared enum."""
    if not raw:
        return "unknown"
    v = raw.strip().lower().replace("_", "-")
    if v in {"active", "issued", "registered", "open", "live"}:
        return "active"
    if v in {"dissolved", "closed", "converted-closed", "removed"}:
        return "dissolved"
    if v in {
        "inactive", "lapsed", "retired", "annulled", "merged", "duplicate",
        "liquidation", "receivership", "administration", "insolvency-proceedings",
    }:
        return "inactive"
    return "unknown"


def normalise_date(raw: str | None) -> str | None:
    """Return a bare ISO date (YYYY-MM-DD) or None. Never guesses."""
    if not raw:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(raw))
    return m.group(0) if m else None


def parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return None


class BaseSource(abc.ABC):
    source_id: str
    publisher: str
    supports_jurisdictions: set[str] | None = None   # None == global

    def __init__(self, client: httpx.AsyncClient, timeout: float) -> None:
        self._client = client
        self._timeout = timeout

    def supports(self, country: str | None) -> bool:
        if self.supports_jurisdictions is None:
            return True
        if not country:
            return True
        return country.upper() in self.supports_jurisdictions

    @abc.abstractmethod
    async def fetch(
        self, *, company_name: str | None, registration_number: str | None, country: str | None
    ) -> SourceRecord:
        """Return a normalised record or raise SourceError. Never returns None."""

    # -- shared HTTP plumbing ------------------------------------------------

    async def _get(self, url: str, **kwargs) -> tuple[dict, int]:
        """GET with uniform error translation. Returns (json_body, latency_ms)."""
        started = time.perf_counter()
        try:
            resp = await self._client.get(url, timeout=self._timeout, **kwargs)
        except httpx.TimeoutException as exc:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_TIMEOUT,
                f"{self.publisher} did not respond within {self._timeout}s",
                detail={"url": url, "exception": type(exc).__name__},
            ) from exc
        except httpx.HTTPError as exc:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_UNAVAILABLE,
                f"{self.publisher} transport failure",
                detail={"url": url, "exception": type(exc).__name__},
            ) from exc

        latency_ms = int((time.perf_counter() - started) * 1000)

        if resp.status_code == 429:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_RATE_LIMITED,
                f"{self.publisher} rate limit exceeded",
                detail={"retry_after": resp.headers.get("retry-after")},
            )
        if resp.status_code in (401, 403):
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_AUTH_FAILED,
                f"{self.publisher} rejected our credentials ({resp.status_code})",
            )
        if resp.status_code == 404:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_NOT_FOUND,
                f"{self.publisher} has no record for this query",
            )
        if resp.status_code >= 500:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_UNAVAILABLE,
                f"{self.publisher} returned {resp.status_code}",
            )
        if resp.status_code >= 400:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                f"{self.publisher} rejected the request ({resp.status_code})",
            )

        try:
            return resp.json(), latency_ms
        except ValueError as exc:
            raise SourceError(
                self.source_id,
                ErrorCode.UPSTREAM_BAD_SCHEMA,
                f"{self.publisher} returned a non-JSON body",
            ) from exc
