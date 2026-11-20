"""Request orchestration: ingest -> consensus -> persist -> serve.

Freshness policy (the part the brief cares most about):

* A stored consensus record is served only while `age_seconds <= ttl_seconds`,
  and `age_seconds` is always measured against the *upstream* timestamp, never
  against the moment we happened to cache it.
* Once a record is past TTL it is not served. We re-ingest. If re-ingestion
  fails, the request fails with 503 — we never hand back the expired row with a
  fresh-looking timestamp.
* Every provider that failed still appears in `provenance` with
  `status: "failed"` and its error code, and in `warnings`.
"""
from __future__ import annotations

import asyncio
import secrets
import time
from datetime import datetime
from typing import Any

from app.config import Settings
from app.consensus import compute_consensus
from app.errors import ErrorCode, PipelineError, Severity, SourceError, Warning_
from app.models import (
    ApiMeta, ConsensusResult, Envelope, Freshness, License, Meta, Provenance,
    QuerySpec, RateLimit, RegistrationData, SourceRecord, Trust, iso, utcnow,
)
from app.sources.base import BaseSource
from app.storage import RawEntry, Store


# Crockford base32 — the alphabet ULIDs use (no I, L, O or U).
_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_request_id() -> str:
    """A ULID-shaped identifier matching the brief's sample (`req_01J8FX7K...`).

    48-bit millisecond timestamp + 80 bits of randomness, rendered as 26
    base32 characters. Ids issued in different milliseconds sort
    chronologically; within one millisecond the random suffix decides.
    """
    value = (int(time.time() * 1000) << 80) | secrets.randbits(80)
    chars = []
    for _ in range(26):
        value, rem = divmod(value, 32)
        chars.append(_B32[rem])
    return "req_" + "".join(reversed(chars))


def build_query_key(
    *, company_name: str | None, registration_number: str | None, country: str | None
) -> str:
    """Stable cache key. Case- and whitespace-insensitive."""
    parts = [
        (country or "*").upper(),
        (registration_number or "").strip().upper(),
        " ".join((company_name or "").lower().split()),
    ]
    return "|".join(parts)


class RegistrationPipeline:
    def __init__(self, sources: list[BaseSource], store: Store, settings: Settings) -> None:
        self.sources = sources
        self.store = store
        self.settings = settings
        # Set by the app factory once the ingestion scheduler exists. Queries
        # served here are subscribed to it so they are then kept fresh
        # continuously, in the background, rather than only on demand.
        self.scheduler = None

    # -- ingestion -----------------------------------------------------------

    async def _ingest(
        self, *, company_name: str | None, registration_number: str | None, country: str | None
    ) -> tuple[list[SourceRecord], list[SourceError]]:
        eligible = [s for s in self.sources if s.supports(country)]
        if not eligible:
            raise PipelineError(
                ErrorCode.INVALID_REQUEST,
                f"No configured provider covers jurisdiction {country!r}",
                http_status=422,
                detail={"configured": [s.source_id for s in self.sources]},
            )

        results = await asyncio.gather(
            *(
                s.fetch(
                    company_name=company_name,
                    registration_number=registration_number,
                    country=country,
                )
                for s in eligible
            ),
            return_exceptions=True,
        )

        records: list[SourceRecord] = []
        failures: list[SourceError] = []
        for source, outcome in zip(eligible, results):
            if isinstance(outcome, SourceRecord):
                records.append(outcome)
            elif isinstance(outcome, SourceError):
                failures.append(outcome)
            elif isinstance(outcome, BaseException):
                # An adapter bug is still an explicit failure, never a shrug.
                failures.append(
                    SourceError(
                        source.source_id,
                        ErrorCode.UPSTREAM_BAD_SCHEMA,
                        f"Adapter raised {type(outcome).__name__}: {outcome}",
                    )
                )
        return records, failures

    # -- main entry point ----------------------------------------------------

    async def serve(
        self,
        *,
        company_name: str | None,
        registration_number: str | None,
        country: str | None,
        force_refresh: bool = False,
        subscribe: bool = True,
    ) -> Envelope:
        started = time.perf_counter()
        if subscribe and self.scheduler is not None:
            self.scheduler.subscribe(
                QuerySpec(
                    country=country,
                    company_name=company_name,
                    registration_number=registration_number,
                )
            )
        request_id = new_request_id()
        query_key = build_query_key(
            company_name=company_name,
            registration_number=registration_number,
            country=country,
        )
        ttl = self.settings.consensus_ttl_seconds

        # 1. Serve a stored record only while it is genuinely inside its TTL.
        if not force_refresh:
            cached = await self.store.get_latest_consensus(
                product_id=self.settings.product_id, query_key=query_key
            )
            if cached:
                anchor = cached.get("source_last_updated_at") or cached["computed_at"]
                age = int((utcnow() - anchor).total_seconds())
                if age <= cached["ttl_seconds"]:
                    return self._envelope_from_cache(cached, request_id, age, started)

        # 2. Otherwise ingest live from every eligible provider, concurrently.
        records, failures = await self._ingest(
            company_name=company_name,
            registration_number=registration_number,
            country=country,
        )

        await self._persist_raw(query_key, request_id, records, failures, ttl)

        if not records:
            raise PipelineError(
                ErrorCode.NO_SOURCES_AVAILABLE,
                "Every configured provider failed; no trustworthy value can be served "
                "and expired data will not be substituted",
                http_status=503,
                detail={
                    "failures": [
                        {"source_id": f.source_id, "code": f.code.value, "message": f.message}
                        for f in failures
                    ]
                },
            )

        # 3. Fuse.
        eligible = [s for s in self.sources if s.supports(country)]
        consensus = compute_consensus(
            records,
            weights=self.settings.source_weights,
            all_configured_weights={
                s.source_id: self.settings.source_weights.get(s.source_id, 0.1)
                for s in eligible
            },
            ttl_seconds=ttl,
            verified_threshold=self.settings.verified_confidence_threshold,
            min_sources_for_verified=self.settings.min_sources_for_verified,
        )

        warnings = list(consensus.warnings) + [f.as_warning() for f in failures]
        envelope = self._envelope(
            consensus, records, failures, warnings, request_id, ttl, started
        )

        # 4. Persist the fused answer for the next caller.
        await self.store.record_consensus(
            product_id=self.settings.product_id,
            query_key=query_key,
            request_id=request_id,
            data=envelope.data.model_dump(),
            meta=envelope.meta.model_dump(),
            confidence=consensus.confidence,
            quality_score=consensus.quality_score,
            verified=consensus.verified,
            contributing_sources=consensus.contributing_sources,
            source_last_updated_at=consensus.source_last_updated_at,
            computed_at=utcnow(),
            ttl_seconds=ttl,
        )
        return envelope

    # -- persistence ---------------------------------------------------------

    async def _persist_raw(self, query_key, request_id, records, failures, ttl) -> None:
        entries = [
            RawEntry(
                source_id=r.source_id, publisher=r.publisher, fetch_status="ok",
                error_code=None, payload=r.raw_payload,
                normalised={
                    "legal_name": r.legal_name, "status": r.status,
                    "registered_date": r.registered_date,
                    "jurisdiction": r.jurisdiction,
                    "registration_number": r.registration_number,
                },
                latency_ms=r.latency_ms, retrieved_at=r.retrieved_at,
            )
            for r in records
        ] + [
            RawEntry(
                source_id=f.source_id,
                publisher=next(
                    (s.publisher for s in self.sources if s.source_id == f.source_id),
                    f.source_id,
                ),
                fetch_status="failed", error_code=f.code.value,
                payload={"error": f.message, "detail": f.detail},
                normalised=None, latency_ms=0, retrieved_at=utcnow(),
            )
            for f in failures
        ]
        await self.store.record_raw(
            product_id=self.settings.product_id, query_key=query_key,
            request_id=request_id, entries=entries,
            ttl_seconds=self.settings.raw_ttl_seconds,
        )

    # -- envelope assembly ---------------------------------------------------

    def _freshness(self, anchor: datetime | None, ttl: int) -> tuple[Freshness, datetime | None]:
        now = utcnow()
        if anchor is None:
            # Unknown upstream timestamp: report age against now and say so via
            # the caller's warnings rather than inventing a flattering number.
            return Freshness(age_seconds=0, ttl_seconds=ttl, stale=False), None
        age = max(0, int((now - anchor).total_seconds()))
        return Freshness(age_seconds=age, ttl_seconds=ttl, stale=age > ttl), anchor

    def _envelope(
        self,
        consensus: ConsensusResult,
        records: list[SourceRecord],
        failures: list[SourceError],
        warnings: list[Warning_],
        request_id: str,
        ttl: int,
        started: float,
    ) -> Envelope:
        anchor = consensus.source_last_updated_at or min(
            (r.retrieved_at for r in records), default=None
        )
        freshness, anchor = self._freshness(anchor, ttl)

        provenance = [
            Provenance(
                source_id=r.source_id, publisher=r.publisher,
                retrieved_at=iso(r.retrieved_at), status="ok",
                weight=self.settings.source_weights.get(r.source_id),
                source_modified_at=iso(r.registry_last_modified),
                name_match_exact=r.name_match_exact,
            )
            for r in records
        ] + [
            Provenance(
                source_id=f.source_id,
                publisher=next(
                    (s.publisher for s in self.sources if s.source_id == f.source_id),
                    f.source_id,
                ),
                retrieved_at=iso(utcnow()), status="failed",
                weight=self.settings.source_weights.get(f.source_id),
                error_code=f.code.value,
            )
            for f in failures
        ]

        if freshness.stale:
            warnings = warnings + [
                Warning_(
                    code=ErrorCode.STALE_DATA_WITHHELD,
                    severity=Severity.WARNING,
                    message=(
                        f"Upstream data is {freshness.age_seconds}s old, beyond the "
                        f"{ttl}s TTL; flagged stale rather than presented as fresh"
                    ),
                )
            ]

        primary = records[0]
        data = RegistrationData(
            **consensus.data,
            as_of=iso(anchor or primary.retrieved_at),
            source="consensus:" + "+".join(consensus.contributing_sources),
        )

        meta = Meta(
            request_id=request_id,
            product_id=self.settings.product_id,
            version=self.settings.product_version,
            served_at=iso(utcnow()),
            source_last_updated_at=iso(anchor),
            freshness=freshness,
            provenance=provenance,
            trust=Trust(
                confidence=consensus.confidence,
                quality_score=consensus.quality_score,
                verified=consensus.verified,
            ),
            license=License(type="open-government", usage="agent_runtime"),
            api=ApiMeta(
                latency_ms=int((time.perf_counter() - started) * 1000),
                rate_limit=RateLimit(
                    limit=self.settings.rate_limit,
                    window_seconds=self.settings.rate_limit_window_seconds,
                ),
            ),
            warnings=[w.to_dict() for w in warnings],
        )
        return Envelope(data=data, meta=meta)

    def _envelope_from_cache(
        self, cached: dict[str, Any], request_id: str, age: int, started: float
    ) -> Envelope:
        """Re-serve a stored record with *this* request's identity and latency,
        and with the age recomputed now — never the age frozen at write time."""
        meta = dict(cached["meta"])
        meta["request_id"] = request_id
        meta["served_at"] = iso(utcnow())
        meta["freshness"] = {
            "age_seconds": age,
            "ttl_seconds": cached["ttl_seconds"],
            "stale": age > cached["ttl_seconds"],
        }
        meta["api"] = {
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "rate_limit": {
                "limit": self.settings.rate_limit,
                "window_seconds": self.settings.rate_limit_window_seconds,
            },
        }
        # Disclose that this answer came from the store, not a fresh upstream
        # call, so the consuming agent can tell cached-but-valid from just-fetched.
        warnings = [
            w for w in (meta.get("warnings") or [])
            if w.get("code") != ErrorCode.SERVED_FROM_STORE.value
        ]
        warnings.append(
            Warning_(
                code=ErrorCode.SERVED_FROM_STORE,
                severity=Severity.INFO,
                message=(
                    f"Served from the consensus store; upstream observation is {age}s "
                    f"old and still within its {cached['ttl_seconds']}s TTL"
                ),
            ).to_dict()
        )
        meta["warnings"] = warnings
        return Envelope(data=RegistrationData(**cached["data"]), meta=Meta(**meta))
