"""Cross-source consensus engine.

Given N independent observations of the same registration fact, produce one
single source of truth plus an honest confidence and quality score.

Method — weighted field-level majority vote with outlier rejection:

1.  Each field is voted on independently. A source only votes on fields it
    actually populated, so a sparse source cannot drag a field toward null.
2.  Votes are weighted by source authority (see `Settings.source_weights`).
    The statutory register outranks a mirror.
3.  Agreement on a field = winning weight / total weight that voted on it.
4.  A source that dissents on most of the fields it voted on is rejected as an
    outlier and the vote is recomputed without it — but only when at least
    three sources responded, because with two there is no majority to appeal to
    and rejecting one would be an arbitrary coin flip.
5.  Confidence = field-importance-weighted agreement x source coverage.
    Coverage is the share of total configured authority that actually answered,
    so losing a provider lowers confidence mechanically rather than silently.
6.  Every disagreement and every rejection is emitted as a warning. Nothing is
    smoothed over.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Iterable

from app.errors import ErrorCode, Severity, Warning_
from app.models import ConsensusResult, SourceRecord, utcnow
from app.sources.base import normalise_name

# How much each field contributes to the headline confidence number.
FIELD_IMPORTANCE: dict[str, float] = {
    "status": 0.35,
    "legal_name": 0.30,
    "registered_date": 0.20,
    "jurisdiction": 0.15,
}

# Comparison keys: how two values from different registers are judged "equal".
COMPARATORS: dict[str, Callable[[Any], Any]] = {
    "status": lambda v: v,
    "legal_name": normalise_name,
    "registered_date": lambda v: v,
    "jurisdiction": lambda v: (v or "").upper(),
    "registration_number": lambda v: str(v or "").upper().lstrip("0"),
}

OUTLIER_DISSENT_THRESHOLD = 0.6
MIN_SOURCES_FOR_OUTLIER_REJECTION = 3


def _vote(
    records: Iterable[SourceRecord], field: str, weights: dict[str, float]
) -> tuple[Any, float, dict[Any, float]]:
    """Return (winning_raw_value, agreement_ratio, tally_by_comparison_key)."""
    key_fn = COMPARATORS[field]
    tally: dict[Any, float] = {}
    representative: dict[Any, Any] = {}

    for rec in records:
        value = getattr(rec, field)
        if value in (None, "", "unknown"):
            continue
        k = key_fn(value)
        if k in ("", None):
            continue
        w = weights.get(rec.source_id, 0.1)
        tally[k] = tally.get(k, 0.0) + w
        # First writer wins the surface form, and sources are ordered by
        # authority, so the most authoritative spelling is the one served.
        representative.setdefault(k, value)

    if not tally:
        return None, 0.0, {}

    total = sum(tally.values())
    winner_key = max(tally, key=lambda k: tally[k])
    return representative[winner_key], tally[winner_key] / total, tally


def _dissent_score(
    rec: SourceRecord, winners: dict[str, Any]
) -> float:
    """Fraction of the fields this source voted on where it lost."""
    voted = lost = 0
    for field, winning in winners.items():
        value = getattr(rec, field)
        if value in (None, "", "unknown") or winning is None:
            continue
        voted += 1
        if COMPARATORS[field](value) != COMPARATORS[field](winning):
            lost += 1
    return (lost / voted) if voted else 0.0


def _freshness_factor(records: list[SourceRecord], ttl_seconds: int, now: datetime) -> float:
    """Decay quality as the oldest contributing observation ages out."""
    stamps = [r.source_last_updated_at or r.retrieved_at for r in records]
    if not stamps:
        return 0.3
    age = (now - min(stamps)).total_seconds()
    return max(0.3, min(1.0, 1.0 - age / (2 * max(ttl_seconds, 1))))


def compute_consensus(
    records: list[SourceRecord],
    *,
    weights: dict[str, float],
    all_configured_weights: dict[str, float],
    ttl_seconds: int,
    verified_threshold: float,
    min_sources_for_verified: int,
    now: datetime | None = None,
) -> ConsensusResult:
    """Fuse N source records into one answer. `records` must be non-empty."""
    if not records:
        raise ValueError("compute_consensus requires at least one source record")

    now = now or utcnow()
    warnings: list[Warning_] = []

    # Most authoritative first, so ties resolve toward the statutory register.
    records = sorted(records, key=lambda r: weights.get(r.source_id, 0.0), reverse=True)

    fields = list(FIELD_IMPORTANCE) + ["registration_number"]

    def run_vote(recs: list[SourceRecord]):
        winners, agreements, tallies = {}, {}, {}
        for f in fields:
            value, agreement, tally = _vote(recs, f, weights)
            winners[f], agreements[f], tallies[f] = value, agreement, tally
        return winners, agreements, tallies

    winners, agreements, tallies = run_vote(records)

    # --- step 4: outlier rejection ----------------------------------------
    accepted = records
    if len(records) >= MIN_SOURCES_FOR_OUTLIER_REJECTION:
        outliers = [
            r for r in records
            if _dissent_score(r, {f: winners[f] for f in FIELD_IMPORTANCE})
            > OUTLIER_DISSENT_THRESHOLD
        ]
        if outliers and len(records) - len(outliers) >= 2:
            for o in outliers:
                warnings.append(
                    Warning_(
                        code=ErrorCode.OUTLIER_REJECTED,
                        severity=Severity.WARNING,
                        message=(
                            f"{o.publisher} disagreed with the majority on most "
                            f"fields and was excluded from the consensus"
                        ),
                        source_id=o.source_id,
                    )
                )
            accepted = [r for r in records if r not in outliers]
            winners, agreements, tallies = run_vote(accepted)

    # --- step 3 disclosure: report every remaining disagreement -----------
    for f in FIELD_IMPORTANCE:
        if len(tallies.get(f, {})) > 1:
            warnings.append(
                Warning_(
                    code=ErrorCode.FIELD_DISAGREEMENT,
                    severity=Severity.WARNING,
                    message=(
                        f"Sources disagree on '{f}'; served the value carrying "
                        f"{agreements[f]:.0%} of source weight"
                    ),
                    detail={
                        "field": f,
                        "agreement": round(agreements[f], 4),
                        "candidates": {str(k): round(v, 4) for k, v in tallies[f].items()},
                    },
                )
            )

    # --- step 5: confidence ------------------------------------------------
    # Fields nobody could answer score 0, which correctly drags confidence down.
    base_agreement = sum(
        FIELD_IMPORTANCE[f] * agreements.get(f, 0.0) for f in FIELD_IMPORTANCE
    ) / sum(FIELD_IMPORTANCE.values())

    total_authority = sum(all_configured_weights.values()) or 1.0
    coverage = sum(weights.get(r.source_id, 0.0) for r in accepted) / total_authority
    confidence = round(base_agreement * coverage, 4)

    # --- quality: completeness x freshness ---------------------------------
    weight_sum = sum(weights.get(r.source_id, 0.1) for r in accepted) or 1.0
    completeness = sum(
        r.completeness() * weights.get(r.source_id, 0.1) for r in accepted
    ) / weight_sum
    quality_score = round(completeness * _freshness_factor(accepted, ttl_seconds, now), 4)

    # --- identity gate ------------------------------------------------------
    # Everything above compares *attributes*. None of it is meaningful unless
    # the sources are describing the SAME legal entity. Searching a registry by
    # name can return a near neighbour — a parent company, a renamed successor,
    # a similarly-named firm — and two different companies both being "active"
    # would otherwise sail through as verified. So identity is a precondition,
    # not just another weighted field.
    inexact = [r for r in accepted if not r.name_match_exact]
    for r in inexact:
        warnings.append(
            Warning_(
                code=ErrorCode.NAME_MATCH_INEXACT,
                severity=Severity.WARNING,
                message=(
                    f"{r.publisher} returned {r.legal_name!r}, which is not an exact "
                    f"match for the name requested; it may be a different legal entity"
                ),
                source_id=r.source_id,
            )
        )

    identity_confirmed = agreements.get("legal_name", 0.0) == 1.0 and not inexact
    if not identity_confirmed and len(accepted) >= min_sources_for_verified:
        warnings.append(
            Warning_(
                code=ErrorCode.IDENTITY_NOT_CONFIRMED,
                severity=Severity.WARNING,
                message=(
                    "Sources could not be confirmed to describe the same legal "
                    "entity, so this answer is not marked verified regardless of "
                    "how well its other fields agree"
                ),
            )
        )

    verified = (
        confidence >= verified_threshold
        and len(accepted) >= min_sources_for_verified
        and agreements.get("status", 0.0) == 1.0
        and identity_confirmed
    )

    if len(accepted) < min_sources_for_verified:
        warnings.append(
            Warning_(
                code=ErrorCode.SINGLE_SOURCE_DEGRADED,
                severity=Severity.WARNING,
                message=(
                    f"Only {len(accepted)} of {len(all_configured_weights)} configured "
                    f"sources contributed; result is unverified and confidence is "
                    f"reduced to reflect the missing corroboration"
                ),
            )
        )

    last_updated = [r.source_last_updated_at for r in accepted if r.source_last_updated_at]

    data = {
        "legal_name": winners.get("legal_name"),
        "registration_number": winners.get("registration_number"),
        "status": winners.get("status") or "unknown",
        "registered_date": winners.get("registered_date"),
        "jurisdiction": winners.get("jurisdiction"),
    }

    return ConsensusResult(
        data=data,
        confidence=confidence,
        quality_score=quality_score,
        verified=verified,
        contributing_sources=[r.source_id for r in accepted],
        field_agreement={f: round(agreements.get(f, 0.0), 4) for f in fields},
        warnings=warnings,
        source_last_updated_at=min(last_updated) if last_updated else None,
    )
