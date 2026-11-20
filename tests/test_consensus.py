"""Consensus engine — the heart of requirement 1.2 (cross-source verification)."""
from __future__ import annotations

from app.consensus import compute_consensus
from app.errors import ErrorCode
from tests.conftest import make_record

WEIGHTS = {"SRC-CH-001": 0.6, "SRC-GLEIF-001": 0.4, "SRC-THIRD-001": 0.3}
ALL = {"SRC-CH-001": 0.6, "SRC-GLEIF-001": 0.4}


def run(records, all_weights=None):
    return compute_consensus(
        records,
        weights=WEIGHTS,
        all_configured_weights=all_weights or ALL,
        ttl_seconds=86_400,
        verified_threshold=0.75,
        min_sources_for_verified=2,
    )


def test_full_agreement_is_verified_with_max_confidence():
    result = run([make_record(), make_record(source_id="SRC-GLEIF-001")])

    assert result.confidence == 1.0
    assert result.verified is True
    assert result.data["status"] == "active"
    assert result.data["legal_name"] == "MONZO BANK LIMITED"
    assert len(result.contributing_sources) == 2


def test_name_spelling_differences_still_count_as_agreement():
    """'Monzo Bank Ltd.' and 'MONZO BANK LIMITED' are the same entity."""
    result = run([
        make_record(legal_name="MONZO BANK LIMITED"),
        make_record(source_id="SRC-GLEIF-001", legal_name="Monzo Bank Ltd."),
    ])

    assert result.field_agreement["legal_name"] == 1.0
    # The more authoritative source's spelling is the one served.
    assert result.data["legal_name"] == "MONZO BANK LIMITED"


def test_status_disagreement_lowers_confidence_and_blocks_verification():
    result = run([
        make_record(status="active"),
        make_record(source_id="SRC-GLEIF-001", status="inactive"),
    ])

    assert result.data["status"] == "active"          # heavier source wins
    assert result.field_agreement["status"] == 0.6
    assert result.confidence < 1.0
    assert result.verified is False
    codes = {w.code for w in result.warnings}
    assert ErrorCode.FIELD_DISAGREEMENT in codes


def test_single_source_is_never_verified_and_is_flagged():
    result = run([make_record()])

    assert result.verified is False
    assert result.confidence == 0.6                    # == coverage, full agreement
    codes = {w.code for w in result.warnings}
    assert ErrorCode.SINGLE_SOURCE_DEGRADED in codes


def test_outlier_is_rejected_when_a_majority_exists():
    """With three sources, the lone dissenter is dropped, not averaged in."""
    all_three = {"SRC-CH-001": 0.6, "SRC-GLEIF-001": 0.4, "SRC-THIRD-001": 0.3}
    result = compute_consensus(
        [
            make_record(),
            make_record(source_id="SRC-THIRD-001"),
            make_record(
                source_id="SRC-GLEIF-001",
                legal_name="Totally Different Holdings",
                status="dissolved",
                registered_date="1999-01-01",
                jurisdiction="US",
            ),
        ],
        weights=WEIGHTS,
        all_configured_weights=all_three,
        ttl_seconds=86_400,
        verified_threshold=0.75,
        min_sources_for_verified=2,
    )

    assert "SRC-GLEIF-001" not in result.contributing_sources
    assert result.data["status"] == "active"
    assert result.field_agreement["status"] == 1.0
    assert ErrorCode.OUTLIER_REJECTED in {w.code for w in result.warnings}


def test_two_sources_never_trigger_outlier_rejection():
    """With two sources there is no majority, so neither may be discarded."""
    result = run([
        make_record(status="active"),
        make_record(source_id="SRC-GLEIF-001", status="dissolved",
                    legal_name="Other Co", registered_date="1999-01-01",
                    jurisdiction="US"),
    ])

    assert len(result.contributing_sources) == 2
    assert ErrorCode.OUTLIER_REJECTED not in {w.code for w in result.warnings}


def test_missing_fields_reduce_quality_not_confidence_silently():
    complete = run([make_record(), make_record(source_id="SRC-GLEIF-001")])
    sparse = run([
        make_record(registered_date=None, registration_number=None),
        make_record(source_id="SRC-GLEIF-001", registered_date=None,
                    registration_number=None),
    ])

    assert sparse.quality_score < complete.quality_score
    # A field nobody answered scores zero agreement — it is not assumed correct.
    assert sparse.field_agreement["registered_date"] == 0.0
    assert sparse.confidence < complete.confidence


def test_stale_observations_decay_the_quality_score():
    fresh = run([make_record(age_seconds=60),
                 make_record(source_id="SRC-GLEIF-001", age_seconds=60)])
    old = run([make_record(age_seconds=150_000),
               make_record(source_id="SRC-GLEIF-001", age_seconds=150_000)])

    assert old.quality_score < fresh.quality_score


def test_scores_are_bounded():
    for result in (
        run([make_record()]),
        run([make_record(), make_record(source_id="SRC-GLEIF-001", status="inactive")]),
    ):
        assert 0.0 <= result.confidence <= 1.0
        assert 0.0 <= result.quality_score <= 1.0


# ---------------------------------------------------------------------------
# Identity gate — sources must describe the SAME legal entity
# ---------------------------------------------------------------------------


def test_disagreeing_names_block_verification_even_when_status_agrees():
    """The live failure this guards against: Companies House returned
    'MONZO BANK LIMITED' while GLEIF returned 'MONZO BANK HOLDING GROUP
    LIMITED' — the parent company. Both were 'active', so the old rule marked
    the answer verified. Two different companies must never verify."""
    result = run([
        make_record(legal_name="MONZO BANK LIMITED", registered_date="2015-02-18"),
        make_record(source_id="SRC-GLEIF-001",
                    legal_name="MONZO BANK HOLDING GROUP LIMITED",
                    registered_date="2023-10-13"),
    ])

    assert result.field_agreement["status"] == 1.0      # both say active
    assert result.verified is False                     # but identity is unproven
    assert ErrorCode.IDENTITY_NOT_CONFIRMED in {w.code for w in result.warnings}


def test_an_inexact_name_match_blocks_verification():
    """Even if both sources happen to agree, a source that admits it could not
    find the exact entity must not produce a verified answer."""
    result = run([
        make_record(),
        make_record(source_id="SRC-GLEIF-001", name_match_exact=False),
    ])

    assert result.field_agreement["legal_name"] == 1.0
    assert result.verified is False
    codes = {w.code for w in result.warnings}
    assert ErrorCode.NAME_MATCH_INEXACT in codes


def test_the_inexact_warning_names_the_source_and_what_it_returned():
    result = run([
        make_record(),
        make_record(source_id="SRC-GLEIF-001",
                    legal_name="MONZO BANK HOLDING GROUP LIMITED",
                    name_match_exact=False),
    ])

    warning = next(w for w in result.warnings if w.code is ErrorCode.NAME_MATCH_INEXACT)
    assert warning.source_id == "SRC-GLEIF-001"
    assert "MONZO BANK HOLDING GROUP LIMITED" in warning.message


def test_exact_matching_names_still_verify_normally():
    """The gate must not break the happy path."""
    result = run([
        make_record(legal_name="MONZO BANK LIMITED"),
        make_record(source_id="SRC-GLEIF-001", legal_name="Monzo Bank Ltd."),
    ])

    assert result.verified is True
    assert not [w for w in result.warnings
                if w.code in (ErrorCode.NAME_MATCH_INEXACT,
                              ErrorCode.IDENTITY_NOT_CONFIRMED)]


def test_pakistan_dual_source_consensus_verifies():
    """Catalog #84 requirement: Pakistan (PK) companies achieve genuine multi-source
    consensus via GLEIF and OpenCorporates."""
    pk_weights = {"SRC-GLEIF-001": 0.30, "SRC-OC-001": 0.25}
    result = compute_consensus(
        [
            make_record(
                source_id="SRC-GLEIF-001",
                publisher="GLEIF Global LEI Index",
                legal_name="HABIB BANK LIMITED",
                status="active",
                registered_date="1947-08-25",
                jurisdiction="PK",
                registration_number="0000001",
            ),
            make_record(
                source_id="SRC-OC-001",
                publisher="OpenCorporates",
                legal_name="Habib Bank Ltd.",
                status="active",
                registered_date="1947-08-25",
                jurisdiction="PK",
                registration_number="0000001",
            ),
        ],
        weights=pk_weights,
        all_configured_weights=pk_weights,
        ttl_seconds=86_400,
        verified_threshold=0.75,
        min_sources_for_verified=2,
    )

    assert result.verified is True
    assert result.confidence == 1.0
    assert result.data["jurisdiction"] == "PK"
    assert result.data["legal_name"] == "HABIB BANK LIMITED"
    assert set(result.contributing_sources) == {"SRC-GLEIF-001", "SRC-OC-001"}


def test_three_source_consensus_rejects_outlier():
    """When three providers respond (CH, GLEIF, OC), a rogue dissenter is dropped."""
    three_weights = {"SRC-CH-001": 0.45, "SRC-GLEIF-001": 0.30, "SRC-OC-001": 0.25}
    result = compute_consensus(
        [
            make_record(source_id="SRC-CH-001", legal_name="BARCLAYS BANK UK PLC", status="active"),
            make_record(source_id="SRC-GLEIF-001", legal_name="Barclays Bank UK Plc", status="active"),
            make_record(
                source_id="SRC-OC-001",
                legal_name="Completely Wrong Company Inc",
                status="dissolved",
                registered_date="1900-01-01",
                jurisdiction="US",
            ),
        ],
        weights=three_weights,
        all_configured_weights=three_weights,
        ttl_seconds=86_400,
        verified_threshold=0.75,
        min_sources_for_verified=2,
    )

    assert "SRC-OC-001" not in result.contributing_sources
    assert result.data["status"] == "active"
    assert result.verified is True
    assert ErrorCode.OUTLIER_REJECTED in {w.code for w in result.warnings}

