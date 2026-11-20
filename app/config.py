"""Central configuration. Everything is environment-driven; nothing is hardcoded
in the request path so the same image runs in dev, test and production."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv(path: Path | None = None) -> None:
    """Load `KEY=value` lines from a project-root `.env` into the environment.

    Written by hand rather than pulling in python-dotenv: it is ~20 lines, and
    one fewer dependency is one fewer thing that fails to build on a brand-new
    interpreter. A real environment variable always wins, so an operator's
    export or a CI secret is never silently overridden by a checked-out file.
    """
    # Tests must never inherit a developer's local `.env` — a stray
    # DATABASE_URL or APP_ENV in one would make the suite pass or fail
    # depending on whose laptop it runs on.
    if os.getenv("APP_ENV") == "test":
        return

    path = path or Path(__file__).resolve().parent.parent / ".env"
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return                                  # absent or unreadable: fine

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        # Tolerate `KEY="value"` and `KEY='value'`, which editors like to add.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key:
            os.environ.setdefault(key, value)


_load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    # --- product identity (appears verbatim in every response envelope) -----
    product_id: str = "gov.business.registration.v1"
    product_version: str = "1.0.0"
    api_path: str = "/v1/gov/business/registration"

    # --- environment --------------------------------------------------------
    app_env: str = os.getenv("APP_ENV", "dev")          # dev | test | prod
    database_url: str | None = os.getenv("DATABASE_URL")

    # --- freshness / lifecycle ---------------------------------------------
    # Company registers are updated daily, so a 24h TTL on the consensus record
    # is the correct freshness contract for this product.
    consensus_ttl_seconds: int = _int("CONSENSUS_TTL_SECONDS", 86_400)
    raw_ttl_seconds: int = _int("RAW_TTL_SECONDS", 172_800)      # keep raw 2x longer for audit
    purge_interval_seconds: int = _int("PURGE_INTERVAL_SECONDS", 300)

    # Continuous live ingestion (Section 1.1). The poller re-fetches every
    # watched query on this interval, independently of HTTP traffic. Kept well
    # inside Companies House's 600-requests-per-5-minutes budget: 50 watched
    # queries x 1 call each per 15 minutes is ~4% of the allowance.
    refresh_interval_seconds: int = _int("REFRESH_INTERVAL_SECONDS", 900)
    watchlist_max: int = _int("WATCHLIST_MAX", 50)
    watchlist_seed: str | None = os.getenv("WATCHLIST")

    # --- upstream providers -------------------------------------------------
    companies_house_key: str | None = os.getenv("COMPANIES_HOUSE_API_KEY")
    companies_house_base: str = os.getenv(
        "COMPANIES_HOUSE_BASE", "https://api.company-information.service.gov.uk"
    )
    gleif_base: str = os.getenv("GLEIF_BASE", "https://api.gleif.org/api/v1")
    opencorporates_base: str = os.getenv(
        "OPENCORPORATES_BASE", "https://api.opencorporates.com/v0.4"
    )
    opencorporates_key: str | None = os.getenv("OPENCORPORATES_API_KEY")

    upstream_timeout_seconds: float = _float("UPSTREAM_TIMEOUT_SECONDS", 4.0)

    # --- consensus tuning ---------------------------------------------------
    # Weights encode how authoritative a source is for this product.
    # Companies House is the UK statutory register; GLEIF is an independent global
    # entity register; OpenCorporates is a global open registry aggregating national filings.
    source_weights: dict[str, float] = field(
        default_factory=lambda: {
            "SRC-CH-001": 0.45,
            "SRC-GLEIF-001": 0.30,
            "SRC-OC-001": 0.25,
        }
    )
    verified_confidence_threshold: float = _float("VERIFIED_CONFIDENCE_THRESHOLD", 0.75)
    min_sources_for_verified: int = _int("MIN_SOURCES_FOR_VERIFIED", 2)

    # --- serving rate limit (reported in meta.api.rate_limit) --------------
    rate_limit: int = _int("RATE_LIMIT", 100)
    rate_limit_window_seconds: int = _int("RATE_LIMIT_WINDOW_SECONDS", 60)

    @property
    def is_test(self) -> bool:
        return self.app_env == "test"


settings = Settings()
