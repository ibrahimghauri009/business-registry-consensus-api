#!/usr/bin/env python3
"""Live smoke test — hits the real Companies House, GLEIF, and OpenCorporates APIs.

Unlike the pytest suite (which is fully mocked and needs no network), this
script proves the adapters work against the actual providers. Use it once
before the demo, and any time an upstream schema might have shifted.

Showcase queries:
    python scripts/smoke_live.py                             # Full consensus: verified=True
    python scripts/smoke_live.py "Monzo Bank Limited"        # Identity gate demo: verified=False
    python scripts/smoke_live.py "Habib Bank Limited" --country PK  # Pakistan multi-source consensus
    python scripts/smoke_live.py --number 09446231 --country GB    # By registration number

Requires COMPANIES_HOUSE_API_KEY for full UK consensus. Without it the run still
succeeds on GLEIF and OpenCorporates and shows degradation gracefully.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

# Force UTF-8 stream handling on Windows to avoid charmap encoding errors
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

from app.config import settings  # noqa: E402
from app.pipeline import RegistrationPipeline  # noqa: E402
from app.sources.companies_house import CompaniesHouseSource  # noqa: E402
from app.sources.gleif import GleifSource  # noqa: E402
from app.sources.opencorporates import OpenCorporatesSource  # noqa: E402
from app.storage import MemoryStore  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


def banner(text: str) -> None:
    print(f"\n{DIM}{'-' * 68}{RESET}\n{text}\n{DIM}{'-' * 68}{RESET}")


async def probe_sources(sources, name, number, country):
    """Call each provider individually so failures are attributable."""
    banner("1. Per-source probe")
    for src in sources:
        if not src.supports(country):
            print(f"  {YELLOW}skip{RESET}  {src.source_id:<16} does not cover {country}")
            continue
        try:
            rec = await src.fetch(
                company_name=name, registration_number=number, country=country
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  {RED}fail{RESET}  {src.source_id:<16} {exc}")
        else:
            print(
                f"  {GREEN}ok{RESET}    {src.source_id:<16} "
                f"{rec.legal_name!r} status={rec.status} "
                f"registered={rec.registered_date} ({rec.latency_ms} ms)"
            )


async def main() -> int:
    parser = argparse.ArgumentParser(description="Live provider smoke test")
    parser.add_argument("company_name", nargs="?", default="Barclays Bank UK PLC")
    parser.add_argument("--number", default=None, help="registration number instead of a name")
    parser.add_argument("--country", default="GB")
    args = parser.parse_args()

    name = None if args.number else args.company_name
    print(f"{DIM}query: name={name!r} number={args.number!r} country={args.country}{RESET}")

    if not settings.companies_house_key and args.country in ("GB", "UK"):
        print(
            f"{YELLOW}COMPANIES_HOUSE_API_KEY is not set — running without statutory UK register.{RESET}"
        )

    async with httpx.AsyncClient(
        headers={"User-Agent": "AI4012-registration-agent/1.0"}
    ) as client:
        sources = [
            CompaniesHouseSource(
                client, settings.upstream_timeout_seconds,
                base=settings.companies_house_base,
                api_key=settings.companies_house_key,
            ),
            GleifSource(client, settings.upstream_timeout_seconds, base=settings.gleif_base),
            OpenCorporatesSource(
                client, settings.upstream_timeout_seconds,
                base=settings.opencorporates_base,
                api_key=settings.opencorporates_key,
            ),
        ]

        await probe_sources(sources, name, args.number, args.country)

        banner("2. Full pipeline (consensus + envelope)")
        pipeline = RegistrationPipeline(sources, MemoryStore(), settings)
        try:
            envelope = await pipeline.serve(
                company_name=name,
                registration_number=args.number,
                country=args.country,
                force_refresh=True,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  {RED}pipeline failed:{RESET} {exc}")
            return 1

        print(json.dumps(envelope.model_dump(), indent=2))

        banner("3. Verdict")
        trust = envelope.meta.trust
        ok_sources = [p for p in envelope.meta.provenance if p.status == "ok"]
        print(f"  sources responding : {len(ok_sources)}/{len(envelope.meta.provenance)}")
        print(f"  confidence         : {trust.confidence}")
        print(f"  quality_score      : {trust.quality_score}")
        print(f"  verified           : {trust.verified}")
        print(f"  age / ttl          : {envelope.meta.freshness.age_seconds}s / "
              f"{envelope.meta.freshness.ttl_seconds}s "
              f"(stale={envelope.meta.freshness.stale})")
        print(f"  latency            : {envelope.meta.api.latency_ms} ms")
        for w in envelope.meta.warnings:
            print(f"  {YELLOW}warning{RESET}          : {w['code']} - {w['message']}")

        return 0 if ok_sources else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
