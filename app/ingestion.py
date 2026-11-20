"""Continuous live ingestion.

Section 1.1 of the brief requires micro-data to be fetched **continuously**, not
only when a caller happens to ask. This worker is that continuous half of the
pipeline: it holds a watchlist of query specifications and re-ingests every one
of them from all providers on a fixed interval, independently of HTTP traffic.

Two things go on the watchlist:

* **Seeds** from the `WATCHLIST` environment variable, so the service starts
  polling from boot with no traffic at all.
* **Anything an agent has actually asked for.** The first request for a company
  subscribes it, so from then on that record is kept fresh in the background and
  subsequent requests hit a warm, in-TTL consensus record instead of paying for
  a cold multi-provider fetch.

The watchlist is bounded (`WATCHLIST_MAX`, least-recently-requested evicted
first) so an agent probing thousands of company names cannot turn the poller
into an unbounded crawler against Companies House's 600-per-5-minutes budget.

Failures are counted and logged per cycle, never swallowed: a refresh that
cannot reach consensus simply leaves the previous record to expire on its own
TTL, which is what makes the staleness flag downstream truthful.
"""
from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING

from app.models import QuerySpec

if TYPE_CHECKING:                                        # pragma: no cover
    from app.pipeline import RegistrationPipeline

log = logging.getLogger("ingestion")


def parse_watchlist(raw: str | None) -> list[QuerySpec]:
    """Parse `WATCHLIST` — semicolon-separated `country|name|number` entries.

        WATCHLIST="GB|Monzo Bank Limited|;GB||09446231"
    """
    specs: list[QuerySpec] = []
    for entry in (raw or "").split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = [p.strip() or None for p in entry.split("|")]
        parts += [None] * (3 - len(parts))
        country, name, number = parts[:3]
        if name or number:
            specs.append(QuerySpec(country=country, company_name=name,
                                   registration_number=number))
    return specs


class IngestionScheduler:
    """Re-ingests every watched query on a fixed interval."""

    def __init__(
        self,
        pipeline: "RegistrationPipeline",
        interval_seconds: float,
        *,
        max_entries: int = 50,
        seeds: list[QuerySpec] | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.interval = interval_seconds
        self.max_entries = max_entries
        self._watchlist: OrderedDict[QuerySpec, None] = OrderedDict()
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

        self.cycles = 0
        self.refreshed = 0
        self.failed = 0
        self.last_cycle: dict[str, int] | None = None

        for spec in seeds or []:
            self.subscribe(spec)

    # -- watchlist -----------------------------------------------------------

    def subscribe(self, spec: QuerySpec) -> None:
        """Track a query, evicting the least-recently-requested when full."""
        if spec in self._watchlist:
            self._watchlist.move_to_end(spec)
            return
        self._watchlist[spec] = None
        while len(self._watchlist) > self.max_entries:
            evicted, _ = self._watchlist.popitem(last=False)
            log.info("watchlist full; stopped polling %s", evicted)

    @property
    def watchlist(self) -> list[QuerySpec]:
        return list(self._watchlist)

    # -- polling -------------------------------------------------------------

    async def run_once(self) -> dict[str, int]:
        """One full refresh sweep. Never raises for an individual query."""
        refreshed = failed = 0
        for spec in self.watchlist:
            try:
                await self.pipeline.serve(
                    company_name=spec.company_name,
                    registration_number=spec.registration_number,
                    country=spec.country,
                    force_refresh=True,
                    subscribe=False,           # polling must not re-subscribe
                )
            except Exception as exc:                      # noqa: BLE001
                failed += 1
                # Logged, counted, and left to expire on its own TTL — the record
                # is never re-stamped as fresh just because a refresh was tried.
                log.warning("refresh failed for %s: %s", spec, exc)
            else:
                refreshed += 1

        self.cycles += 1
        self.refreshed += refreshed
        self.failed += failed
        self.last_cycle = {
            "watched": len(self._watchlist),
            "refreshed": refreshed,
            "failed": failed,
        }
        if self._watchlist:
            log.info("ingest cycle: watched=%d refreshed=%d failed=%d",
                     len(self._watchlist), refreshed, failed)
        return self.last_cycle

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                return                                   # stop requested
            except asyncio.TimeoutError:
                pass
            try:
                await self.run_once()
            except Exception:                             # noqa: BLE001
                log.exception("ingest cycle aborted; retrying next interval")

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop(), name="live-ingest")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):    # noqa: BLE001
                pass
            self._task = None
