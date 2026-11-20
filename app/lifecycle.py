"""Background lifecycle worker.

Runs `purge_expired()` on a fixed interval: summarise expired consensus rows
into daily rollups, then delete them and any expired raw payloads. Failures are
logged, never swallowed into silence, and never abort the loop — a purge that
dies quietly is how a "TTL" becomes decoration.
"""
from __future__ import annotations

import asyncio
import logging

from app.storage import Store

log = logging.getLogger("lifecycle")


class LifecycleWorker:
    def __init__(self, store: Store, interval_seconds: int) -> None:
        self.store = store
        self.interval = interval_seconds
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.last_result: dict[str, int] | None = None
        self.runs = 0
        self.failures = 0

    async def run_once(self) -> dict[str, int]:
        result = await self.store.purge_expired()
        self.last_result = result
        self.runs += 1
        if any(result.values()):
            log.info(
                "ttl purge: summarised=%s consensus_purged=%s raw_purged=%s",
                result["summarised"], result["consensus_purged"], result["raw_purged"],
            )
        return result

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception:                                # noqa: BLE001
                self.failures += 1
                log.exception("ttl purge failed; retrying next interval")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                continue

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop(), name="ttl-purge")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):      # noqa: BLE001
                pass
            self._task = None
