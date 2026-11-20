"""Storage engine.

`PostgresStore` is the production implementation: PostgreSQL with indexed JSONB
columns and TTL enforced by the SQL functions in sql/schema.sql.

`MemoryStore` is a test double with identical semantics (including expiry and
summarise-then-purge) so the lifecycle tests can run in CI without a database.
It is refused at startup outside APP_ENV=test — a test double must never be a
production fallback, silent or otherwise.
"""
from __future__ import annotations

import abc
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from app.errors import ErrorCode, PipelineError
from app.models import utcnow


@dataclass
class RawEntry:
    source_id: str
    publisher: str
    fetch_status: str            # 'ok' | 'failed'
    error_code: str | None
    payload: dict[str, Any]
    normalised: dict[str, Any] | None
    latency_ms: int
    retrieved_at: datetime


class Store(abc.ABC):
    @abc.abstractmethod
    async def init(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def record_raw(
        self, *, product_id: str, query_key: str, request_id: str,
        entries: list[RawEntry], ttl_seconds: int,
    ) -> None: ...

    @abc.abstractmethod
    async def record_consensus(
        self, *, product_id: str, query_key: str, request_id: str,
        data: dict, meta: dict, confidence: float, quality_score: float,
        verified: bool, contributing_sources: list[str],
        source_last_updated_at: datetime | None, computed_at: datetime,
        ttl_seconds: int,
    ) -> None: ...

    @abc.abstractmethod
    async def get_latest_consensus(
        self, *, product_id: str, query_key: str
    ) -> dict[str, Any] | None: ...

    @abc.abstractmethod
    async def purge_expired(self) -> dict[str, int]: ...


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


class PostgresStore(Store):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool = None

    async def init(self) -> None:
        try:
            from psycopg_pool import AsyncConnectionPool
        except ImportError as exc:                       # pragma: no cover
            raise PipelineError(
                ErrorCode.STORAGE_UNAVAILABLE,
                "psycopg[binary,pool] is required for PostgresStore",
            ) from exc

        self._pool = AsyncConnectionPool(self._dsn, min_size=1, max_size=10, open=False)
        await self._pool.open(wait=True, timeout=10)
        # Schema is idempotent, so applying it on boot keeps dev and CI honest.
        from pathlib import Path

        ddl = (Path(__file__).resolve().parent.parent / "sql" / "schema.sql").read_text()
        async with self._pool.connection() as conn:
            await conn.execute(ddl)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def record_raw(self, *, product_id, query_key, request_id, entries, ttl_seconds):
        if not entries:
            return
        rows = [
            (
                product_id, query_key, e.source_id, e.publisher, request_id,
                e.fetch_status, e.error_code, json.dumps(e.payload),
                json.dumps(e.normalised) if e.normalised is not None else None,
                e.latency_ms, e.retrieved_at, ttl_seconds,
                e.retrieved_at + timedelta(seconds=ttl_seconds),
            )
            for e in entries
        ]
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO raw_ingest
                      (product_id, query_key, source_id, publisher, request_id,
                       fetch_status, error_code, payload, normalised, latency_ms,
                       retrieved_at, ttl_seconds, expires_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s)
                    """,
                    rows,
                )

    async def record_consensus(
        self, *, product_id, query_key, request_id, data, meta, confidence,
        quality_score, verified, contributing_sources, source_last_updated_at,
        computed_at, ttl_seconds,
    ):
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO consensus_record
                  (product_id, query_key, request_id, data, meta, confidence,
                   quality_score, verified, contributing_sources,
                   source_last_updated_at, computed_at, ttl_seconds, expires_at)
                VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    product_id, query_key, request_id, json.dumps(data),
                    json.dumps(meta), confidence, quality_score, verified,
                    contributing_sources, source_last_updated_at, computed_at,
                    ttl_seconds, computed_at + timedelta(seconds=ttl_seconds),
                ),
            )

    async def get_latest_consensus(self, *, product_id, query_key):
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT data, meta, confidence, quality_score, verified,
                           contributing_sources, source_last_updated_at,
                           computed_at, ttl_seconds, expires_at
                    FROM consensus_record
                    WHERE product_id = %s AND query_key = %s
                    ORDER BY computed_at DESC
                    LIMIT 1
                    """,
                    (product_id, query_key),
                )
                row = await cur.fetchone()
        if row is None:
            return None
        cols = [
            "data", "meta", "confidence", "quality_score", "verified",
            "contributing_sources", "source_last_updated_at", "computed_at",
            "ttl_seconds", "expires_at",
        ]
        return dict(zip(cols, row))

    async def purge_expired(self) -> dict[str, int]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM purge_expired()")
                row = await cur.fetchone()
        return {
            "summarised": row[0] if row else 0,
            "consensus_purged": row[1] if row else 0,
            "raw_purged": row[2] if row else 0,
        }


# ---------------------------------------------------------------------------
# In-memory double (tests only)
# ---------------------------------------------------------------------------


@dataclass
class MemoryStore(Store):
    raw: list[dict] = field(default_factory=list)
    consensus: list[dict] = field(default_factory=list)
    rollups: dict[tuple, dict] = field(default_factory=dict)
    audit: list[dict] = field(default_factory=list)

    async def init(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def record_raw(self, *, product_id, query_key, request_id, entries, ttl_seconds):
        for e in entries:
            self.raw.append(
                {
                    "product_id": product_id, "query_key": query_key,
                    "request_id": request_id, "source_id": e.source_id,
                    "publisher": e.publisher, "fetch_status": e.fetch_status,
                    "error_code": e.error_code, "payload": e.payload,
                    "normalised": e.normalised, "latency_ms": e.latency_ms,
                    "retrieved_at": e.retrieved_at, "ttl_seconds": ttl_seconds,
                    "expires_at": e.retrieved_at + timedelta(seconds=ttl_seconds),
                }
            )

    async def record_consensus(
        self, *, product_id, query_key, request_id, data, meta, confidence,
        quality_score, verified, contributing_sources, source_last_updated_at,
        computed_at, ttl_seconds,
    ):
        self.consensus.append(
            {
                "product_id": product_id, "query_key": query_key,
                "request_id": request_id, "data": data, "meta": meta,
                "confidence": confidence, "quality_score": quality_score,
                "verified": verified, "contributing_sources": contributing_sources,
                "source_last_updated_at": source_last_updated_at,
                "computed_at": computed_at, "ttl_seconds": ttl_seconds,
                "expires_at": computed_at + timedelta(seconds=ttl_seconds),
            }
        )

    async def get_latest_consensus(self, *, product_id, query_key):
        rows = [
            r for r in self.consensus
            if r["product_id"] == product_id and r["query_key"] == query_key
        ]
        return max(rows, key=lambda r: r["computed_at"]) if rows else None

    async def purge_expired(self) -> dict[str, int]:
        now = utcnow()

        expired = [r for r in self.consensus if r["expires_at"] <= now]
        for r in expired:                                    # summarise first
            key = (r["product_id"], r["query_key"], r["computed_at"].date())
            bucket = self.rollups.setdefault(
                key, {"observations": 0, "confidence_sum": 0.0, "modal_status": None}
            )
            bucket["observations"] += 1
            bucket["confidence_sum"] += float(r["confidence"])
            bucket["avg_confidence"] = round(
                bucket["confidence_sum"] / bucket["observations"], 4
            )
            bucket["modal_status"] = r["data"].get("status")

        self.consensus = [r for r in self.consensus if r["expires_at"] > now]
        raw_before = len(self.raw)
        self.raw = [r for r in self.raw if r["expires_at"] > now]

        result = {
            "summarised": len(expired),
            "consensus_purged": len(expired),
            "raw_purged": raw_before - len(self.raw),
        }
        self.audit.append({"ran_at": now, **result})
        return result


def build_store(*, database_url: str | None, app_env: str) -> Store:
    """Pick a store. A missing DSN outside tests is fatal, never a quiet demotion."""
    if database_url:
        return PostgresStore(database_url)
    if app_env == "test":
        return MemoryStore()
    raise PipelineError(
        ErrorCode.STORAGE_UNAVAILABLE,
        "DATABASE_URL is not set. PostgreSQL is required outside APP_ENV=test; "
        "the in-memory store is a test double and will not be used as a fallback.",
    )
