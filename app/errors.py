"""Structured error taxonomy.

Design rule for this assignment: **no silent fallbacks**. Every failure mode is
a named, typed object that travels all the way to the caller — either as a
`warnings[]` entry (partial degradation, request still served) or as an HTTP
error envelope (nothing trustworthy to serve). Nothing is swallowed, and stale
data is never dressed up as fresh.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from enum import Enum


class ErrorCode(str, Enum):
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
    UPSTREAM_RATE_LIMITED = "UPSTREAM_RATE_LIMITED"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    UPSTREAM_AUTH_FAILED = "UPSTREAM_AUTH_FAILED"
    UPSTREAM_BAD_SCHEMA = "UPSTREAM_BAD_SCHEMA"
    UPSTREAM_NOT_FOUND = "UPSTREAM_NOT_FOUND"
    SOURCE_NOT_CONFIGURED = "SOURCE_NOT_CONFIGURED"

    NO_SOURCES_AVAILABLE = "NO_SOURCES_AVAILABLE"
    NO_CONSENSUS = "NO_CONSENSUS"
    SINGLE_SOURCE_DEGRADED = "SINGLE_SOURCE_DEGRADED"
    SERVED_FROM_STORE = "SERVED_FROM_STORE"
    STALE_DATA_WITHHELD = "STALE_DATA_WITHHELD"
    FIELD_DISAGREEMENT = "FIELD_DISAGREEMENT"
    OUTLIER_REJECTED = "OUTLIER_REJECTED"
    NAME_MATCH_INEXACT = "NAME_MATCH_INEXACT"
    IDENTITY_NOT_CONFIRMED = "IDENTITY_NOT_CONFIRMED"

    STORAGE_UNAVAILABLE = "STORAGE_UNAVAILABLE"
    INVALID_REQUEST = "INVALID_REQUEST"
    RATE_LIMITED = "RATE_LIMITED"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class Warning_:
    """A non-fatal, fully-disclosed degradation. Serialised into meta.warnings."""

    code: ErrorCode
    severity: Severity
    message: str
    source_id: str | None = None
    detail: dict | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["code"] = self.code.value
        d["severity"] = self.severity.value
        return {k: v for k, v in d.items() if v is not None}


class PipelineError(Exception):
    """Fatal: there is nothing trustworthy to serve. Becomes an HTTP error body."""

    http_status = 500

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        http_status: int | None = None,
        detail: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}
        if http_status is not None:
            self.http_status = http_status

    def to_envelope(self, request_id: str, product_id: str) -> dict:
        return {
            "error": {
                "code": self.code.value,
                "message": self.message,
                "detail": self.detail,
            },
            "meta": {"request_id": request_id, "product_id": product_id},
        }


class SourceError(Exception):
    """Raised inside a provider adapter. Caught by the orchestrator, recorded as
    provenance + a warning, and counted against the confidence score."""

    def __init__(
        self,
        source_id: str,
        code: ErrorCode,
        message: str,
        *,
        detail: dict | None = None,
    ) -> None:
        super().__init__(f"[{source_id}] {code.value}: {message}")
        self.source_id = source_id
        self.code = code
        self.message = message
        self.detail = detail or {}

    def as_warning(self) -> Warning_:
        return Warning_(
            code=self.code,
            severity=Severity.ERROR,
            message=self.message,
            source_id=self.source_id,
            detail=self.detail or None,
        )
