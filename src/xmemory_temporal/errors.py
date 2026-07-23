"""Translate xmemory API errors into Temporal failures.

Temporal owns retries, not this library — the guide is explicit, and so is the
``xmemory-ai`` client ("the client never retries automatically"). This is the
single place where a client-raised exception becomes an ``ApplicationError``
carrying a retryability verdict.

Rules:

* Branch on ``.code``, never the bare HTTP status.
* An unrecognized *code* is retryable, never fatal — a stricter reader that
  crashes on a value a newer server emits breaks during rolling deploys
  (``maxims/SERIALIZATION.md``).
* A deterministic *client-side* error (a malformed request the server never
  saw) is the opposite: non-retryable, because retrying replays the same bad
  input.
* **Never echo the raw exception string into the failure message.** For a
  transport failure ``xmemory-ai`` raises ``XmemoryAPIError("Connection error: "
  + str(e))`` with the httpx error embedded — internal hostnames, ports, URL
  paths. Temporal persists the ``ApplicationError`` message to cleartext history
  and renders it in the Web UI without the server's sanitizer, so we emit a
  fixed per-type message and carry only ``code`` / ``status`` in details.

The vocabulary is the server's ``ErrorCode`` enum (``common/dto/response.py``).
Note ``402`` means ``QUOTA_EXCEEDED`` only — trials were removed end-to-end and
``xmemory-ai`` dropped ``TRIAL_ENDED`` from its contract in 0.11. Do not
reintroduce it.
"""

import asyncio
import logging
from datetime import timedelta
from typing import Any

import httpx
from pydantic import ValidationError
from temporalio.exceptions import ApplicationError

logger = logging.getLogger(__name__)

# --- Stable `type=` strings -------------------------------------------------
# A public contract: users write `RetryPolicy(non_retryable_error_types=[...])`
# against these, so renaming one is a breaking change. Pinned by literal-value
# tests, not just round-tripped through the constants.

TYPE_UNAVAILABLE = "XmemoryUnavailable"
TYPE_SERVER_ERROR = "XmemoryServerError"
TYPE_RATE_LIMITED = "XmemoryRateLimited"
TYPE_DAILY_QUOTA_EXCEEDED = "XmemoryDailyQuotaExceeded"
TYPE_MONTHLY_QUOTA_EXCEEDED = "XmemoryMonthlyQuotaExceeded"
TYPE_QUOTA_EXCEEDED = "XmemoryQuotaExceeded"
TYPE_AUTH_FAILED = "XmemoryAuthFailed"
TYPE_NOT_FOUND = "XmemoryNotFound"
TYPE_BAD_REQUEST = "XmemoryBadRequest"
TYPE_SCHEMA_REJECTED = "XmemorySchemaRejected"
# Job-level outcomes of a durable (write_async) write, raised by the poll loop in
# workflow_api.py rather than by to_application_error. Kept here so the full set
# of public `type=` strings lives in one place, is pinned by tests, and is
# documented in the README error table.
TYPE_WRITE_FAILED = "XmemoryWriteFailed"
TYPE_WRITE_NOT_FOUND = "XmemoryWriteNotFound"
TYPE_WRITE_TIMEOUT = "XmemoryWriteTimeout"
TYPE_NOT_BOUND = "XmemoryNotBound"
TYPE_UNKNOWN = "XmemoryUnknown"

NON_RETRYABLE_TYPES: tuple[str, ...] = (
    TYPE_MONTHLY_QUOTA_EXCEEDED,
    TYPE_QUOTA_EXCEEDED,
    TYPE_AUTH_FAILED,
    TYPE_NOT_FOUND,
    TYPE_BAD_REQUEST,
    TYPE_SCHEMA_REJECTED,
    TYPE_WRITE_FAILED,
    TYPE_WRITE_NOT_FOUND,
    TYPE_WRITE_TIMEOUT,
    TYPE_NOT_BOUND,
)

# Fixed, history-safe messages. Never include the raw exception string.
_MESSAGES: dict[str, str] = {
    TYPE_UNAVAILABLE: "xmemory is unreachable",
    TYPE_SERVER_ERROR: "xmemory returned a server error",
    TYPE_RATE_LIMITED: "xmemory rate-limited the request",
    TYPE_DAILY_QUOTA_EXCEEDED: "xmemory daily quota exceeded",
    TYPE_MONTHLY_QUOTA_EXCEEDED: "xmemory monthly quota exceeded",
    TYPE_QUOTA_EXCEEDED: "xmemory quota exceeded",
    TYPE_AUTH_FAILED: "xmemory rejected the credentials",
    TYPE_NOT_FOUND: "xmemory resource not found",
    TYPE_BAD_REQUEST: "xmemory rejected the request as invalid",
    TYPE_SCHEMA_REJECTED: "xmemory rejected the schema change",
    TYPE_UNKNOWN: "xmemory returned an unrecognized error",
}

# --- Server error codes (`common/dto/response.py::ErrorCode`) ---------------

_RETRYABLE_CODES = frozenset({"INTERNAL_ERROR", "SERVICE_UNAVAILABLE"})
_AUTH_CODES = frozenset({"UNAUTHORIZED", "FORBIDDEN"})
_BAD_REQUEST_CODES = frozenset({"VALIDATION_ERROR", "INVALID_INPUT", "ALREADY_EXISTS", "CONFLICT"})
# A queued write that exhausted its own retry budget. The server normally
# surfaces this item-embedded on write_status (handled by the poll loop's FAILED
# branch), not as a request-level code — this mapping is defensive, in case a
# future server returns it request-level. Retrying the request cannot help.
_EXHAUSTED_CODES = frozenset({"MAX_RETRIES_EXCEEDED"})

# Schema-evolution endpoints use lowercase discriminators; none succeed on retry.
_SCHEMA_CODES = frozenset(
    {
        "stale_proposal_version",
        "stale_schema_version",
        "dependency_closure_failed",
        "destructive_confirmation_required",
        "non_additive_change_requires_plan",
        "migration_not_found",
        "instance_not_initialised",
    }
)

_DAILY_QUOTA_KIND = "daily_quota_exceeded"
_MONTHLY_QUOTA_KIND = "monthly_quota_exceeded"


def _retry_delay(exc: Any) -> timedelta | None:
    """Prefer the server's own hint over blind exponential backoff."""
    seconds = getattr(exc, "retry_after", None)
    if seconds is None:
        details = getattr(exc, "details", None) or {}
        if isinstance(details, dict):
            seconds = details.get("retry_after_seconds")
    if seconds is None:
        return None
    try:
        value = int(seconds)
    except (TypeError, ValueError):
        return None
    return timedelta(seconds=value) if value > 0 else None


def _quota_verdict(exc: Any) -> tuple[str, bool]:
    """Split ``QUOTA_EXCEEDED`` by which window was exhausted.

    A daily window resets within hours (worth a durable retry, and the server
    sends ``Retry-After``); a monthly one does not. Absent/unknown kind falls
    back to the client's conservative non-retryable reading.
    """
    details = getattr(exc, "details", None) or {}
    kind = details.get("kind") if isinstance(details, dict) else None
    if kind == _DAILY_QUOTA_KIND:
        return TYPE_DAILY_QUOTA_EXCEEDED, True
    if kind == _MONTHLY_QUOTA_KIND:
        return TYPE_MONTHLY_QUOTA_EXCEEDED, False
    return TYPE_QUOTA_EXCEEDED, False


def _build(
    error_type: str, *, retryable: bool, code: Any = None, status: Any = None, delay: timedelta | None = None
) -> ApplicationError:
    return ApplicationError(
        _MESSAGES.get(error_type, "xmemory request failed"),
        {"code": code, "status": status},
        type=error_type,
        non_retryable=not retryable,
        next_retry_delay=delay if retryable else None,
    )


def to_application_error(exc: BaseException) -> ApplicationError:
    """Map any client-raised exception onto a Temporal ``ApplicationError``."""
    code = getattr(exc, "code", None)
    status = getattr(exc, "status", None)

    # Not an xmemory API error at all: either a transport failure (retryable) or
    # a deterministic client-side error like a malformed read_mode/scope that
    # never reached the server (non-retryable — a retry replays the same input).
    if not _is_api_error(exc):
        if isinstance(exc, (httpx.TransportError, asyncio.TimeoutError, ConnectionError, TimeoutError)):
            return _build(TYPE_UNAVAILABLE, retryable=True)
        if isinstance(exc, (ValidationError, ValueError, TypeError)):
            return _build(TYPE_BAD_REQUEST, retryable=False)
        logger.warning("xmemory raised an unexpected %s; treating as retryable", type(exc).__name__)
        return _build(TYPE_UNKNOWN, retryable=True)

    if code == "QUOTA_EXCEEDED":
        error_type, retryable = _quota_verdict(exc)
    elif code == "RATE_LIMITED":
        error_type, retryable = TYPE_RATE_LIMITED, True
    elif code in _RETRYABLE_CODES:
        error_type, retryable = TYPE_SERVER_ERROR, True
    elif code in _AUTH_CODES:
        error_type, retryable = TYPE_AUTH_FAILED, False
    elif code == "NOT_FOUND":
        error_type, retryable = TYPE_NOT_FOUND, False
    elif code in _BAD_REQUEST_CODES:
        error_type, retryable = TYPE_BAD_REQUEST, False
    elif code in _EXHAUSTED_CODES:
        error_type, retryable = TYPE_WRITE_FAILED, False
    elif code in _SCHEMA_CODES:
        error_type, retryable = TYPE_SCHEMA_REJECTED, False
    elif code is not None:
        logger.warning("xmemory returned an unrecognized error code %r (HTTP %s)", code, status)
        error_type, retryable = TYPE_UNKNOWN, True
    else:
        # An API error with no structured code: a bare HTTP status or a wrapped
        # transport failure.
        error_type, retryable = _verdict_from_status(status)

    return _build(error_type, retryable=retryable, code=code, status=status, delay=_retry_delay(exc))


def _is_api_error(exc: BaseException) -> bool:
    # Match by attribute shape rather than importing the client class, keeping
    # this module dependency-light and tolerant of a fake in tests.
    return hasattr(exc, "code") and hasattr(exc, "status") and hasattr(exc, "retry_after")


def _verdict_from_status(status: int | None) -> tuple[str, bool]:
    if status is None:
        return TYPE_UNAVAILABLE, True
    if status == 408 or status >= 500:
        return TYPE_SERVER_ERROR, True
    if status == 429:
        return TYPE_RATE_LIMITED, True
    if status in (401, 403):
        return TYPE_AUTH_FAILED, False
    if status == 404:
        return TYPE_NOT_FOUND, False
    if 400 <= status < 500:
        return TYPE_BAD_REQUEST, False
    return TYPE_UNKNOWN, True
