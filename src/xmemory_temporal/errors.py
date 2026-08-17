"""Translate xmemory API errors into Temporal failures.

Temporal owns retries, not this library (nor the ``xmemory-ai`` client, which
never retries automatically). This is the single place where a client-raised
exception becomes an ``ApplicationError`` carrying a retryability verdict.

Rules:

* Branch on ``.code``, never the bare HTTP status.
* An unrecognized *code* is retryable, never fatal — a stricter reader that
  crashes on a value a newer server emits breaks during rolling deploys.
* A deterministic *client-side* error (a malformed request the server never
  saw) is the opposite: non-retryable, because retrying replays the same bad
  input.
* **Never echo the raw exception string into the failure message.** On a
  transport failure the client embeds the httpx error (internal hostnames,
  ports, URL paths), and Temporal persists that message to cleartext history.
  Emit a fixed per-type message and carry only ``code`` / ``status`` in details.

Note ``402`` means ``QUOTA_EXCEEDED`` only — trials were removed end-to-end and
``xmemory-ai`` dropped ``TRIAL_ENDED`` in 0.11. Do not reintroduce it.
"""

import asyncio
import re
import logging
from datetime import timedelta
from typing import Any

import httpx
from pydantic import ValidationError
from temporalio.exceptions import ApplicationError

logger = logging.getLogger(__name__)

# --- Stable `type=` strings -------------------------------------------------
# A public contract: users match on these in `RetryPolicy`, so renaming one is a
# breaking change. Pinned by literal-value tests, not just via the constants.

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
# Job-level outcomes of a durable write, raised by the poll loop rather than by
# to_application_error. Kept here so every public `type=` string lives together.
TYPE_WRITE_FAILED = "XmemoryWriteFailed"
TYPE_WRITE_NOT_FOUND = "XmemoryWriteNotFound"
TYPE_WRITE_TIMEOUT = "XmemoryWriteTimeout"
TYPE_NOT_BOUND = "XmemoryNotBound"
# Worker-side misconfiguration raised by activities.py, kept distinct from
# NotBound because the remedy differs: one is a missing plugin registration, the
# other an activity scheduled with neither close timeout.
TYPE_NO_DEADLINE = "XmemoryNoDeadline"
# The activity's deadline is already spent. Retryable: Temporal decides whether
# another attempt still fits, and failing here only avoids a doomed request.
TYPE_DEADLINE_EXPIRED = "XmemoryDeadlineExpired"
# Caller-supplied options that cannot be honored, raised before any backend
# call so a rejected option never leaves a queued write behind.
TYPE_BAD_OPTIONS = "XmemoryBadOptions"
# The worker clock disagrees with the service badly enough that the activity's
# remaining time cannot be established. Retryable: another worker with a synced
# clock can run this attempt, and refusing beats guessing.
TYPE_CLOCK_UNUSABLE = "XmemoryClockUnusable"
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
    TYPE_NO_DEADLINE,
    TYPE_BAD_OPTIONS,
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

# --- Server error codes (the xmemory API's `ErrorCode` vocabulary) ----------

_RETRYABLE_CODES = frozenset({"INTERNAL_ERROR", "SERVICE_UNAVAILABLE"})
_AUTH_CODES = frozenset({"UNAUTHORIZED", "FORBIDDEN"})
_BAD_REQUEST_CODES = frozenset({"VALIDATION_ERROR", "INVALID_INPUT", "ALREADY_EXISTS", "CONFLICT"})
# A queued write that exhausted its own retry budget. The server normally reports
# this on write_status, so this mapping is defensive; retrying cannot help.
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

# Everything this module classifies. Only these may reach workflow history: an
# unrecognized code is an unvetted server string, and identifier-shaped is not the
# same as safe -- a value like `Alice_has_HIV` passes any shape test one can write.
_KNOWN_CODES = frozenset(
    {"QUOTA_EXCEEDED", "RATE_LIMITED", "NOT_FOUND"}
    | _RETRYABLE_CODES
    | _AUTH_CODES
    | _BAD_REQUEST_CODES
    | _EXHAUSTED_CODES
    | _SCHEMA_CODES
)

_DAILY_QUOTA_KIND = "daily_quota_exceeded"
_MONTHLY_QUOTA_KIND = "monthly_quota_exceeded"


# Upper bound on a server-supplied retry hint. The value is echoed into
# `next_retry_delay`, so an implausible one would stall the next attempt for its
# full duration; clamp rather than trust it unconditionally.
_MAX_RETRY_DELAY_SECONDS = 3600


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
    if value <= 0:
        return None
    return timedelta(seconds=min(value, _MAX_RETRY_DELAY_SECONDS))


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
        # Known codes only; see _KNOWN_CODES.
        {"code": code if code in _KNOWN_CODES else None, "status": status},
        type=error_type,
        non_retryable=not retryable,
        next_retry_delay=delay if retryable else None,
    )


# A server error code is an identifier, not free text. Anything else is either a
# client bug or something that should not be persisted: `details` goes into
# cleartext workflow history, and an unhashable value (a dict) would crash the
# lookups below outright.
_CODE_SHAPE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def _safe_code(raw: Any) -> str | None:
    """The code if it looks like a code, else ``None`` (with a shape-only log)."""
    if raw is None:
        return None
    if isinstance(raw, str) and _CODE_SHAPE.match(raw):
        return raw
    logger.warning(
        "xmemory returned an error code that is not an identifier (%s, %d chars); "
        "ignoring it for classification and keeping it out of logs and workflow history",
        type(raw).__name__,
        len(raw) if isinstance(raw, (str, bytes)) else -1,
    )
    return None


def to_application_error(exc: BaseException) -> ApplicationError:
    """Map any client-raised exception onto a Temporal ``ApplicationError``."""
    raw_code = getattr(exc, "code", None)
    code = _safe_code(raw_code)
    raw_status = getattr(exc, "status", None)
    # Same reasoning: only a plain integer status is persisted.
    status = raw_status if isinstance(raw_status, int) and not isinstance(raw_status, bool) else None

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
        # Length, not the value: an unrecognized code is an unvetted server string,
        # and worker logs travel. The server's own logs have the value.
        logger.warning(
            "xmemory returned an unrecognized error code (%d chars, HTTP %s); treating it as retryable",
            len(code),
            status,
        )
        error_type, retryable = TYPE_UNKNOWN, True
    elif raw_code is not None:
        # Present but not a usable identifier: treat like an unknown code, and let
        # only the sanitized (absent) value reach history.
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
