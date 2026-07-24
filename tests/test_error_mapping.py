"""The error-classification table — the artifact Temporal's review asks for.

Each case asserts the ``type=`` string and the retryability verdict, driven by
constructing an ``XmemoryAPIError`` exactly as the client would. The unknown-code
row is the important one: a code we do not recognize must stay retryable and must
not raise (``maxims/SERIALIZATION.md``).
"""

from datetime import timedelta

import pytest

from xmemory_temporal import errors
from xmemory_temporal.errors import to_application_error

from .fakes import api_error

# (label, kwargs to api_error, expected type, expected non_retryable)
CASES = [
    ("500", dict(status=500), errors.TYPE_SERVER_ERROR, False),
    ("503_status", dict(status=503), errors.TYPE_SERVER_ERROR, False),
    ("408", dict(status=408), errors.TYPE_SERVER_ERROR, False),
    ("transport_no_status", dict(status=None), errors.TYPE_UNAVAILABLE, False),
    ("internal_error_code", dict(status=500, code="INTERNAL_ERROR"), errors.TYPE_SERVER_ERROR, False),
    ("service_unavailable_code", dict(status=503, code="SERVICE_UNAVAILABLE"), errors.TYPE_SERVER_ERROR, False),
    ("rate_limited", dict(status=429, code="RATE_LIMITED"), errors.TYPE_RATE_LIMITED, False),
    (
        "quota_daily",
        dict(status=402, code="QUOTA_EXCEEDED", details={"kind": "daily_quota_exceeded"}),
        errors.TYPE_DAILY_QUOTA_EXCEEDED,
        False,
    ),
    (
        "quota_monthly",
        dict(status=402, code="QUOTA_EXCEEDED", details={"kind": "monthly_quota_exceeded"}),
        errors.TYPE_MONTHLY_QUOTA_EXCEEDED,
        True,
    ),
    ("quota_no_kind", dict(status=402, code="QUOTA_EXCEEDED"), errors.TYPE_QUOTA_EXCEEDED, True),
    ("unauthorized", dict(status=401, code="UNAUTHORIZED"), errors.TYPE_AUTH_FAILED, True),
    ("forbidden", dict(status=403, code="FORBIDDEN"), errors.TYPE_AUTH_FAILED, True),
    ("not_found", dict(status=404, code="NOT_FOUND"), errors.TYPE_NOT_FOUND, True),
    ("validation", dict(status=422, code="VALIDATION_ERROR"), errors.TYPE_BAD_REQUEST, True),
    ("conflict", dict(status=409, code="CONFLICT"), errors.TYPE_BAD_REQUEST, True),
    (
        "schema_rejected",
        dict(status=409, code="destructive_confirmation_required"),
        errors.TYPE_SCHEMA_REJECTED,
        True,
    ),
    ("unknown_code", dict(status=418, code="SOMETHING_NEW"), errors.TYPE_UNKNOWN, False),
]


@pytest.mark.parametrize("label,kwargs,expected_type,expected_non_retryable", CASES, ids=[c[0] for c in CASES])
def test_error_mapping(label: str, kwargs: dict, expected_type: str, expected_non_retryable: bool) -> None:
    app_err = to_application_error(api_error(**kwargs))
    assert app_err.type == expected_type, label
    assert app_err.non_retryable is expected_non_retryable, label


def test_unknown_code_does_not_raise() -> None:
    # The whole point: a stricter reader must not crash on a newer server's code.
    app_err = to_application_error(api_error(status=418, code="BRAND_NEW_CODE"))
    assert app_err.type == errors.TYPE_UNKNOWN
    assert app_err.non_retryable is False


def test_retry_after_becomes_next_retry_delay() -> None:
    app_err = to_application_error(api_error(status=429, code="RATE_LIMITED", retry_after=7))
    assert app_err.next_retry_delay == timedelta(seconds=7)


def test_retry_after_from_details() -> None:
    err = api_error(
        status=402, code="QUOTA_EXCEEDED", details={"kind": "daily_quota_exceeded", "retry_after_seconds": 30}
    )
    app_err = to_application_error(err)
    assert app_err.type == errors.TYPE_DAILY_QUOTA_EXCEEDED
    assert app_err.next_retry_delay == timedelta(seconds=30)


def test_non_retryable_never_carries_delay() -> None:
    app_err = to_application_error(
        api_error(
            status=402, code="QUOTA_EXCEEDED", details={"kind": "monthly_quota_exceeded", "retry_after_seconds": 30}
        )
    )
    assert app_err.non_retryable is True
    assert app_err.next_retry_delay is None


def test_non_retryable_types_listing_matches_behavior() -> None:
    # NON_RETRYABLE_TYPES is a public contract; keep it honest against the table.
    seen_non_retryable = {to_application_error(api_error(**kwargs)).type for _, kwargs, _, nr in CASES if nr}
    for t in seen_non_retryable:
        assert t in errors.NON_RETRYABLE_TYPES, t


def test_type_string_literals_are_pinned() -> None:
    # A rename must fail HERE, not silently pass because the CASES table builds
    # expected values from the same constants. These strings are a public
    # RetryPolicy contract.
    assert errors.TYPE_UNAVAILABLE == "XmemoryUnavailable"
    assert errors.TYPE_SERVER_ERROR == "XmemoryServerError"
    assert errors.TYPE_RATE_LIMITED == "XmemoryRateLimited"
    assert errors.TYPE_DAILY_QUOTA_EXCEEDED == "XmemoryDailyQuotaExceeded"
    assert errors.TYPE_MONTHLY_QUOTA_EXCEEDED == "XmemoryMonthlyQuotaExceeded"
    assert errors.TYPE_QUOTA_EXCEEDED == "XmemoryQuotaExceeded"
    assert errors.TYPE_AUTH_FAILED == "XmemoryAuthFailed"
    assert errors.TYPE_NOT_FOUND == "XmemoryNotFound"
    assert errors.TYPE_BAD_REQUEST == "XmemoryBadRequest"
    assert errors.TYPE_SCHEMA_REJECTED == "XmemorySchemaRejected"
    assert errors.TYPE_WRITE_FAILED == "XmemoryWriteFailed"
    assert errors.TYPE_WRITE_NOT_FOUND == "XmemoryWriteNotFound"
    assert errors.TYPE_WRITE_TIMEOUT == "XmemoryWriteTimeout"
    assert errors.TYPE_NOT_BOUND == "XmemoryNotBound"
    assert errors.TYPE_UNKNOWN == "XmemoryUnknown"


def test_transport_string_is_not_leaked_into_history() -> None:
    # F3: a transport failure carries the raw httpx string (internal hostnames /
    # ports / URL paths) in its message. The mapped ApplicationError, persisted
    # to cleartext Temporal history, must NOT echo it.
    from xmemory._exceptions import XmemoryAPIError

    leaky = XmemoryAPIError(
        "Connection error: HTTPSConnectionPool(host='internal-db.local', port=5432): timed out",
        status=None,
        code=None,
    )
    app = to_application_error(leaky)
    assert app.type == errors.TYPE_UNAVAILABLE
    assert "internal-db.local" not in str(app)
    assert "5432" not in str(app)


def test_max_retries_exceeded_is_non_retryable() -> None:
    app = to_application_error(api_error(status=500, code="MAX_RETRIES_EXCEEDED"))
    assert app.type == errors.TYPE_WRITE_FAILED
    assert app.non_retryable is True


def test_client_side_input_error_fails_fast() -> None:
    # F11(2): a deterministic client-side error (e.g. a malformed read_mode/scope
    # rejected while building the request) is not an API error and must be
    # non-retryable — a retry replays the same bad input.
    app = to_application_error(ValueError("bad scope"))
    assert app.type == errors.TYPE_BAD_REQUEST
    assert app.non_retryable is True


def test_transport_exception_object_is_retryable() -> None:
    import httpx

    app = to_application_error(httpx.ConnectError("boom"))
    assert app.type == errors.TYPE_UNAVAILABLE
    assert app.non_retryable is False
