"""Worker-side configuration.

Carries the *name* of the env var holding the API key, never the key, so the
config is safe to log, serialize, and persist into Temporal history.
"""

import os
from datetime import timedelta
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict

DEFAULT_API_KEY_ENV = "XMEM_API_KEY"
# The xmemory client reads this when no url is passed, so it is an endpoint source
# this plugin has to validate rather than let through unchecked.
_URL_ENV = "XMEM_API_URL"
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
DEFAULT_CLIENT_MARGIN_SECONDS = 5


def client_timeout_seconds(
    activity_seconds: float,
    margin_seconds: int = DEFAULT_CLIENT_MARGIN_SECONDS,
) -> float:
    """Client budget for an activity whose Temporal deadline is ``activity_seconds``.

    Always strictly below that deadline, so the client fails first with an
    attributable xmemory error. A budget at or under the margin gets a
    proportional one instead of the fixed one, and a nonsensical margin (zero,
    negative) is ignored rather than inverting the ordering it exists to keep.
    """
    if activity_seconds <= 0:
        raise ValueError("activity_seconds must be positive")
    if margin_seconds <= 0 or activity_seconds <= margin_seconds:
        return activity_seconds * 0.8
    return float(activity_seconds - margin_seconds)


class XmemoryTimeouts(BaseModel):
    """Default ``start_to_close`` budgets applied by ``xmemory_for_workflow()``.

    The workflow owns the real budget; activities derive their client timeout
    from whatever Temporal assigned.
    """

    model_config = ConfigDict(frozen=True)

    read_seconds: int = 120
    write_seconds: int = 180
    write_start_seconds: int = 30
    write_status_seconds: int = 30

    @property
    def read(self) -> timedelta:
        return timedelta(seconds=self.read_seconds)

    @property
    def write(self) -> timedelta:
        return timedelta(seconds=self.write_seconds)

    @property
    def write_start(self) -> timedelta:
        return timedelta(seconds=self.write_start_seconds)

    @property
    def write_status(self) -> timedelta:
        return timedelta(seconds=self.write_status_seconds)


class XmemoryConfig(BaseModel):
    """Worker-side configuration for the xmemory plugin.

    No credential: the key is read from ``os.environ[api_key_env]``, or passed
    in-process via ``XmemoryPlugin(config, api_key=...)``. Activity budgets are
    not here either; they belong to ``xmemory_for_workflow``.
    """

    model_config = ConfigDict(frozen=True)

    instance_id: str
    url: str | None = None
    api_key_env: str = DEFAULT_API_KEY_ENV
    # Gap between a call's Temporal deadline and its client timeout. Inverted,
    # Temporal could abandon a write_start whose POST still enqueues server-side.
    client_margin_seconds: int = DEFAULT_CLIENT_MARGIN_SECONDS
    default_extraction_logic: str = "fast"
    # Treat an unmeasurable pre-interceptor time as zero instead of refusing the
    # activity. Only correct where the service clock is synthetic: the time-skipping
    # test server advances it past the worker's, which looks exactly like a worker
    # clock that is genuinely behind. Leave it off in production. See TESTING.md.
    allow_unmeasurable_clock: bool = False
    # Log the server's `error_detail` verbatim when a write fails. Off by default:
    # it can echo memory text or internal endpoints. Off, the log names the failed
    # write and the detail's size.
    log_server_error_detail: bool = False

    def resolve_url(self) -> str | None:
        """Validate the *effective* endpoint, or ``None`` for the default.

        The key travels as a bearer token, so the endpoint decides who receives
        it. Validating only ``config.url`` was not enough: with it unset the
        xmemory client falls back to ``XMEM_API_URL``, which would then reach the
        wire unchecked. Resolving that fallback here and returning it explicitly
        makes this the only path to an endpoint.
        """
        if self.url is not None:
            return validate_endpoint(self.url, source="xmemory url")
        from_env = os.environ.get(_URL_ENV)
        if from_env is None:
            return None
        return validate_endpoint(from_env, source=f"${_URL_ENV}")

    def resolve_api_key(self) -> str:
        """Read the API key from the environment.

        Raises at worker start rather than on the first activity, so a
        misconfigured worker fails visibly.
        """
        key = os.environ.get(self.api_key_env)
        if not key:
            raise ValueError(
                f"xmemory API key not found: environment variable {self.api_key_env!r} is unset or empty. "
                f"Set it on the worker process, or pass XmemoryPlugin(config, api_key=...)."
            )
        return key


def validate_endpoint(candidate: str, *, source: str) -> str:
    """Return ``candidate`` if the API key may safely be sent to it.

    Plaintext is refused off-box because the key is a bearer token, and a scheme
    the client cannot speak is refused outright rather than at the first request.
    """
    if not candidate.strip():
        raise ValueError(f"{source} was supplied but empty; unset it to use the default endpoint")
    parsed = urlparse(candidate)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError(f"{source} is not a valid URL: {candidate!r}")
    if parsed.username or parsed.password:
        raise ValueError(f"{source} must not embed credentials; the API key is passed separately")
    if parsed.scheme not in ("https", "http"):
        raise ValueError(f"{source} must use https (got scheme {parsed.scheme!r})")
    if parsed.scheme == "http" and parsed.hostname not in _LOOPBACK_HOSTS:
        raise ValueError(
            f"{source} must use https (got {candidate!r}); the API key is sent as a bearer token. "
            f"Plaintext http is accepted only for loopback hosts."
        )
    return candidate
