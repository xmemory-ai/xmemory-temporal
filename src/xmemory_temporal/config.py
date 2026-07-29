"""Worker-side configuration.

Carries the *name* of the env var holding the API key, never the key, so the
config is safe to log, serialize, and persist into Temporal history.
"""

import os
from datetime import timedelta

from pydantic import BaseModel, ConfigDict

DEFAULT_API_KEY_ENV = "XMEM_API_KEY"
DEFAULT_CLIENT_MARGIN_SECONDS = 5


def client_timeout_seconds(
    activity_seconds: float,
    margin_seconds: int = DEFAULT_CLIENT_MARGIN_SECONDS,
) -> float:
    """Client budget for an activity whose Temporal deadline is ``activity_seconds``.

    Always strictly below that deadline, so the client fails first with an
    attributable xmemory error. Budgets at or under the margin get a
    proportional one, so the ordering holds for every positive budget.
    """
    if activity_seconds <= margin_seconds:
        return max(0.1, activity_seconds * 0.8)
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
    # Gap between a call's Temporal deadline and its client timeout. If inverted,
    # Temporal could abandon a write_start whose POST still enqueues server-side,
    # and a later durable-write retry would double-enqueue.
    client_margin_seconds: int = DEFAULT_CLIENT_MARGIN_SECONDS
    default_extraction_logic: str = "fast"
    # Summaries are visible to anyone with namespace access, and memory text is
    # often personal, so content is redacted unless a caller opts in.
    include_content_in_summary: bool = False

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
