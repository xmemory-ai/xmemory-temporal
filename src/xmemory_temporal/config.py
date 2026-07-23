"""Configuration for the xmemory Temporal plugin.

Nothing in this module ever carries secret material. ``XmemoryConfig`` holds the
*name* of the environment variable that supplies the API key, never the key
itself, so the config stays safe to log, to serialize, and — should a caller
pass it as an activity argument — to persist into Temporal workflow history,
which is stored in the clear.
"""

import os
from datetime import timedelta

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_API_KEY_ENV = "XMEM_API_KEY"


class XmemoryTimeouts(BaseModel):
    """Per-activity ``start_to_close`` budgets, in seconds.

    The guide is explicit that a tool declaration must always specify a
    ``start_to_close_timeout``, because LLMs will not set one. These are the
    defaults every ``WorkflowXmemory`` call
    applies; users override per call.
    """

    model_config = ConfigDict(frozen=True)

    read_seconds: int = 120
    write_seconds: int = 180
    write_start_seconds: int = 30
    write_status_seconds: int = 30
    # The client (httpx) timeout for a call is its activity's start_to_close
    # MINUS this margin, so the client always gives up first and the failure is
    # an attributable xmemory error rather than an opaque Temporal activity
    # timeout. This must hold per activity: an inversion (client timeout above
    # the activity budget) on the non-heartbeating write_start would let Temporal
    # abandon the attempt while POST /write_async keeps running and can still
    # enqueue server-side — the workflow believes the enqueue failed while a
    # write was queued, and a later durable-write retry double-enqueues.
    client_margin_seconds: int = 5

    def client_timeout(self, activity_seconds: int) -> float:
        """httpx timeout for a call whose activity budget is ``activity_seconds``."""
        return float(max(1, activity_seconds - self.client_margin_seconds))

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

    Deliberately contains no credential. The worker process resolves the key
    from ``os.environ[api_key_env]`` at client-construction time; callers who
    would rather pass it in-process use ``XmemoryPlugin(config, api_key=...)``,
    which keeps it off this object entirely.
    """

    model_config = ConfigDict(frozen=True)

    instance_id: str
    url: str | None = None
    api_key_env: str = DEFAULT_API_KEY_ENV
    timeouts: XmemoryTimeouts = Field(default_factory=XmemoryTimeouts)
    default_extraction_logic: str = "fast"
    # Activity summaries render in the Temporal UI, visible to anyone with
    # namespace access. Memory text is frequently personal, so content is
    # redacted out of summaries unless a caller opts in.
    include_content_in_summary: bool = False

    def resolve_api_key(self) -> str:
        """Read the API key from the environment.

        Raises immediately (at worker start, via the plugin's run context)
        rather than on the first activity execution, so a misconfigured worker
        fails visibly instead of failing every workflow that touches memory.
        """
        key = os.environ.get(self.api_key_env)
        if not key:
            raise ValueError(
                f"xmemory API key not found: environment variable {self.api_key_env!r} is unset or empty. "
                f"Set it on the worker process, or pass XmemoryPlugin(config, api_key=...)."
            )
        return key
