"""The xmemory activities.

All xmemory I/O happens here and nowhere else — workflow code only schedules
these. The client is *injected* rather than looked up from a module-level cache:
process-global mutable state leaks across tests and cannot represent a worker
serving two instances, and ``maxims/CONCURRENCY.md`` only permits process-local
state as a best-effort optimization that degrades gracefully. This isn't one.

Every method is a plain ``async def``. ``xmemory-ai`` ships a native
``AsyncInstanceAPI``, so there is no thread pool and no ``asyncio.to_thread``
anywhere in this package — which is also why the Python and TypeScript ports
have the same structure.
"""

import contextvars
from typing import Any, Callable

from temporalio import activity
from temporalio.exceptions import ApplicationError

from xmemory_temporal.config import XmemoryConfig
from xmemory_temporal.dto import (
    ReadInput,
    ReadOutput,
    WriteInput,
    WriteOutput,
    WriteStartOutput,
    WriteStatusInput,
    WriteStatusOutput,
    project_read,
    project_write,
    project_write_start,
    project_write_status,
)
from xmemory_temporal.errors import TYPE_NOT_BOUND, to_application_error
from xmemory_temporal.protocol import XmemoryInstanceProtocol

# Activity names are pinned explicitly so renaming a Python method can never
# break replay of workflows already in flight.
ACTIVITY_READ = "xmemory_read"
ACTIVITY_WRITE = "xmemory_write"
ACTIVITY_WRITE_START = "xmemory_write_start"
ACTIVITY_WRITE_STATUS = "xmemory_write_status"


# The bound client lives in a ContextVar, not an attribute. ONE plugin object is
# shared across every Worker built from a Client (Temporal filters the same
# objects, not copies), and each Worker enters run_context independently. A
# shared attribute would be last-bind-wins across Workers, and a Worker doing I/O
# after another shut down its client would hit a closed httpx client. run_context
# sets this per-Worker within the worker's run, so each Worker's activity
# executions — which inherit that context — resolve THEIR OWN client.
_bound_instance: contextvars.ContextVar[XmemoryInstanceProtocol | None] = contextvars.ContextVar(
    "xmemory_bound_instance", default=None
)


class XmemoryActivities:
    """The xmemory activity functions; the client is bound per-Worker (contextvar)."""

    def __init__(self, config: XmemoryConfig) -> None:
        self._config = config

    @staticmethod
    def bind(instance: XmemoryInstanceProtocol) -> contextvars.Token:
        """Bind the live client for the current context. Returns a reset token."""
        return _bound_instance.set(instance)

    @staticmethod
    def unbind(token: contextvars.Token) -> None:
        _bound_instance.reset(token)

    @property
    def instance(self) -> XmemoryInstanceProtocol:
        inst = _bound_instance.get()
        if inst is None:
            # A configuration error — no run context bound a client (activities
            # registered without the plugin). Fail fast: retrying can never bind
            # it, so this must be non-retryable, not the default-retryable bare
            # exception.
            raise ApplicationError(
                "xmemory activities are not bound to a client — register XmemoryPlugin on the "
                "Client (the Worker inherits it) rather than registering the activity functions "
                "directly.",
                type=TYPE_NOT_BOUND,
                non_retryable=True,
            )
        return inst

    @activity.defn(name=ACTIVITY_READ)
    async def read(self, request: ReadInput) -> ReadOutput:
        # Resolve the instance OUTSIDE the try so an unbound-client error keeps
        # its non-retryable ApplicationError (above) instead of being re-mapped.
        instance = self.instance
        kwargs: dict[str, Any] = {}
        if request.read_mode is not None:
            kwargs["read_mode"] = request.read_mode
        if request.scope is not None:
            kwargs["scope"] = request.scope
        if request.read_id is not None:
            kwargs["read_id"] = request.read_id
        try:
            result = await instance.read(
                request.query,
                timeout=self._config.timeouts.client_timeout(self._config.timeouts.read_seconds),
                **kwargs,
            )
        except Exception as exc:
            # `from None`, not `from exc`: Temporal serializes the whole cause
            # chain into cleartext history, and the original XmemoryAPIError's
            # message is NOT server-sanitized (it can embed the httpx string /
            # URL path). We already carry code/status in the sanitized failure;
            # dropping the cause keeps the raw transport detail out of history.
            # (Merely omitting `from exc` is insufficient — the implicit
            # __context__ is serialized too; only `from None` suppresses it.)
            raise to_application_error(exc) from None
        return project_read(result)

    @activity.defn(name=ACTIVITY_WRITE)
    async def write(self, request: WriteInput) -> WriteOutput:
        instance = self.instance
        try:
            result = await instance.write(
                request.text,
                timeout=self._config.timeouts.client_timeout(self._config.timeouts.write_seconds),
                **self._write_kwargs(request),
            )
        except Exception as exc:
            raise to_application_error(exc) from None
        return project_write(result)

    @activity.defn(name=ACTIVITY_WRITE_START)
    async def write_start(self, request: WriteInput) -> WriteStartOutput:
        instance = self.instance
        try:
            result = await instance.write_async(
                request.text,
                timeout=self._config.timeouts.client_timeout(self._config.timeouts.write_start_seconds),
                **self._write_kwargs(request),
            )
        except Exception as exc:
            raise to_application_error(exc) from None
        return project_write_start(result)

    @activity.defn(name=ACTIVITY_WRITE_STATUS)
    async def write_status(self, request: WriteStatusInput) -> WriteStatusOutput:
        instance = self.instance
        try:
            result = await instance.write_status(
                request.write_id,
                timeout=self._config.timeouts.client_timeout(self._config.timeouts.write_status_seconds),
            )
        except Exception as exc:
            raise to_application_error(exc) from None
        return project_write_status(result)

    def _write_kwargs(self, request: WriteInput) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        logic = request.extraction_logic or self._config.default_extraction_logic
        if logic is not None:
            kwargs["extraction_logic"] = logic
        if request.diff_engine is not None:
            kwargs["diff_engine"] = request.diff_engine
        return kwargs

    def as_sequence(self) -> list[Callable[..., Any]]:
        """The bound methods to hand to ``SimplePlugin(activities=...)``."""
        return [self.read, self.write, self.write_start, self.write_status]
