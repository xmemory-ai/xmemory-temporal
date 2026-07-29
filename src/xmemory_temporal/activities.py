"""The xmemory activities: the only place this package does I/O.

The client is injected per Worker; each call's client timeout is derived from
the deadline Temporal assigned the activity. ``xmemory-ai`` is natively async,
so these are plain ``async def`` with no thread pool.
"""

import contextvars
from typing import Any, Callable

from temporalio import activity
from temporalio.exceptions import ApplicationError

from xmemory_temporal.config import XmemoryConfig, XmemoryTimeouts, client_timeout_seconds
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

# Pinned so renaming a method cannot break replay of in-flight workflows.
ACTIVITY_READ = "xmemory_read"
ACTIVITY_WRITE = "xmemory_write"
ACTIVITY_WRITE_START = "xmemory_write_start"
ACTIVITY_WRITE_STATUS = "xmemory_write_status"

# Used only when Temporal reports no per-attempt deadline (schedule_to_close only).
_FALLBACK_BUDGET = XmemoryTimeouts()


# A ContextVar, not an attribute: one plugin object is shared across every Worker
# built from a Client, so an attribute would be last-bind-wins and a Worker could
# reach another's closed client. Each Worker's run_context binds its own.
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
            # Activities registered without the plugin. Non-retryable: no retry
            # can bind a client.
            raise ApplicationError(
                "xmemory activities are not bound to a client — register XmemoryPlugin on the "
                "Client (the Worker inherits it) rather than registering the activity functions "
                "directly.",
                type=TYPE_NOT_BOUND,
                non_retryable=True,
            )
        return inst

    def _client_timeout(self, fallback_seconds: int) -> float:
        """Client budget for this call, derived from the activity's own deadline.

        Deriving it, rather than keeping a second worker-side copy, is what
        makes "the client gives up first" hold by construction when a workflow
        lowers its timeout.
        """
        budget = activity.info().start_to_close_timeout
        seconds = budget.total_seconds() if budget is not None else fallback_seconds
        return client_timeout_seconds(seconds, self._config.client_margin_seconds)

    @activity.defn(name=ACTIVITY_READ)
    async def read(self, request: ReadInput) -> ReadOutput:
        # Outside the try: an unbound-client error must keep its non-retryable
        # ApplicationError rather than being re-mapped.
        instance = self.instance
        kwargs: dict[str, Any] = {}
        if request.read_mode is not None:
            kwargs["read_mode"] = request.read_mode
        if request.scope is not None:
            # The client validates this into its own ReadScope model; hand it a
            # plain mapping so we never import a vendor type into the DTO layer.
            kwargs["scope"] = {
                "objects": [{"type": o.type, "key": o.key} for o in request.scope.objects],
                "relations_scope": request.scope.relations_scope,
            }
        if request.read_id is not None:
            kwargs["read_id"] = request.read_id
        try:
            result = await instance.read(
                request.query,
                timeout=self._client_timeout(_FALLBACK_BUDGET.read_seconds),
                **kwargs,
            )
        except Exception as exc:
            # `from None`, not `from exc`: Temporal serializes the cause chain
            # into cleartext history and the client's message is unsanitized.
            # Only `from None` suppresses the implicit __context__ too.
            raise to_application_error(exc) from None
        return project_read(result)

    @activity.defn(name=ACTIVITY_WRITE)
    async def write(self, request: WriteInput) -> WriteOutput:
        instance = self.instance
        try:
            result = await instance.write(
                request.text,
                timeout=self._client_timeout(_FALLBACK_BUDGET.write_seconds),
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
                timeout=self._client_timeout(_FALLBACK_BUDGET.write_start_seconds),
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
                timeout=self._client_timeout(_FALLBACK_BUDGET.write_status_seconds),
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
