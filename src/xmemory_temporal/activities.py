"""The xmemory activities: the only place this package does I/O.

The client is injected per Worker; each call's client timeout is derived from
the deadline Temporal assigned the activity. ``xmemory-ai`` is natively async,
so these are plain ``async def`` with no thread pool.
"""

import contextvars
from typing import Any, Callable

from temporalio import activity
from temporalio.exceptions import ApplicationError

from xmemory_temporal.config import XmemoryConfig, client_timeout_seconds
from xmemory_temporal.deadline import attempt_clock_unusable, remaining_budget_seconds
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
from xmemory_temporal.errors import (
    TYPE_BAD_OPTIONS,
    TYPE_CLOCK_UNUSABLE,
    TYPE_DEADLINE_EXPIRED,
    TYPE_NO_DEADLINE,
    TYPE_NOT_BOUND,
    to_application_error,
)
from xmemory_temporal.protocol import XmemoryInstanceProtocol

# Pinned so renaming a method cannot break replay of in-flight workflows.
ACTIVITY_READ = "xmemory_read"
ACTIVITY_WRITE = "xmemory_write"
ACTIVITY_WRITE_START = "xmemory_write_start"
ACTIVITY_WRITE_STATUS = "xmemory_write_status"

# A ContextVar, not an attribute: one plugin object can serve several workers, and
# an attribute would be last-bind-wins. Each worker's run_context binds its own.
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
            # Registered without the plugin; no retry can bind a client.
            raise ApplicationError(
                "xmemory activities are not bound to a client — register XmemoryPlugin on the "
                "Client (the Worker inherits it) rather than registering the activity functions "
                "directly.",
                type=TYPE_NOT_BOUND,
                non_retryable=True,
            )
        return inst

    def _client_timeout(self) -> float:
        """Client budget for this call, derived from the activity's own deadline.

        Deriving it, rather than keeping a second worker-side copy, is what
        makes "the client gives up first" hold by construction when a workflow
        lowers its timeout.

        Temporal requires one of the two close timeouts on every activity, so
        the ``None`` branch is unreachable in practice; both fields are typed
        optional, and a silent default there is exactly the second copy this
        design exists to avoid.
        """
        info = activity.info()
        if attempt_clock_unusable(info) and not self._config.allow_unmeasurable_clock:
            # Refuse rather than guess. The time Temporal spent before our
            # interceptor cannot be measured against a worker clock that reads
            # behind the service, and any substitute could let the client outlive
            # the activity and duplicate a write on retry.
            raise ApplicationError(
                f"activity {info.activity_type} cannot be timed: this worker's clock reads behind the "
                f"service, so the deadline already spent is unmeasurable. Sync the worker clock (NTP).",
                type=TYPE_CLOCK_UNUSABLE,
            )
        # No explicit elapsed: the helper reads the stamp the plugin's outermost
        # interceptor took, so whatever ran before this function counts too.
        remaining = remaining_budget_seconds(info, allow_unmeasurable=self._config.allow_unmeasurable_clock)
        if remaining is None:
            raise ApplicationError(
                f"activity {info.activity_type} was scheduled without a deadline: set "
                "start_to_close_timeout or schedule_to_close_timeout on it.",
                type=TYPE_NO_DEADLINE,
                non_retryable=True,
            )
        if remaining <= 0:
            # Temporal has already given up on this attempt. Rounding the
            # remainder up to a token budget would send a request whose result
            # nobody will read, and for a write the server could still accept it.
            raise ApplicationError(
                f"activity {info.activity_type} is past its deadline",
                type=TYPE_DEADLINE_EXPIRED,
            )
        return client_timeout_seconds(remaining, self._config.client_margin_seconds)

    @activity.defn(name=ACTIVITY_READ)
    async def read(self, request: ReadInput) -> ReadOutput:
        # Outside the try: an unbound-client or missing-deadline error must keep
        # its non-retryable ApplicationError rather than being re-mapped.
        instance = self.instance
        timeout = self._client_timeout()
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
                timeout=timeout,
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
        timeout = self._client_timeout()
        self._reject_empty_mutations(request)
        try:
            result = await instance.write(
                request.text,
                timeout=timeout,
                **self._write_kwargs(request),
            )
        except Exception as exc:
            raise to_application_error(exc) from None
        return project_write(result)

    @activity.defn(name=ACTIVITY_WRITE_START)
    async def write_start(self, request: WriteInput) -> WriteStartOutput:
        instance = self.instance
        timeout = self._client_timeout()
        self._reject_empty_mutations(request)
        try:
            result = await instance.write_async(
                request.text,
                timeout=timeout,
                **self._write_kwargs(request),
            )
        except Exception as exc:
            raise to_application_error(exc) from None
        return project_write_start(result)

    @activity.defn(name=ACTIVITY_WRITE_STATUS)
    async def write_status(self, request: WriteStatusInput) -> WriteStatusOutput:
        instance = self.instance
        timeout = self._client_timeout()
        try:
            result = await instance.write_status(
                request.write_id,
                timeout=timeout,
            )
        except Exception as exc:
            raise to_application_error(exc) from None
        detail = getattr(result, "error_detail", None)
        if detail:
            # Never the return value: history keeps activity results in the clear.
            # And not verbatim in the log either unless asked for, since the detail
            # is not promised user-safe and logs travel. What is always safe to
            # record is which write failed and how much detail there was.
            if self._config.log_server_error_detail:
                activity.logger.warning("xmemory write %s failed: %s", request.write_id, detail)
            else:
                activity.logger.warning(
                    "xmemory write %s failed; the server sent %d characters of detail, withheld from the log "
                    "and from workflow history (set log_server_error_detail=True to include it)",
                    request.write_id,
                    len(str(detail)),
                )
        return project_write_status(result)

    @staticmethod
    def _reject_empty_mutations(request: WriteInput) -> None:
        """Refuse an empty mutation list before any request goes out.

        The client answers it with a plain exception, which the mapper can only read
        as retryable -- so Temporal would keep retrying a request that cannot succeed.
        Called outside the try blocks, whose handler would remap this verdict.
        """
        if request.structured_mutations is not None and len(request.structured_mutations) == 0:
            raise ApplicationError(
                "xmemory write was given an empty structured_mutations list; omit it to write text instead",
                type=TYPE_BAD_OPTIONS,
                non_retryable=True,
            )

    def _write_kwargs(self, request: WriteInput) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if request.structured_mutations is not None:
            # A structured write carries its own keys, so the server applies it
            # without running the extractor; text and extraction_logic are moot.
            kwargs["structured_mutations"] = request.structured_mutations
            return kwargs
        logic = request.extraction_logic or self._config.default_extraction_logic
        if logic is not None:
            kwargs["extraction_logic"] = logic
        if request.diff_engine is not None:
            kwargs["diff_engine"] = request.diff_engine
        return kwargs

    def as_sequence(self) -> list[Callable[..., Any]]:
        """The bound methods to hand to ``SimplePlugin(activities=...)``."""
        return [self.read, self.write, self.write_start, self.write_status]
