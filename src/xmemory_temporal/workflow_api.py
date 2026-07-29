"""The workflow-facing xmemory surface.

``WorkflowXmemory`` mirrors ``AsyncInstanceAPI`` method-for-method, so existing
agent call sites keep working and just dispatch to an activity. Replay-safe by
construction: only ``execute_activity`` and ``sleep``, no I/O or wall-clock.
"""

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal.activities import (
        ACTIVITY_READ,
        ACTIVITY_WRITE,
        ACTIVITY_WRITE_START,
        ACTIVITY_WRITE_STATUS,
    )
    from xmemory_temporal.dto import (
        ReadInput,
        ReadOutput,
        WriteInput,
        WriteOutput,
        WriteStartOutput,
        WriteStatusInput,
        WriteStatusOutput,
    )
    from xmemory_temporal.config import XmemoryTimeouts
    from xmemory_temporal.errors import TYPE_WRITE_FAILED, TYPE_WRITE_NOT_FOUND, TYPE_WRITE_TIMEOUT

# The workflow owns every activity budget: what is set here is what Temporal
# enforces AND what each activity derives its client timeout from, so the two can
# never disagree. `XmemoryTimeouts` is only the source of the default numbers.
_DEFAULTS = XmemoryTimeouts()

# Terminal `WriteQueueStatus` values.
_STATUS_COMPLETED = "completed"
_STATUS_FAILED = "failed"
_STATUS_NOT_FOUND = "not_found"
# Listed explicitly so an unseen server state is treated as unknown and keeps
# polling, rather than being mistaken for terminal.
_STATUS_IN_PROGRESS = frozenset({"queued", "processing", "extracting", "extracted", "applying"})

# Reads are idempotent, so they retry generously.
_DEFAULT_READ_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=10,
)
# At-most-once: xmemory assigns primary keys with a model, so a re-extraction can
# normalize the same value differently and fork the record. A failed write is
# surfaced to the workflow rather than retried. See the README's idempotency
# section for when opting in is safe.
_DEFAULT_WRITE_RETRY = RetryPolicy(maximum_attempts=1)
# Polling write_status is idempotent (read-only), so it may retry freely.
_DEFAULT_POLL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=20),
    maximum_attempts=10,
)
# `write_async` does not promise the id is queryable on the next request, so a
# healthy queued write can briefly read back not_found. Tolerate it this long
# before treating it as terminal (`failed` stays terminal immediately).
_DEFAULT_NOT_FOUND_GRACE = timedelta(seconds=10)


class WorkflowXmemory:
    """Instance-scoped memory handle usable from inside a workflow.

    Construct via ``xmemory_for_workflow``, which returns one shared handle
    per workflow execution.
    """

    def __init__(
        self,
        *,
        read_timeout: timedelta,
        write_timeout: timedelta,
        write_start_timeout: timedelta,
        write_status_timeout: timedelta,
        read_retry_policy: RetryPolicy | None = None,
        write_retry_policy: RetryPolicy | None = None,
        poll_retry_policy: RetryPolicy | None = None,
        include_content_in_summary: bool = False,
    ) -> None:
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._write_start_timeout = write_start_timeout
        self._write_status_timeout = write_status_timeout
        self._read_retry = read_retry_policy or _DEFAULT_READ_RETRY
        self._write_retry = write_retry_policy or _DEFAULT_WRITE_RETRY
        self._poll_retry = poll_retry_policy or _DEFAULT_POLL_RETRY
        self._include_content = include_content_in_summary

    # --- read ---------------------------------------------------------------

    async def read(
        self,
        query: str,
        *,
        read_mode: str | None = None,
        scope: dict[str, Any] | None = None,
        read_id: str | None = None,
    ) -> ReadOutput:
        return await workflow.execute_activity(
            ACTIVITY_READ,
            ReadInput(query=query, read_mode=read_mode, scope=scope, read_id=read_id),
            # Activities are referenced by string name, so Temporal cannot infer
            # the return type — `result_type` is what makes the data converter
            # reconstruct our dataclass instead of handing back a raw dict.
            result_type=ReadOutput,
            start_to_close_timeout=self._read_timeout,
            retry_policy=self._read_retry,
            summary=self._summary("read", query),
        )

    # --- synchronous write --------------------------------------------------

    async def write(
        self,
        text: str,
        *,
        extraction_logic: str | None = None,
        diff_engine: bool | None = None,
    ) -> WriteOutput:
        return await workflow.execute_activity(
            ACTIVITY_WRITE,
            WriteInput(text=text, extraction_logic=extraction_logic, diff_engine=diff_engine),
            result_type=WriteOutput,
            start_to_close_timeout=self._write_timeout,
            retry_policy=self._write_retry,
            summary=self._summary("write", text, extraction_logic),
        )

    # --- durable async write ------------------------------------------------

    async def write_async_start(
        self,
        text: str,
        *,
        extraction_logic: str | None = "deep",
        diff_engine: bool | None = None,
    ) -> WriteStartOutput:
        """Enqueue a write and return its id, without waiting for it to finish."""
        return await workflow.execute_activity(
            ACTIVITY_WRITE_START,
            WriteInput(text=text, extraction_logic=extraction_logic, diff_engine=diff_engine),
            result_type=WriteStartOutput,
            start_to_close_timeout=self._write_start_timeout,
            retry_policy=self._write_retry,
            summary=self._summary("write_start", text, extraction_logic),
        )

    async def write_status(self, write_id: str) -> WriteStatusOutput:
        return await workflow.execute_activity(
            ACTIVITY_WRITE_STATUS,
            WriteStatusInput(write_id=write_id),
            result_type=WriteStatusOutput,
            start_to_close_timeout=self._write_status_timeout,
            retry_policy=self._poll_retry,
            summary=f"xmemory write_status: {write_id}",
        )

    async def write_durable(
        self,
        text: str,
        *,
        extraction_logic: str | None = "deep",
        diff_engine: bool | None = None,
        poll_interval: timedelta | None = None,
        max_poll_interval: timedelta | None = None,
        max_wait: timedelta = timedelta(minutes=15),
        not_found_grace: timedelta | None = None,
    ) -> WriteStatusOutput:
        """Enqueue a write and poll it to completion, durably.

        The poll loop lives in workflow history, so a slow extraction survives
        worker restarts, redeploys, and rolling upgrades — the whole reason to
        put Temporal in front of xmemory. The enqueue (``write_async_start``) is
        the only non-idempotent step; the extraction itself is observed through
        idempotent, freely-retryable polls.

        A ``not_found`` status is tolerated for ``not_found_grace`` after the
        enqueue (see ``_DEFAULT_NOT_FOUND_GRACE``): the write id may not be
        queryable immediately, so failing the durable write on the first poll
        would kill healthy writes on any backend with enqueue→visible lag.
        """
        start = await self.write_async_start(text, extraction_logic=extraction_logic, diff_engine=diff_engine)

        delay = poll_interval or timedelta(seconds=2)
        cap = max_poll_interval or timedelta(seconds=30)
        now = workflow.now()
        deadline = now + max_wait
        not_found_deadline = now + (not_found_grace or _DEFAULT_NOT_FOUND_GRACE)

        while True:
            status = await self.write_status(start.write_id)
            not_found_is_terminal = workflow.now() >= not_found_deadline
            terminal = self._interpret_status(status, not_found_is_terminal=not_found_is_terminal)
            if terminal is not None:
                return terminal
            if workflow.now() + delay >= deadline:
                raise ApplicationError(
                    f"xmemory write {start.write_id} did not complete within {max_wait}",
                    {"write_id": start.write_id, "last_status": status.write_status},
                    type=TYPE_WRITE_TIMEOUT,
                    non_retryable=True,
                )
            await workflow.sleep(delay)
            delay = min(delay * 1.5, cap)

    @staticmethod
    def _interpret_status(status: WriteStatusOutput, *, not_found_is_terminal: bool = True) -> WriteStatusOutput | None:
        """Return the status if terminal-success, raise on terminal-failure, else ``None``.

        ``not_found_is_terminal=False`` (during the post-enqueue grace window)
        treats a ``not_found`` as non-terminal so the loop keeps polling.
        """
        value = status.write_status
        if value == _STATUS_COMPLETED:
            return status
        if value == _STATUS_FAILED:
            # Fixed message: `error_detail` is not promised user-safe, so it stays
            # out of the cleartext history title and lives in `details` instead.
            raise ApplicationError(
                f"xmemory write {status.write_id} failed",
                {"write_id": status.write_id, "error_detail": status.error_detail},
                type=TYPE_WRITE_FAILED,
                non_retryable=True,
            )
        if value == _STATUS_NOT_FOUND:
            if not not_found_is_terminal:
                # Enqueue may not be queryable yet, so keep polling.
                return None
            raise ApplicationError(
                f"xmemory write {status.write_id} not found",
                {"write_id": status.write_id},
                type=TYPE_WRITE_NOT_FOUND,
                non_retryable=True,
            )
        # In-progress or unrecognized: keep polling (bounded by max_wait). The
        # enum has grown before; a new state must not fail in-flight writes.
        if value not in _STATUS_IN_PROGRESS:
            workflow.logger.warning(
                "xmemory returned an unrecognized write status %r for %s; continuing to poll",
                value,
                status.write_id,
            )
        return None

    def _summary(self, op: str, content: str, logic: str | None = None) -> str:
        label = f"xmemory {op}"
        if logic:
            label += f" ({logic})"
        if self._include_content:
            return f"{label}: {content[:60]}"
        return f"{label}: {len(content)} chars"


def xmemory_for_workflow(
    *,
    read_timeout: timedelta = _DEFAULTS.read,
    write_timeout: timedelta = _DEFAULTS.write,
    write_start_timeout: timedelta = _DEFAULTS.write_start,
    write_status_timeout: timedelta = _DEFAULTS.write_status,
    read_retry_policy: RetryPolicy | None = None,
    write_retry_policy: RetryPolicy | None = None,
    poll_retry_policy: RetryPolicy | None = None,
    include_content_in_summary: bool = False,
) -> WorkflowXmemory:
    """Return a ``WorkflowXmemory`` handle for this workflow.

    The handle carries only configuration — no per-call mutable state — so
    constructing a fresh one on each call is correct and cheap; callers may hold
    one or make new ones freely. Timeouts default to the same values as
    ``XmemoryTimeouts``.
    """
    return WorkflowXmemory(
        read_timeout=read_timeout,
        write_timeout=write_timeout,
        write_start_timeout=write_start_timeout,
        write_status_timeout=write_status_timeout,
        read_retry_policy=read_retry_policy,
        write_retry_policy=write_retry_policy,
        poll_retry_policy=poll_retry_policy,
        include_content_in_summary=include_content_in_summary,
    )
