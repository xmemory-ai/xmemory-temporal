"""The workflow-facing xmemory surface.

``WorkflowXmemory`` mirrors ``AsyncInstanceAPI`` method-for-method, so existing
agent call sites keep working and just dispatch to an activity. Replay-safe by
construction: only ``execute_activity`` and ``sleep``, no I/O or wall-clock.
"""

import dataclasses as dc
import math
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError, RetryState

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal.activities import (
        ACTIVITY_READ,
        ACTIVITY_WRITE,
        ACTIVITY_WRITE_START,
        ACTIVITY_WRITE_STATUS,
    )
    from xmemory_temporal.dto import (
        ReadInput,
        ReadScope,
        ReadOutput,
        WriteInput,
        WriteOutput,
        WriteStartOutput,
        WriteStatusInput,
        WriteStatusOutput,
    )
    from xmemory_temporal.config import XmemoryTimeouts
    from xmemory_temporal.errors import (
        TYPE_BAD_OPTIONS,
        TYPE_WRITE_FAILED,
        TYPE_WRITE_NOT_FOUND,
        TYPE_WRITE_TIMEOUT,
    )

# The workflow owns every activity budget: what is set here is what Temporal
# enforces and what each activity derives its client timeout from.
_DEFAULTS = XmemoryTimeouts()

# Measured, not estimated: one poll costs 17 history events and ten cost 116, so 11
# per poll on about 6 fixed. The budget is what this helper will spend of a
# workflow's history -- well under Temporal's limit, since the rest is the caller's.
_EVENTS_PER_POLL = 11
_FIXED_EVENTS = 6
_POLL_EVENT_BUDGET = 10_000


def _projected_polls(delay: timedelta, cap: timedelta, max_wait: timedelta, limit: int) -> int:
    """How many polls this cadence makes, backoff included, counted up to ``limit``.

    Not one iteration per poll: a microsecond cadence over fifteen minutes is ~900
    million of them, which hangs the workflow task. The growth phase is logarithmic
    and the capped phase is division. Seconds as floats, because ``timedelta.max``
    is a legal ``max_wait`` and arithmetic on it overflows.
    """
    wait_s = max_wait.total_seconds()
    interval_s = delay.total_seconds()
    cap_s = cap.total_seconds()
    spent = 0.0
    polls = 0
    while spent < wait_s and polls <= limit:
        polls += 1
        spent += interval_s
        interval_s = min(interval_s * 1.5, cap_s)
        if interval_s >= cap_s:
            # At the cap the cadence is constant, so the remainder is arithmetic.
            remaining = wait_s - spent
            if remaining > 0:
                polls += math.ceil(remaining / cap_s)
            break
    # +1 for the final observation taken at the deadline.
    return min(polls + 1, limit)


# `TemporalTimeout:<TYPE>` in non_retryable_error_types is Temporal's reserved
# syntax for "do not retry this timeout kind". Only these names mean anything.
_TIMEOUT_KINDS = frozenset({"START_TO_CLOSE", "SCHEDULE_TO_START", "SCHEDULE_TO_CLOSE", "HEARTBEAT"})


def _validate_retry_policy(policy: RetryPolicy, name: str) -> None:
    """Reject a policy Temporal would refuse, before the write is queued.

    Written out rather than calling the SDK's private validator, because the SDK and
    the service disagree and the service decides. Measured against a real server: a
    sub-1 backoff coefficient is refused even at ``maximum_attempts=1``, while
    ``initial_interval=0`` there is accepted. Whatever the service refuses fails the
    first poll's command construction, which is after the enqueue.
    """
    problems: list[str] = []
    single_attempt = policy.maximum_attempts == 1
    coefficient = policy.backoff_coefficient
    if not math.isfinite(coefficient) or coefficient < 1.0:
        problems.append(f"backoff_coefficient must be a finite number >= 1, got {coefficient}")
    # Only meaningful when a retry can happen; with one attempt the service accepts
    # a zero interval, so rejecting it here would refuse a working configuration.
    if not single_attempt and policy.initial_interval.total_seconds() <= 0:
        problems.append(f"initial_interval must be positive, got {policy.initial_interval}")
    if policy.maximum_attempts < 0:
        problems.append(f"maximum_attempts must not be negative, got {policy.maximum_attempts}")
    if policy.maximum_interval is not None and not single_attempt:
        if policy.maximum_interval.total_seconds() <= 0:
            problems.append(f"maximum_interval must be positive, got {policy.maximum_interval}")
        elif policy.maximum_interval < policy.initial_interval:
            problems.append(
                f"maximum_interval ({policy.maximum_interval}) must not be below "
                f"initial_interval ({policy.initial_interval})"
            )
    for value in policy.non_retryable_error_types or ():
        # These become protobuf strings; a non-string fails that conversion when the
        # activity command is built, which is after the enqueue.
        if not isinstance(value, str):
            problems.append(f"non_retryable_error_types must all be strings, got {type(value).__name__}")
            break
        if value.startswith("TemporalTimeout:") and value.removeprefix("TemporalTimeout:") not in _TIMEOUT_KINDS:
            problems.append(
                f"non_retryable_error_types entry {value!r} uses Temporal's reserved TemporalTimeout: prefix "
                f"with an unknown timeout kind; expected one of {sorted(_TIMEOUT_KINDS)}"
            )
    if problems:
        raise ApplicationError(
            f"write_durable: {name} is unusable: " + "; ".join(problems),
            type=TYPE_BAD_OPTIONS,
            non_retryable=True,
        )


# Terminal `WriteQueueStatus` values.
_STATUS_COMPLETED = "completed"
_STATUS_FAILED = "failed"
_STATUS_NOT_FOUND = "not_found"
# Listed explicitly, so an unseen server state keeps polling instead of being
# mistaken for terminal.
_STATUS_IN_PROGRESS = frozenset({"queued", "processing", "extracting", "extracted", "applying"})

# Reads are idempotent, so they retry generously.
_DEFAULT_READ_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=10,
)
# At-most-once: primary keys are model-extracted, so a re-extraction can normalize
# differently and fork the record. See the README on opting in.
_DEFAULT_WRITE_RETRY = RetryPolicy(maximum_attempts=1)
# Polling write_status is idempotent (read-only), so it may retry freely.
_DEFAULT_POLL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=20),
    maximum_attempts=10,
)


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
        scope: ReadScope | None = None,
        read_id: str | None = None,
    ) -> ReadOutput:
        return await workflow.execute_activity(
            ACTIVITY_READ,
            ReadInput(query=query, read_mode=read_mode, scope=scope, read_id=read_id),
            # Activities are referenced by name, so `result_type` is what makes the
            # converter rebuild our dataclass instead of a raw dict.
            result_type=ReadOutput,
            start_to_close_timeout=self._read_timeout,
            retry_policy=self._read_retry,
            summary=self._summary("read", query),
        )

    # --- synchronous write --------------------------------------------------

    async def write(
        self,
        text: str = "",
        *,
        extraction_logic: str | None = None,
        diff_engine: bool | None = None,
        structured_mutations: list[dict[str, Any]] | None = None,
    ) -> WriteOutput:
        """Write memory, from free ``text`` or explicit ``structured_mutations``.

        Structured mutations carry their own primary keys and skip extraction, so
        they apply deterministically and are safe to retry.
        """
        return await workflow.execute_activity(
            ACTIVITY_WRITE,
            WriteInput(
                text=text,
                extraction_logic=extraction_logic,
                diff_engine=diff_engine,
                structured_mutations=structured_mutations,
            ),
            result_type=WriteOutput,
            start_to_close_timeout=self._write_timeout,
            retry_policy=self._write_retry,
            summary=self._summary("write", text, extraction_logic),
        )

    # --- durable async write ------------------------------------------------

    async def write_async_start(
        self,
        text: str = "",
        *,
        extraction_logic: str | None = None,
        diff_engine: bool | None = None,
        structured_mutations: list[dict[str, Any]] | None = None,
    ) -> WriteStartOutput:
        """Enqueue a write and return its id, without waiting for it to finish.

        No extraction logic is forced: an omitted value resolves worker-side to
        ``XmemoryConfig.default_extraction_logic`` (itself ``fast``), the same as
        every other call here. ``write_durable`` asks for ``deep`` explicitly,
        because waiting minutes for a shallow extraction is not what it is for.
        """
        return await workflow.execute_activity(
            ACTIVITY_WRITE_START,
            WriteInput(
                text=text,
                extraction_logic=extraction_logic,
                diff_engine=diff_engine,
                structured_mutations=structured_mutations,
            ),
            result_type=WriteStartOutput,
            start_to_close_timeout=self._write_start_timeout,
            retry_policy=self._write_retry,
            summary=self._summary("write_start", text, extraction_logic),
        )

    async def write_status(self, write_id: str) -> WriteStatusOutput:
        """Poll a queued write once. The caller owns the overall wait."""
        return await self._poll_status(write_id, None)

    async def _poll_status(
        self, write_id: str, remaining: timedelta | None, *, single_attempt: bool = False
    ) -> WriteStatusOutput:
        """``write_status`` with an optional deadline for ``write_durable``.

        ``remaining`` bounds the poll, retries included. Without a
        schedule-to-close, one rate-limited poll can back off (up to the hour the
        ``Retry-After`` clamp allows) far past the caller's ``max_wait``, and the
        loop would surface that as ``XmemoryRateLimited`` rather than
        ``XmemoryWriteTimeout``.

        ``single_attempt`` is for the last look taken *at* the deadline, which has no
        remaining wait to be bounded by. One attempt is what "one last look" means:
        letting it retry turned the documented one-poll grace into a whole retry
        chain, and a 10s ``max_wait`` could run 35s.
        """
        return await workflow.execute_activity(
            ACTIVITY_WRITE_STATUS,
            WriteStatusInput(write_id=write_id),
            result_type=WriteStatusOutput,
            start_to_close_timeout=self._write_status_timeout,
            schedule_to_close_timeout=remaining,
            retry_policy=dc.replace(self._poll_retry, maximum_attempts=1) if single_attempt else self._poll_retry,
            summary=f"xmemory write_status: {write_id}",
        )

    async def write_durable(
        self,
        text: str = "",
        *,
        extraction_logic: str | None = "deep",
        diff_engine: bool | None = None,
        structured_mutations: list[dict[str, Any]] | None = None,
        poll_interval: timedelta | None = None,
        max_poll_interval: timedelta | None = None,
        max_wait: timedelta = timedelta(minutes=15),
    ) -> WriteStatusOutput:
        """Enqueue a write and poll it to completion, durably.

        The poll loop lives in workflow history, so a slow extraction survives
        worker restarts, redeploys, and rolling upgrades — the whole reason to
        put Temporal in front of xmemory. The enqueue (``write_async_start``) is
        the only non-idempotent step; the extraction itself is observed through
        idempotent, freely-retryable polls.

        A ``not_found`` is terminal immediately: ``write_async`` is transactional,
        so the id it returns is always queryable.
        """
        # `is not None`, not `or`: `or` would silently substitute the default for a
        # small timedelta, and the TypeScript port's `??` does not.
        delay = poll_interval if poll_interval is not None else timedelta(seconds=2)
        cap = max_poll_interval if max_poll_interval is not None else timedelta(seconds=30)
        # Validated before the enqueue, the one non-idempotent step: a rejected
        # option must not leave a queued write nobody is waiting on.
        for name, value in (
            ("max_wait", max_wait),
            ("poll_interval", delay),
            ("max_poll_interval", cap),
            ("write_status_timeout", self._write_status_timeout),
        ):
            # A zero poll interval hot-polls against Temporal's ~1ms timer floor,
            # and a zero status timeout schedules an activity that cannot complete.
            if value <= timedelta(0):
                raise ApplicationError(
                    f"write_durable: {name} must be positive, got {value}",
                    type=TYPE_BAD_OPTIONS,
                    non_retryable=True,
                )
        if cap < delay:
            raise ApplicationError(
                f"write_durable: max_poll_interval ({cap}) must not be below poll_interval ({delay})",
                type=TYPE_BAD_OPTIONS,
                non_retryable=True,
            )
        # Temporal validates the policy when it builds the first poll's command,
        # which is after the enqueue.
        _validate_retry_policy(self._poll_retry, "poll_retry_policy")

        # Polling is the one thing here that grows history without bound, and that
        # history is shared with the rest of the caller's workflow. Reject a cadence
        # whose worst case cannot fit, rather than failing mid-wait.
        poll_limit = _POLL_EVENT_BUDGET // _EVENTS_PER_POLL + 1
        worst_case_polls = _projected_polls(delay, cap, max_wait, poll_limit)
        projected_events = _FIXED_EVENTS + worst_case_polls * _EVENTS_PER_POLL
        if projected_events > _POLL_EVENT_BUDGET:
            raise ApplicationError(
                f"write_durable: poll_interval {delay} over max_wait {max_wait} projects about "
                f"{projected_events} history events ({worst_case_polls} polls), past the {_POLL_EVENT_BUDGET} "
                f"this helper will spend of your workflow's history. Raise poll_interval or lower max_wait.",
                type=TYPE_BAD_OPTIONS,
                non_retryable=True,
            )

        start = await self.write_async_start(
            text,
            extraction_logic=extraction_logic,
            diff_engine=diff_engine,
            structured_mutations=structured_mutations,
        )
        deadline = workflow.now() + max_wait

        warned_history = False
        last_status = "unknown"
        final = False
        while True:
            left = deadline - workflow.now()
            if left <= timedelta(0) and not final:
                raise self._max_wait_elapsed(start.write_id, max_wait, last_status)
            # Ordinary polls are bounded by the wait. The last observation happens
            # *at* the deadline and gets one ordinary budget: the grace on top.
            bound = self._write_status_timeout if final else min(left, self._write_status_timeout)
            hint = None
            try:
                status = await self._poll_status(start.write_id, bound, single_attempt=final)
                last_status = status.write_status
                terminal = self._interpret_status(status)
                if terminal is not None:
                    return terminal
                backoff = delay
            except ActivityError as exc:
                cause = exc.cause
                # Cancellation must reach the caller as cancellation, not as a
                # timeout verdict we invented.
                if exc.retry_state == RetryState.CANCEL_REQUESTED or isinstance(cause, CancelledError):
                    raise
                # Non-retryable because we typed it so, or because the caller's
                # policy says so. Masking either as a timeout buries their decision.
                if isinstance(cause, ApplicationError) and cause.non_retryable:
                    raise
                if exc.retry_state == RetryState.NON_RETRYABLE_FAILURE:
                    raise
                # A poll ending is not the wait ending: Temporal stops one when
                # its retries are spent or no further attempt fits.
                hint = getattr(cause, "next_retry_delay", None)
                backoff = max(delay, hint) if hint is not None else delay
            # This helper cannot call continue_as_new from inside the caller's
            # workflow, so surface Temporal's signal instead.
            if not warned_history and workflow.info().is_continue_as_new_suggested():
                warned_history = True
                workflow.logger.warning(
                    "xmemory write_durable has polled %s into a history Temporal now suggests "
                    "continuing-as-new; run it in a child workflow, or raise max_poll_interval",
                    start.write_id,
                )
            if final:
                raise self._max_wait_elapsed(start.write_id, max_wait, last_status)
            # Decided on the clock, not the budget allocated before the poll: a
            # fast reply must leave room for the next one.
            left = deadline - workflow.now()
            if backoff < left:
                await workflow.sleep(backoff)
                delay = min(delay * 1.5, cap)
                continue
            # `>`, not `>=`: a hint equal to the wait has just elapsed when the
            # deadline arrives, so the last look is on the server's own pacing.
            if hint is not None and hint > left:
                # The server will not answer before the deadline, so a last look
                # would only arrive early.
                await workflow.sleep(max(left, timedelta(0)))
                raise self._max_wait_elapsed(start.write_id, max_wait, last_status)
            # Our own cadence does not fit. Wait the rest of the wait out, then
            # take one last look: the write may still land inside it.
            final = True
            await workflow.sleep(max(left, timedelta(0)))

    @staticmethod
    def _max_wait_elapsed(write_id: str, max_wait: timedelta, last_status: str) -> ApplicationError:
        return ApplicationError(
            f"xmemory write {write_id} did not complete within {max_wait}",
            {"write_id": write_id, "last_status": last_status},
            type=TYPE_WRITE_TIMEOUT,
            non_retryable=True,
        )

    @staticmethod
    def _interpret_status(status: WriteStatusOutput) -> WriteStatusOutput | None:
        """Return the status if terminal-success, raise on terminal-failure, else ``None``."""
        value = status.write_status
        if value == _STATUS_COMPLETED:
            return status
        if value == _STATUS_FAILED:
            # The server's detail never leaves the worker: Temporal persists both
            # activity results and failure details to cleartext history.
            raise ApplicationError(
                f"xmemory write {status.write_id} failed",
                {"write_id": status.write_id, "write_status": status.write_status},
                type=TYPE_WRITE_FAILED,
                non_retryable=True,
            )
        if value == _STATUS_NOT_FOUND:
            # `write_async` is transactional, so a returned id is always
            # queryable. A not_found here means the write is genuinely gone.
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
