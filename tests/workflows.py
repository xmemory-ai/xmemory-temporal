"""Workflow definitions used across the integration tests.

Kept in one module so the same workflows are registered by every test and the
replayer sees a stable set of workflow types.
"""

from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal import xmemory_for_workflow


@workflow.defn
class ReadWorkflow:
    @workflow.run
    async def run(self, query: str) -> Any:
        # `reader_result` is Any — a natural-language string or a structured
        # object depending on read mode and schema. Return it as-is (annotated
        # Any) so the client decodes whatever the backend actually sent; a `str`
        # annotation would crash the client decode on a dict answer.
        mem = xmemory_for_workflow()
        out = await mem.read(query)
        return out.reader_result


@workflow.defn
class WriteWorkflow:
    @workflow.run
    async def run(self, text: str, budget_s: float = 180.0) -> str:
        mem = xmemory_for_workflow(write_timeout=timedelta(seconds=budget_s))
        out = await mem.write(text)
        return out.write_id


@workflow.defn
class OptInRetryWriteWorkflow:
    """Writes with an explicit retryable policy — the opt-in path for schemas
    whose primary keys are literal/deterministic (safe to retry)."""

    @workflow.run
    async def run(self, text: str) -> str:
        mem = xmemory_for_workflow(
            write_retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), backoff_coefficient=2.0, maximum_attempts=3
            )
        )
        out = await mem.write(text)
        return out.write_id


@workflow.defn
class DurableWriteWorkflow:
    """Durable write, with its cadence supplied by the test.

    The defaults are the ordinary case, so callers that pass only ``text`` (the
    replay and live-e2e tests, which hand this class to generic helpers) need to
    know nothing about the rest.
    """

    @workflow.run
    async def run(
        self,
        text: str,
        poll_s: float = 1.0,
        cap_s: float = 30.0,
        wait_s: float = 900.0,
        single_attempt: bool = False,
    ) -> str:
        # `single_attempt` pins each poll to one Temporal attempt. Counting the
        # fake's write_status calls otherwise conflates two things: how many times
        # the *loop* polled, and how many times Temporal retried a failing poll
        # activity inside one iteration. Tests about the loop's own cadence want
        # only the former.
        mem = xmemory_for_workflow(poll_retry_policy=RetryPolicy(maximum_attempts=1) if single_attempt else None)
        out = await mem.write_durable(
            text,
            poll_interval=timedelta(seconds=poll_s),
            max_poll_interval=timedelta(seconds=cap_s),
            max_wait=timedelta(seconds=wait_s),
        )
        return out.write_status


@workflow.defn
class ReadThenWriteWorkflow:
    """Two logical ops in one workflow — exercises per-op scheduling counts."""

    @workflow.run
    async def run(self, text: str) -> str:
        mem = xmemory_for_workflow()
        await mem.read("before")
        out = await mem.write(text)
        return out.write_id


@workflow.defn
class DoubleWriteWorkflow:
    """A sensitivity control for the side-effects test.

    It issues two logical writes with a durable wait between them (so the
    forced-replay harness genuinely re-runs the workflow across the eviction).
    A correct, replay-safe implementation must report *exactly two* writes — no
    more (replay must not duplicate) and no fewer (both must happen). It is what
    proves the harness's "exactly one" assertion for a single write is not
    vacuously true: the same harness reports two when there are two.
    """

    @workflow.run
    async def run(self, text: str) -> int:
        mem = xmemory_for_workflow()
        await mem.write(f"{text} (1)")
        await workflow.sleep(timedelta(seconds=1))
        await mem.write(f"{text} (2)")
        return 2


# --- interceptor test fixtures (kept here so the workflow module the sandbox
#     imports never pulls in the client / fakes at top level) --------------


@activity.defn(name="user_activity")
async def user_activity(payload: str) -> str:
    """A plain user activity the auto-capture interceptor should observe."""
    return f"handled: {payload}"


@workflow.defn
class UserWorkflow:
    """A plain user activity, on a deadline the test chooses."""

    @workflow.run
    async def run(self, payload: str, budget_s: float = 30.0) -> str:
        return await workflow.execute_activity(
            "user_activity", payload, start_to_close_timeout=timedelta(seconds=budget_s)
        )


@workflow.defn
class StrictPollDurableWriteWorkflow:
    """Durable write whose poll policy forbids retrying rate limits.

    Marking an error type non-retryable is a deliberate statement that it must
    surface, so the loop has to propagate it rather than poll on to a timeout.
    """

    @workflow.run
    async def run(self, text: str) -> str:
        mem = xmemory_for_workflow(
            poll_retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                non_retryable_error_types=["XmemoryRateLimited"],
            )
        )
        out = await mem.write_durable(text, poll_interval=timedelta(seconds=1), max_wait=timedelta(seconds=60))
        return out.write_status


@workflow.defn
class BadPollPolicyDurableWriteWorkflow:
    """A poll policy Temporal will refuse when it builds the activity command."""

    @workflow.run
    async def run(self, backoff: float, attempts: int) -> str:
        mem = xmemory_for_workflow(
            poll_retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), backoff_coefficient=backoff, maximum_attempts=attempts
            )
        )
        out = await mem.write_durable("remember", max_wait=timedelta(seconds=60))
        return out.write_status


@workflow.defn
class SingleAttemptBadCoefficientWorkflow:
    """A sub-1 backoff coefficient with one attempt.

    The SDK accepts this (the coefficient is never applied) and so did an earlier
    version of our validation -- but the service refuses it regardless, after the
    enqueue, and the workflow then fails its task forever.
    """

    @workflow.run
    async def run(self) -> str:
        mem = xmemory_for_workflow(poll_retry_policy=RetryPolicy(maximum_attempts=1, backoff_coefficient=0.5))
        out = await mem.write_durable("remember", max_wait=timedelta(seconds=60))
        return out.write_status


@workflow.defn
class ReservedTimeoutTypeWorkflow:
    """A reserved timeout-kind spelling that names no real timeout."""

    @workflow.run
    async def run(self) -> str:
        mem = xmemory_for_workflow(
            poll_retry_policy=RetryPolicy(non_retryable_error_types=["TemporalTimeout:not-real"])
        )
        out = await mem.write_durable("remember", max_wait=timedelta(seconds=60))
        return out.write_status
