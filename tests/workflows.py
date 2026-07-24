"""Workflow definitions used across the integration tests.

Kept in one module so the same workflows are registered by every test and the
replayer sees a stable set of workflow types.
"""

from datetime import timedelta
from typing import Any

from temporalio import activity, workflow

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
    async def run(self, text: str) -> str:
        mem = xmemory_for_workflow()
        out = await mem.write(text)
        return out.write_id


@workflow.defn
class OptInRetryWriteWorkflow:
    """Writes with an explicit retryable policy — the opt-in path for schemas
    whose primary keys are literal/deterministic (safe to retry)."""

    @workflow.run
    async def run(self, text: str) -> str:
        from temporalio.common import RetryPolicy

        mem = xmemory_for_workflow(
            write_retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), backoff_coefficient=2.0, maximum_attempts=3
            )
        )
        out = await mem.write(text)
        return out.write_id


@workflow.defn
class DurableWriteWorkflow:
    @workflow.run
    async def run(self, text: str) -> str:
        mem = xmemory_for_workflow()
        out = await mem.write_durable(
            text,
            poll_interval=timedelta(seconds=1),
            max_wait=timedelta(minutes=15),
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
    @workflow.run
    async def run(self, payload: str) -> str:
        return await workflow.execute_activity("user_activity", payload, start_to_close_timeout=timedelta(seconds=30))
