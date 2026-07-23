"""The side-effects-on-replay test Temporal's review specifically asks for.

``Worker(..., max_cached_workflows=0)`` evicts the workflow after every task, so
the workflow is replayed from history on each step — the condition under which a
non-replay-safe implementation duplicates its side effects.

Two assertions per workflow:

* **history-level (authoritative)** — N logical memory ops produce exactly N
  ``ActivityTaskScheduled`` events. This is the pattern Temporal's guide names,
  and it is retry-independent: each intended call is one scheduled event no
  matter how many times the activity retries or the workflow replays.
* **ledger-level (cross-check)** — the injected fake actually saw each write
  exactly once. On its own an in-memory counter can be inflated by activity
  retries, so the history count above is authoritative; the ledger complements it.

Plus a **sensitivity control**: the same forced-replay harness applied to a
workflow that legitimately writes twice reports exactly two. That is what makes
the "exactly one" assertion meaningful — the harness reports the true count, not
a constant 1. A test that cannot fail proves nothing.
"""

import uuid

from temporalio.client import WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from xmemory_temporal import XmemoryConfig, XmemoryPlugin

from .fakes import FakeXmemoryInstance
from .workflows import (
    DoubleWriteWorkflow,
    DurableWriteWorkflow,
    ReadThenWriteWorkflow,
    WriteWorkflow,
)


async def _scheduled_activity_count(handle: WorkflowHandle) -> int:
    count = 0
    async for event in handle.fetch_history_events():
        if event.HasField("activity_task_scheduled_event_attributes"):
            count += 1
    return count


async def _run_replayed(env: WorkflowEnvironment, fake: FakeXmemoryInstance, workflow, arg: str) -> WorkflowHandle:
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[workflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=fake)],
        max_cached_workflows=0,  # force replay from history on every task
    ):
        wf_id = f"wf-{uuid.uuid4()}"
        await env.client.execute_workflow(workflow.run, arg, id=wf_id, task_queue=tq)
        return env.client.get_workflow_handle(wf_id)


async def test_single_write_scheduled_once(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    handle = await _run_replayed(env, fake, WriteWorkflow, "remember this")

    assert await _scheduled_activity_count(handle) == 1
    assert fake.count("write") == 1


async def test_two_ops_scheduled_once_each(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    handle = await _run_replayed(env, fake, ReadThenWriteWorkflow, "remember this")

    # one read + one write == two scheduled activities, despite forced replay
    assert await _scheduled_activity_count(handle) == 2
    assert fake.count("read") == 1
    assert fake.count("write") == 1


async def test_durable_write_writes_once(env: WorkflowEnvironment) -> None:
    from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]

    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    handle = await _run_replayed(env, fake, DurableWriteWorkflow, "remember this")

    # Exactly one enqueue no matter how many times the poll loop replays.
    assert fake.count("write_async") == 1
    assert await _scheduled_activity_count(handle) >= 1


async def test_sensitivity_two_writes_report_two(env: WorkflowEnvironment) -> None:
    # The same forced-replay harness, applied to a workflow that legitimately
    # writes twice, must report exactly two — proving "exactly one" above is the
    # real count and not a constant. Neither duplicated (replay-unsafe) nor
    # dropped (a genuine op lost).
    fake = FakeXmemoryInstance()
    handle = await _run_replayed(env, fake, DoubleWriteWorkflow, "remember this")

    assert await _scheduled_activity_count(handle) == 2
    assert fake.count("write") == 2
