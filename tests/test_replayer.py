"""Replay recorded histories against the current code.

This is the guard against the highest-consequence, least-obvious regression:
changing ``WorkflowXmemory`` in a way that breaks determinism for workflows
already running in production. The replayer re-runs the workflow logic against a
real recorded history and fails on any nondeterminism.

Scope, stated plainly: this records the history live (in the time-skipping env)
and immediately replays it with the *same* code, so it catches nondeterminism
within a run but cannot catch a change that breaks replay of a *past* build
(the record and replay always agree because they are the same code). Guarding
against that needs representative histories checked into the repo and replayed
by a future build — a follow-up (there is no ``tests/histories/`` corpus yet).
"""

import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from xmemory_temporal import XmemoryConfig, XmemoryPlugin

from .fakes import FakeXmemoryInstance
from .workflows import DurableWriteWorkflow, ReadThenWriteWorkflow


async def _record(env: WorkflowEnvironment, fake: FakeXmemoryInstance, workflow, arg: str):
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[workflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=fake)],
    ):
        wf_id = f"wf-{uuid.uuid4()}"
        await env.client.execute_workflow(workflow.run, arg, id=wf_id, task_queue=tq)
        handle = env.client.get_workflow_handle(wf_id)
        return await handle.fetch_history()


async def test_replay_read_then_write(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    history = await _record(env, fake, ReadThenWriteWorkflow, "remember this")

    replayer = Replayer(
        workflows=[ReadThenWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=FakeXmemoryInstance())],
    )
    # Raises on any nondeterminism.
    await replayer.replay_workflow(history)


async def test_replay_durable_write(env: WorkflowEnvironment) -> None:
    from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]

    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    history = await _record(env, fake, DurableWriteWorkflow, "remember this")

    replayer = Replayer(
        workflows=[DurableWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=FakeXmemoryInstance())],
    )
    await replayer.replay_workflow(history)
