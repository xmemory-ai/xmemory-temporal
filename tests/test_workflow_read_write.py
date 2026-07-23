"""Read and write through a real (time-skipping) Temporal worker + the plugin."""

import uuid

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from xmemory_temporal import XmemoryConfig, XmemoryPlugin, errors

from .fakes import FakeXmemoryInstance, api_error
from .workflows import ReadWorkflow, WriteWorkflow


def _plugin(fake: FakeXmemoryInstance) -> XmemoryPlugin:
    return XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=fake)


async def _worker(env: WorkflowEnvironment, fake: FakeXmemoryInstance, tq: str) -> Worker:
    return Worker(
        env.client,
        task_queue=tq,
        workflows=[ReadWorkflow, WriteWorkflow],
        plugins=[_plugin(fake)],
    )


async def test_read_roundtrip(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance(read_answer="Alice likes tea")
    tq = f"tq-{uuid.uuid4()}"
    async with await _worker(env, fake, tq):
        result = await env.client.execute_workflow(
            ReadWorkflow.run, "what does Alice like?", id=f"wf-{uuid.uuid4()}", task_queue=tq
        )
    assert result == "Alice likes tea"
    assert fake.count("read") == 1


async def test_write_roundtrip(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    tq = f"tq-{uuid.uuid4()}"
    async with await _worker(env, fake, tq):
        write_id = await env.client.execute_workflow(
            WriteWorkflow.run, "Alice likes tea", id=f"wf-{uuid.uuid4()}", task_queue=tq
        )
    assert write_id == "w1"
    assert fake.count("write") == 1


async def test_non_retryable_write_fails_without_retry(env: WorkflowEnvironment) -> None:
    # A monthly-quota error is non-retryable AND the write default is at-most-once
    # (maximum_attempts=1), so the fake must see exactly one write and the
    # workflow must fail.
    fake = FakeXmemoryInstance()
    fake.fail_write_times(5, api_error(status=402, code="QUOTA_EXCEEDED", details={"kind": "monthly_quota_exceeded"}))
    tq = f"tq-{uuid.uuid4()}"
    async with await _worker(env, fake, tq):
        with pytest.raises(WorkflowFailureError) as ei:
            await env.client.execute_workflow(WriteWorkflow.run, "x", id=f"wf-{uuid.uuid4()}", task_queue=tq)
    assert fake.count("write") == 1
    # The underlying ApplicationError type is preserved through the failure chain
    # (WorkflowFailureError -> ActivityError -> ApplicationError).
    # Temporal chains failures via its own `.cause` attribute.
    app_err: BaseException | None = ei.value.cause
    while app_err is not None and not isinstance(app_err, ApplicationError):
        app_err = getattr(app_err, "cause", None)
    assert isinstance(app_err, ApplicationError)
    assert app_err.type == errors.TYPE_MONTHLY_QUOTA_EXCEEDED


async def test_write_is_at_most_once_by_default(env: WorkflowEnvironment) -> None:
    # Writes default to at-most-once: even a *retryable* (500) failure is NOT
    # retried, because a lost-response retry could duplicate (PK extraction is
    # non-deterministic). One attempt, then the workflow fails.
    fake = FakeXmemoryInstance()
    fake.fail_write_times(5, api_error(status=500))
    tq = f"tq-{uuid.uuid4()}"
    async with await _worker(env, fake, tq):
        with pytest.raises(WorkflowFailureError):
            await env.client.execute_workflow(WriteWorkflow.run, "x", id=f"wf-{uuid.uuid4()}", task_queue=tq)
    assert fake.count("write") == 1


async def test_write_retries_when_opted_in(env: WorkflowEnvironment) -> None:
    # The opt-in path: a workflow that sets a retryable write_retry_policy (for a
    # deterministic-PK schema) DOES retry a transient 500 to success.
    from .workflows import OptInRetryWriteWorkflow

    fake = FakeXmemoryInstance()
    fake.fail_write_times(2, api_error(status=500))
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(env.client, task_queue=tq, workflows=[OptInRetryWriteWorkflow], plugins=[_plugin(fake)]):
        write_id = await env.client.execute_workflow(
            OptInRetryWriteWorkflow.run, "x", id=f"wf-{uuid.uuid4()}", task_queue=tq
        )
    assert fake.count("write") == 3  # two failures + one success
    assert write_id == "w1"
