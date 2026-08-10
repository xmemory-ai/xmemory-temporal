"""The durable write loop: termination, backoff, and failure handling.

All of these model a multi-minute deep write but run in milliseconds — the whole
argument for polling from the workflow rather than inside one long activity: the
time-skipping environment fast-forwards ``workflow.sleep`` instantly.
"""

import uuid

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]

from xmemory_temporal import XmemoryConfig, XmemoryPlugin, errors

from .fakes import FakeXmemoryInstance
from .workflows import DurableWriteWorkflow


def _app_error(exc: BaseException) -> ApplicationError:
    """Walk Temporal's `.cause` chain down to the underlying ApplicationError."""
    err: BaseException | None = exc
    while err is not None and not isinstance(err, ApplicationError):
        err = getattr(err, "cause", None)
    assert isinstance(err, ApplicationError)
    return err


async def _run(env: WorkflowEnvironment, fake: FakeXmemoryInstance) -> str:
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[DurableWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=fake)],
    ):
        return await env.client.execute_workflow(
            DurableWriteWorkflow.run, "remember this", id=f"wf-{uuid.uuid4()}", task_queue=tq
        )


async def test_polls_to_completion(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    fake.status_sequence(
        [
            WriteQueueStatus.QUEUED,
            WriteQueueStatus.PROCESSING,
            WriteQueueStatus.EXTRACTING,
            WriteQueueStatus.COMPLETED,
        ]
    )
    status = await _run(env, fake)
    assert status == "completed"
    # one enqueue + four polls
    assert fake.count("write_async") == 1
    assert fake.count("write_status") == 4


async def test_failed_status_raises(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.FAILED], error_detail="extractor exploded")
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    app_err = _app_error(ei.value)
    assert app_err.type == errors.TYPE_WRITE_FAILED
    # The raw server `error_detail` must NOT appear in the failure message (the
    # cleartext history title), but IS carried in details for debuggability.
    assert "extractor exploded" not in (app_err.message or "")
    assert any("extractor exploded" in str(d) for d in app_err.details)


async def test_not_found_raises_immediately(env: WorkflowEnvironment) -> None:
    # `write_async` is transactional, so the id it returned is always queryable.
    # A not_found means the write is genuinely gone: fail on the first poll
    # rather than masking a backend that violated that contract.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.NOT_FOUND])
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    assert _app_error(ei.value).type == errors.TYPE_WRITE_NOT_FOUND
    assert fake.count("write_status") == 1


async def test_two_phase_intermediate_states_are_non_terminal(env: WorkflowEnvironment) -> None:
    # The two-phase pipeline states (extracting/extracted/applying) must all be
    # treated as "keep polling", never terminal.
    fake = FakeXmemoryInstance()
    fake.status_sequence(
        [
            WriteQueueStatus.EXTRACTING,
            WriteQueueStatus.EXTRACTED,
            WriteQueueStatus.APPLYING,
            WriteQueueStatus.COMPLETED,
        ]
    )
    status = await _run(env, fake)
    assert status == "completed"
    assert fake.count("write_status") == 4


async def test_unknown_status_keeps_polling(env: WorkflowEnvironment) -> None:
    # A status the client enum does not know (a future server state added
    # during a rolling deploy) must be treated as non-terminal — keep polling —
    # NOT fail the in-flight durable write. Here an unknown state precedes
    # completion; the loop rides through it.
    fake = FakeXmemoryInstance()
    fake.status_sequence(["indexing", "indexing", WriteQueueStatus.COMPLETED])
    status = await _run(env, fake)
    assert status == "completed"
    assert fake.count("write_status") == 3


async def test_max_wait_timeout_raises(env: WorkflowEnvironment) -> None:
    # A write that never completes is bounded by max_wait and fails with a
    # distinct, non-retryable timeout (not an infinite poll loop). Time-skipping
    # fast-forwards the 15-minute deadline instantly.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING])  # never terminal
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    assert "XmemoryWriteTimeout" in str(ei.value.cause)


async def test_zero_poll_interval_is_honored(env: WorkflowEnvironment) -> None:
    # timedelta(0) is falsy, so resolving with `or` would silently substitute the
    # 2s default. Read the timers back out of history: an honored zero produces
    # zero-length sleeps, the default would produce 2s then 3s.
    from .workflows import ZeroPollDurableWriteWorkflow

    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    tq = f"tq-{uuid.uuid4()}"
    wf_id = f"wf-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[ZeroPollDurableWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=fake)],
    ):
        status = await env.client.execute_workflow(ZeroPollDurableWriteWorkflow.run, "x", id=wf_id, task_queue=tq)
    assert status == "completed"

    timers = []
    async for event in env.client.get_workflow_handle(wf_id).fetch_history_events():
        if event.HasField("timer_started_event_attributes"):
            d = event.timer_started_event_attributes.start_to_fire_timeout
            timers.append(d.seconds + d.nanos / 1e9)
    assert timers, "the poll loop should have started timers"
    assert all(t == 0 for t in timers), f"explicit zero was overridden: {timers}"
