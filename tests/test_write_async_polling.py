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


async def test_not_found_raises_after_grace(env: WorkflowEnvironment) -> None:
    # A write id that stays not_found past the grace window is a real terminal
    # failure (the write truly does not exist).
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.NOT_FOUND])  # clamps: not_found forever
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    assert _app_error(ei.value).type == errors.TYPE_WRITE_NOT_FOUND


async def test_not_found_within_grace_keeps_polling(env: WorkflowEnvironment) -> None:
    # A not_found on the first polls means the enqueue is not visible yet, NOT
    # that the write is gone. The loop must tolerate it during the grace window
    # and succeed once the write becomes queryable.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.NOT_FOUND, WriteQueueStatus.NOT_FOUND, WriteQueueStatus.COMPLETED])
    status = await _run(env, fake)
    assert status == "completed"
    assert fake.count("write_status") == 3


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
    # F5: a status the client enum does not know (a future server state added
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
