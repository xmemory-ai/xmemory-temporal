"""Auto-capture interceptor: projection, sampling, and fail-open behavior."""

import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from xmemory_temporal import AutoCaptureConfig, XmemoryConfig, XmemoryPlugin

from .fakes import FakeXmemoryInstance
from .workflows import UserWorkflow, WriteWorkflow, user_activity


async def _run(env: WorkflowEnvironment, fake: FakeXmemoryInstance, auto_capture: AutoCaptureConfig) -> None:
    tq = f"tq-{uuid.uuid4()}"
    plugin = XmemoryPlugin(XmemoryConfig(instance_id="inst-1"), instance=fake, auto_capture=auto_capture)
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[UserWorkflow],
        activities=[user_activity],
        plugins=[plugin],
    ):
        await env.client.execute_workflow(
            UserWorkflow.run, "the user said hello", id=f"wf-{uuid.uuid4()}", task_queue=tq
        )


# Capture goes through the ENQUEUE path (write_async), not a full synchronous
# write, so the fake records it as "write_async".
async def test_projection_captures_result(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    await _run(env, fake, AutoCaptureConfig(project=lambda name, result: f"[{name}] {result}"))
    writes = [c for c in fake.calls if c.method == "write_async"]
    assert len(writes) == 1
    assert "handled: the user said hello" in writes[0].text_or_query


async def test_projection_none_skips_capture(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    await _run(env, fake, AutoCaptureConfig(project=lambda name, result: None))
    assert fake.count("write_async") == 0


async def test_zero_sample_rate_skips(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    await _run(
        env,
        fake,
        AutoCaptureConfig(project=lambda name, result: "remember", sample_rate=0.0),
    )
    assert fake.count("write_async") == 0


async def test_capture_failure_does_not_fail_activity(env: WorkflowEnvironment) -> None:
    from .fakes import api_error

    fake = FakeXmemoryInstance()
    fake.fail_write_times(10, api_error(status=500))
    # The user workflow must still complete even though every capture enqueue fails.
    await _run(env, fake, AutoCaptureConfig(project=lambda name, result: "remember"))
    # Capture was attempted (and swallowed), the activity result was unaffected.
    assert fake.count("write_async") >= 1


async def test_own_write_activity_is_not_captured(env: WorkflowEnvironment) -> None:
    # The recursion guard, exercised for real. A workflow that calls mem.write()
    # dispatches the `xmemory_write` ACTIVITY, which DOES pass through the
    # auto-capture interceptor. The guard (activity name starts with "xmemory_")
    # must skip it — otherwise capture would re-capture xmemory's own writes.
    # With a projection that fires on everything, the only memory op is the
    # user's write itself: the capture path must NOT fire. (Delete the guard and
    # write_async goes to 1, failing this test — i.e. it is not vacuous.)
    fake = FakeXmemoryInstance()
    tq = f"tq-{uuid.uuid4()}"
    plugin = XmemoryPlugin(
        XmemoryConfig(instance_id="inst-1"),
        instance=fake,
        auto_capture=AutoCaptureConfig(project=lambda name, result: f"[{name}]"),
    )
    async with Worker(env.client, task_queue=tq, workflows=[WriteWorkflow], plugins=[plugin]):
        await env.client.execute_workflow(WriteWorkflow.run, "remember me", id=f"wf-{uuid.uuid4()}", task_queue=tq)
    assert fake.count("write") == 1  # the user's write happened
    assert fake.count("write_async") == 0  # its result was NOT captured (guard worked)


def test_sampling_bucket_is_stable_and_crc32_based() -> None:
    # F4: the sampling bucket must be stable across processes (not the
    # process-salted builtin hash()), so a retry on another worker samples the
    # same way. Pin it to the exact crc32 formula.
    import zlib

    from xmemory_temporal.interceptor import sampling_bucket

    aid = "activity-abc-123"
    assert sampling_bucket(aid) == sampling_bucket(aid)
    assert sampling_bucket(aid) == (zlib.crc32(aid.encode("utf-8")) % 1000) / 1000.0
    assert 0.0 <= sampling_bucket(aid) < 1.0
