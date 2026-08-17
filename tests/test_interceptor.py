"""Auto-capture interceptor: projection, sampling, and fail-open behavior."""

import asyncio
import uuid
import pytest
import threading
import time
import zlib
from typing import Any

from typing_extensions import override

from temporalio import activity
from temporalio.client import Client
from temporalio.client import Interceptor as ClientInterceptor
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Worker,
)

from xmemory_temporal import AutoCaptureConfig, XmemoryConfig, XmemoryPlugin
from xmemory_temporal.interceptor import (
    ProjectorPool,
    ProjectorPoolBusy,
    capture_budget_seconds,
    sampling_bucket,
)

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


async def test_capture_is_skipped_when_the_activity_deadline_leaves_no_room(env: WorkflowEnvironment) -> None:
    # Capture runs inside the wrapped activity, so it spends that activity's
    # budget. On a deadline this tight the enqueue must be dropped rather than
    # pushing the activity over it: a timeout there would fail, and retry, an
    # activity that had already produced its result.
    fake = FakeXmemoryInstance()
    tq = f"tq-{uuid.uuid4()}"
    plugin = XmemoryPlugin(
        XmemoryConfig(instance_id="inst-1"),
        instance=fake,
        auto_capture=AutoCaptureConfig(project=lambda name, result: f"[{name}] {result}"),
    )
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[UserWorkflow],
        activities=[user_activity],
        plugins=[plugin],
    ):
        # A one-second budget leaves no room for a capture enqueue.
        out = await env.client.execute_workflow(
            UserWorkflow.run, args=["hello", 1.0], id=f"wf-{uuid.uuid4()}", task_queue=tq
        )

    assert out == "handled: hello"  # the wrapped activity is untouched
    assert fake.count("write_async") == 0  # capture was dropped, not attempted


def test_capture_budget_never_outlives_the_activity_deadline() -> None:
    # The arithmetic behind the skip above, without the wall-clock.
    # Plenty of room: the configured ceiling applies.
    assert capture_budget_seconds(29.5, 5.0, 5.0) == 5.0
    # Nearly spent: the remainder wins over the ceiling.
    assert capture_budget_seconds(8.0, 5.0, 5.0) == 3.0
    # Nothing left once the margin is honored: skip rather than overrun.
    assert capture_budget_seconds(3.0, 5.0, 5.0) is None

    # A nonsensical margin must not widen the budget past the remainder. Zero
    # reserves no completion gap; a negative one is arithmetic that would hand
    # capture more time than the activity has, so both fall back to a
    # proportional reserve and stay strictly inside what is left.
    for margin in (0.0, -5.0, -5000.0):
        budget = capture_budget_seconds(1.0, 5.0, margin)
        assert budget is not None and budget < 1.0, f"margin {margin} overbudgets capture"
    assert capture_budget_seconds(1000.0, 5.0, -5000.0) == 5.0  # the ceiling still binds
    assert capture_budget_seconds(10.0, -1.0, 1.0) is None  # so does a nonsensical ceiling


def test_sampling_bucket_is_stable_and_crc32_based() -> None:
    # The sampling bucket must be stable across processes (not the
    # process-salted builtin hash()), so a retry on another worker samples the
    # same way. Pin it to the exact crc32 formula.
    aid = "activity-abc-123"
    assert sampling_bucket(aid) == sampling_bucket(aid)
    assert sampling_bucket(aid) == (zlib.crc32(aid.encode("utf-8")) % 1000) / 1000.0
    assert 0.0 <= sampling_bucket(aid) < 1.0


def test_auto_capture_is_the_outermost_interceptor() -> None:
    # Capture measures how much of the activity's deadline is left, so it has to
    # wrap the whole attempt: registered inside another interceptor it cannot see
    # the time that one spends before and after it. The worker wraps in reverse,
    # so outermost means first.
    class _Other(Interceptor):
        pass

    other = _Other()
    plugin = XmemoryPlugin(
        XmemoryConfig(instance_id="inst-1"),
        instance=FakeXmemoryInstance(),
        auto_capture=AutoCaptureConfig(project=lambda name, result: "remember"),
    )
    config = plugin.configure_worker({"interceptors": [other]})  # type: ignore[typeddict-item]

    registered = list(config.get("interceptors") or [])
    assert registered[-1] is other, "a user's interceptor must keep its place"
    assert registered[0] is not other, "auto-capture must be registered first (outermost)"


class _SlowClientInterceptor(ClientInterceptor, Interceptor):
    """A worker interceptor owned by the Client, so registered outside ours."""

    @override
    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        class _Inbound(ActivityInboundInterceptor):
            @override
            async def execute_activity(self, input: ExecuteActivityInput) -> Any:
                await asyncio.sleep(2.0)
                return await self.next.execute_activity(input)

        return _Inbound(next)


async def _write_budget(client: Client, *, budget_s: float, margin_s: int = 1) -> float:
    """Run one write and report the timeout the client library was handed.

    The margin is pinned small so the timeout stays `remaining - margin` for every
    size used here. At the default margin a large enough window crosses into the
    proportional rule, and the two accountings then land on opposite sides of it --
    which makes a single upper bound meaningless.
    """
    fake = FakeXmemoryInstance()
    tq = f"tq-{uuid.uuid4()}"
    plugin = XmemoryPlugin(XmemoryConfig(instance_id="inst-1", client_margin_seconds=margin_s), instance=fake)
    async with Worker(client, task_queue=tq, workflows=[WriteWorkflow], plugins=[plugin]):
        await client.execute_workflow(
            WriteWorkflow.run, args=["remember me", budget_s], id=f"wf-{uuid.uuid4()}", task_queue=tq
        )
    return float(fake.calls[-1].kwargs["timeout"])


async def test_interceptor_time_counts_against_the_activity_deadline(local_env: WorkflowEnvironment) -> None:
    # An activity can only measure its own body, so without a stamp taken at the outer
    # boundary a slow interceptor is invisible: the deadline looks fresh and the client
    # is handed a budget Temporal has already spent.
    #
    # Asserted as a *difference* between two runs of the same workflow, one with the
    # slow interceptor and one without. An absolute bound on elapsed time fails on a
    # loaded runner instead of on the behaviour; load moves both runs together, and
    # only the accounting moves them apart. Real-clock fixture, because a skipped
    # timer would put the service clock past the worker's.
    class _Slow(Interceptor):
        @override
        def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
            class _Inbound(ActivityInboundInterceptor):
                @override
                async def execute_activity(self, input: ExecuteActivityInput) -> object:
                    await asyncio.sleep(2.5)
                    return await self.next.execute_activity(input)

            return _Inbound(next)

    async def budget_with(*interceptors: Interceptor) -> float:
        fake = FakeXmemoryInstance()
        tq = f"tq-{uuid.uuid4()}"
        plugin = XmemoryPlugin(XmemoryConfig(instance_id="inst-1", client_margin_seconds=1), instance=fake)
        async with Worker(
            local_env.client,
            task_queue=tq,
            workflows=[WriteWorkflow],
            plugins=[plugin],
            interceptors=list(interceptors),
        ):
            await local_env.client.execute_workflow(
                WriteWorkflow.run, args=["remember me", 20.0], id=f"wf-{uuid.uuid4()}", task_queue=tq
            )
        return float(fake.calls[-1].kwargs["timeout"])

    baseline = await budget_with()
    charged = await budget_with(_Slow())
    # The interceptor burns 2.5s, so the charged run must lose about that much. Without
    # the stamp the two runs come out within noise of each other.
    assert baseline - charged > 1.5, f"interceptor time not charged: baseline {baseline}s vs charged {charged}s"


async def test_a_projector_still_sees_the_activity_context(env: WorkflowEnvironment) -> None:
    # The projector runs on a worker thread, and `run_in_executor` does not carry
    # contextvars the way `asyncio.to_thread` does. Without an explicit context
    # copy, a projector that reads `activity.info()` raises "Not in activity
    # context" -- capture then silently stops, since capture failures are swallowed.
    seen: list[str] = []

    def project(name: str, result: object) -> str:
        seen.append(activity.info().activity_type)
        return f"remembered {name}"

    fake = FakeXmemoryInstance()
    await _run(env, fake, AutoCaptureConfig(project=project))

    assert seen == ["user_activity"], "the projector could not read the activity context"
    assert fake.count("write_async") == 1


async def test_capture_is_refused_rather_than_queued_when_threads_are_busy() -> None:
    # A projector that blocks never returns its thread. Queueing more work would
    # retain every activity result handed to it while nothing could run, so admission
    # is bounded and the extra capture is skipped instead.
    pool = ProjectorPool(max_threads=1)
    pool.acquire()  # a worker is using it
    try:
        blocked = asyncio.create_task(pool.run(lambda: time.sleep(5), timeout=0.1))
        await asyncio.sleep(0.05)  # let it occupy the only slot
        with pytest.raises(ProjectorPoolBusy):
            await pool.run(lambda: "second", timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            await blocked
        # Still occupied: the slot belongs to the blocked call until it returns.
        with pytest.raises(ProjectorPoolBusy):
            await pool.run(lambda: "third", timeout=1)
    finally:
        pool.release()

    # With no worker using it, the pool refuses work rather than running projectors
    # for a worker that has stopped.
    with pytest.raises(ProjectorPoolBusy):
        await pool.run(lambda: "after release", timeout=1)


async def test_one_worker_stopping_does_not_disable_capture_for_another() -> None:
    # The pool belongs to the plugin, and one plugin can configure several workers.
    # Shutting it down when any single worker stopped left siblings -- and
    # replacement workers -- silently capturing nothing.
    pool = ProjectorPool(max_threads=2)
    pool.acquire()  # worker A
    pool.acquire()  # worker B
    pool.release()  # worker A stops
    assert await pool.run(lambda: "B still captures", timeout=1) == "B still captures"
    pool.release()  # worker B stops
    with pytest.raises(ProjectorPoolBusy):
        await pool.run(lambda: "nobody left", timeout=1)
    pool.acquire()  # a replacement worker revives it
    try:
        assert await pool.run(lambda: "C captures", timeout=1) == "C captures"
    finally:
        pool.release()


async def test_a_wedged_projector_thread_does_not_outlive_the_process() -> None:
    # ThreadPoolExecutor threads are joined at interpreter exit, so one projector
    # blocked forever kept the process alive after its worker had stopped. These are
    # daemon threads instead.
    pool = ProjectorPool(max_threads=1)
    pool.acquire()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await pool.run(lambda: time.sleep(30), timeout=0.1)
    finally:
        pool.release()
    wedged = [t for t in threading.enumerate() if t.name == "xmemory-project"]
    assert wedged, "expected the blocked projector thread to still be running"
    assert all(t.daemon for t in wedged), "a projector thread must not block interpreter exit"
