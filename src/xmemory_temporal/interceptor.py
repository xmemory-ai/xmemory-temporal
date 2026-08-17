"""Opt-in auto-capture of activity results into xmemory.

An *activity* interceptor, not a workflow one: workflow interceptors re-run on
every replay, so I/O there breaks determinism.

Guardrails: ``project`` decides what to remember, ``sample_rate`` bounds fan-out,
capture is clamped to what is left of the activity's deadline, and a capture
failure never fails that activity.
"""

import asyncio
import contextvars
import logging
import threading
import zlib
from dataclasses import dataclass
from typing import Any, Callable

from typing_extensions import override

from temporalio import activity
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)

from xmemory_temporal.config import XmemoryConfig
from xmemory_temporal.deadline import (
    clear_attempt_start,
    mark_attempt_start,
    remaining_budget_seconds,
)
from xmemory_temporal.dto import WriteInput

logger = logging.getLogger(__name__)

# Never capture our own writes, or capture would recurse. This also skips a
# user activity named `xmemory_*`. See the README's auto-capture caveat.
_OWN_ACTIVITY_PREFIX = "xmemory_"


@dataclass(frozen=True)
class AutoCaptureConfig:
    """How to auto-capture activity results into memory.

    ``project`` receives ``(activity_name, result)`` and returns the text to
    remember, or ``None`` to skip. No default: raw payloads are JSON blobs the
    extraction engine cannot use.
    """

    project: Callable[[str, Any], str | None]
    # Fraction of eligible activities to capture. Deterministic per activity, so a
    # retry samples the same way.
    sample_rate: float = 1.0
    extraction_logic: str = "fast"
    # Ceiling on what a capture enqueue may add to the wrapped activity's
    # elapsed time. The interceptor lowers it further when less than this is
    # left of the activity's deadline.
    capture_timeout_seconds: float = 5.0
    # Threads capture may use for `project`. A projector that hangs consumes one
    # permanently, so this caps how many can be lost.
    projector_threads: int = 4


class AttemptClockInterceptor(Interceptor):
    """Stamps when each attempt entered this worker.

    Registered first, so the stamp precedes every other interceptor. Without it an
    activity only measures its own body and hands the client too large a budget.
    """

    @override
    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _AttemptClockInbound(next)


class _AttemptClockInbound(ActivityInboundInterceptor):
    @override
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        token = mark_attempt_start()
        try:
            return await self.next.execute_activity(input)
        finally:
            clear_attempt_start(token)


def build_auto_capture_interceptor(
    activities: Any,
    config: XmemoryConfig,
    auto_capture: AutoCaptureConfig,
) -> Interceptor:
    """Return a worker ``Interceptor`` that captures activity results."""
    return _AutoCaptureWorkerInterceptor(activities, config, auto_capture)


class ProjectorPool:
    """Runs user projectors off the event loop, bounded, and never at exit's expense.

    * *Daemon threads, not a ThreadPoolExecutor.* Executor threads are joined at
      interpreter exit, so one wedged projector kept the process alive.
    * *Bounded admission, not a queue.* A queue would keep accepting work, and
      retaining every result handed to it, while nothing could run.
    * *Reference-counted.* One plugin can configure several workers, and one
      stopping must not disable capture for the others.
    """

    def __init__(self, max_threads: int) -> None:
        self._slots = threading.BoundedSemaphore(max(1, max_threads))
        self._users = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Register a worker as using this pool."""
        with self._lock:
            self._users += 1

    def release(self) -> None:
        """Unregister a worker. Capture stops only once none are left."""
        with self._lock:
            self._users = max(0, self._users - 1)

    @property
    def open(self) -> bool:
        with self._lock:
            return self._users > 0

    async def run(self, fn: Callable[..., Any], *args: Any, timeout: float) -> Any:
        """Run ``fn`` under ``timeout``, or raise ``ProjectorPoolBusy``.

        The projector runs inside a copy of the caller's context, so
        ``activity.info()`` and anything else contextvar-based still works --
        ``run_in_executor`` alone does not carry it, unlike ``asyncio.to_thread``.
        """
        if not self.open or not self._slots.acquire(blocking=False):
            raise ProjectorPoolBusy
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        context = contextvars.copy_context()

        def deliver(setter: Callable[[Any], None], value: Any) -> None:
            # The waiter may already have timed out and moved on.
            if not future.done():
                setter(value)

        def work() -> None:
            try:
                result = context.run(fn, *args)
            except BaseException as exc:  # noqa: BLE001 - relayed to the awaiting side
                loop.call_soon_threadsafe(deliver, future.set_exception, exc)
            else:
                loop.call_soon_threadsafe(deliver, future.set_result, result)
            finally:
                self._slots.release()

        threading.Thread(target=work, name="xmemory-project", daemon=True).start()
        # Shielded, so a timeout stops *this* coroutine waiting without cancelling a
        # future the thread is still going to resolve.
        return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)


class ProjectorPoolBusy(Exception):
    """No projector slot is free, or no worker is using the pool; capture is skipped."""


class _AutoCaptureWorkerInterceptor(Interceptor):
    def __init__(self, activities: Any, config: XmemoryConfig, auto_capture: AutoCaptureConfig) -> None:
        self._activities = activities
        self._config = config
        self._auto_capture = auto_capture
        self._projector_pool = ProjectorPool(auto_capture.projector_threads)

    def worker_started(self) -> None:
        self._projector_pool.acquire()

    def worker_stopped(self) -> None:
        self._projector_pool.release()

    @override
    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _AutoCaptureActivityInbound(
            next, self._activities, self._config, self._auto_capture, self._projector_pool
        )


class _AutoCaptureActivityInbound(ActivityInboundInterceptor):
    def __init__(
        self,
        next: ActivityInboundInterceptor,
        activities: Any,
        config: XmemoryConfig,
        auto_capture: AutoCaptureConfig,
        projector_pool: ProjectorPool,
    ) -> None:
        super().__init__(next)
        self._activities = activities
        self._config = config
        self._auto_capture = auto_capture
        self._projector_pool = projector_pool

    @override
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        result = await self.next.execute_activity(input)
        try:
            await self._maybe_capture(result)
        except (Exception, asyncio.TimeoutError):
            # Capture must never fail the wrapped activity: errors and timeouts alike.
            logger.warning("xmemory auto-capture skipped; the wrapped activity is unaffected", exc_info=True)
        return result

    async def _maybe_capture(self, result: Any) -> None:
        name = activity.info().activity_type
        if name.startswith(_OWN_ACTIVITY_PREFIX):
            return
        if not self._should_sample():
            return
        budget = self._capture_budget()
        if budget is None:
            logger.debug("xmemory auto-capture skipped: the wrapped activity's deadline is spent")
            return
        # `project` is the caller's synchronous code, so a budget around it is not
        # enough: a slow one spends the activity's deadline itself. It runs in
        # capture's own threads, so a projector that blocks costs a capture slot
        # rather than starving the worker's other `to_thread` calls.
        try:
            text = await self._projector_pool.run(self._auto_capture.project, name, result, timeout=budget)
        except asyncio.TimeoutError:
            logger.warning("xmemory auto-capture skipped: project() exceeded the capture budget")
            return
        except ProjectorPoolBusy:
            logger.warning(
                "xmemory auto-capture skipped: every projector thread is busy. A projector that blocks never "
                "returns its thread, so work is refused rather than queued."
            )
            return
        if not text:
            return
        # Re-checked, because the projector just spent some of it.
        budget = self._capture_budget()
        if budget is None:
            logger.debug("xmemory auto-capture skipped: the wrapped activity's deadline is spent")
            return
        # Enqueue (write_async), not a full synchronous write. Deep extraction
        # still happens server-side; we do not wait for it.
        await asyncio.wait_for(
            self._activities.write_start(WriteInput(text=text, extraction_logic=self._auto_capture.extraction_logic)),
            timeout=budget,
        )

    def _capture_budget(self) -> float | None:
        # No `elapsed=` override: that would skip the pre-interceptor accounting and
        # let capture push an already-successful activity past its deadline.
        remaining = remaining_budget_seconds(activity.info(), allow_unmeasurable=self._config.allow_unmeasurable_clock)
        if remaining is None:
            return None
        return capture_budget_seconds(
            remaining,
            self._auto_capture.capture_timeout_seconds,
            self._config.client_margin_seconds,
        )

    def _should_sample(self) -> bool:
        rate = self._auto_capture.sample_rate
        if rate >= 1.0:
            return True
        if rate <= 0.0:
            return False
        return sampling_bucket(activity.info().activity_id) < rate


def capture_budget_seconds(remaining_seconds: float, ceiling_seconds: float, margin_seconds: float) -> float | None:
    """Seconds capture may take, or ``None`` when it must be skipped.

    Capture spends the wrapped activity's deadline, so an activity that has nearly
    used its budget would be pushed past it and retried, discarding a result it had
    already produced.
    """
    # A margin of zero or less would leave no completion gap, or hand capture more
    # time than the activity has left. Fall back to a proportional reserve.
    usable = remaining_seconds - margin_seconds if margin_seconds > 0 else remaining_seconds * 0.8
    if usable <= 0 or ceiling_seconds <= 0:
        return None
    return min(ceiling_seconds, usable)


def sampling_bucket(activity_id: str) -> float:
    """Stable [0, 1) bucket for an activity id.

    ``zlib.crc32`` rather than the builtin ``hash()``, which is salted per process:
    a retry on another worker would otherwise land in a different bucket. The TS
    port uses a different process-stable hash, which is fine — an activity always
    runs in one runtime.
    """
    return (zlib.crc32(activity_id.encode("utf-8")) % 1000) / 1000.0
