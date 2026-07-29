"""Opt-in auto-capture of activity results into xmemory.

Off by default; enable with ``XmemoryPlugin(config, auto_capture=...)``.

An **activity** interceptor, not a workflow one: a workflow interceptor re-runs
on every replay, so any I/O there breaks determinism, usually only visible
under cache eviction or a worker restart. Activity interceptors sit outside the
replay path.

Four guardrails: a user-supplied ``project`` decides what to remember (``None``
skips); ``sample_rate`` bounds fan-out; capture is an enqueue bounded well below
the wrapped activity's budget; and a capture failure never fails that activity.
"""

import asyncio
import logging
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
from xmemory_temporal.dto import WriteInput

logger = logging.getLogger(__name__)

# Never capture our own writes, or capture would recurse. This also skips a
# user activity named `xmemory_*`. See the README's auto-capture caveat.
_OWN_ACTIVITY_PREFIX = "xmemory_"


@dataclass(frozen=True)
class AutoCaptureConfig:
    """How to auto-capture activity results into memory.

    ``project`` receives ``(activity_name, result)`` and returns the text to
    remember, or ``None`` to skip. There is no default: capturing raw payloads
    would write JSON blobs an extraction engine cannot use, so opting in means
    saying what to remember.
    """

    project: Callable[[str, Any], str | None]
    # Fraction of eligible activities to capture. Defaults to 1.0 (sampling is
    # opt-in). Deterministic per activity id, so a retry samples the same way.
    sample_rate: float = 1.0
    extraction_logic: str = "fast"
    # Cap on what a capture enqueue may add to the wrapped activity's budget.
    capture_timeout_seconds: float = 5.0


def build_auto_capture_interceptor(
    activities: Any,
    config: XmemoryConfig,
    auto_capture: AutoCaptureConfig,
) -> Interceptor:
    """Return a worker ``Interceptor`` that captures activity results."""
    return _AutoCaptureWorkerInterceptor(activities, config, auto_capture)


class _AutoCaptureWorkerInterceptor(Interceptor):
    def __init__(self, activities: Any, config: XmemoryConfig, auto_capture: AutoCaptureConfig) -> None:
        self._activities = activities
        self._config = config
        self._auto_capture = auto_capture

    @override
    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _AutoCaptureActivityInbound(next, self._activities, self._config, self._auto_capture)


class _AutoCaptureActivityInbound(ActivityInboundInterceptor):
    def __init__(
        self,
        next: ActivityInboundInterceptor,
        activities: Any,
        config: XmemoryConfig,
        auto_capture: AutoCaptureConfig,
    ) -> None:
        super().__init__(next)
        self._activities = activities
        self._config = config
        self._auto_capture = auto_capture

    @override
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        result = await self.next.execute_activity(input)
        try:
            await self._maybe_capture(result)
        except (Exception, asyncio.TimeoutError):
            # Never let capture fail — or exceed its own budget on — the wrapped
            # activity. Swallow errors and timeouts alike.
            logger.warning("xmemory auto-capture skipped; the wrapped activity is unaffected", exc_info=True)
        return result

    async def _maybe_capture(self, result: Any) -> None:
        name = activity.info().activity_type
        if name.startswith(_OWN_ACTIVITY_PREFIX):
            return
        if not self._should_sample():
            return
        text = self._auto_capture.project(name, result)
        if not text:
            return
        # Enqueue (write_async), not a full synchronous write, and bound it hard
        # below the wrapped activity's budget. Deep extraction still happens
        # server-side; we do not wait for it.
        await asyncio.wait_for(
            self._activities.write_start(WriteInput(text=text, extraction_logic=self._auto_capture.extraction_logic)),
            timeout=self._auto_capture.capture_timeout_seconds,
        )

    def _should_sample(self) -> bool:
        rate = self._auto_capture.sample_rate
        if rate >= 1.0:
            return True
        if rate <= 0.0:
            return False
        return sampling_bucket(activity.info().activity_id) < rate


def sampling_bucket(activity_id: str) -> float:
    """Stable [0, 1) bucket for an activity id.

    Uses ``zlib.crc32`` rather than the builtin ``hash()``, whose value for a str is
    salted per process (``PYTHONHASHSEED``), so across a multi-worker fleet a
    retry on another worker would land in a different bucket and flip the
    sampling decision. crc32 is process-stable everywhere. (The TS port uses a
    different but also process-stable hash — a Java-hashCode-style
    ``h*31 + charCode`` — so the two languages bucket differently; that is fine,
    since a given activity always runs in one language runtime.)
    """
    return (zlib.crc32(activity_id.encode("utf-8")) % 1000) / 1000.0
