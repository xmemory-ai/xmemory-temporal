"""Opt-in auto-capture of activity results into xmemory.

Off by default. Enable with ``XmemoryPlugin(config, auto_capture=AutoCaptureConfig(...))``.

**Why an activity interceptor and not a workflow interceptor.** A
``WorkflowInboundInterceptor`` runs *inside workflow context* and is re-executed
on every replay; the moment it performs I/O — or reaches for the client — it
breaks determinism, and it typically appears to work in dev, diverging only
under cache eviction or a worker restart. That is exactly the bug class the
partner review targets. An ``ActivityInboundInterceptor`` runs outside the
replay path entirely, so the hazard does not exist here.

Four guardrails, all mandatory:

* a user-supplied ``project`` decides what (if anything) to remember — returning
  ``None`` skips capture, so nothing is written without an explicit opt-in;
* ``sample_rate`` bounds fan-out, since a chatty agent can otherwise turn one
  memory write into hundreds and burn quota invisibly;
* capture is an **enqueue** (``write_async``) bounded by a short timeout well
  below the wrapped activity's budget — a full synchronous write here would add
  its latency to the user activity's ``start_to_close`` and could time it out,
  re-running an already-successful (possibly non-idempotent) activity;
* a capture failure — error *or* timeout — is swallowed: remembering something
  must never fail, or slow, the user's actual activity.
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

# Activity names this package itself registers — never capture our own writes,
# or auto-capture would recurse. NOTE this also silently skips any of the USER's
# own activities named with this prefix; a user activity called "xmemory_sync"
# would not be auto-captured. Documented in the README auto-capture section.
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
    # Fraction of eligible activities to capture, in [0.0, 1.0]. Defaults to 1.0
    # (capture every eligible activity) — sampling is opt-in, so set below 1.0 to
    # bound fan-out / quota on a chatty agent. Deterministic per activity (a
    # stable hash of the activity id), not random, so a retry samples the same way.
    sample_rate: float = 1.0
    extraction_logic: str = "fast"
    # Hard cap on how long a capture enqueue may add to the wrapped activity.
    # Kept small — capture is best-effort and must not eat the activity's budget.
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

    Uses ``zlib.crc32``, NOT the builtin ``hash()`` — ``hash()`` of a str is
    salted per process (``PYTHONHASHSEED``), so across a multi-worker fleet a
    retry on another worker would land in a different bucket and flip the
    sampling decision. crc32 is process-stable everywhere. (The TS port uses a
    different but also process-stable hash — a Java-hashCode-style
    ``h*31 + charCode`` — so the two languages bucket differently; that is fine,
    since a given activity always runs in one language runtime.)
    """
    return (zlib.crc32(activity_id.encode("utf-8")) % 1000) / 1000.0
