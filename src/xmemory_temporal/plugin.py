"""``XmemoryPlugin`` — the single line a Temporal user adds.

Register on the client only (``Client.connect(plugins=[...])``); Workers inherit
it. Passing it to both registers the activities twice and fails with "More than
one activity named xmemory_read".
"""

import logging
from typing import Any
from contextlib import asynccontextmanager

import httpx
from temporalio.plugin import SimplePlugin
from temporalio.worker import Interceptor as WorkerInterceptor
from temporalio.worker import WorkerConfig
from typing_extensions import override

from xmemory_temporal.activities import XmemoryActivities
from xmemory_temporal.client_factory import open_instance
from xmemory_temporal.config import XmemoryConfig
from xmemory_temporal.interceptor import AttemptClockInterceptor, AutoCaptureConfig, build_auto_capture_interceptor
from xmemory_temporal.protocol import XmemoryInstanceProtocol

logger = logging.getLogger(__name__)


def _is_worker_interceptor(candidate: object) -> bool:
    """Whether a Client-configured interceptor actually wraps activity execution.

    Being a worker ``Interceptor`` is not enough: the base class supplies a
    pass-through ``intercept_activity``, and Temporal's own time-skipping client
    interceptor inherits it. Only an override can spend time around an activity, so
    only an override matters here.
    """
    if not isinstance(candidate, WorkerInterceptor):
        return False
    return type(candidate).intercept_activity is not WorkerInterceptor.intercept_activity


# Appears in users' logs; keep stable.
PLUGIN_NAME = "xmemory"


class XmemoryPlugin(SimplePlugin):
    """Register xmemory memory activities on a Temporal worker.

    Installs no data converter: one would rewrite every payload on the worker,
    not just xmemory's. Compose your own if you need one.
    """

    def __init__(
        self,
        config: XmemoryConfig,
        *,
        api_key: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        instance: XmemoryInstanceProtocol | None = None,
        auto_capture: AutoCaptureConfig | None = None,
    ) -> None:
        self._config = config
        self._activities = XmemoryActivities(config)

        # Registered in configure_worker: SimplePlugin appends, which would make
        # ours innermost.
        self._auto_capture_interceptor = (
            build_auto_capture_interceptor(self._activities, config, auto_capture) if auto_capture is not None else None
        )

        super().__init__(
            name=PLUGIN_NAME,
            activities=self._activities.as_sequence(),
            run_context=lambda: self._run_context(api_key=api_key, http_client=http_client, instance=instance),
        )

    @override
    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        config = super().configure_worker(config)
        # The attempt clock goes first: every deadline here measures from it.
        ours: list[object] = [AttemptClockInterceptor()]
        if self._auto_capture_interceptor is not None:
            # First, so it is the OUTERMOST interceptor (the worker wraps in
            # reverse): capture measures how much of the activity's deadline is
            # left, and an interceptor registered inside another cannot see the
            # time that one spends before and after it. The trade is that a user
            # interceptor no longer wraps our capture call.
            ours.append(self._auto_capture_interceptor)
        config["interceptors"] = [*ours, *(config.get("interceptors") or [])]  # type: ignore[list-item]
        self._check_outer_interceptors(config)
        return config

    def _check_outer_interceptors(self, config: WorkerConfig) -> None:
        """Refuse, or warn about, anything that will wrap this plugin's interceptors.

        "Outermost" is only true of what the worker was configured with here. The
        SDK prepends interceptors carried by the Client, so those run *around* ours:
        the time they spend before the activity is at least visible through
        ``started_time``, but what they spend *after* it is invisible, and no
        accounting here can reserve for it.

        For plain activities that costs a slightly optimistic budget, which the
        client margin absorbs — a warning is enough. For auto-capture it breaks a
        guarantee: a capture that fits its budget can still be pushed past
        Start-to-Close by that outer work, and Temporal then retries an activity
        that already succeeded, running the projector and the capture again. Since
        the README promises capture cannot cause that, the combination is refused
        rather than quietly weakened.
        """
        client = config.get("client")
        # `active_config=True`: the initial config omits interceptors contributed by
        # *other* Client plugins, and Temporal wraps activities with those too.
        # Guarded, because the keyword is newer than the SDK range this supports.
        try:
            configured = client.config(active_config=True) if client is not None else {}  # type: ignore[union-attr]
        except TypeError:
            configured = getattr(client, "config", lambda: {})() or {}
        carried = [i for i in (configured.get("interceptors") or []) if _is_worker_interceptor(i)]
        if not carried:
            return
        if self._auto_capture_interceptor is not None:
            raise ValueError(
                f"xmemory auto-capture cannot be used with {len(carried)} activity interceptor(s) carried by the "
                f"Temporal Client. They wrap this plugin's own, so what they spend after an activity body is "
                f"invisible here, and capture could push an already-successful activity past its "
                f"start_to_close — Temporal would retry it and run your projector twice. Register those "
                f"interceptors on the Worker (listed after XmemoryPlugin) instead, or construct "
                f"XmemoryPlugin without auto_capture."
            )
        logger.warning(
            "xmemory: %d activity interceptor(s) come from the Temporal Client and wrap this plugin's own. "
            "Time they spend after the activity body is invisible to xmemory's deadline accounting: the client "
            "margin covers a small overrun, but that work is unbounded, so enough of it can still let Temporal "
            "time the activity out after the backend already applied the write. Register them on the Worker "
            "(after XmemoryPlugin), or keep them short.",
            len(carried),
        )

    def _run_context(
        self,
        *,
        api_key: str | None,
        http_client: httpx.AsyncClient | None,
        instance: XmemoryInstanceProtocol | None,
    ):
        inner = (
            # Caller supplied a handle (tests / custom transport): bind it as-is.
            _bound(self._activities, instance)
            if instance is not None
            else _opened(self._activities, self._config, api_key=api_key, http_client=http_client)
        )
        return _with_capture_shutdown(inner, self._auto_capture_interceptor)


@asynccontextmanager
async def _with_capture_shutdown(inner: Any, capture: Any):
    """Track this worker's use of capture's projector threads.

    Reference-counted rather than closed outright: one plugin can configure several
    workers, and stopping one must not disable capture for a sibling or for a
    replacement worker started later. The threads themselves are daemons, so a
    wedged projector cannot hold up shutdown either way.
    """
    if capture is not None:
        capture.worker_started()
    try:
        async with inner:
            yield
    finally:
        if capture is not None:
            capture.worker_stopped()


@asynccontextmanager
async def _bound(activities: XmemoryActivities, instance: XmemoryInstanceProtocol):
    # Per-Worker binding; reset so a sibling Worker's is never clobbered.
    token = activities.bind(instance)
    try:
        yield
    finally:
        activities.unbind(token)


@asynccontextmanager
async def _opened(
    activities: XmemoryActivities,
    config: XmemoryConfig,
    *,
    api_key: str | None,
    http_client: httpx.AsyncClient | None,
):
    async with open_instance(config, api_key=api_key, http_client=http_client) as instance:
        token = activities.bind(instance)
        try:
            yield
        finally:
            activities.unbind(token)
