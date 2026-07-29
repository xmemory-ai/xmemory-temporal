"""``XmemoryPlugin`` — the single line a Temporal user adds.

Register on the client only (``Client.connect(plugins=[...])``); Workers inherit
it. Passing it to both registers the activities twice and fails with "More than
one activity named xmemory_read".
"""

from contextlib import asynccontextmanager

import httpx
from temporalio.plugin import SimplePlugin

from xmemory_temporal.activities import XmemoryActivities
from xmemory_temporal.client_factory import open_instance
from xmemory_temporal.config import XmemoryConfig
from xmemory_temporal.interceptor import AutoCaptureConfig, build_auto_capture_interceptor
from xmemory_temporal.protocol import XmemoryInstanceProtocol

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

        interceptors = []
        if auto_capture is not None:
            interceptors.append(build_auto_capture_interceptor(self._activities, config, auto_capture))

        super().__init__(
            name=PLUGIN_NAME,
            activities=self._activities.as_sequence(),
            interceptors=interceptors or None,
            run_context=lambda: self._run_context(api_key=api_key, http_client=http_client, instance=instance),
        )

    def _run_context(
        self,
        *,
        api_key: str | None,
        http_client: httpx.AsyncClient | None,
        instance: XmemoryInstanceProtocol | None,
    ):
        if instance is not None:
            # Caller supplied a handle (tests / custom transport): bind it as-is.
            return _bound(self._activities, instance)
        return _opened(self._activities, self._config, api_key=api_key, http_client=http_client)


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
