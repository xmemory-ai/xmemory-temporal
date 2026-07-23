"""``XmemoryPlugin`` — the single line a Temporal user adds.

Built on ``temporalio.plugin.SimplePlugin``. Register it on the **client only**
(``Client.connect(plugins=[XmemoryPlugin(config)])``): Temporal automatically
applies a client's plugins to every Worker built from that client, where the
activities and the run context take effect. Do **not** also pass it to the
``Worker`` — the SDK would then apply it twice and fail with "More than one
activity named xmemory_read". (Registering on the worker instead of the client
also works; just never both.)
"""

from contextlib import asynccontextmanager

import httpx
from temporalio.plugin import SimplePlugin

from xmemory_temporal.activities import XmemoryActivities
from xmemory_temporal.client_factory import open_instance
from xmemory_temporal.config import XmemoryConfig
from xmemory_temporal.interceptor import AutoCaptureConfig, build_auto_capture_interceptor
from xmemory_temporal.protocol import XmemoryInstanceProtocol

# The plugin name shows up in users' logs; keep it stable and descriptive. The
# reference string users see in docs is `xmemory_temporal.XmemoryPlugin`.
PLUGIN_NAME = "xmemory"


class XmemoryPlugin(SimplePlugin):
    """Register xmemory memory activities on a Temporal worker.

    Deliberately does NOT install a namespace-wide data converter: that would
    rewrite every payload flowing through the worker, not just xmemory's, which
    a memory library has no business doing. Compose your own converter if you
    need one.
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
            # Test / advanced seam: caller supplied the handle directly. Bind it
            # and skip opening a real client entirely.
            return _bound(self._activities, instance)
        return _opened(self._activities, self._config, api_key=api_key, http_client=http_client)


@asynccontextmanager
async def _bound(activities: XmemoryActivities, instance: XmemoryInstanceProtocol):
    # Bind for THIS Worker's run context (per-Worker, not plugin-lifetime), then
    # reset so a sibling Worker's binding is never clobbered.
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
