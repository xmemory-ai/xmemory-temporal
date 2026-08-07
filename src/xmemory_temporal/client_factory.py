"""One xmemory client per worker, shared by all concurrent activities.

Per-invocation clients would mean a TCP+TLS handshake per memory op.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from xmemory_temporal.config import XmemoryConfig, XmemoryTimeouts, client_timeout_seconds
from xmemory_temporal.protocol import XmemoryInstanceProtocol


@asynccontextmanager
async def open_instance(
    config: XmemoryConfig,
    *,
    api_key: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> AsyncGenerator[XmemoryInstanceProtocol, None]:
    """Yield a bound instance handle, closing the client on exit.

    ``api_key`` overrides the environment lookup; ``http_client`` supplies a
    caller-owned transport (which the xmemory client will not close).
    """
    # Lazy import: injecting a fake must not require xmemory-ai at all.
    from xmemory import AsyncXmemoryClient

    key = api_key or config.resolve_api_key()
    # Fallback only; every activity overrides this per call (activities.py).
    default_timeout = client_timeout_seconds(XmemoryTimeouts().read_seconds, config.client_margin_seconds)
    kwargs: dict[str, Any] = {"api_key": key, "timeout": default_timeout}
    if http_client is not None:
        # `url` + `http_client` together is rejected; the caller sets base_url.
        kwargs["http_client"] = http_client
    elif config.url is not None:
        kwargs["url"] = config.url

    # Closes only a client-owned transport, so a caller's stays open.
    async with AsyncXmemoryClient(**kwargs) as client:
        yield client.instance(config.instance_id)
