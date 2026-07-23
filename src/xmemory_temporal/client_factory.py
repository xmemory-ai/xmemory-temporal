"""Construct and own the async xmemory client for a worker's lifetime.

One ``AsyncXmemoryClient`` (and its httpx connection pool) is opened per worker
and shared across all concurrent activity executions. Building a client per
activity invocation would mean a fresh TCP+TLS handshake per memory op, which is
ruinous when an agent reads memory several times per turn.
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx

from xmemory_temporal.config import XmemoryConfig
from xmemory_temporal.protocol import XmemoryInstanceProtocol


@asynccontextmanager
async def open_instance(
    config: XmemoryConfig,
    *,
    api_key: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> AsyncIterator[XmemoryInstanceProtocol]:
    """Yield a bound instance handle, closing the client on exit.

    ``api_key`` overrides the environment lookup; ``http_client`` supplies a
    caller-owned transport (which the xmemory client will not close).
    """
    # Imported lazily so importing this package never hard-requires xmemory-ai
    # at module load — useful for tests that inject a fake and never open a real
    # client.
    from xmemory import AsyncXmemoryClient

    key = api_key or config.resolve_api_key()
    # Default per-request timeout for the client. Every activity overrides this
    # with a value derived from its own start_to_close (see activities.py); this
    # is only the fallback for any un-overridden call.
    kwargs: dict[str, Any] = {"api_key": key, "timeout": config.timeouts.client_timeout(config.timeouts.read_seconds)}
    if http_client is not None:
        # The client rejects `url` + `http_client` together — the caller sets
        # base_url on their own transport. It also leaves a caller-supplied
        # client open on close (`_owns_client` is False), which is what we want.
        kwargs["http_client"] = http_client
    elif config.url is not None:
        kwargs["url"] = config.url

    # `async with` closes only a client-owned transport, so this is safe for
    # both the owned and the caller-supplied case.
    async with AsyncXmemoryClient(**kwargs) as client:
        yield client.instance(config.instance_id)
