"""End-to-end against a real xmemory backend and a local Temporal dev server.

Skipped unless ``XMEM_API_KEY`` and ``XMEM_INSTANCE_ID`` are set. Run before a
release:

    XMEM_API_KEY=xmem_... XMEM_INSTANCE_ID=... \\
        uv run --directory integrations/temporal/python pytest -m live

It writes a fact and then reads it back through real activities, proving the
whole path — plugin, client, and durable write loop — works against production
shapes, not just the fake.
"""

import asyncio
import contextlib
import os
import uuid

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from xmemory_temporal import XmemoryConfig, XmemoryPlugin

from .workflows import DurableWriteWorkflow, ReadWorkflow

pytestmark = pytest.mark.live

_HAS_CREDS = bool(os.environ.get("XMEM_API_KEY") and os.environ.get("XMEM_INSTANCE_ID"))
skip_no_creds = pytest.mark.skipif(not _HAS_CREDS, reason="set XMEM_API_KEY and XMEM_INSTANCE_ID to run")


@skip_no_creds
async def test_write_then_read_roundtrip() -> None:
    instance_id = os.environ["XMEM_INSTANCE_ID"]
    config = XmemoryConfig(instance_id=instance_id, url=os.environ.get("XMEM_API_URL"))

    subject = f"TestSubject{uuid.uuid4().hex[:8]}"
    fact = f"{subject} is a software engineer based in Berlin."

    # NOTE: manage the ephemeral server without `async with`. Its shutdown() can
    # surface asyncio.CancelledError under pytest-asyncio's loop finalization
    # (observed with a real xmemory client, whose httpx pool closes at worker
    # exit) — and inside `async with` that cancelled teardown would replace a
    # perfectly good test result with a failure. The server is a throwaway torn
    # down with the process, so a cancelled shutdown is benign; suppress it.
    env = await WorkflowEnvironment.start_local()
    try:
        tq = f"tq-{uuid.uuid4()}"
        async with Worker(
            env.client,
            task_queue=tq,
            workflows=[DurableWriteWorkflow, ReadWorkflow],
            plugins=[XmemoryPlugin(config)],
        ):
            status = await env.client.execute_workflow(
                DurableWriteWorkflow.run, fact, id=f"wf-{uuid.uuid4()}", task_queue=tq
            )
            assert status == "completed"

            answer = await env.client.execute_workflow(
                ReadWorkflow.run, f"Where is {subject} based?", id=f"wf-{uuid.uuid4()}", task_queue=tq
            )
        assert answer is not None
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await env.shutdown()
