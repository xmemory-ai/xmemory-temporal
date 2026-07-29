"""A minimal Temporal worker wired to xmemory.

First create an instance with a matching schema (once):

    export XMEM_INSTANCE_ID="$(XMEM_API_KEY=xmem_... uv run python examples/setup_memory.py)"

Start a local Temporal dev server:

    temporal server start-dev

Then run the worker:

    XMEM_API_KEY=xmem_... uv run python examples/worker.py

and, in another terminal, ``python examples/run_workflow.py`` to drive it.
"""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from xmemory_temporal import XmemoryConfig, XmemoryPlugin

from agent_workflow import TASK_QUEUE, SupportAgentWorkflow


async def main() -> None:
    config = XmemoryConfig(
        instance_id=os.environ["XMEM_INSTANCE_ID"],
        url=os.environ.get("XMEM_API_URL"),
    )
    plugin = XmemoryPlugin(config)

    # Register the plugin on the CLIENT only. Temporal automatically applies a
    # client's plugins to every Worker built from that client, so passing it to
    # the Worker as well would register the activities twice
    # ("More than one activity named xmemory_read").
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"), plugins=[plugin])

    worker = Worker(client, task_queue=TASK_QUEUE, workflows=[SupportAgentWorkflow])
    print(f"worker running on task queue {TASK_QUEUE!r} — Ctrl-C to stop")
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Ctrl-C cancels `worker.run()`, which drains the worker; swallow the
        # resulting KeyboardInterrupt so the example exits quietly.
        print("\nworker stopped")
