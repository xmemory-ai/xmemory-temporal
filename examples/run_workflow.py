"""Kick off the example workflow against a running worker."""

import asyncio
import os
import uuid

from temporalio.client import Client

from agent_workflow import TASK_QUEUE, SupportAgentWorkflow


async def main() -> None:
    client = await Client.connect(os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"))
    result = await client.execute_workflow(
        SupportAgentWorkflow.run,
        args=["Alex", "I prefer email over phone calls."],
        id=f"support-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )
    print("agent recalled:", result)


if __name__ == "__main__":
    asyncio.run(main())
