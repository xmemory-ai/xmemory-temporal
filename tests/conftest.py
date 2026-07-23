"""Shared fixtures: a time-skipping Temporal environment and a task-queue id."""

from collections.abc import AsyncIterator

import pytest_asyncio
from temporalio.testing import WorkflowEnvironment


@pytest_asyncio.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    environment = await WorkflowEnvironment.start_time_skipping()
    try:
        yield environment
    finally:
        await environment.shutdown()
