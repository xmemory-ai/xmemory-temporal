"""Shared fixtures: Temporal test environments."""

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


@pytest_asyncio.fixture
async def local_env() -> AsyncIterator[WorkflowEnvironment]:
    """A real-clock environment, for assertions about elapsed wall time.

    The time-skipping server runs its own clock, so `started_time` there is not
    comparable to the worker's `now()` -- deadline code falls back to its
    monotonic stamp, which is safe but hides what these tests measure.
    """
    environment = await WorkflowEnvironment.start_local()
    try:
        yield environment
    finally:
        await environment.shutdown()
