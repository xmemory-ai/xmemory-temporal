"""Activities in isolation, via ``ActivityEnvironment``."""

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from xmemory_temporal import XmemoryConfig, errors
from xmemory_temporal.activities import XmemoryActivities
from xmemory_temporal.dto import ReadInput, WriteInput, WriteStatusInput

from .fakes import FakeXmemoryInstance, api_error


def _acts(instance: FakeXmemoryInstance) -> XmemoryActivities:
    acts = XmemoryActivities(XmemoryConfig(instance_id="inst-1"))
    acts.bind(instance)
    return acts


async def test_read_projects_result() -> None:
    fake = FakeXmemoryInstance(read_answer="Alice likes tea")
    acts = _acts(fake)
    out = await ActivityEnvironment().run(acts.read, ReadInput(query="what does Alice like?"))
    assert out.reader_result == "Alice likes tea"
    assert fake.count("read") == 1


async def test_write_projects_write_id() -> None:
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    out = await ActivityEnvironment().run(acts.write, WriteInput(text="Alice likes tea"))
    assert out.write_id == "w1"
    # default extraction logic flows through
    assert fake.calls[-1].kwargs.get("extraction_logic") == "fast"


async def test_write_start_returns_id() -> None:
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    out = await ActivityEnvironment().run(acts.write_start, WriteInput(text="x", extraction_logic="deep"))
    assert out.write_id == "w1"
    assert fake.calls[-1].kwargs.get("extraction_logic") == "deep"


async def test_write_status_projects_enum_to_str() -> None:
    from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]

    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING])
    acts = _acts(fake)
    out = await ActivityEnvironment().run(acts.write_status, WriteStatusInput(write_id="w1"))
    assert out.write_status == "processing"  # plain str, no enum in history


async def test_client_error_becomes_application_error() -> None:
    fake = FakeXmemoryInstance()
    fake.fail_write_times(1, api_error(status=401, code="UNAUTHORIZED"))
    acts = _acts(fake)
    with pytest.raises(ApplicationError) as ei:
        await ActivityEnvironment().run(acts.write, WriteInput(text="x"))
    assert ei.value.type == errors.TYPE_AUTH_FAILED
    assert ei.value.non_retryable is True


async def test_unbound_activity_fails_fast_non_retryable() -> None:
    # An unbound client is a configuration error — it must fail fast, not retry.
    acts = XmemoryActivities(XmemoryConfig(instance_id="inst-1"))
    with pytest.raises(ApplicationError) as ei:
        await ActivityEnvironment().run(acts.read, ReadInput(query="q"))
    assert ei.value.type == errors.TYPE_NOT_BOUND
    assert ei.value.non_retryable is True


def test_activity_names_are_pinned() -> None:
    # These literals are baked into recorded workflow histories; renaming a
    # constant breaks replay of in-flight workflows, so a change must fail HERE.
    from xmemory_temporal import activities

    assert activities.ACTIVITY_READ == "xmemory_read"
    assert activities.ACTIVITY_WRITE == "xmemory_write"
    assert activities.ACTIVITY_WRITE_START == "xmemory_write_start"
    assert activities.ACTIVITY_WRITE_STATUS == "xmemory_write_status"


async def test_binding_is_per_context_not_shared() -> None:
    # N3: one XmemoryActivities is shared across every Worker built from a Client.
    # Two concurrent run-contexts binding DIFFERENT clients must not clobber each
    # other (the old shared attribute was last-bind-wins). With the ContextVar,
    # each context — copied at task creation — resolves its own instance.
    import asyncio

    acts = XmemoryActivities(XmemoryConfig(instance_id="inst-1"))
    fake_a, fake_b = FakeXmemoryInstance(), FakeXmemoryInstance()
    resolved: dict[str, object] = {}

    async def run_context(name: str, fake: FakeXmemoryInstance) -> None:
        token = acts.bind(fake)
        try:
            await asyncio.sleep(0.01)  # force the two tasks to interleave
            resolved[name] = acts.instance
        finally:
            acts.unbind(token)

    await asyncio.gather(
        asyncio.create_task(run_context("a", fake_a)),
        asyncio.create_task(run_context("b", fake_b)),
    )
    assert resolved["a"] is fake_a
    assert resolved["b"] is fake_b


def _failure_chain_text(failure) -> str:
    parts = [failure.message, failure.stack_trace]
    if failure.HasField("cause"):
        parts.append(_failure_chain_text(failure.cause))
    return " ".join(p for p in parts if p)


async def test_no_transport_detail_in_serialized_failure_chain() -> None:
    # F3 / N2: the raw transport string must not reach the failure Temporal
    # persists to cleartext history — not in the message AND not in the cause
    # chain. `raise ... from None` drops __cause__ and suppresses __context__;
    # `from exc` (or a bare raise) would re-leak the unsanitized XmemoryAPIError.
    from temporalio.api.failure.v1 import Failure
    from temporalio.converter import DefaultFailureConverter, DefaultPayloadConverter
    from xmemory._exceptions import XmemoryAPIError

    leaky = XmemoryAPIError("Connection error: host='internal-db.local' port=5432", status=None, code=None)
    fake = FakeXmemoryInstance()
    fake.fail_write_times(1, leaky)
    acts = _acts(fake)
    with pytest.raises(ApplicationError) as ei:
        await ActivityEnvironment().run(acts.write, WriteInput(text="x"))

    assert ei.value.__cause__ is None  # `from None` dropped the raw cause

    failure = Failure()
    DefaultFailureConverter().to_failure(ei.value, DefaultPayloadConverter(), failure)
    text = _failure_chain_text(failure)
    assert "internal-db.local" not in text
    assert "5432" not in text
