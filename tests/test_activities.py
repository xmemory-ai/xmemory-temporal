"""Activities in isolation, via ``ActivityEnvironment``."""

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from xmemory_temporal import XmemoryConfig, XmemoryTimeouts, errors
from xmemory_temporal.activities import XmemoryActivities
from xmemory_temporal.deadline import (
    attempt_clock_unusable,
    clear_attempt_start,
    mark_attempt_start,
    remaining_budget_seconds,
)
from xmemory_temporal.dto import ReadInput, WriteInput, WriteStatusInput

from .fakes import FakeXmemoryInstance, api_error


def _env(**overrides: object) -> ActivityEnvironment:
    """An ActivityEnvironment with a live deadline.

    The mock anchors `started_time` and `scheduled_time` at the epoch, which now
    reads as long expired: the activities fail before doing I/O rather than
    running past a deadline Temporal has abandoned. Real Temporal always sends
    current timestamps, so the tests supply them too.
    """
    env = ActivityEnvironment()
    now = datetime.now(timezone.utc)
    defaults: dict[str, object] = {
        "started_time": now,
        "scheduled_time": now,
        "current_attempt_scheduled_time": now,
    }
    env.info = dataclasses.replace(env.info, **{**defaults, **overrides})  # type: ignore[arg-type]
    return env


def _acts(instance: FakeXmemoryInstance) -> XmemoryActivities:
    acts = XmemoryActivities(XmemoryConfig(instance_id="inst-1"))
    acts.bind(instance)
    return acts


async def test_read_projects_result() -> None:
    fake = FakeXmemoryInstance(read_answer="Alice likes tea")
    acts = _acts(fake)
    out = await _env().run(acts.read, ReadInput(query="what does Alice like?"))
    assert out.reader_result == "Alice likes tea"
    assert fake.count("read") == 1


async def test_write_projects_write_id() -> None:
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    out = await _env().run(acts.write, WriteInput(text="Alice likes tea"))
    assert out.write_id == "w1"
    # default extraction logic flows through
    assert fake.calls[-1].kwargs.get("extraction_logic") == "fast"


async def test_write_start_returns_id() -> None:
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    out = await _env().run(acts.write_start, WriteInput(text="x", extraction_logic="deep"))
    assert out.write_id == "w1"
    assert fake.calls[-1].kwargs.get("extraction_logic") == "deep"


async def test_write_status_projects_enum_to_str() -> None:
    from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]

    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING])
    acts = _acts(fake)
    out = await _env().run(acts.write_status, WriteStatusInput(write_id="w1"))
    assert out.write_status == "processing"  # plain str, no enum in history


async def test_client_error_becomes_application_error() -> None:
    fake = FakeXmemoryInstance()
    fake.fail_write_times(1, api_error(status=401, code="UNAUTHORIZED"))
    acts = _acts(fake)
    with pytest.raises(ApplicationError) as ei:
        await _env().run(acts.write, WriteInput(text="x"))
    assert ei.value.type == errors.TYPE_AUTH_FAILED
    assert ei.value.non_retryable is True


async def test_unbound_activity_fails_fast_non_retryable() -> None:
    # An unbound client is a configuration error — it must fail fast, not retry.
    acts = XmemoryActivities(XmemoryConfig(instance_id="inst-1"))
    with pytest.raises(ApplicationError) as ei:
        await _env().run(acts.read, ReadInput(query="q"))
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
    # One XmemoryActivities is shared across every Worker built from a Client.
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
    # The raw transport string must not reach the failure Temporal
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
        await _env().run(acts.write, WriteInput(text="x"))

    assert ei.value.__cause__ is None  # `from None` dropped the raw cause

    failure = Failure()
    DefaultFailureConverter().to_failure(ei.value, DefaultPayloadConverter(), failure)
    text = _failure_chain_text(failure)
    assert "internal-db.local" not in text
    assert "5432" not in text


async def test_client_timeout_tracks_the_activity_deadline() -> None:
    # The invariant this redesign exists for: the client budget derives from the
    # deadline Temporal assigned this attempt, so a workflow that lowers its
    # start_to_close lowers the client timeout with it. Previously the client
    # read a separate worker-side number and a short workflow budget silently
    # inverted the order (Temporal abandoning the attempt while httpx ran on).
    fake = FakeXmemoryInstance(read_answer="ok")
    acts = _acts(fake)

    for budget in (timedelta(seconds=3), timedelta(seconds=45), timedelta(seconds=120)):
        env = ActivityEnvironment()
        # Realistic anchors: the mock defaults to epoch, which would read as an
        # expired deadline and quietly turn this into a constant-floor test.
        now = datetime.now(timezone.utc)
        env.info = dataclasses.replace(
            env.info,
            start_to_close_timeout=budget,
            schedule_to_close_timeout=budget,
            started_time=now,
            scheduled_time=now,
        )
        await env.run(acts.read, ReadInput(query="q"))
        used = fake.calls[-1].kwargs["timeout"]
        seconds = budget.total_seconds()
        # Below the budget, but *tracking* it: `used < seconds` alone passes even
        # when the derivation collapses to a constant floor for every budget.
        assert used < seconds, f"client must give up first for a {budget} budget"
        assert used >= seconds * 0.5, f"client budget {used}s does not track a {budget} deadline"


async def test_client_timeout_raises_without_a_deadline() -> None:
    # Temporal requires one of the two close timeouts, so this is unreachable in
    # practice; assert the *typed* failure rather than merely "something raised",
    # because to_application_error would turn a re-mapped one into a retryable
    # XmemoryUnknown and retry a misconfiguration that can never come good.
    fake = FakeXmemoryInstance(read_answer="ok")
    acts = _acts(fake)
    env = _env(start_to_close_timeout=None, schedule_to_close_timeout=None)

    with pytest.raises(ApplicationError) as ei:
        await env.run(acts.read, ReadInput(query="q"))

    assert ei.value.type == errors.TYPE_NO_DEADLINE
    assert ei.value.non_retryable is True
    assert fake.calls == []


async def test_schedule_to_close_alone_is_a_valid_deadline() -> None:
    # The only shape that reaches the second half of the `or`.
    fake = FakeXmemoryInstance(read_answer="ok")
    acts = _acts(fake)
    budget = timedelta(seconds=45)
    env = _env(start_to_close_timeout=None, schedule_to_close_timeout=budget)

    await env.run(acts.read, ReadInput(query="q"))

    used = fake.calls[-1].kwargs["timeout"]
    assert used < budget.total_seconds(), "the client must still give up first"


def test_workflow_defaults_match_the_shared_timeout_defaults() -> None:
    # Guards the drift that caused the original defect: the workflow facade and
    # XmemoryTimeouts must not grow independent copies of these numbers again.
    import inspect

    from xmemory_temporal import xmemory_for_workflow

    params = inspect.signature(xmemory_for_workflow).parameters
    t = XmemoryTimeouts()
    assert params["read_timeout"].default == t.read
    assert params["write_timeout"].default == t.write
    assert params["write_start_timeout"].default == t.write_start
    assert params["write_status_timeout"].default == t.write_status


async def test_structured_mutations_skip_extraction() -> None:
    # A structured write carries its own primary keys, so the client gets the
    # mutations verbatim and no extraction_logic: the server applies it without
    # running the extractor, which is what makes it deterministic to retry.
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    mutations = [
        {
            "object_mutation": {
                "object_type": "Customer",
                "update": {"key": {"customer_id": "c-1"}, "values": {"tier": "gold"}},
            }
        }
    ]
    await _env().run(acts.write, WriteInput(structured_mutations=mutations))
    call = fake.calls[-1]
    assert call.kwargs["structured_mutations"] == mutations
    assert "extraction_logic" not in call.kwargs
    assert call.text_or_query == ""


async def test_text_write_still_sends_extraction_logic() -> None:
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    await _env().run(acts.write, WriteInput(text="Alice likes tea"))
    assert fake.calls[-1].kwargs.get("extraction_logic") == "fast"
    assert "structured_mutations" not in fake.calls[-1].kwargs


def test_deadline_arithmetic_fails_closed_on_a_disagreeing_clock() -> None:
    # Two clocks are unavoidable: the monotonic stamp cannot see payload decoding
    # or client-carried interceptors, and `started_time` is a service reading. The
    # pre-interceptor time is their difference, used only when that difference is
    # non-negative; a worker clock reading behind the service makes it unmeasurable
    # and this arithmetic refuses instead of guessing.
    now = datetime.now(timezone.utc)
    base = ActivityEnvironment().info

    # A 60s window with 50s spent queuing before the attempt started leaves 10.
    queued = dataclasses.replace(
        base,
        start_to_close_timeout=None,
        schedule_to_close_timeout=timedelta(seconds=60),
        scheduled_time=now - timedelta(seconds=50),
        started_time=now,
    )
    assert remaining_budget_seconds(queued, 0.0) == 10.0

    # Spent entirely before the attempt: expired, not renewed.
    expired = dataclasses.replace(queued, scheduled_time=now - timedelta(seconds=61))
    left = remaining_budget_seconds(expired, 0.0)
    assert left is not None and left <= 0

    # Time inside the attempt counts against both windows, whoever spent it —
    # including an interceptor that ran before the activity body.
    assert remaining_budget_seconds(queued, 5.0) == 5.0
    stc = dataclasses.replace(base, start_to_close_timeout=timedelta(seconds=30), schedule_to_close_timeout=None)
    assert remaining_budget_seconds(stc, 25.0) == 5.0

    # A worker clock behind the service (entry looks like it has not happened
    # yet) must not inflate the budget past the window.
    skewed = dataclasses.replace(
        queued, started_time=now + timedelta(seconds=60), scheduled_time=now + timedelta(seconds=10)
    )
    assert remaining_budget_seconds(skewed, 0.0) == 10.0

    # Default path, no stamp: time between Temporal starting the attempt and the
    # plugin's interceptor running is charged even though nothing measured it.
    late = dataclasses.replace(
        base,
        start_to_close_timeout=timedelta(seconds=30),
        schedule_to_close_timeout=None,
        started_time=now - timedelta(seconds=10),
    )
    left = remaining_budget_seconds(late)
    assert left is not None and 19.0 < left <= 20.0

    # And the larger of the two always wins, so neither can hide spent time.
    token = mark_attempt_start()
    try:
        stamped = remaining_budget_seconds(late)
        assert stamped is not None and stamped <= 20.0
    finally:
        clear_attempt_start(token)


async def test_an_expired_deadline_makes_no_backend_call() -> None:
    # The helper reporting <= 0 is not enough: rounding that up to a token budget
    # would still send a request whose result nobody reads, and a write could
    # still be accepted after Temporal abandoned the attempt.
    fake = FakeXmemoryInstance(read_answer="ok")
    acts = _acts(fake)
    env = _env(
        start_to_close_timeout=None,
        schedule_to_close_timeout=timedelta(seconds=30),
        scheduled_time=datetime.now(timezone.utc) - timedelta(seconds=31),
    )

    with pytest.raises(ApplicationError) as ei:
        await env.run(acts.read, ReadInput(query="q"))

    assert ei.value.type == errors.TYPE_DEADLINE_EXPIRED
    assert fake.calls == [], "no request may be sent once the deadline is spent"


def test_a_worker_clock_behind_the_service_fails_closed() -> None:
    # `started_time` is the only source for time spent before our interceptor ran.
    # When the worker clock reads behind the service that figure goes negative and
    # no bound on the real value exists: a reserve would be a guess, and a guess
    # that under-shoots lets the client outlive the activity and duplicate a write
    # on retry. Refuse instead, retryably, so a worker with a synced clock can run
    # the attempt. This drives the real calculation: no `elapsed` is passed.
    now = datetime.now(timezone.utc)
    base = ActivityEnvironment().info
    # The service says the attempt began 60s from now: impossible, so the clocks
    # disagree and the pre-interceptor time cannot be measured.
    behind = dataclasses.replace(
        base,
        start_to_close_timeout=timedelta(seconds=10),
        schedule_to_close_timeout=None,
        started_time=now + timedelta(seconds=60),
        scheduled_time=now + timedelta(seconds=60),
    )
    token = mark_attempt_start()
    try:
        assert attempt_clock_unusable(behind), "a clock reading behind the service must be reported"
        left = remaining_budget_seconds(behind)
        # With the clocks agreeing, the measured figure is used and the window is
        # nearly whole.
        agreeing = dataclasses.replace(behind, started_time=now, scheduled_time=now)
        assert not attempt_clock_unusable(agreeing)
        usable = remaining_budget_seconds(agreeing)
    finally:
        clear_attempt_start(token)
    assert left is not None and left <= 0.0, f"skew must fail closed, got {left}s of a 10s window"
    assert usable is not None and usable > 9.0, f"a usable clock must keep the window, got {usable}"


def test_pre_interceptor_time_is_charged_without_a_clock_test() -> None:
    # The integration test that drives a real Worker measures elapsed wall time and
    # so is load-sensitive by nature. This pins the same property deterministically:
    # the attempt began 2s ago by the service's record, the stamp was taken just
    # now, so 2s of the window is already gone.
    now = datetime.now(timezone.utc)
    info = dataclasses.replace(
        ActivityEnvironment().info,
        start_to_close_timeout=timedelta(seconds=10),
        schedule_to_close_timeout=None,
        started_time=now - timedelta(seconds=2),
        scheduled_time=now - timedelta(seconds=2),
    )
    token = mark_attempt_start()
    try:
        left = remaining_budget_seconds(info)
    finally:
        clear_attempt_start(token)
    # ~8s, not the ~10s the stamp alone would report.
    assert left is not None and 7.5 < left < 8.5, f"pre-interceptor time not charged: {left}"


async def test_an_unmeasurable_clock_makes_no_backend_call() -> None:
    # Refusing has to happen before any I/O: the point is that we cannot know how
    # much of the deadline is already gone, so any request might be one the client
    # outlives -- which for a write means a duplicate on retry.
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    now = datetime.now(timezone.utc)
    env = _env(started_time=now + timedelta(seconds=60), scheduled_time=now + timedelta(seconds=60))
    with pytest.raises(ApplicationError) as ei:
        await env.run(acts.write, WriteInput(text="Alice likes tea"))
    assert ei.value.type == errors.TYPE_CLOCK_UNUSABLE
    assert ei.value.non_retryable is False, "another worker with a synced clock should get a turn"
    assert fake.calls == [], "no request may be sent when the deadline cannot be measured"

    # The same activity succeeds once the clocks agree.
    assert await _env().run(acts.write, WriteInput(text="Alice likes tea"))
    assert fake.count("write") == 1


async def test_an_enqueue_does_not_force_deep_extraction() -> None:
    # write_async_start used to default to "deep", which quietly overrode the
    # configured default and changed what a caller pays for. Only write_durable asks
    # for deep, because waiting minutes for a shallow extraction is not its point.
    import inspect

    from xmemory_temporal.workflow_api import WorkflowXmemory

    start = inspect.signature(WorkflowXmemory.write_async_start).parameters["extraction_logic"]
    durable = inspect.signature(WorkflowXmemory.write_durable).parameters["extraction_logic"]
    assert start.default is None, f"write_async_start still forces {start.default!r}"
    assert durable.default == "deep", f"write_durable should ask for deep, got {durable.default!r}"

    # And an omitted value resolves worker-side to the configured default.
    fake = FakeXmemoryInstance()
    acts = XmemoryActivities(XmemoryConfig(instance_id="inst-1", default_extraction_logic="fast"))
    acts.bind(fake)
    await _env().run(acts.write_start, WriteInput(text="x"))
    assert fake.calls[-1].kwargs.get("extraction_logic") == "fast"


async def test_an_empty_mutation_list_is_a_bad_option_not_a_retryable_failure() -> None:
    # The client answers `[]` with a plain exception, which the mapper can only read
    # as retryable -- so Temporal would retry a request that cannot succeed. The check
    # sits outside the try block, whose handler would otherwise remap this verdict.
    fake = FakeXmemoryInstance()
    acts = _acts(fake)
    for call in (acts.write, acts.write_start):
        with pytest.raises(ApplicationError) as ei:
            await _env().run(call, WriteInput(text="", structured_mutations=[]))
        assert ei.value.type == errors.TYPE_BAD_OPTIONS
        assert ei.value.non_retryable is True
    assert fake.calls == [], "no request may be sent for an unusable mutation list"
