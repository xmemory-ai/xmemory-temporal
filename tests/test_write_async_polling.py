"""The durable write loop: termination, backoff, and failure handling.

All of these model a multi-minute deep write but run in milliseconds — the whole
argument for polling from the workflow rather than inside one long activity: the
time-skipping environment fast-forwards ``workflow.sleep`` instantly.
"""

import uuid
from datetime import timedelta

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from xmemory._models import WriteQueueStatus  # type: ignore[import-not-found]

from xmemory_temporal import XmemoryConfig, XmemoryPlugin, errors

from .fakes import FakeXmemoryInstance, api_error
from .workflows import (
    BadPollPolicyDurableWriteWorkflow,
    DurableWriteWorkflow,
    ReservedTimeoutTypeWorkflow,
    SingleAttemptBadCoefficientWorkflow,
    StrictPollDurableWriteWorkflow,
)


def _app_error(exc: BaseException) -> ApplicationError:
    """Walk Temporal's `.cause` chain down to the underlying ApplicationError."""
    err: BaseException | None = exc
    while err is not None and not isinstance(err, ApplicationError):
        err = getattr(err, "cause", None)
    assert isinstance(err, ApplicationError)
    return err


async def _run(
    env: WorkflowEnvironment,
    fake: FakeXmemoryInstance,
    *,
    poll_s: float = 1.0,
    cap_s: float = 30.0,
    wait_s: float = 900.0,
    wf_id: str | None = None,
    single_attempt: bool = False,
) -> str:
    """Drive one durable write against `fake`, with the cadence under test."""
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[DurableWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1", allow_unmeasurable_clock=True), instance=fake)],
    ):
        return await env.client.execute_workflow(
            DurableWriteWorkflow.run,
            args=["remember this", poll_s, cap_s, wait_s, single_attempt],
            id=wf_id or f"wf-{uuid.uuid4()}",
            task_queue=tq,
        )


async def _history(env: WorkflowEnvironment, wf_id: str) -> list:
    """Every history event for a finished workflow, for assertions on shape."""
    return [event async for event in env.client.get_workflow_handle(wf_id).fetch_history_events()]


async def test_polls_to_completion(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    fake.status_sequence(
        [
            WriteQueueStatus.QUEUED,
            WriteQueueStatus.PROCESSING,
            WriteQueueStatus.EXTRACTING,
            WriteQueueStatus.COMPLETED,
        ]
    )
    status = await _run(env, fake)
    assert status == "completed"
    # one enqueue + four polls
    assert fake.count("write_async") == 1
    assert fake.count("write_status") == 4


async def test_failed_status_raises(env: WorkflowEnvironment) -> None:
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.FAILED], error_detail="extractor exploded")
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    app_err = _app_error(ei.value)
    assert app_err.type == errors.TYPE_WRITE_FAILED


async def test_failed_status_keeps_the_server_detail_out_of_history(env: WorkflowEnvironment) -> None:
    # Temporal persists failure messages AND details in the clear, and the
    # server's `error_detail` is not promised user-safe (it can echo memory text
    # or internal endpoints). It belongs in the worker log, nowhere else.
    fake = FakeXmemoryInstance()
    fake.status_sequence(
        [WriteQueueStatus.PROCESSING, WriteQueueStatus.FAILED],
        error_detail="boom at internal-db.local:5432",
    )
    wf_id = f"wf-{uuid.uuid4()}"
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake, wf_id=wf_id)
    app_err = _app_error(ei.value)

    assert "internal-db.local" not in (app_err.message or "")
    assert all("internal-db.local" not in str(d) for d in app_err.details)

    # The final error is only one of the places it could leak: an activity that
    # returned the detail would persist it in its own result payload. Scan every
    # persisted event, not just the failure.
    for event in await _history(env, wf_id):
        assert b"internal-db.local" not in event.SerializeToString(), (
            f"server error detail persisted in {event.WhichOneof('attributes')}"
        )


async def test_not_found_raises_immediately(env: WorkflowEnvironment) -> None:
    # `write_async` is transactional, so the id it returned is always queryable.
    # A not_found means the write is genuinely gone: fail on the first poll
    # rather than masking a backend that violated that contract.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.NOT_FOUND])
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    assert _app_error(ei.value).type == errors.TYPE_WRITE_NOT_FOUND
    assert fake.count("write_status") == 1


async def test_two_phase_intermediate_states_are_non_terminal(env: WorkflowEnvironment) -> None:
    # The two-phase pipeline states (extracting/extracted/applying) must all be
    # treated as "keep polling", never terminal.
    fake = FakeXmemoryInstance()
    fake.status_sequence(
        [
            WriteQueueStatus.EXTRACTING,
            WriteQueueStatus.EXTRACTED,
            WriteQueueStatus.APPLYING,
            WriteQueueStatus.COMPLETED,
        ]
    )
    status = await _run(env, fake)
    assert status == "completed"
    assert fake.count("write_status") == 4


async def test_unknown_status_keeps_polling(env: WorkflowEnvironment) -> None:
    # A status the client enum does not know (a future server state added
    # during a rolling deploy) must be treated as non-terminal — keep polling —
    # NOT fail the in-flight durable write. Here an unknown state precedes
    # completion; the loop rides through it.
    fake = FakeXmemoryInstance()
    fake.status_sequence(["indexing", "indexing", WriteQueueStatus.COMPLETED])
    status = await _run(env, fake)
    assert status == "completed"
    assert fake.count("write_status") == 3


async def test_max_wait_timeout_raises(env: WorkflowEnvironment) -> None:
    # A write that never completes is bounded by max_wait and fails with a
    # distinct, non-retryable timeout (not an infinite poll loop). Time-skipping
    # fast-forwards the 15-minute deadline instantly.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING])  # never terminal
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    assert "XmemoryWriteTimeout" in str(ei.value.cause)


async def test_a_tiny_poll_interval_is_honored_but_zero_is_rejected(env: WorkflowEnvironment) -> None:
    # A small timedelta must reach the loop rather than being replaced by the 2s
    # default -- resolving with `or` would substitute it. Read the timers back out
    # of history: an honored 1ms produces ~zero-length sleeps, the default would
    # produce 2s then 3s. The wait is short because an uncapped 1ms cadence over a
    # long one projects more history events than this helper will spend (see below).
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    wf_id = f"wf-{uuid.uuid4()}"
    status = await _run(env, fake, poll_s=0.001, cap_s=0.001, wait_s=0.5, wf_id=wf_id)
    assert status == "completed"

    timers = []
    for event in await _history(env, wf_id):
        if event.HasField("timer_started_event_attributes"):
            d = event.timer_started_event_attributes.start_to_fire_timeout
            timers.append(d.seconds + d.nanos / 1e9)
    assert timers, "the loop must have slept between polls"
    assert all(t < 0.5 for t in timers), f"the interval was not honored: {timers}"

    # Zero itself is refused: it hot-polls against Temporal's ~1ms timer floor and
    # exhausts history for no gain. Refused before the enqueue, so no write is
    # left queued behind the rejection.
    fake_zero = FakeXmemoryInstance()
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake_zero, poll_s=0, cap_s=0)
    assert _app_error(ei.value).type == errors.TYPE_BAD_OPTIONS
    assert fake_zero.count("write_async") == 0, "a rejected option must not enqueue a write"


async def test_each_poll_is_bounded_by_the_remaining_wait(env: WorkflowEnvironment) -> None:
    # `max_wait` has to bound the polls themselves, not just the gaps between
    # them. Without a schedule-to-close, a single rate-limited poll can back off
    # (up to the hour the Retry-After clamp allows) long past `max_wait`, and the
    # caller sees XmemoryRateLimited instead of XmemoryWriteTimeout. Read the
    # bound back out of history rather than trusting the call site.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    wf_id = f"wf-{uuid.uuid4()}"
    await _run(env, fake, wait_s=900, wf_id=wf_id)

    bounds = [
        event.activity_task_scheduled_event_attributes.schedule_to_close_timeout.ToTimedelta()
        for event in await _history(env, wf_id)
        if event.HasField("activity_task_scheduled_event_attributes")
        and event.activity_task_scheduled_event_attributes.activity_type.name == "xmemory_write_status"
    ]

    assert bounds, "no write_status activity was scheduled"
    assert all(timedelta(0) < b <= timedelta(seconds=900) for b in bounds), bounds


async def test_expired_wait_reports_a_write_timeout_not_the_last_poll_error(env: WorkflowEnvironment) -> None:
    # Each poll carries a schedule-to-close of the remaining wait, so Temporal
    # ends the poll when the wait runs out and surfaces that attempt's last
    # error, which is retryable by definition. The caller's contract is
    # `max_wait` elapsed, so the loop must translate it back rather than leaking
    # e.g. XmemoryRateLimited out of a write_durable that simply ran out of time.
    fake = FakeXmemoryInstance()
    fake.fail_status_always(api_error(status=429, code="RATE_LIMITED", retry_after=3600))
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake)
    app_err = _app_error(ei.value)
    assert app_err.type == errors.TYPE_WRITE_TIMEOUT
    assert app_err.non_retryable is True


async def test_the_wait_never_overruns_max_wait(env: WorkflowEnvironment) -> None:
    # `max_wait` bounds the whole thing, polls included. Giving the last poll a
    # fresh status budget instead would let a 10s wait run for 10s + that budget.
    # The poll must actually spend its budget for that to show up as elapsed time,
    # so it is rate-limited into retrying.
    fake = FakeXmemoryInstance()
    fake.fail_status_always(api_error(status=429, code="RATE_LIMITED", retry_after=5))
    wf_id = f"wf-{uuid.uuid4()}"
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake, poll_s=30, wait_s=10, wf_id=wf_id)

    assert _app_error(ei.value).type == errors.TYPE_WRITE_TIMEOUT
    assert fake.count("write_status") >= 1, "it must observe the write at least once"

    times = [event.event_time.ToDatetime() for event in await _history(env, wf_id)]
    elapsed = (max(times) - min(times)).total_seconds()
    # 10s of waiting, plus one final observation. That last look is a single attempt:
    # letting it retry turned the documented one-poll grace into a whole retry chain
    # bounded only by write_status_timeout, and this ran 35s.
    assert elapsed <= 11, f"ran {elapsed}s for a 10s max_wait"


async def test_a_retry_hint_longer_than_the_wait_is_not_second_guessed(env: WorkflowEnvironment) -> None:
    # The server asked for an hour and only fifteen minutes remain, so another
    # poll would arrive before it is willing to answer. Wait the wait out and
    # report the timeout rather than re-polling on our own cadence.
    fake = FakeXmemoryInstance()
    fake.fail_status_always(api_error(status=429, code="RATE_LIMITED", retry_after=3600))
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake, single_attempt=True)

    assert _app_error(ei.value).type == errors.TYPE_WRITE_TIMEOUT
    assert fake.count("write_status") == 1, "polled again before the server said it would answer"


async def test_a_fast_poll_does_not_end_the_wait(env: WorkflowEnvironment) -> None:
    # Bounding a poll by the remaining wait must not be read as "this is the last
    # one": a status that comes back immediately leaves the whole wait available,
    # and the write may well complete on the next poll.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    out = await _run(env, fake, poll_s=1, wait_s=10)

    assert out == "completed"
    assert fake.count("write_status") == 2


async def test_a_late_completion_is_still_observed(env: WorkflowEnvironment) -> None:
    # A cadence longer than the wait must not mean "look once and give up": the
    # write can still land inside max_wait, so take one last look as late as it
    # can complete rather than sleeping through the remainder blind.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    out = await _run(env, fake, poll_s=30, wait_s=10)

    assert out == "completed"
    assert fake.count("write_status") == 2


async def test_the_whole_wait_is_actually_waited_out(env: WorkflowEnvironment) -> None:
    # A cadence longer than the wait must not collapse it: the final observation
    # belongs *at* the deadline, so sleeping zero and giving up immediately turns a
    # 10s wait into a 0.05s one.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING])  # never terminal
    wf_id = f"wf-{uuid.uuid4()}"
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake, poll_s=30, wait_s=10, wf_id=wf_id)

    assert _app_error(ei.value).type == errors.TYPE_WRITE_TIMEOUT
    times = [event.event_time.ToDatetime() for event in await _history(env, wf_id)]
    elapsed = (max(times) - min(times)).total_seconds()
    assert 9 <= elapsed <= 11, f"a 10s wait took {elapsed}s"


async def test_an_equal_retry_hint_still_prevents_an_early_poll(env: WorkflowEnvironment) -> None:
    # "Retry after 2s" means exactly that, even when our own cadence is also 2s:
    # comparing the hint against the cadence rather than against zero let an equal
    # hint through and polled an endpoint that had just asked us to wait.
    fake = FakeXmemoryInstance()
    fake.fail_status_always(api_error(status=429, code="RATE_LIMITED", retry_after=2))
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake, poll_s=2, wait_s=1, single_attempt=True)

    assert _app_error(ei.value).type == errors.TYPE_WRITE_TIMEOUT
    assert fake.count("write_status") == 1, "polled again inside the hinted window"


async def test_a_policy_level_non_retryable_poll_error_is_surfaced(env: WorkflowEnvironment) -> None:
    # A retry policy can mark an error type non-retryable even when we type it
    # retryable. Temporal then reports NON_RETRYABLE_FAILURE while the cause
    # itself still says retryable, so the loop has to read the retry state --
    # otherwise the caller's own policy is masked as our timeout verdict.
    fake = FakeXmemoryInstance()
    fake.fail_status_always(api_error(status=429))
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[StrictPollDurableWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1", allow_unmeasurable_clock=True), instance=fake)],
    ):
        with pytest.raises(WorkflowFailureError) as ei:
            await env.client.execute_workflow(
                StrictPollDurableWriteWorkflow.run, "remember this", id=f"wf-{uuid.uuid4()}", task_queue=tq
            )
    app_err = _app_error(ei.value)
    assert app_err.type == errors.TYPE_RATE_LIMITED, f"masked as {app_err.type}"
    assert fake.count("write_status") == 1, "a forbidden retry must not be polled again"


async def test_an_unusable_poll_policy_is_rejected_before_the_enqueue(env: WorkflowEnvironment) -> None:
    # Temporal validates a retry policy when it builds the activity command, which
    # for the first poll is after the enqueue: the write is already queued and the
    # workflow then fails its task on the same ValueError forever, with nobody
    # waiting on the write. Reject it up front instead.
    fake = FakeXmemoryInstance()
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[BadPollPolicyDurableWriteWorkflow],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1", allow_unmeasurable_clock=True), instance=fake)],
    ):
        with pytest.raises(WorkflowFailureError) as ei:
            await env.client.execute_workflow(
                BadPollPolicyDurableWriteWorkflow.run,
                args=[0.5, 2],  # a backoff coefficient below 1 is refused by Temporal
                id=f"wf-{uuid.uuid4()}",
                task_queue=tq,
            )
    assert _app_error(ei.value).type == errors.TYPE_BAD_OPTIONS
    assert fake.count("write_async") == 0, "an unusable poll policy must not enqueue a write"


async def test_a_cadence_that_cannot_fit_history_is_rejected(env: WorkflowEnvironment) -> None:
    # A millisecond cadence over the default fifteen-minute wait is ~900k polls and
    # several million history events: Temporal would kill the workflow partway
    # through the wait, after the write was already queued. Say so up front.
    fake = FakeXmemoryInstance()
    with pytest.raises(WorkflowFailureError) as ei:
        await _run(env, fake, poll_s=0.001, cap_s=0.001, wait_s=900)
    err = _app_error(ei.value)
    assert err.type == errors.TYPE_BAD_OPTIONS
    assert "poll_interval" in (err.message or ""), err.message
    assert fake.count("write_async") == 0, "a rejected cadence must not enqueue a write"

    # A cadence whose worst case fits is left alone -- including a multi-hour wait
    # at the default cadence, which is the documented child-workflow case. Dividing
    # the wait by the initial interval used to reject that: it ignored the backoff
    # this loop actually applies.
    fake_ok = FakeXmemoryInstance()
    assert await _run(env, fake_ok, poll_s=1, wait_s=900) == "completed"
    fake_hours = FakeXmemoryInstance()
    assert await _run(env, fake_hours, poll_s=2, cap_s=30, wait_s=4 * 3600) == "completed"
    # And a small initial interval is fine when backoff carries it away.
    fake_tiny = FakeXmemoryInstance()
    assert await _run(env, fake_tiny, poll_s=0.001, cap_s=30, wait_s=900) == "completed"


async def test_the_final_look_is_a_single_attempt(env: WorkflowEnvironment) -> None:
    # `max_wait` bounds the waiting and the last observation happens *at* the
    # deadline, so that poll is a grace on top of the wait. It has no remaining wait
    # to bound it, which means a retry policy is the only thing that does: letting it
    # retry turned "one last look" into a whole retry chain bounded only by
    # write_status_timeout, and a 10s max_wait was observed running 35s.
    #
    # Asserted on what the loop *schedules*, not on elapsed time: whether a retry
    # chain actually materialises depends on the server's timing, so an elapsed-time
    # assertion passes for the wrong reasons more often than it catches this.
    fake = FakeXmemoryInstance()
    fake.status_sequence([WriteQueueStatus.PROCESSING, WriteQueueStatus.COMPLETED])
    wf_id = f"wf-{uuid.uuid4()}"
    assert await _run(env, fake, poll_s=30, wait_s=5, wf_id=wf_id) == "completed"

    scheduled = [
        event.activity_task_scheduled_event_attributes
        for event in await _history(env, wf_id)
        if event.HasField("activity_task_scheduled_event_attributes")
    ]
    polls = [a for a in scheduled if a.activity_type.name == "xmemory_write_status"]
    assert len(polls) >= 2, f"expected an ordinary poll and a final one, got {len(polls)}"
    assert polls[-1].retry_policy.maximum_attempts == 1, "the final look must not retry"
    assert polls[0].retry_policy.maximum_attempts != 1, "ordinary polls keep the configured policy"
    # And it is still bounded by the status timeout, not by nothing.
    assert polls[-1].schedule_to_close_timeout.seconds > 0


@pytest.mark.parametrize("workflow_cls", [SingleAttemptBadCoefficientWorkflow, ReservedTimeoutTypeWorkflow])
async def test_a_policy_the_service_refuses_is_rejected_before_the_enqueue(
    env: WorkflowEnvironment, workflow_cls: type
) -> None:
    # The SDK and the service disagree about these two, and the service is the one
    # that matters: a sub-1 backoff coefficient is refused even with a single attempt
    # (measured against a real server), and Temporal's reserved `TemporalTimeout:`
    # prefix must name a real timeout kind. Either way the failure lands when the
    # first poll's command is built -- after the enqueue -- so the workflow fails its
    # task forever with a write nobody is waiting on.
    fake = FakeXmemoryInstance()
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=tq,
        workflows=[workflow_cls],
        plugins=[XmemoryPlugin(XmemoryConfig(instance_id="inst-1", allow_unmeasurable_clock=True), instance=fake)],
    ):
        with pytest.raises(WorkflowFailureError) as ei:
            await env.client.execute_workflow(
                workflow_cls.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=tq,  # type: ignore[attr-defined]
            )
    assert _app_error(ei.value).type == errors.TYPE_BAD_OPTIONS
    assert fake.count("write_async") == 0, "an unusable poll policy must not enqueue a write"
