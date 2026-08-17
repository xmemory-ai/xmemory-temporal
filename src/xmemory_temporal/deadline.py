"""What is left of an activity's Temporal deadline.

Time inside the attempt comes from a monotonic stamp taken when the attempt
entered this worker; time before the attempt comes from service timestamps.
The one unavoidable cross-clock reading is the work Temporal did before any of
our interceptors existed (payload decoding, client-carried interceptors), which
only ``started_time`` can see. It is used defensively: it can shorten the budget
but never lengthen it, and when the worker clock reads *behind* the service the
figure becomes unmeasurable — at which point this module fails closed rather than
substituting a guess, because no bound on the unmeasured time exists.
"""

import contextvars
import time
from datetime import datetime, timezone

from temporalio.activity import Info

# Stamped by the plugin's outermost interceptor, so elapsed time covers every
# interceptor that ran before the activity function.
_ATTEMPT_START: contextvars.ContextVar[float | None] = contextvars.ContextVar("xmemory_attempt_start", default=None)


def mark_attempt_start() -> contextvars.Token:
    """Stamp the start of this activity attempt. Returns a reset token."""
    return _ATTEMPT_START.set(time.monotonic())


def clear_attempt_start(token: contextvars.Token) -> None:
    _ATTEMPT_START.reset(token)


def _elapsed_parts(info: Info | None) -> tuple[float, bool]:
    """``(measured_elapsed, pre_attempt_unmeasurable)`` for this attempt."""
    started = _ATTEMPT_START.get()
    since_stamp = 0.0 if started is None else max(time.monotonic() - started, 0.0)
    if info is None:
        return since_stamp, False
    since_entry = (datetime.now(timezone.utc) - info.started_time).total_seconds()
    if since_entry - since_stamp >= 0.0:
        return since_entry, False
    # The worker clock reads behind the service, so the pre-interceptor time is
    # real but unmeasurable. Say so, rather than calling it zero.
    return since_stamp, True


def attempt_clock_unusable(info: Info) -> bool:
    """Whether this attempt's elapsed time can be established at all.

    True when the worker clock reads *behind* the service: ``started_time`` then
    appears not to have happened yet, so the work Temporal did before our
    interceptor -- payload decoding, client-carried interceptors -- is real but
    unmeasurable, and no bound on it can be derived. Callers must fail rather than
    guess: a reserve chosen here would be arbitrary, and if it under-shoots the
    client outlives the activity and a write can be duplicated on retry.
    """
    return _elapsed_parts(info)[1]


def elapsed_in_attempt(info: Info | None = None) -> float:
    """Seconds this attempt has already spent on this worker.

    The stamp is taken by the plugin's outermost interceptor, but Temporal decodes
    payloads and runs client-carried interceptors before any of ours, so the stamp
    alone under-counts. ``started_time`` covers that pre-interceptor work, at the
    cost of being a service reading against our clock.

    The two are combined by *decomposition*, not by ``max``: the pre-interceptor
    time is ``since_entry - since_stamp``. When that is non-negative the clocks
    agree well enough to use it. When it is negative the worker clock reads behind
    the service, which makes the pre-interceptor time real but unmeasurable —
    ``max`` would silently call it zero and hand back a budget Temporal has already
    partly spent. This function then reports only what the stamp measured;
    ``remaining_budget_seconds`` is where that case is refused (see
    ``attempt_clock_unusable``), because a caller with a deadline is the one that
    has to fail closed.
    """
    return _elapsed_parts(info)[0]


def remaining_budget_seconds(
    info: Info, elapsed: float | None = None, *, allow_unmeasurable: bool = False
) -> float | None:
    """Seconds left on the running activity, or ``None`` if it has no deadline.

    The result can be zero or negative: an expired deadline must read as expired,
    never as a fresh window, or the client would keep working after Temporal has
    abandoned the activity.
    """
    if elapsed is None:
        spent, unmeasurable = _elapsed_parts(info)
        if unmeasurable and not allow_unmeasurable:
            # Fail closed: there is no honest number here, and every wrong one is
            # wrong in the dangerous direction.
            return 0.0
    else:
        spent = elapsed
    remaining = []
    if info.start_to_close_timeout is not None:
        # Runs from when this worker started the attempt, which is what the stamp
        # marks. Anything that preceded the stamp is either measured (via
        # started_time) or has already caused this to fail closed above.
        remaining.append(info.start_to_close_timeout.total_seconds() - spent)
    if info.schedule_to_close_timeout is not None:
        # Runs from first scheduling: queue and earlier attempts already spent
        # some. Both timestamps are the service's, so their difference is
        # skew-free; the rest of the attempt is the monotonic stamp.
        before_attempt = (info.started_time - info.scheduled_time).total_seconds()
        remaining.append(info.schedule_to_close_timeout.total_seconds() - before_attempt - spent)
    if not remaining:
        return None
    return min(remaining)
