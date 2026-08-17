# xmemory-temporal

Durable agent memory for [Temporal](https://temporal.io) — add
[xmemory](https://xmemory.ai) reads and writes to your workflows as replay-safe
Temporal Activities, with one plugin line on your Worker.

> An agent's memory is exactly the state you don't want to lose when a worker
> crashes mid-turn. Putting xmemory behind Temporal makes a memory write a
> durable step: it survives process death, redeploys, and rolling upgrades, and
> Temporal — not your code — owns its retries and timeouts.

A TypeScript port ships as
[`@xmemory/temporal`](https://github.com/xmemory-ai/xmemory-temporal-ts). It
mirrors this API, with small differences where the two SDKs differ.

### Memory is untrusted data, both ways

What goes in is user-controlled text. What comes back is that text plus whatever
the extraction engine made of it. Neither is a safe source of instructions: a read
result in a prompt is the indirect prompt-injection path, and the same string in a
shell or a query is the ordinary injection path. Quote it, bound it, and keep it
out of anything that decides what to do next.

### One worker, one instance

The activities bind to whatever instance the plugin configured, so **every worker
polling a task queue must share that configuration**. Per-tenant isolation means a
task queue per tenant, not a per-workflow option.

Which queue is a trust decision: derive the tenant from an authenticated identity,
never from a caller- or model-supplied value. A workflow argument naming a task
queue is a request to read someone else's memory.

## What you get

- **Memory as Activities.** `read`, `write`, `write_async_start` + `write_status` run
  as Activities (all I/O stays out of workflow code, so workflows replay
  deterministically).
- **A durable deep write.** `write_durable(text)` enqueues a write and polls it
  to completion from the workflow, so a multi-minute extraction survives worker
  restarts — the poll state lives in workflow history, not a worker process.
- **A near-zero-diff migration.** The workflow handle mirrors the client's methods,
  so `inst.read(...)` / `inst.write(...)` keep working and just dispatch to an
  Activity. Two differences: the enqueue is `write_async_start`, and results are
  this package's own DTOs so a client field rename cannot break replay.
- **Temporal-owned retries and timeouts.** xmemory errors map to typed
  `ApplicationError`s with retryable/non-retryable verdicts, so you can tune
  `RetryPolicy` against stable error-type strings.
- **Opt-in auto-capture** of activity results into memory, via an Activity
  interceptor that never touches the replay path.

## Install

```bash
pip install xmemory-temporal
```

Requires Python 3.10+ and `temporalio` 1.30+.

## Quickstart

Register the plugin on your **Client**; the Worker inherits it automatically:

```python
from temporalio.client import Client
from temporalio.worker import Worker
from xmemory_temporal import XmemoryConfig, XmemoryPlugin

config = XmemoryConfig(instance_id="<your-instance-id>")  # reads XMEM_API_KEY from the env
plugin = XmemoryPlugin(config)

client = await Client.connect("localhost:7233", plugins=[plugin])
# The Worker inherits the client's plugins automatically — do NOT pass it again here.
worker = Worker(client, task_queue="my-agent", workflows=[MyWorkflow])
```

Then call memory from inside a workflow:

```python
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal import xmemory_for_workflow

@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self, user_name: str, user_message: str) -> str:
        mem = xmemory_for_workflow()
        # A memory store has no ambient "current user" — name whom the fact is
        # about, then recall by that name (or pass scope= to bind to a record).
        await mem.write_durable(f"{user_name}: {user_message}")   # durable, survives restarts
        answer = await mem.read(f"what do we know about {user_name}?")
        return str(answer.reader_result)                          # reader_result is Any
```

> **Register the plugin once, never twice.** Put it on the Client
> (`Client.connect(plugins=[plugin])`); the Worker inherits its client's plugins,
> so do *not* also pass it to `Worker(...)`, which registers the activities twice
> and fails with "More than one activity named xmemory_read". (Worker-only also
> works; just never both.)

Runnable scripts live in [`examples/`](./examples). They call `worker.run()`
directly to stay readable; in production, install SIGINT/SIGTERM handlers so a
deploy drains the worker instead of killing it mid-activity ([Temporal's
guidance](https://docs.temporal.io/encyclopedia/workers/worker-shutdown#graceful-shutdown)).

## Timeouts

**The workflow owns every activity budget.** `xmemory_for_workflow()` sets each
call's `start_to_close_timeout`, and the activity derives its xmemory client
timeout from the deadline Temporal actually assigned it, always a margin below,
so the client gives up first and you get an attributable xmemory error instead of
an opaque Temporal activity timeout.

```python
from datetime import timedelta

mem = xmemory_for_workflow(
    read_timeout=timedelta(seconds=60),     # a deep read on a large instance
    write_timeout=timedelta(minutes=5),
)
```

The client timeout is derived, not configured separately, so lowering a workflow's
budget lowers the client's with it. `XmemoryTimeouts` supplies the defaults;
`XmemoryConfig(client_margin_seconds=...)` tunes the gap.

**A timed-out write is indeterminate.** Nothing distinguishes "never arrived" from
"arrived, response lost", so write activities default to `maximum_attempts=1` and
surface the failure instead of retrying. Treat a timed-out write as *may or may not
have happened*, and reconcile with a read if it matters.

## Durable writes

`write_durable(text)` enqueues a deep write and polls it to completion from the
workflow, so the wait is a Temporal timer in server-side history rather than a
blocked activity slot. Redeploy the worker fleet mid-write and nothing is lost:
the poll loop resumes on the new worker and completes.

```python
status = await mem.write_durable(text, max_wait=timedelta(minutes=15))
```

Each poll costs about eleven history events (an activity, a timer, and the workflow
tasks driving them). Backoff slows that growth until the interval reaches
`max_poll_interval`, after which history grows linearly with the wait. A cadence
whose worst case would not fit is rejected up front, and the loop warns once
Temporal suggests continuing as new.

This helper cannot call `continue_as_new` for you — it runs inside *your* workflow,
and restarting that would discard your state. For multi-hour waits, run
`write_durable` in a child workflow.

`max_wait` bounds the *waiting*: every poll is scheduled to finish inside it. The
one exception is the last observation, which happens **at** the deadline so a write
that lands late is still seen rather than reported as a timeout — so a call can
return up to one status poll after `max_wait`. When the server has asked for a
retry delay longer than the wait has left, that final poll is skipped instead:
arriving before the server said it would answer is worse than not looking.

For the fire-and-forget pattern (kick off several writes, keep working, join
before the turn ends), `write_async_start()` and `write_status()` are public too.

## Credentials never reach workflow history

The config holds the **name** of the environment variable that supplies the API
key (`XMEM_API_KEY` by default), never the key itself — so nothing secret is ever
serialized into activity arguments, which Temporal persists in the clear. Pass
the key in-process instead with `XmemoryPlugin(config, api_key=...)` if you
prefer.

**Your memory text and queries, however, *are* in history.** Queries, written text,
and `reader_result` are activity payloads, persisted in the clear and visible in the
Web UI. The error mapping keeps raw transport strings and the server's failure
detail out of failures (set `log_server_error_detail=True` to log the reason
worker-side), but the payloads themselves remain.

`include_content_in_summary=False` (the default) only affects the one-line activity
*summary*. For sensitive memory text, install a Temporal **Payload Codec**; this
plugin does not impose one, since a codec applies to every payload in the
namespace, not just xmemory's.

## Replay safety and idempotency

Two things keep memory operations correct under retries and replay:

- **Replay never re-issues an operation.** All I/O is in Activities; workflow
  code only schedules Activities and sleeps. Temporal replays workflow code but
  never re-runs a completed Activity, so a replay never repeats a memory read or
  write. The suite proves this with a forced-replay (`max_cached_workflows=0`)
  side-effects test.
- **Writes default to at-most-once.** Primary-key dedup looks like it would make
  retries safe, but PK extraction is non-deterministic: a model normalizes the same
  value differently across runs (`Dr. Robert Kim` vs `Robert Kim`), and a
  disagreement forks a new row. So a lost-response retry can duplicate. Write
  Activities default to `maximum_attempts=1` and surface the failure to your
  workflow. Reads and status polls are idempotent and retry generously.

**Structured writes are the reliable way to make a write retryable.** Pass
explicit mutations instead of free text and the primary key is one you supply, so
nothing is extracted and re-applying the write is deterministic:

```python
mem = xmemory_for_workflow(write_retry_policy=RetryPolicy(maximum_attempts=3))
await mem.write(
    structured_mutations=[
        {
            "object_mutation": {
                "object_type": "Customer",
                "update": {"key": {"customer_id": "c-1"}, "values": {"tier": "gold"}},
            }
        }
    ]
)
```

A mutation is a `create`, `update`, or `delete` on one object or relation. An
update or delete names the key it addresses, so a retry hits the same row. A create
does not — the server assigns the key — so a create is not safe to retry.

For text writes, opt into retries only when your primary keys are literal
identifiers appearing verbatim in the text, such as a `customer_id` you supply. That
is a convention you keep, not something the API enforces.

[`examples/setup_memory.py`](./examples/setup_memory.py) shows creating an
instance with a schema.

## Error handling

xmemory errors become `ApplicationError`s with stable `type` strings you can
match in a `RetryPolicy` (`non_retryable_error_types=[...]`). The mapping is
derived from the server's error codes:

| xmemory condition | `type` | Retryable? |
|---|---|---|
| transport error / timeout / HTTP ≥ 500 / 408 | `XmemoryServerError` / `XmemoryUnavailable` | yes |
| `RATE_LIMITED` (429) | `XmemoryRateLimited` | yes — honors `Retry-After` |
| `QUOTA_EXCEEDED` + `daily_quota_exceeded` | `XmemoryDailyQuotaExceeded` | yes (long backoff) |
| `QUOTA_EXCEEDED` + `monthly_quota_exceeded` | `XmemoryMonthlyQuotaExceeded` | no |
| `QUOTA_EXCEEDED` (kind unknown) | `XmemoryQuotaExceeded` | no |
| `UNAUTHORIZED` / `FORBIDDEN` | `XmemoryAuthFailed` | no |
| `NOT_FOUND` | `XmemoryNotFound` | no |
| validation / conflict / schema-evolution rejections | `XmemoryBadRequest` / `XmemorySchemaRejected` | no |
| activities registered without the plugin | `XmemoryNotBound` | no |
| an activity scheduled with neither close timeout | `XmemoryNoDeadline` | no |
| durable-write options that cannot be honored | `XmemoryBadOptions` | no — the same arguments fail identically |
| this worker's clock cannot time the attempt | `XmemoryClockUnusable` | yes — a worker with a synced clock can run it |
| the activity's deadline is already spent | `XmemoryDeadlineExpired` | yes — Temporal decides if another attempt fits |
| an unrecognized code | `XmemoryUnknown` | yes (never fatal) |

Plus three raised by the durable write loop (`write_durable`), from a polled
`write_status` — all non-retryable:

| durable-write outcome | `type` |
|---|---|
| the queued write reported `failed` | `XmemoryWriteFailed` |
| the queued write id was `not_found` | `XmemoryWriteNotFound` |
| polling exceeded `max_wait` | `XmemoryWriteTimeout` |

An unrecognized code stays retryable and never raises: a client that crashed on a
newer server's code would break during rolling deploys.

> **Note.** 402 means `QUOTA_EXCEEDED` only. `TRIAL_ENDED` was removed from the
> xmemory contract when trials were retired end-to-end; do not rely on it.

## Auto-capture (opt-in)

```python
from xmemory_temporal import AutoCaptureConfig, XmemoryPlugin

plugin = XmemoryPlugin(
    config,
    auto_capture=AutoCaptureConfig(
        project=lambda activity_name, result: summarize(result),  # return None to skip
        sample_rate=0.25,
    ),
)
```

Off by default. It runs as an **Activity** interceptor, outside the replay path;
`project` decides what to remember, sampling bounds fan-out, and a capture failure
never fails the wrapped activity. Capture is an enqueue (`write_async`) clamped to
what the activity has left of its deadline, and skipped when nothing is left, so it
cannot push the activity past its `start_to_close`.

Two things sit outside that budget:

- **Your `project` function.** Nothing here can interrupt synchronous code. The
  activity stops *waiting* at the capture budget, but the projector thread is held
  until it returns, so later captures are skipped. Keep projections cheap.
- **Activity interceptors carried by the Temporal Client.** Those wrap this
  plugin's own, so what they spend after the activity body is invisible here. That
  combination is refused at worker setup — register such interceptors on the
  worker, after `XmemoryPlugin`.

> **Auto-capture is at-least-once.** If a worker dies after the capture enqueue but
> before the activity's completion is recorded, the activity runs again and captures
> again — and since primary keys are extracted, a duplicate can fork an entity.
> Sampling reduces how many activities are eligible, but it is stable per activity,
> so it does not help once one is selected. Capture facts a duplicate would not
> corrupt.

> **Naming caveat.** Auto-capture skips any activity whose name starts with
> `xmemory_` (to avoid capturing its own writes). If you name one of *your* own
> activities `xmemory_...`, it will be silently skipped. It also never captures
> Queries.

## Testing

```bash
uv sync --dev
uv run pytest                 # everything except the live e2e (it self-skips)
uv run ruff check src tests examples
uv run pyright src tests examples
```

The suite runs with no live backend (a fake instance is injected), except a
`live`-marked end-to-end test that needs `XMEM_API_KEY` + `XMEM_INSTANCE_ID`.
See [`TESTING.md`](./TESTING.md) for the full strategy.

## Legal

- Privacy policy: <https://xmemory.ai/privacy-policy.html>
- Terms: <https://xmemory.ai/terms-and-conditions.html>

**MIT licensed** — see [`LICENSE`](./LICENSE). The grant covers this integration's
code only. The xmemory service and its technology remain proprietary to xmemory
Inc.; using it requires valid credentials and is governed by the Terms above. The
scope and trademark notices live in [`NOTICE`](./NOTICE), kept separate so the
package classifies cleanly as MIT.
