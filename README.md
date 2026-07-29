# xmemory-temporal

Durable agent memory for [Temporal](https://temporal.io) — add
[xmemory](https://xmemory.ai) reads and writes to your workflows as replay-safe
Temporal Activities, with one plugin line on your Worker.

> An agent's memory is exactly the state you don't want to lose when a worker
> crashes mid-turn. Putting xmemory behind Temporal makes a memory write a
> durable step: it survives process death, redeploys, and rolling upgrades, and
> Temporal — not your code — owns its retries and timeouts.

A TypeScript port with the same API ships as
[`@xmemory/temporal`](https://github.com/xmemory-ai/xmemory-temporal-ts).

## What you get

- **Memory as Activities.** `read`, `write`, `write_async` + `write_status` run
  as Activities (all I/O stays out of workflow code, so workflows replay
  deterministically).
- **A durable deep write.** `write_durable(text)` enqueues a write and polls it
  to completion from the workflow, so a multi-minute extraction survives worker
  restarts — the poll state lives in workflow history, not a worker process.
- **A near-zero-diff migration.** The workflow-side handle mirrors the plain
  xmemory client's methods, so agent code that already calls `inst.read(...)` /
  `inst.write(...)` keeps working — it just dispatches to an Activity.
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

Runnable end-to-end scripts live in [`examples/`](./examples): create an instance
with a schema, run a worker, and drive a support-agent workflow. They call
`worker.run()` directly to stay readable. In production, install SIGINT/SIGTERM
handlers so a deploy drains the worker instead of killing it mid-activity; see
[Temporal's worker shutdown guidance](https://docs.temporal.io/encyclopedia/workers/worker-shutdown#graceful-shutdown).

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

Because the client timeout is *derived* rather than configured separately, the
two can never disagree: lowering a workflow's budget lowers the client's with it.
`XmemoryTimeouts` supplies the defaults (120s read, 180s write, 30s enqueue and
poll); `XmemoryConfig(client_margin_seconds=...)` tunes the gap between the two.

## Durable writes

`write_durable(text)` enqueues a deep write and polls it to completion from the
workflow, so the wait is a Temporal timer in server-side history rather than a
blocked activity slot. Redeploy the worker fleet mid-write and nothing is lost:
the poll loop resumes on the new worker and completes.

```python
status = await mem.write_durable(text, max_wait=timedelta(minutes=15))
```

Each poll adds an activity and a timer to workflow history: roughly 105 events at
the defaults (15 minutes), about 1,500 for a four-hour wait. A long `max_wait`
with a short `max_poll_interval` can approach Temporal's per-workflow event
limit, and the loop logs a warning once Temporal itself suggests continuing as
new. This helper cannot call `continue_as_new` for you, since it runs inside
*your* workflow and restarting that would discard your state. For multi-hour
waits, run `write_durable` in a child workflow, where continue-as-new is yours to
use.

For the fire-and-forget pattern (kick off several writes, keep working, join
before the turn ends), `write_async_start()` and `write_status()` are public too.

## Credentials never reach workflow history

The config holds the **name** of the environment variable that supplies the API
key (`XMEM_API_KEY` by default), never the key itself — so nothing secret is ever
serialized into activity arguments, which Temporal persists in the clear. Pass
the key in-process instead with `XmemoryPlugin(config, api_key=...)` if you
prefer.

**Your memory text and queries, however, *are* in history.** The query you `read`
and the text you `write` are activity inputs, and the error mapping keeps raw
transport strings out of failure *messages* (a failed durable write carries the
server's reason in the error `details`, not the cleartext history title) — but the
inputs themselves, and the `reader_result`, are persisted to cleartext Temporal
history and shown in the Web UI. `include_content_in_summary=False` (the default) only keeps content out of
the one-line activity *summary*; it does not remove it from the payload. If your
memory text is sensitive, install a Temporal **Payload Codec** to encrypt
payloads at the edge — this plugin deliberately does not impose one, since a
codec applies namespace-wide to every payload, not just xmemory's.

## Replay safety and idempotency

Two things keep memory operations correct under retries and replay:

- **Replay never re-issues an operation.** All I/O is in Activities; workflow
  code only schedules Activities and sleeps. Temporal replays workflow code but
  never re-runs a completed Activity, so a replay never repeats a memory read or
  write. The suite proves this with a forced-replay (`max_cached_workflows=0`)
  side-effects test.
- **Writes default to at-most-once.** It is tempting to lean on xmemory's
  primary-key dedup to make retries safe — a re-write of the same fact should
  update the same record. But **PK extraction is non-deterministic**: xmemory
  authors primary keys with a model that can normalize the same value differently
  across runs (e.g. `Dr. Robert Kim` vs `Robert Kim`), and a disagreement forks
  the entity into a **new** row. So a lost-response retry can duplicate. Rather
  than risk that silently, write Activities default to `maximum_attempts=1`: a
  failed write is surfaced to your workflow, which decides to retry, compensate,
  or fail. Reads and status-polls (idempotent) retry generously.

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

A mutation is a `create`, `update`, or `delete` on one object or relation, and
it carries the key explicitly, so a retry addresses the same row instead of
forking a new one. An `update` in particular re-applies identically.

For text writes, opt into retries only when your primary keys are literal
identifiers that appear verbatim in the text, such as a `customer_id` you supply,
so the extractor has no room to normalise them differently on a second pass. That
is a convention you have to keep, not something the API enforces.

**Scoped writes**, which xmemory is adding in the near future, will bind a text
write to a known record and guarantee a stable primary key, closing the gap for
text writes too.

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
| an unrecognized code | `XmemoryUnknown` | yes (never fatal) |

Plus three raised by the durable write loop (`write_durable`), from a polled
`write_status` — all non-retryable:

| durable-write outcome | `type` |
|---|---|
| the queued write reported `failed` | `XmemoryWriteFailed` |
| the queued write id was `not_found` | `XmemoryWriteNotFound` |
| polling exceeded `max_wait` | `XmemoryWriteTimeout` |

An unrecognized error code stays retryable and never raises — a stricter client
that crashed on a newer server's code would break during rolling deploys.

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

Off by default. It runs as an **Activity** interceptor (outside the replay
path), requires a `project` function that decides what — if anything — to
remember, samples to bound fan-out, and never fails the wrapped activity if a
capture write errors. Capture is an **enqueue** (`write_async`) bounded by a
short timeout, so it can never slow the wrapped activity past its
`start_to_close`.

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

**MIT licensed** — see [`LICENSE`](./LICENSE). The MIT grant covers only this
integration's own code (a thin client over the xmemory API). The xmemory service
and its underlying technology — the backend, memory engine, schemas,
extraction/reader models, and hosted infrastructure — remain **proprietary to
xmemory Inc.** and are not licensed here; use of the service requires valid
credentials and is governed by the Terms above. These supplemental scope /
proprietary-service / trademark notices live in [`NOTICE`](./NOTICE), kept
separate from `LICENSE` so the package classifies cleanly as MIT.
