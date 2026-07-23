# xmemory × Temporal

Durable agent memory for [Temporal](https://temporal.io) — add
[xmemory](https://xmemory.ai) reads and writes to your workflows as replay-safe
Temporal Activities, with one plugin line on your Worker.

Ships in two languages that mirror each other:

- **Python** — [`python/`](./python), published as `xmemory-temporal` (PyPI)
- **TypeScript** — [`typescript/`](./typescript), published as `@xmemory/temporal` (npm)

> An agent's memory is exactly the state you don't want to lose when a worker
> crashes mid-turn. Putting xmemory behind Temporal makes a memory write a
> durable step: it survives process death, redeploys, and rolling upgrades, and
> Temporal — not your code — owns its retries and timeouts.

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
- **Temporal-owned retries.** xmemory errors are mapped to typed
  `ApplicationFailure`s with retryable/non-retryable verdicts (see below), so
  you can tune `RetryPolicy` against stable error-type strings.
- **Opt-in auto-capture** of activity results into memory, via an Activity
  interceptor that never touches the replay path.

## Quickstart (Python)

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

## Quickstart (TypeScript)

```ts
import { NativeConnection, Worker } from '@temporalio/worker';
import { XmemoryPlugin } from '@xmemory/temporal';

const plugin = new XmemoryPlugin({ instanceId: '<your-instance-id>' }); // reads XMEM_API_KEY
const connection = await NativeConnection.connect({ address: 'localhost:7233' });
const worker = await Worker.create({
  connection,
  taskQueue: 'my-agent',
  workflowsPath: require.resolve('./workflows'),
  plugins: [plugin],
});
```

```ts
// workflows.ts
import { xmemoryForWorkflow } from '@xmemory/temporal';

export async function myWorkflow(userName: string, userMessage: string): Promise<unknown> {
  const mem = xmemoryForWorkflow();
  // Name whom the fact is about — a memory store has no ambient "current user".
  await mem.writeDurable(`${userName}: ${userMessage}`);
  return (await mem.read(`what do we know about ${userName}?`)).readerResult;
}
```

Importing `xmemoryForWorkflow` from the package root inside workflow code is safe:
the package is marked side-effect-free, so Temporal's workflow bundler tree-shakes
the plugin and the xmemory client (non-workflow-safe modules) out of the sandbox
bundle.

Register the plugin **once**, never twice:

- **Python** — put it on the **Client** (`Client.connect(plugins=[plugin])`). The
  Worker inherits its client's plugins automatically, so do *not* also pass it to
  `Worker(...)` — doing so registers the activities twice and fails with "More
  than one activity named xmemory_read". (Worker-only also works; just never both.)
- **TypeScript** — put it on the **Worker** (`Worker.create({ plugins: [plugin] })`),
  as shown above. The TS plugin is a `WorkerPlugin`; the client does not carry it.

## Credentials never reach workflow history

The config holds the **name** of the environment variable that supplies the API
key (`XMEM_API_KEY` by default), never the key itself — so nothing secret is ever
serialized into activity arguments, which Temporal persists in the clear. Pass
the key in-process instead with `XmemoryPlugin(config, api_key=...)` /
`new XmemoryPlugin(config, { apiKey })` if you prefer.

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

**Opt into write retries only when your primary keys are literal identifiers
present verbatim in the text** — a `customer_id` / `interaction_id` you supply,
which re-extract deterministically. Then a retry is a safe no-op update:

```python
mem = xmemory_for_workflow(write_retry_policy=RetryPolicy(maximum_attempts=3))
```
```ts
const mem = xmemoryForWorkflow({ writeRetryPolicy: { maximumAttempts: 3 } });
```

The general fix — an upstream idempotency key that makes *any* schema's writes
safe to retry — is tracked as a prerequisite in
[`PUBLISHING-LATER.md`](./PUBLISHING-LATER.md).
[`examples/setup_memory.py`](./python/examples/setup_memory.py) shows creating an
instance with a schema.

## Error handling

xmemory errors become `ApplicationFailure`s with stable `type` strings you can
match in a `RetryPolicy`. The mapping is derived from the server's error codes:

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
integrations/temporal/gate.sh          # lint + typecheck + tests, both languages
```

Both suites run with no live backend (a fake instance is injected), except a
`live`-marked end-to-end test that needs `XMEM_API_KEY` + `XMEM_INSTANCE_ID`.

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
