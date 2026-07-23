---
id: xmemory
title: xmemory integration
sidebar_label: xmemory
description: Add durable, schema-grounded agent memory to your Temporal Workflows in Python with the xmemory plugin.
tags:
  - python-sdk
  - integrations
  - agents
keywords:
  - temporal
  - python sdk
  - xmemory
  - agent memory
  - durable memory
  - plugin
---

> Add durable, schema-grounded agent memory to your Temporal Workflows in Python with the [xmemory](https://xmemory.ai) plugin.

[xmemory](https://xmemory.ai) is a memory store for agents — it holds durable, schema-grounded knowledge your Workflows can recall across runs, users, and services. The **xmemory Temporal plugin** turns every memory read and write into a replay-safe Temporal Activity, added to your Worker with a single line. A memory write becomes a durable step that survives worker crashes, redeploys, and rolling upgrades, with Temporal — not your code — owning its retries and timeouts.

> ⓘ **Preview.** The plugin is under review for Temporal's AI Partner Ecosystem and is not yet on PyPI. Install it from the [source repository](https://github.com/xmemory-ai/xmemory-temporal); the PyPI release (`xmemory-temporal`) follows once the review completes.

## Prerequisites

- Familiarity with the [Temporal Python SDK](https://docs.temporal.io/develop/python)
- A local Temporal dev server (`temporal server start-dev`) or a Temporal Cloud namespace
- An [xmemory](https://xmemory.ai) API key and instance

## Configure your Worker to use xmemory

1. Install the plugin (from source until the PyPI release):

   ```bash
   pip install "git+https://github.com/xmemory-ai/xmemory-temporal.git"
   ```

2. Provide your xmemory API key through the environment:

   ```bash
   export XMEM_API_KEY="your-xmemory-api-key"
   ```

3. Register the plugin on your Temporal **Client** — the Worker inherits it automatically:

   ```python {6-7}
   from temporalio.client import Client
   from temporalio.worker import Worker
   from xmemory_temporal import XmemoryConfig, XmemoryPlugin
   from my_workflows import CustomerSupportWorkflow

   config = XmemoryConfig(instance_id="<your-instance-id>")  # reads XMEM_API_KEY
   plugin = XmemoryPlugin(config)

   client = await Client.connect("localhost:7233", plugins=[plugin])
   # The Worker inherits the Client's plugins — do not pass the plugin again.
   worker = Worker(client, task_queue="xmemory-support", workflows=[CustomerSupportWorkflow])
   await worker.run()
   ```

   > 💡 Register the plugin **once**. In Python, put it on the Client and the Worker inherits it; passing it to both registers the Activities twice and raises `More than one activity named xmemory_read`.

## Develop a Workflow that uses memory

Inside a Workflow, call `xmemory_for_workflow()` to get a handle whose methods dispatch to Activities. The call sites are identical to the plain xmemory client — only where the handle comes from changes, which keeps migration near-zero-diff.

```python {11,14,20}
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal import xmemory_for_workflow


@workflow.defn
class CustomerSupportWorkflow:
    @workflow.run
    async def run(self, customer_id: str, interaction_id: str, message: str) -> str:
        mem = xmemory_for_workflow()

        # Recall what the agent already knows about this customer.
        context = await mem.read(f"What should support know about {customer_id}?")

        response = f"Interaction {interaction_id} recorded for {customer_id}."

        # A durable write: enqueue + poll to completion, surviving worker restarts.
        await mem.write_durable(
            f"interaction_id: {interaction_id}. customer_id: {customer_id}. "
            f"message: {message}. response: {response}. status: completed."
        )
        return str(context.reader_result)
```

All memory I/O runs in Activities, so the Workflow stays deterministic and replay-safe: Temporal replays your Workflow code but never re-runs a completed Activity, so a replay never repeats a memory read or write.

## Durable writes

A deep xmemory extraction can take minutes. `write_durable` enqueues the write and then polls its status **from the Workflow**, so the wait is a Temporal timer in server-side history rather than a blocked Activity slot. Redeploy your Worker fleet mid-write and nothing is lost — the poll loop resumes on the new worker and completes.

```python
status = await mem.write_durable(text, max_wait=timedelta(minutes=15))
```

For the fire-and-forget pattern — kick off several writes, keep working, join before the turn ends — `write_async_start()` and `write_status()` are public too.

## Retries and idempotency

Writes default to **at-most-once** (`maximum_attempts=1`). xmemory assigns primary keys with a model, and that assignment is non-deterministic — a re-extraction can normalize the same value differently (`Dr. Robert Kim` vs `Robert Kim`) and fork the record — so a lost-response retry could duplicate. A failed write surfaces to your Workflow, which decides to retry, compensate, or fail. Reads and status-polls are idempotent and retry freely.

Opt into write retries only when your primary keys are literal identifiers present verbatim in the text (for example a `customer_id` / `interaction_id` you supply), which re-extract deterministically:

```python
mem = xmemory_for_workflow(write_retry_policy=RetryPolicy(maximum_attempts=3))
```

## Handle errors with typed failures

xmemory errors become `ApplicationError`s with stable `type` strings you can match in a `RetryPolicy` (`non_retryable_error_types=[...]`):

| xmemory condition | `type` | Retryable? |
| --- | --- | --- |
| transport error / timeout / HTTP ≥ 500 / 408 | `XmemoryServerError` / `XmemoryUnavailable` | yes |
| `RATE_LIMITED` (429) | `XmemoryRateLimited` | yes — honors `Retry-After` |
| daily quota exceeded | `XmemoryDailyQuotaExceeded` | yes (long backoff) |
| monthly quota exceeded | `XmemoryMonthlyQuotaExceeded` | no |
| `UNAUTHORIZED` / `FORBIDDEN` | `XmemoryAuthFailed` | no |
| `NOT_FOUND` | `XmemoryNotFound` | no |
| validation / schema-evolution rejections | `XmemoryBadRequest` / `XmemorySchemaRejected` | no |
| an unrecognized code | `XmemoryUnknown` | yes (never fatal) |

The durable-write loop adds `XmemoryWriteFailed`, `XmemoryWriteNotFound`, and `XmemoryWriteTimeout`, all non-retryable.

## Keep credentials and sensitive text out of history

The config carries the **name** of the environment variable holding your API key, never the key itself, so nothing secret is serialized into Activity arguments (which Temporal persists in the clear). Your memory text and queries, however, are Activity inputs and are stored in cleartext Workflow history — install a Temporal [Payload Codec](https://docs.temporal.io/develop/python/converters-and-encryption) if that content is sensitive.

## Capture Activity results into memory (optional)

Auto-capture is off by default. Enable it to record the results of your own Activities into memory through an Activity interceptor that never touches the replay path:

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

## Learn more

- Source repositories: [xmemory-temporal](https://github.com/xmemory-ai/xmemory-temporal) (Python) and [xmemory-temporal-ts](https://github.com/xmemory-ai/xmemory-temporal-ts) (TypeScript)
- [xmemory documentation](https://xmemory.ai)
