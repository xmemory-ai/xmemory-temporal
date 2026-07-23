# xmemory integration

> Add durable, schema-grounded agent memory to your Temporal Workflows in Python with the xmemory plugin.

Temporal's integration with [xmemory](https://xmemory.ai) lets you read and write agent memory directly from your Workflow code while Temporal handles Durable Execution. xmemory is a memory store for agents: it holds durable, schema-grounded knowledge your Workflows can recall across runs, users, and services.

Like all API calls, xmemory reads and writes are non-deterministic. In a [Temporal Application](/glossary#temporal-application), that means you cannot call xmemory directly from a [Workflow](/glossary#workflow); it must run as an [Activity](/glossary#activity). The xmemory plugin handles this automatically: the workflow-side handle you call (`read`, `write`, `write_durable`) dispatches to Activities behind the scenes. This preserves the plain xmemory client's developer experience while Temporal handles Durable Execution for you — a memory write becomes a durable step that survives worker crashes, redeploys, and rolling upgrades.

The code in this guide is based on the [examples in the xmemory-temporal repository](https://github.com/xmemory-ai/xmemory-temporal/tree/main/examples).

> **Preview**
>
>    The plugin is under review for Temporal's AI Partner Ecosystem and is not yet on PyPI. Install it from the [source repository](https://github.com/xmemory-ai/xmemory-temporal); the PyPI release (`xmemory-temporal`) follows once the review completes.

## Prerequisites

- This guide assumes you are already familiar with xmemory. If you aren't, refer to the [xmemory documentation](https://xmemory.ai) for more details.
- If you are new to Temporal, we also recommend you read the [Understanding Temporal](/evaluate/understanding-temporal) document or take the [Temporal 101](https://learn.temporal.io/courses/temporal_101/) course to understand the basics of Temporal.
- Ensure you have set up your local development environment by following the [Set up your local with the Python SDK](/develop/python/set-up-your-local-python) guide. When you are done, leave the Temporal Development Server running if you want to test your code locally.
- An [xmemory](https://xmemory.ai) API key and instance.

## Configure Workers to use xmemory

Workers are the compute layer of a Temporal Application. They are responsible for executing the code that defines your [Workflows](/glossary#workflow) and [Activities](/glossary#activity). Before you can execute a Workflow that uses xmemory, you need to create a Worker and configure it to use the xmemory plugin.

Follow the steps below to configure your Worker.

1. Install the `xmemory-temporal` package (from source until the PyPI release).

   ```bash
   pip install "git+https://github.com/xmemory-ai/xmemory-temporal.git"
   ```

2. Provide your xmemory API key through the environment. The plugin reads it by name, so it is never serialized into Workflow history.

   ```bash
   export XMEM_API_KEY="your-xmemory-api-key"
   ```

3. Create a `worker.py` file and register the xmemory plugin on your Temporal Client. The Worker inherits the Client's plugins automatically.

   ```python {6-7}
   from temporalio.client import Client
   from temporalio.worker import Worker
   from xmemory_temporal import XmemoryConfig, XmemoryPlugin
   from workflows import CustomerSupportWorkflow

   config = XmemoryConfig(instance_id="<your-instance-id>")  # reads XMEM_API_KEY
   plugin = XmemoryPlugin(config)

   client = await Client.connect("localhost:7233", plugins=[plugin])
   worker = Worker(client, task_queue="xmemory-support", workflows=[CustomerSupportWorkflow])
   await worker.run()
   ```

   In the Worker options, you are specifying that the Worker polls the `xmemory-support` Task Queue in the `default` Namespace. Make sure that you configure your Client application to use the same Task Queue and Namespace.

4. Run the Worker. This Worker will now poll the Temporal Service for work on the `xmemory-support` Task Queue until you stop it.

   ```bash
   python worker.py
   ```

> **💡 Tip:**
>
>    Register the plugin only once. In Python, put it on the Client and the Worker inherits it; passing it to both registers the Activities twice and raises `More than one activity named xmemory_read`.

## Develop a durable memory Workflow

If you weren't using Temporal, you would read and write memory with the xmemory client directly:

```python
inst = AsyncXmemoryClient(api_key=...).instance(instance_id)
context = await inst.read(f"What should support know about {customer_id}?")
await inst.write("... interaction summary ...")
```

To add Durable Execution, implement the same logic as a Temporal Workflow. Call `xmemory_for_workflow()` to get a handle with the same methods. The call sites are identical, so migration is near-zero-diff.

```python {13,15,19}
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from xmemory_temporal import xmemory_for_workflow


@workflow.defn
class CustomerSupportWorkflow:
    @workflow.run
    async def run(self, customer_id: str, interaction_id: str, message: str) -> str:
        mem = xmemory_for_workflow()

        context = await mem.read(f"What should support know about {customer_id}?")

        response = f"Interaction {interaction_id} recorded for {customer_id}."

        await mem.write_durable(
            f"interaction_id: {interaction_id}. customer_id: {customer_id}. "
            f"message: {message}. response: {response}. status: completed."
        )
        return str(context.reader_result)
```

All memory I/O runs in Activities, so the Workflow stays deterministic and replay-safe: Temporal replays your Workflow code but never re-runs a completed Activity, so a replay never repeats a memory read or write.

## Write durably

A deep xmemory extraction can take minutes. `write_durable` enqueues the write and then polls its status from the Workflow, so the wait is a Temporal timer in server-side history rather than a blocked Activity slot. Redeploy your Worker fleet mid-write and nothing is lost — the poll loop resumes on the new worker and completes.

```python
status = await mem.write_durable(text, max_wait=timedelta(minutes=15))
```

For the fire-and-forget pattern — kick off several writes, keep working, join before the turn ends — `write_async_start()` and `write_status()` are public too.

## Retries and idempotency

Writes default to at-most-once (`maximum_attempts=1`). xmemory assigns primary keys with a model, and that assignment is non-deterministic — a re-extraction can normalize the same value differently (`Dr. Robert Kim` vs `Robert Kim`) and fork the record — so a lost-response retry could duplicate. A failed write surfaces to your Workflow, which decides to retry, compensate, or fail. Reads and status-polls are idempotent and retry freely.

Opt into write retries only when your primary keys are literal identifiers present verbatim in the text, such as a `customer_id` or `interaction_id` you supply, which re-extract deterministically:

```python
mem = xmemory_for_workflow(write_retry_policy=RetryPolicy(maximum_attempts=3))
```

## Handle errors with typed failures

xmemory errors become `ApplicationError`s with stable `type` strings you can match in a `RetryPolicy` with `non_retryable_error_types`:

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

The config carries the name of the environment variable holding your API key, never the key itself, so nothing secret is serialized into Activity arguments, which Temporal persists in the clear. Your memory text and queries, however, are Activity inputs and are stored in cleartext Workflow history — install a Temporal [Payload Codec](/develop/python/converters-and-encryption) if that content is sensitive.

## Capture Activity results into memory

Auto-capture is off by default. Enable it to record the results of your own Activities into memory through an Activity interceptor that never touches the replay path.

```python {5-8}
from xmemory_temporal import AutoCaptureConfig, XmemoryPlugin

plugin = XmemoryPlugin(
    config,
    auto_capture=AutoCaptureConfig(
        project=lambda activity_name, result: summarize(result),  # return None to skip
        sample_rate=0.25,
    ),
)
```

See the full example in the [xmemory-temporal repository](https://github.com/xmemory-ai/xmemory-temporal/tree/main/examples).
