# Testing

The test strategy is built around one question Temporal's review cares about most:
**does a memory operation ever run twice — or fail to run — under retries and
replay?** Everything below exists to answer that with evidence, not assertion.

Two principles shape the suite:

- **No live backend by default.** Every test except the opt-in end-to-end one
  injects a fake xmemory instance, so the suite is deterministic, fast, and
  runnable offline (including in CI with no secrets). The fake is a call ledger,
  which is what the replay-safety tests assert against.
- **Real Temporal, skipped time.** Integration tests run an actual Temporal
  worker against a time-skipping test server, so `workflow.sleep` and durable
  poll loops that model a 15-minute write complete in milliseconds while still
  exercising the real scheduling, activity, and replay machinery.

## Test layers

### 1. Unit

Each module verified in isolation, no worker involved.

- **`test_config.py`** — env-var sourcing; the API key never appears in
  `model_dump_json()` (a regression guard against leaking it into history); a
  missing key raises eagerly at config resolution (`resolve_api_key`).
- **`test_error_mapping.py`** — table-driven over every `XmemoryAPIError` →
  `ApplicationError` mapping, using synthetic errors constructed directly (no
  live HTTP): status/code combinations, `Retry-After` → `next_retry_delay`, and
  the unknown-code case (must stay retryable and never raise).
- **`test_activities.py`** — activities in isolation via `ActivityEnvironment`:
  vendor result → our DTO projection, per-context client binding (the
  `ContextVar` isolation), no transport detail leaking into the serialized
  failure chain, and the activity-name string literals pinned.
- **`test_facade_parity.py`** — an `inspect.signature` reflection test asserting
  the workflow-side `WorkflowXmemory` stays method-compatible with the real
  async client, so a client release can't silently drift the migration story.

### 2. Integration (real worker, time-skipped)

- **`test_workflow_read_write.py`** — read/write round-trips through a real
  worker and the plugin; call counts; the **at-most-once write default** and the
  **opt-in retry** path.
- **`test_write_async_polling.py`** — the durable-write loop: polls to
  completion, terminal `failed`, `not_found` grace window (tolerated on early
  polls, terminal once it persists), and `max_wait` timeout — all in
  milliseconds despite modeling a multi-minute write.
- **`test_interceptor.py`** — auto-capture: projection, sampling (deterministic
  crc32 bucket), fail-open (a capture error never fails the wrapped activity),
  and the recursion guard (xmemory's own write activity is never re-captured).

### 3. Replay safety — the mandatory test

- **`test_replay_side_effects.py`** — runs the worker with
  `max_cached_workflows=0`, which evicts the workflow after every task and forces
  a full replay from history. For the direct read/write workflows the **history
  level** is the exact assertion — N logical operations produce exactly N
  `ActivityTaskScheduled` events, the retry-independent pattern Temporal's guide
  names (each intended call is one scheduled event, regardless of retries or
  replays) — with a **ledger-level** cross-check that the fake saw each write
  once. For the durable-write workflow the poll loop makes the scheduled-event
  total variable, so there the ledger's exact `write_async == 1` pins the single
  enqueue and the history count is only a lower bound (`>= 1`). A deliberate
  **sensitivity control** (a double-write workflow) proves the harness reports
  *two* when there are two — so the "exactly one" assertions can actually fail.

### 4. Replayer

- **`test_replayer.py`** — records a workflow history in the time-skipping
  environment and replays it with `Replayer`, catching nondeterminism within a
  run. A representative checked-in history corpus (to catch a future build
  breaking replay of a *past* one) is a documented follow-up.

### 5. End-to-end (live)

- **`test_e2e_live.py`** — marked `@pytest.mark.live` and **skipped unless**
  `XMEM_API_KEY` and `XMEM_INSTANCE_ID` are set. It runs one write → poll → read
  round-trip against a real xmemory backend. Run it before a release, and pair it
  with the manual durability demo below.

## The injected fake

`tests/fakes.py::FakeXmemoryInstance` implements the same narrow protocol the
plugin depends on and records every call as a `CallRecord`. It is scriptable
(`fail_write_times(n, exc)`, `status_sequence([...])`) and is the replay test's
call ledger: the exact `ActivityTaskScheduled` event count is authoritative for
the direct read/write workflows, while the ledger's own counts pin the
durable-write enqueue (`write_async == 1`). Because the instance is *injected*
through the plugin, no test needs monkeypatching.

## Running the tests

```bash
uv sync --dev

# Everything except the live e2e (it self-skips without credentials):
uv run pytest

# Lint, format check, and type check (the same targets CI runs):
uv run ruff check src tests examples
uv run ruff format --check src tests examples
uv run pyright src tests examples

# The live end-to-end test, against a real backend:
XMEM_API_KEY=xmem_... XMEM_INSTANCE_ID=... uv run pytest -m live
```

### Manual end-to-end run

Drives the full path (dev server, worker, real backend) through the scripts in
[`examples/`](./examples). Three terminals:

```bash
# Credentials. XMEM_API_URL matters: the client falls back to
# https://api.xmemory.ai when it is unset, so a staging key needs it set.
export XMEM_API_KEY=xmem_...
export XMEM_API_URL=https://api.stg.xmemory.ai      # omit only for production

# Terminal 1: a local Temporal dev server (UI on http://localhost:8233)
temporal server start-dev

# Terminal 2: create an instance with a name-keyed schema, then run the worker
export XMEM_INSTANCE_ID="$(uv run python examples/setup_memory.py)"
uv run python examples/worker.py
#   worker running on task queue 'xmemory-example' - Ctrl-C to stop

# Terminal 3: drive one workflow
uv run python examples/run_workflow.py
#   agent recalled: {'answer': 'Prefers email over phone calls.'}
```

The workflow does a `write_durable` and then reads the fact back, so a successful
run exercises the plugin, the client, and the durable poll loop against a real
backend. Inspect the run in the Temporal UI to check that activity summaries
render legibly.

**The durability demo.** The whole value proposition is that a durable write
survives worker death, so also **kill the worker mid-`write_durable` and restart
it**: the poll loop must resume from history and complete rather than restarting
the write.

## Continuous integration

In this monorepo the package is linted, type-checked, and tested in its own
environment via `integrations/temporal/gate.sh`, which for Python runs, each with
`uv run --directory`:

1. `ruff check` + `ruff format --check` on `src tests examples`
2. `pyright` on `src tests examples`
3. `pytest` — no marker filter, so the live e2e is excluded only by the
   `XMEM_API_KEY` / `XMEM_INSTANCE_ID` `skipif` in `test_e2e_live.py`; it runs
   when those are exported.

The integration is not yet published — see [`PUBLISHING-LATER.md`](../PUBLISHING-LATER.md).
The standalone mirror repository (private during the Temporal review) runs the
same checks in GitHub Actions as a matrix over **Python 3.10 / 3.12 / 3.13**,
each leg reporting as a check named `Python 3.10` / `Python 3.12` / `Python 3.13`,
gated by branch protection before any PyPI release.

> `examples/` is deliberately included in lint/type-check — the examples are the
> advertised migration story, so a broken one fails the gate rather than shipping.
