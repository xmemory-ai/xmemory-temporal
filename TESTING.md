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
  missing key fails at worker start, not on the first activity.
- **`test_error_mapping.py`** — table-driven over every `XmemoryAPIError` →
  `ApplicationError` mapping, driven through `respx` so real HTTP
  statuses/codes/`Retry-After` headers exercise the real client code path.
  Includes the unknown-code case (must stay retryable and never raise).
- **`test_activities.py`** — activities in isolation via `ActivityEnvironment`:
  vendor result → our DTO projection, per-context client binding (the
  `ContextVar` isolation), no transport detail leaking into the serialized
  failure chain, and the activity-name string literals pinned.
- **`test_facade_parity.py`** — an `inspect.signature` reflection test asserting
  the workflow-side `WorkflowXmemory` stays method-compatible with the real
  async client, so a client release can't silently drift the migration story.

### 2. Integration (real worker, time-skipped)

- **`test_workflow_read_write.py`** — read/write round-trips through a real
  worker and the plugin; per-call sequencing; the **at-most-once write default**
  and the **opt-in retry** path.
- **`test_write_async_polling.py`** — the durable-write loop: polls to
  completion, backoff growth, terminal `failed`, `not_found` grace window
  (tolerated on early polls, terminal once it persists), and `max_wait`
  timeout — all in milliseconds despite modeling a multi-minute write.
- **`test_interceptor.py`** — auto-capture: projection, sampling (deterministic
  crc32 bucket), fail-open (a capture error never fails the wrapped activity),
  and the recursion guard (xmemory's own write activity is never re-captured).

### 3. Replay safety — the mandatory test

- **`test_replay_side_effects.py`** — runs the worker with
  `max_cached_workflows=0`, which evicts the workflow after every task and forces
  a full replay from history. The **primary** assertion is at the **history
  level** — N logical operations produce exactly N `ActivityTaskScheduled`
  events, the retry-independent pattern Temporal's guide names (each intended call
  is one scheduled event, regardless of retries or replays). A complementary
  **ledger-level** cross-check confirms the injected fake saw each logical write
  exactly once. A deliberate **sensitivity control** (a double-write workflow)
  proves the harness reports *two* when there are two — so the "exactly one"
  assertions can actually fail.

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
(`fail_write_times(n, exc)`, `status_sequence([...])`) and provides the replay
test's complementary ledger cross-check (the authoritative assertion there is the
`ActivityTaskScheduled` event count). Because the instance is *injected* through the plugin,
no test needs monkeypatching.

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

### Manual durability demo

The whole value proposition is that a durable write survives worker death. Verify
it by running the example worker (`examples/worker.py`) against a local
`temporal server start-dev` and a real instance, then **killing the worker
mid-`write_durable` and restarting it** — the poll loop must resume from history
and complete.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request, as a matrix over
**Python 3.10 / 3.12 / 3.13** (floor matching `xmemory-ai`, through current):

1. `uv sync --dev` (locked install)
2. `ruff check` + `ruff format --check` on `src tests examples`
3. `pyright` on `src tests examples`
4. `pytest -m "not live"` (the live test is excluded — CI has no backend)

Each matrix leg reports as a check named `Python 3.10` / `Python 3.12` /
`Python 3.13`. `main` is branch-protected: all three must pass before a pull
request can merge.

> `examples/` is deliberately included in lint/type-check — the examples are the
> advertised migration story, so a broken one fails the gate rather than shipping.
