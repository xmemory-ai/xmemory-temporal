# Testing

The suite answers one question with evidence: **does a memory operation ever run
twice — or fail to run — under retries and replay?**

Two principles shape it:

- **No live backend by default.** Every test but the opt-in end-to-end one injects
  a fake instance, so the suite runs offline and in CI with no secrets. The fake is
  a call ledger, which is what the replay-safety tests assert against.
- **Real Temporal, skipped time.** Integration tests run a real worker against a
  time-skipping test server, so a 15-minute poll loop completes in milliseconds
  while still exercising real scheduling, activity, and replay machinery.

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
  completion, terminal `failed`, `not_found` (terminal on the first result,
  since `write_async` is transactional), and `max_wait` timeout — all in
  milliseconds despite modeling a multi-minute write.
- **`test_interceptor.py`** — auto-capture: projection, sampling (deterministic
  crc32 bucket), fail-open (a capture error never fails the wrapped activity),
  and the recursion guard (xmemory's own write activity is never re-captured).

### 3. Replay safety

- **`test_replay_side_effects.py`** — runs with `max_cached_workflows=0`, evicting
  the workflow after every task and forcing a full replay. For direct read/write
  workflows the assertion is exact: N logical operations produce N
  `ActivityTaskScheduled` events, a count that is retry-independent. The durable
  workflow's poll loop makes that total variable, so there the ledger's
  `write_async == 1` pins the single enqueue instead. A double-write control proves
  the harness reports two when there are two, so "exactly one" can actually fail.

### 4. Replayer

- **`test_replayer.py`** — records a workflow history in the time-skipping
  environment and replays it with `Replayer`, catching nondeterminism within a
  run. A representative checked-in history corpus (to catch a future build
  breaking replay of a *past* one) is a documented follow-up.

> **Replay across builds.** `test_replayer.py` records and replays within one
> build, which catches nondeterminism inside a version but not between them.
> `write_durable` polls from the *caller's* workflow, so its command sequence is
> part of their history: changing the loop breaks an execution already in flight.
> That is safe only because nothing has been released yet. From the first release
> on, any change to the loop needs `workflow.patched(...)`, the old branch kept,
> and a checked-in history to replay against.

### 5. End-to-end (live)

- **`test_e2e_live.py`** — marked `@pytest.mark.live` and **skipped unless**
  `XMEM_API_KEY` and `XMEM_INSTANCE_ID` are set. It runs one write → poll → read
  round-trip against a real xmemory backend. Run it before a release, and pair it
  with the manual durability demo below.

## The injected fake

`tests/fakes.py::FakeXmemoryInstance` implements the protocol the plugin depends on
and records every call. It is scriptable (`fail_write_times(n, exc)`,
`status_sequence([...])`) and serves as the replay tests' ledger. Because the
instance is injected through the plugin, no test needs monkeypatching.

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
# Credentials. The client defaults to https://api.xmemory.ai, so set
# XMEM_API_URL when your key belongs to some other environment.
export XMEM_API_KEY=xmem_...

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

The workflow writes durably and reads the fact back, so a successful run exercises
the plugin, the client, and the poll loop against a real backend.

**The durability demo.** Kill the worker mid-`write_durable` and restart it: the
poll loop must resume from history and complete, not restart the write.

## Continuous integration

GitHub Actions runs a matrix over **Python 3.10 / 3.11 / 3.12 / 3.13**, each leg
reporting as its own check (`Python 3.10` and so on) and gated by branch
protection before any release. Every leg runs:

1. `ruff check` + `ruff format --check` on `src tests examples`
2. `pyright` on `src tests examples`
3. `pytest` — no marker filter, so the live e2e is excluded only by the
   `XMEM_API_KEY` / `XMEM_INSTANCE_ID` `skipif` in `test_e2e_live.py`; it runs
   when those are exported.

The release workflow re-runs all three, then checks that the git tag matches
`pyproject.toml`, that both artifacts pass `twine check --strict`, and that
`LICENSE` and `NOTICE` are actually in them.

> `examples/` is deliberately included in lint/type-check — the examples are the
> advertised migration story, so a broken one fails the gate rather than shipping.

## Clock assumptions

Deadlines are measured from a monotonic stamp taken by the plugin's outermost
interceptor, plus `started_time` for what Temporal did before it — payload decoding
and Client-carried interceptors. That reading crosses clock domains, so:

* Clock **ahead** of the service, or agreeing: pre-interceptor time is
  `now - started_time` minus the stamp, and can only shorten the budget.
* Clock **behind** the service: the figure goes negative, which is impossible, so
  the time is real but unmeasurable. The activity fails with retryable
  `XmemoryClockUnusable` rather than guess — a reserve that under-shoots lets the
  client outlive the activity and duplicate a write. A worker with a synced clock
  can run the attempt.

**How much of a Client-carried interceptor is charged is not deterministic.**
`started_time` is the service's record of when the attempt began, and it can be
stamped partway through such an interceptor's work. Measured against a 2s
interceptor, the charge ranged from 2.00s (most runs) down to 0.80s under load. So
this is a best-effort improvement, not a guarantee, and the client margin covers the
remainder — which is why there is no test asserting a particular charge for that
case. The arithmetic itself is pinned by unit tests that supply `started_time`
directly, and an interceptor registered on the *worker* — inside our stamp — is
charged deterministically and is tested that way.

Keep worker clocks in NTP sync. The one exception is why
`XmemoryConfig(allow_unmeasurable_clock=True)` exists: the time-skipping test
server advances the service clock past the worker's whenever a timer is skipped,
which looks exactly like a worker clock that is behind. The time-skipping tests set
that flag; the `local_env` tests do not, so the strict path stays covered.

The TypeScript port cannot do this — its SDK exposes no attempt-start timestamp, so
it reserves a capped allowance instead. See that repository's TESTING.md.
