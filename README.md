# Agent Stateful Scaffold

A production-shaped starter kit for stateful LLM workflows: LangGraph +
Postgres checkpointing, human-in-the-loop approval gates, and fault
recovery, with a control plane you can actually deploy.

Everything here has been run end-to-end against real Postgres, twice over
— once to build it, once to stress-test it against an external review.
Both passes caught real bugs, fixed below. This isn't a pattern sketch;
`docker compose up` gets you a working submit → pause → approve → resume
cycle, including a second gate for when the post-approval step itself
fails.

## Quickstart

```bash
cp .env.example .env
# fill in ANTHROPIC_API_KEY / OPENAI_API_KEY if your workflow calls an LLM
docker compose up --build
```

```bash
# Submit a claim above the auto-approve threshold -- it pauses.
curl -X POST localhost:8000/claims \
  -H "Content-Type: application/json" \
  -d '{"claimant_name":"Zoe","amount_usd":7500,"description":"urgent replacement"}'
# -> {"status": "paused_for_approval", "next": ["await_approval"], ...}

# Approve it (use the thread_id from the response above).
curl -X POST localhost:8000/claims/<thread_id>/resume \
  -H "Content-Type: application/json" \
  -d '{"decision":"approved","approver_id":"mgr_99","notes":"looks fine"}'
# -> {"status": "completed", ...}

# Full checkpoint history, for audit/debugging.
curl localhost:8000/claims/<thread_id>/history

# Submit without blocking on the pre-approval work (useful if assess_risk
# calls a slow external service in your domain).
curl -X POST "localhost:8000/claims?run_async=true" \
  -H "Content-Type: application/json" \
  -d '{"claimant_name":"Jack","amount_usd":50}'
# -> {"thread_id": "...", "status": "submitted"}  -- poll GET /claims/{id}
```

## Architecture

```
Raw request
   |
   v
[validate]  <-- entry gate: reject bad input before it costs anything
   |
   v
[assess_risk]  <-- wrapped in @with_error_recovery (retry -> fail-safe)
   |
   v (threshold routing)
[auto_approve] --------> [finalize] --> done
   |                        ^  |
[await_approval]             |  (finalize fails, retries exhausted)
   kind="approval"            v
   |                  [await_retry_decision]
   |  (resume, advisory-lock-guarded,       kind="retry"
   |   expected_kind checked)                |  retry -> back to finalize
   v                                         |  abandon -> END (FAILED)
[finalize] --> done
```

Two distinct interrupt "kinds" run through the same generic `resume()`
path: `await_approval` (customer-facing) and `await_retry_decision`
(operator-facing, only reached if the post-approval step itself fails).
`resume()` validates the caller's `expected_kind` against whichever is
actually pending, so calling the wrong endpoint against the wrong gate is
rejected instead of silently applying a resume payload the target node
wasn't expecting — see Known Limitations for why that check exists.

## How to adapt this for your own domain

1. **Define your state schema** in `app/workflows/<your_domain>.py`,
   extending `BaseWorkflowState`:
   ```python
   class ContentReviewState(BaseWorkflowState):
       draft: str
       compliance_notes: str | None = None
   ```
2. **Write your nodes** as plain async functions `(state) -> dict`. Wrap
   any node that calls an external API in `@with_error_recovery(...)`.
3. **Put a human gate where you need one** — call `interrupt({"kind": "...", ...})`
   inside a node. Always set `"kind"` so `BaseAgentWorkflow` can report
   the right status and so `resume()` can validate the right endpoint is
   being used against it. The rest of the payload is exactly what a
   caller sees when they fetch the paused thread's state — design it as
   the approval UI's data contract.
4. **If a step downstream of a human gate can itself fail** (a payout
   call, a publish call), wrap it in `@with_error_recovery` and add a
   second interrupt-based gate for the failure case, mirroring
   `await_retry_decision` in `claim_processor.py`. Don't route a failed
   post-approval step straight to `END` — that's a silent data loss path,
   and don't auto-loop it either — a synchronous retry inside one
   `ainvoke()` isn't a real retry, it just re-fails immediately.
5. **Subclass `BaseAgentWorkflow`**, set `state_schema`, implement
   `build_graph()` returning an *uncompiled* `StateGraph` — the base
   class compiles it with the shared checkpointer.
6. **Add routes** in `app/main.py` (or a new router) mirroring the
   `claims` routes: submit / resume / retry / status / history.

The second reference workflow this scaffold is designed to support —
multi-step content review (`research → draft → compliance → manager
approval → publish`) — is a straightforward extension of this same
pattern; not included yet, see Known Limitations.

## What's actually validated here (and what isn't)

**Validated with real Postgres integration runs**
(`tests/test_workflow.py`, 10/10 passing, plus manual end-to-end curl
flows re-run after every change below):
- Low-value claims auto-approve without ever pausing.
- High-value claims pause at `await_approval` and resume correctly.
- **Concurrent double-resume is rejected**, via a non-blocking Postgres
  advisory lock (`pg_try_advisory_lock`) scoped to `thread_id` — tested
  with genuinely overlapping `asyncio.gather()` calls, not just
  sequential ones, so the lock's non-blocking behavior is actually
  exercised.
- **Post-approval failures pause for an operator, not a silent fail**:
  if `finalize` exhausts its automatic retries, the thread pauses at
  `await_retry_decision` instead of marking the claim FAILED outright.
  An operator can retry the same step without re-triggering customer
  approval, or abandon it.
- **Wrong-gate resume attempts are rejected**: calling `/retry` against a
  thread that's actually paused for customer approval (or vice versa)
  fails with a 409 and leaves the thread's state untouched.
- Resuming an unknown or non-paused thread fails loudly instead of
  silently no-op'ing.
- Full checkpoint history is queryable per thread.

**Two real bugs this process caught** — both are the kind that pass a
"looks right" read and only surface under an actual Postgres run:

1. `interrupt()` suspends execution **inside** a node, before that node's
   return value is ever applied — so a `status` field the node was
   *going* to set (e.g. `"paused_for_approval"`) never gets persisted.
   Effective status is derived from `snapshot.tasks[*].interrupts` in
   `BaseAgentWorkflow._effective_status()`, never trusted from
   application state. If you add new interrupt points, don't reintroduce
   a status-field-set-by-the-interrupted-node pattern — it silently
   doesn't work.
2. Adding a second interrupt kind (the retry gate) initially had no
   guard against calling the wrong resume endpoint against the wrong
   pending interrupt — `/retry` against an approval-paused thread
   returned 200 and silently injected a resume payload the approval node
   wasn't expecting, corrupting `approval_decision` to `null`. Fixed by
   validating `expected_kind` against the pending interrupt's declared
   `"kind"` before applying anything. If you add a third gate, make sure
   its resume call also passes `expected_kind` — nothing enforces this
   at the type level, only by convention.

Also worth knowing if you extend the locking behavior: the original
advisory-lock design held a SQL transaction open for the *entire*
`ainvoke()` call, including any external API/LLM time inside the graph —
that's fine for a crashed worker (Postgres releases the lock when the
connection drops) but bad for a merely *slow* one (blocks all other
resumes with no bound, and holds a long transaction open). Current design
uses a non-blocking `pg_try_advisory_lock`, releases it explicitly in
`finally`, and bounds the invoke itself with `asyncio.wait_for`.

**Not yet built** (be aware before treating this as fully "production
ready"):
- **Auth is a single static API key**, not per-approver/per-operator
  identity or roles — fine for a demo, not for real approval
  accountability. Swap `require_api_key` for real auth (OAuth/JWT + an
  approver directory) before this touches real approvals or retries.
- **No checkpoint pruning/retention policy** — `aprune()` exists on the
  saver but isn't wired up. Long-lived threads with many resumes will
  grow the checkpoints table indefinitely.
- **No per-thread token/cost tracking middleware** — add it as a wrapper
  around your LLM calls inside node bodies, keyed by `thread_id`, written
  to its own table (don't overload the checkpoint state with cost
  accounting).
- **No interactive visualizer** (Streamlit/React) — the `/history`
  endpoint gives you the raw data to build one against.
- **`hitl_approval_timeout_seconds` is defined in config but not
  enforced** — nothing currently expires a stale pending approval or a
  stale pending retry decision. Add a scheduled job that checks
  `get_state_history` age and force-fails threads past the timeout, if
  that matters for your domain.
- **Second reference workflow (content review) not implemented** — the
  claim processor is the fully-worked example; content review is
  described above as the pattern to copy but isn't shipped as code.
- **No worker-crash reconciliation sweep** — if a worker dies mid-`ainvoke`
  on a *non-interrupt* node (not the approval or retry gates, an ordinary
  node like `assess_risk`), the thread is left with a checkpoint pointing
  at that node as `next`, and nothing automatically re-drives it. A
  status endpoint will show it as `running` indefinitely. A periodic job
  that finds threads with `next` set but no recent checkpoint update and
  either retries or force-fails them is not yet built.

## Project layout

```
agent-stateful-scaffold/
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── requirements.txt
├── pytest.ini
├── README.md
├── PROJECT_STATUS.md              # stagewise checklist + delivery roadmap
├── app/
│   ├── main.py                    # FastAPI: submit, resume, retry, status, history
│   ├── core/
│   │   ├── config.py              # Pydantic settings, env-driven
│   │   ├── checkpointer.py        # AsyncPostgresSaver lifecycle (pool, setup, close, timeouts)
│   │   └── error_handling.py      # @with_error_recovery: retry -> fail-safe
│   └── workflows/
│       ├── base.py                # BaseAgentWorkflow: submit/resume/history/rollback, lock + kind guard
│       └── claim_processor.py     # Reference implementation, incl. post-approval retry gate
└── tests/
    └── test_workflow.py           # Integration tests against real Postgres (10/10)
```
