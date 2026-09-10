# Project status

Stagewise record of what's been built and verified, and what's left to
turn this from a scaffold into a deployable product. Every "done" item
below was verified against a real running Postgres instance, not just
read for correctness — two real bugs turned up doing that (see README,
"What's actually validated here").

## Stage 1 — Initial scaffold

| Item | Status |
|---|---|
| Project structure (`core/`, `workflows/`, `tools/`, `tests/`) | ✅ Done |
| `config.py` — Pydantic settings, env-driven | ✅ Done |
| `checkpointer.py` — `AsyncPostgresSaver` lifecycle | ✅ Done |
| `error_handling.py` — retry/fail-safe decorator | ✅ Done |
| `base.py` — `BaseAgentWorkflow` engine | ✅ Done |
| `claim_processor.py` — reference workflow | ✅ Done |
| `main.py` — FastAPI control plane | ✅ Done |
| Docker Compose + Dockerfile + `.env.example` | ✅ Done |
| README (adaptation guide + limitations) | ✅ Done |

## Stage 2 — Real verification

| Item | Status |
|---|---|
| Local Postgres + real checkpoint migrations run | ✅ Done |
| Integration tests against real Postgres | ✅ Done |
| Bug: `status` field not set before `interrupt()` suspends | ✅ Fixed |
| Full HTTP flow tested live (submit → pause → resume → 409 → history) | ✅ Done |
| Lint clean (`pyflakes`) | ✅ Done |

## Stage 3 — Architecture diagram

| Item | Status |
|---|---|
| Structural diagram: client → API → LangGraph pipeline → Postgres | ✅ Done |

## Stage 4 — Hardening from external review

| Item | Status |
|---|---|
| Advisory lock: corrected diagnosis, fixed transaction-hold-for-full-invoke design | ✅ Fixed |
| Status/history endpoints | ✅ Already existed (confirmed against review claim) |
| Lock/statement timeouts on connection pool | ✅ Done |
| DLQ / post-approval retry gate (`await_retry_decision`) | ✅ Done |
| Async submission (`?run_async=true`) | ✅ Done |
| Bug: `/retry` usable against the wrong (approval) gate, corrupted state | ✅ Fixed |
| Test suite expanded to 10/10, including a true-concurrency test | ✅ Done |
| Re-verified over live HTTP after every fix | ✅ Done |

## Stage 5 — Documentation

| Item | Status |
|---|---|
| README rewritten to reflect current architecture and both bugs | ✅ Done |
| This status/roadmap document | ✅ Done |

## Not done (open items, not silently skipped)

| Item | Status |
|---|---|
| Real auth (per-user/role, not a static API key) | ❌ Open |
| Checkpoint pruning/retention policy | ❌ Open |
| Per-thread token/cost tracking middleware | ❌ Open |
| Interactive visualizer (Streamlit/React) | ❌ Open |
| Second reference workflow (content review) | ❌ Open — pattern documented, not shipped |
| `hitl_approval_timeout_seconds` enforcement | ❌ Open — config exists, unused |
| Worker-crash reconciliation sweep for non-interrupt nodes | ❌ Open |

---

## What to do next to ship this as a product

The scaffold proves the pattern end-to-end and is safe to build a real
workflow on top of. It is **not** yet safe to put real money, real
approvals, or real customer data through in production. Below is a
rough phase order — each phase is roughly "what breaks first if you skip
it," not a fixed timeline.

### Phase A — Before any real approval flows through this (security & correctness)

1. **Replace the static API key with real auth.** Every approver and
   operator needs an identifiable, revocable identity — OAuth2/OIDC
   against your existing IdP, or at minimum per-user API keys with a
   roles table. `approver_id`/`operator_id` are currently caller-supplied
   strings with no verification; anyone with the shared API key can claim
   to be any approver. This is the single highest-priority gap.
2. **Enforce `hitl_approval_timeout_seconds`.** Add a scheduled job
   (Celery beat, a cron container, or a simple `asyncio` loop) that scans
   for threads paused past their timeout and force-fails or escalates
   them. Right now a claim can sit paused forever with no alert.
3. **Add the worker-crash reconciliation sweep.** A periodic job that
   finds threads with `next` set but no checkpoint update in N minutes,
   and either retries the node or surfaces it to an ops dashboard. Without
   this, a crash mid-execution on an ordinary node is invisible.
4. **Audit logging as a first-class concern, not just `get_state_history`.**
   Checkpoint history is a debugging tool, not a compliance log — it
   wasn't designed to be tamper-evident or to answer "who approved this
   and when" quickly. Consider a dedicated append-only audit table
   written alongside (not instead of) the checkpointer.

### Phase B — Before this runs at meaningful scale (operability)

5. **Checkpoint pruning.** Wire up `aprune()` on a schedule (e.g. drop
   checkpoints for threads completed > 90 days ago, keep the final one).
   Unbounded growth will eventually degrade every query against the
   checkpoints table.
6. **Structured metrics, not just log lines.** Emit counters/histograms
   for: threads paused vs. auto-resolved, time-to-approval, retry-gate
   hit rate, lock-conflict rate. These tell you if the threshold or retry
   policy needs tuning before someone complains.
7. **Cost/token tracking middleware**, keyed by `thread_id`, in its own
   table. Needed the moment a node calls a real LLM — otherwise you can't
   answer "what did this claim cost to process," especially for threads
   that got retried multiple times.
8. **Load-test the advisory lock path.** The current design is correct
   for the concurrency this scaffold was tested at, but hasn't been
   pushed against high-thread-count contention. Confirm `pg_try_advisory_lock`
   throughput and connection pool sizing under realistic concurrent
   resume volume before relying on it at scale.

### Phase C — Before non-engineers use this (product surface)

9. **Build the approval UI.** The API is the contract; nobody wants to
   `curl` an approval. A minimal React/Streamlit frontend against
   `/claims`, `/resume`, `/retry`, `/history` is the fastest path to
   something a real approver would use — this was flagged as
   "interactive visualizer" in the original scope and is still open.
10. **Notifications.** Pausing silently isn't useful — approvers and
    operators need to be told a decision is waiting (email/Slack webhook
    on transition into `paused_for_approval` / `paused_for_retry`).
11. **Second reference workflow (content review).** Prove the
    `BaseAgentWorkflow` abstraction generalizes past claims by actually
    building the second documented example — this is also the fastest
    way to find any claim-specific assumptions that leaked into the base
    class.

### Phase D — Longer-horizon hardening

12. **Multi-tenancy**, if this serves more than one organization: scope
    `thread_id`s and checkpoints by tenant, not just by UUID uniqueness.
13. **Disaster recovery drill.** Confirm Postgres backup/restore actually
    preserves in-flight paused threads correctly, not just completed ones
    — a paused thread mid-restore is the case that will actually get
    tested in production, by accident.
14. **CI pipeline** running `tests/test_workflow.py` against a throwaway
    Postgres container on every PR — currently these tests are run
    manually. This is cheap to add and should happen early, not last.
