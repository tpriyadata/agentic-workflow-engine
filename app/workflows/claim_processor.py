"""
Reference implementation #1: claim/ticket approval.

Flow:
    validate -> assess_risk -> [auto-approve | pause for human] -> finalize
                                                                       |
                                                          (finalize fails) -> await_retry_decision
                                                                       |            |
                                                                  retry -----------+
                                                                  abandon -> END (FAILED)

Below the risk threshold, the claim auto-approves and finalizes in one
`submit()` call. At or above threshold, execution pauses at `await_approval`
(an interrupt) until someone calls resume() with an approve/reject decision.

`finalize` stands in for the step that actually matters post-approval --
e.g. calling a payout API. If it fails (transient errors retry automatically
via @with_error_recovery; if those are exhausted), the thread pauses again
at `await_retry_decision` instead of either silently marking the claim
failed or auto-looping. An operator can retry the same finalize step
*without* re-triggering the customer-facing approval gate, or abandon it.

This is the template to copy for a new domain: swap the node bodies, keep
the state-machine and interrupt shape -- including the two distinct kinds
of interrupt this file uses (kind="approval" vs kind="retry"), which is
how BaseAgentWorkflow tells them apart in status reporting.
"""
from __future__ import annotations

from typing import Literal

from langgraph.graph import END, StateGraph
from langgraph.types import interrupt

from app.core.error_handling import with_error_recovery
from app.workflows.base import BaseAgentWorkflow, BaseWorkflowState, WorkflowStatus

AUTO_APPROVE_THRESHOLD_USD = 1000.0


class ClaimState(BaseWorkflowState):
    claim_id: str
    claimant_name: str
    amount_usd: float
    description: str = ""

    risk_score: float | None = None
    auto_approved: bool | None = None
    approver_id: str | None = None
    approval_decision: Literal["approved", "rejected"] | None = None
    approval_notes: str | None = None
    retry_requested: bool | None = None

    # Demo/test-only hook to force the finalize step to fail, so the
    # retry-gate path is exercisable without a real payout integration.
    # Not something a real deployment would keep on its state schema --
    # swap `_call_payout_api` below for a real client call and drop this.
    simulate_payout_failure: bool = False


async def validate(state: ClaimState) -> dict:
    """Entry gate: reject malformed claims before they cost an LLM call or
    reach a human. This is the boundary check the Phase-4 discipline calls
    for -- cheap, deterministic, first."""
    if state.amount_usd <= 0:
        return {
            "status": WorkflowStatus.FAILED,
            "error": f"invalid claim amount: {state.amount_usd}",
        }
    return {"last_completed_node": "validate"}


def route_after_validate(state: ClaimState) -> str:
    return "end" if state.status == WorkflowStatus.FAILED else "assess_risk"


@with_error_recovery(max_retries=2)
async def assess_risk(state: ClaimState) -> dict:
    """Stand-in for a real risk model / fraud-check API call. Wrapped in
    error recovery because in production this hits an external service and
    can time out -- swap the body for your actual call."""
    score = min(state.amount_usd / 10000, 1.0)
    if "urgent" in state.description.lower():
        score = min(score + 0.2, 1.0)
    return {"risk_score": round(score, 3), "last_completed_node": "assess_risk"}


def route_after_risk(state: ClaimState) -> str:
    """Threshold-based routing: this is the actual business rule for when a
    human must be in the loop. A pre-approval failure (assess_risk
    exhausted its retries) ends the run here -- no decision has been
    presented to a human yet, so there's nothing to retry post-approval;
    the caller just resubmits."""
    if state.status == WorkflowStatus.FAILED:
        return "end"
    if state.amount_usd >= AUTO_APPROVE_THRESHOLD_USD or (state.risk_score or 0) >= 0.5:
        return "await_approval"
    return "auto_approve"


async def auto_approve(state: ClaimState) -> dict:
    return {
        "auto_approved": True,
        "approval_decision": "approved",
        "last_completed_node": "auto_approve",
    }


async def await_approval(state: ClaimState) -> dict:
    """The customer-facing HITL gate. `interrupt()` pauses the graph here
    -- durably, via the checkpointer -- until resume() supplies a decision.
    `kind: "approval"` is what BaseAgentWorkflow reads to report this as
    PAUSED_FOR_APPROVAL rather than PAUSED_FOR_RETRY."""
    decision = interrupt(
        {
            "kind": "approval",
            "reason": "manual_approval_required",
            "claim_id": state.claim_id,
            "amount_usd": state.amount_usd,
            "risk_score": state.risk_score,
        }
    )
    # expected resume payload: {"decision": "approved"|"rejected", "approver_id": str, "notes": str}
    return {
        "auto_approved": False,
        "approval_decision": decision.get("decision"),
        "approver_id": decision.get("approver_id"),
        "approval_notes": decision.get("notes"),
        "last_completed_node": "await_approval",
    }


async def _call_payout_api(state: ClaimState) -> None:
    """Stand-in for the real post-approval side effect. Swap this for your
    actual payout/ticket-closing call. Raises ConnectionError to simulate
    a transient outage -- @with_error_recovery on `finalize` retries this
    automatically before giving up."""
    if state.simulate_payout_failure:
        raise ConnectionError("payout API unreachable")


@with_error_recovery(max_retries=2)
async def finalize(state: ClaimState) -> dict:
    await _call_payout_api(state)
    return {"status": WorkflowStatus.COMPLETED, "last_completed_node": "finalize"}


def route_after_finalize(state: ClaimState) -> str:
    """If finalize exhausted its automatic retries, don't silently mark the
    claim failed and don't auto-loop (a synchronous retry loop inside one
    ainvoke() call isn't a real retry -- it just burns the same failure
    again immediately). Pause for an operator instead."""
    return "retry_gate" if state.status == WorkflowStatus.FAILED else "end"


async def await_retry_decision(state: ClaimState) -> dict:
    """Post-approval failure gate. Distinct from `await_approval` -- this
    one asks an operator "retry the payout" or "abandon", not "approve
    this claim". `kind: "retry"` is what makes BaseAgentWorkflow report
    this as PAUSED_FOR_RETRY."""
    decision = interrupt(
        {
            "kind": "retry",
            "reason": "post_approval_step_failed",
            "claim_id": state.claim_id,
            "error": state.error,
        }
    )
    # expected resume payload: {"action": "retry"|"abandon"}. An operator
    # confirming the underlying outage is fixed can also clear the demo
    # failure hook here -- state changes belong in a node's return value,
    # not injected out-of-band via aupdate_state (which doesn't preserve
    # in-flight interrupt bookkeeping the way a normal node return does).
    updates: dict = {"retry_requested": decision.get("action") == "retry"}
    if "simulate_payout_failure" in decision:
        updates["simulate_payout_failure"] = decision["simulate_payout_failure"]
    return updates


def route_after_retry_gate(state: ClaimState) -> str:
    return "finalize" if state.retry_requested else "end"


class ClaimProcessorWorkflow(BaseAgentWorkflow[ClaimState]):
    state_schema = ClaimState
    interrupt_before: list[str] = []  # not used here -- we interrupt via interrupt() instead

    def build_graph(self) -> StateGraph:
        graph = StateGraph(ClaimState)
        graph.add_node("validate", validate)
        graph.add_node("assess_risk", assess_risk)
        graph.add_node("auto_approve", auto_approve)
        graph.add_node("await_approval", await_approval)
        graph.add_node("finalize", finalize)
        graph.add_node("await_retry_decision", await_retry_decision)

        graph.set_entry_point("validate")
        graph.add_conditional_edges(
            "validate", route_after_validate, {"end": END, "assess_risk": "assess_risk"}
        )
        graph.add_conditional_edges(
            "assess_risk",
            route_after_risk,
            {"auto_approve": "auto_approve", "await_approval": "await_approval", "end": END},
        )
        graph.add_edge("auto_approve", "finalize")
        graph.add_edge("await_approval", "finalize")
        graph.add_conditional_edges(
            "finalize", route_after_finalize, {"retry_gate": "await_retry_decision", "end": END}
        )
        graph.add_conditional_edges(
            "await_retry_decision",
            route_after_retry_gate,
            {"finalize": "finalize", "end": END},
        )
        return graph
