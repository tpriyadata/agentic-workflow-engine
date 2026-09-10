"""
Tests target the failure modes that matter for a stateful HITL system, not
just the happy path:

- low-value claim auto-approves without ever pausing
- high-value claim pauses at await_approval and resumes correctly
- resuming a thread that isn't paused is rejected (WorkflowNotPausedError)
- resuming an unknown thread is rejected (WorkflowNotFoundError)
- state history contains every intermediate checkpoint

Requires a running Postgres (see docker-compose.yml). Point DATABASE_URL at
a disposable test database before running.
"""
import asyncio
import uuid

import pytest
import pytest_asyncio

from app.core.checkpointer import close_checkpointer, init_checkpointer
from app.workflows.base import (
    WorkflowNotFoundError,
    WorkflowNotPausedError,
    WorkflowResumeInProgressError,
    WorkflowStatus,
)
from app.workflows.claim_processor import ClaimProcessorWorkflow, ClaimState


@pytest_asyncio.fixture
async def checkpointer():
    saver = await init_checkpointer()
    yield saver
    await close_checkpointer()


@pytest_asyncio.fixture
def workflow(checkpointer):
    return ClaimProcessorWorkflow(checkpointer=checkpointer)


@pytest.mark.asyncio
async def test_low_value_claim_auto_approves(workflow):
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Alice", amount_usd=50.0,
    )
    result = await workflow.submit(state)

    assert result["status"] == WorkflowStatus.COMPLETED.value
    assert result["values"]["auto_approved"] is True
    assert result["next"] == []  # reached END, nothing pending


@pytest.mark.asyncio
async def test_high_value_claim_pauses_then_resumes(workflow):
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Bob", amount_usd=5000.0,
    )
    submitted = await workflow.submit(state)

    assert submitted["status"] == WorkflowStatus.PAUSED_FOR_APPROVAL.value
    assert "await_approval" in submitted["next"]

    resumed = await workflow.resume(
        thread_id=thread_id,
        resume_value={"decision": "approved", "approver_id": "mgr_1", "notes": "ok"},
        approver_id="mgr_1",
    )

    assert resumed["status"] == WorkflowStatus.COMPLETED.value
    assert resumed["values"]["approval_decision"] == "approved"
    assert resumed["values"]["approver_id"] == "mgr_1"
    assert resumed["next"] == []


@pytest.mark.asyncio
async def test_resuming_a_completed_thread_is_rejected(workflow):
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Carol", amount_usd=10.0,
    )
    await workflow.submit(state)  # auto-approves immediately, never pauses

    with pytest.raises(WorkflowNotPausedError):
        await workflow.resume(
            thread_id=thread_id,
            resume_value={"decision": "approved", "approver_id": "mgr_1"},
            approver_id="mgr_1",
        )


@pytest.mark.asyncio
async def test_resuming_unknown_thread_raises_not_found(workflow):
    with pytest.raises(WorkflowNotFoundError):
        await workflow.resume(
            thread_id=str(uuid.uuid4()),
            resume_value={"decision": "approved", "approver_id": "mgr_1"},
            approver_id="mgr_1",
        )


@pytest.mark.asyncio
async def test_double_resume_second_call_rejected(workflow):
    """The core race this scaffold exists to prevent: two approvers hit
    resume for the same paused thread. First succeeds, second must fail
    loudly instead of double-executing finalize()."""
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Dave", amount_usd=9000.0,
    )
    await workflow.submit(state)

    payload = {"decision": "approved", "approver_id": "mgr_1"}
    first = await workflow.resume(thread_id=thread_id, resume_value=payload, approver_id="mgr_1")
    assert first["status"] == WorkflowStatus.COMPLETED.value

    with pytest.raises(WorkflowNotPausedError):
        await workflow.resume(thread_id=thread_id, resume_value=payload, approver_id="mgr_2")


@pytest.mark.asyncio
async def test_truly_concurrent_resume_second_call_gets_conflict(workflow):
    """Unlike the sequential test above, this actually overlaps two resume
    calls in time (via asyncio.gather) so the non-blocking advisory lock
    itself is exercised, not just the post-resume status check."""
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Frank", amount_usd=9500.0,
    )
    await workflow.submit(state)

    payload = {"decision": "approved", "approver_id": "mgr_1"}
    results = await asyncio.gather(
        workflow.resume(thread_id=thread_id, resume_value=payload, approver_id="mgr_1"),
        workflow.resume(thread_id=thread_id, resume_value=payload, approver_id="mgr_2"),
        return_exceptions=True,
    )

    successes = [r for r in results if isinstance(r, dict)]
    conflicts = [r for r in results if isinstance(r, WorkflowResumeInProgressError)]
    assert len(successes) == 1
    assert len(conflicts) == 1
    assert successes[0]["status"] == WorkflowStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_wrong_gate_kind_is_rejected(workflow):
    """The bug this test pins: calling resume with expected_kind='retry'
    against a thread paused for customer approval must be rejected, not
    silently applied with a resume_value shape the approval gate wasn't
    expecting."""
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Kim", amount_usd=9200.0,
    )
    submitted = await workflow.submit(state)
    assert submitted["status"] == WorkflowStatus.PAUSED_FOR_APPROVAL.value

    with pytest.raises(WorkflowNotPausedError):
        await workflow.resume(
            thread_id=thread_id,
            resume_value={"action": "retry"},
            approver_id="ops_1",
            expected_kind="retry",
        )

    # thread must still be untouched -- still paused for approval, not
    # silently advanced with garbage data.
    still_paused = await workflow.get_state(thread_id)
    assert still_paused["status"] == WorkflowStatus.PAUSED_FOR_APPROVAL.value
    assert still_paused["values"]["approval_decision"] is None


@pytest.mark.asyncio
async def test_state_history_records_every_checkpoint(workflow):
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Erin", amount_usd=25.0,
    )
    await workflow.submit(state)

    history = await workflow.get_state_history(thread_id)
    assert len(history) >= 3  # validate, assess_risk, auto_approve/finalize at minimum


@pytest.mark.asyncio
async def test_finalize_failure_pauses_for_retry_not_silent_fail(workflow):
    """The DLQ gap: if finalize (payout call) fails after a human already
    approved, the thread must pause for an operator decision -- not
    silently mark FAILED and not require re-running customer approval."""
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Grace", amount_usd=8000.0,
        simulate_payout_failure=True,
    )
    submitted = await workflow.submit(state)
    assert submitted["status"] == WorkflowStatus.PAUSED_FOR_APPROVAL.value

    approved = await workflow.resume(
        thread_id=thread_id,
        resume_value={"decision": "approved", "approver_id": "mgr_1"},
        approver_id="mgr_1",
    )
    # finalize's @with_error_recovery retried internally and still failed
    # (simulate_payout_failure stays True), so it should now be paused for
    # an operator -- not FAILED, not COMPLETED.
    assert approved["status"] == WorkflowStatus.PAUSED_FOR_RETRY.value
    assert "await_retry_decision" in approved["next"]

    # Operator abandons: thread ends FAILED, no infinite loop.
    abandoned = await workflow.resume(
        thread_id=thread_id,
        resume_value={"action": "abandon"},
        approver_id="ops_1",
    )
    assert abandoned["status"] == WorkflowStatus.FAILED.value
    assert abandoned["next"] == []


@pytest.mark.asyncio
async def test_finalize_retry_succeeds_without_reapproval(workflow):
    """Operator retries and this time finalize succeeds -- proving the
    retry path re-runs finalize itself, not the customer approval gate."""
    thread_id = str(uuid.uuid4())
    state = ClaimState(
        thread_id=thread_id, claim_id=thread_id,
        claimant_name="Hank", amount_usd=7000.0,
        simulate_payout_failure=True,
    )
    await workflow.submit(state)
    paused = await workflow.resume(
        thread_id=thread_id,
        resume_value={"decision": "approved", "approver_id": "mgr_1"},
        approver_id="mgr_1",
    )
    assert paused["status"] == WorkflowStatus.PAUSED_FOR_RETRY.value

    # Operator confirms the outage is fixed and retries in one call.
    retried = await workflow.resume(
        thread_id=thread_id,
        resume_value={"action": "retry", "simulate_payout_failure": False},
        approver_id="ops_1",
    )
    assert retried["status"] == WorkflowStatus.COMPLETED.value
    # approval_decision was set once, at the original approval -- retry
    # never touched await_approval again.
    assert retried["values"]["approval_decision"] == "approved"
    assert retried["values"]["approver_id"] == "mgr_1"
