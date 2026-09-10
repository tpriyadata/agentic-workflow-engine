"""
FastAPI control plane. Thin on purpose -- all real logic lives in
BaseAgentWorkflow / ClaimProcessorWorkflow. This layer's job is auth,
request validation, and translating HTTP into workflow calls.
"""
from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from app.core.checkpointer import get_checkpointer, init_checkpointer, close_checkpointer
from app.core.config import get_settings
from app.workflows.base import (
    WorkflowNotFoundError,
    WorkflowNotPausedError,
    WorkflowResumeInProgressError,
)
from app.workflows.claim_processor import ClaimProcessorWorkflow, ClaimState

logging.basicConfig(level=get_settings().log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_checkpointer()
    logger.info("app.startup complete")
    yield
    await close_checkpointer()
    logger.info("app.shutdown complete")


app = FastAPI(title="Agent Stateful Scaffold", lifespan=lifespan)


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    settings = get_settings()
    if settings.api_key is None:
        return  # auth disabled -- fine for local dev, set api_key for anything else
    if x_api_key != settings.api_key.get_secret_value():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API key")


def get_claim_workflow() -> ClaimProcessorWorkflow:
    return ClaimProcessorWorkflow(checkpointer=get_checkpointer())


# ----------------------------------------------------------------------
# Request/response models
# ----------------------------------------------------------------------

class SubmitClaimRequest(BaseModel):
    claimant_name: str
    amount_usd: float = Field(gt=0)
    description: str = ""


class ResumeRequest(BaseModel):
    decision: str = Field(pattern="^(approved|rejected)$")
    approver_id: str
    notes: str | None = None


class RetryDecisionRequest(BaseModel):
    """Distinct from ResumeRequest -- this answers 'retry the failed
    finalize step or abandon it', not 'approve this claim'. Routed to the
    same generic workflow.resume(); which interrupt it applies to is
    resolved by BaseAgentWorkflow against whatever's actually pending."""
    action: str = Field(pattern="^(retry|abandon)$")
    operator_id: str
    notes: str | None = None


class SubmitAcceptedResponse(BaseModel):
    thread_id: str
    status: str = "submitted"


class ThreadResponse(BaseModel):
    thread_id: str
    status: str
    next: list[str]
    values: dict[str, Any]


# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------

@app.post(
    "/claims",
    dependencies=[Depends(require_api_key)],
    response_model=None,
)
async def submit_claim(
    req: SubmitClaimRequest,
    background_tasks: BackgroundTasks,
    run_async: bool = False,
    workflow: ClaimProcessorWorkflow = Depends(get_claim_workflow),
) -> ThreadResponse | SubmitAcceptedResponse:
    """By default, runs synchronously and returns the resulting state (fine
    for this reference workflow -- validate/assess_risk are fast). Pass
    `?run_async=true` for domains where the pre-interrupt work is slow
    (e.g. a real LLM call in assess_risk): returns 202 immediately with the
    thread_id, and the caller polls GET /claims/{thread_id} for status --
    the same status endpoint either mode uses."""
    thread_id = str(uuid.uuid4())
    initial_state = ClaimState(
        thread_id=thread_id,
        claim_id=thread_id,
        claimant_name=req.claimant_name,
        amount_usd=req.amount_usd,
        description=req.description,
    )
    if not run_async:
        return await workflow.submit(initial_state)

    async def _run() -> None:
        try:
            await workflow.submit(initial_state)
        except Exception:
            logger.exception("workflow.submit.background_failed thread_id=%s", thread_id)

    background_tasks.add_task(_run)
    return SubmitAcceptedResponse(thread_id=thread_id)


@app.post(
    "/claims/{thread_id}/resume",
    response_model=ThreadResponse,
    dependencies=[Depends(require_api_key)],
)
async def resume_claim(
    thread_id: str,
    req: ResumeRequest,
    workflow: ClaimProcessorWorkflow = Depends(get_claim_workflow),
) -> dict:
    try:
        return await workflow.resume(
            thread_id=thread_id,
            resume_value={
                "decision": req.decision,
                "approver_id": req.approver_id,
                "notes": req.notes,
            },
            approver_id=req.approver_id,
            expected_kind="approval",
        )
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except WorkflowNotPausedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except WorkflowResumeInProgressError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post(
    "/claims/{thread_id}/retry",
    response_model=ThreadResponse,
    dependencies=[Depends(require_api_key)],
)
async def retry_claim(
    thread_id: str,
    req: RetryDecisionRequest,
    workflow: ClaimProcessorWorkflow = Depends(get_claim_workflow),
) -> dict:
    """For the post-approval failure gate (finalize exhausted its retries).
    409s the same way /resume does if the thread isn't actually paused
    there -- e.g. it's paused for customer approval instead, or already
    finished."""
    try:
        return await workflow.resume(
            thread_id=thread_id,
            resume_value={"action": req.action},
            approver_id=req.operator_id,
            expected_kind="retry",
        )
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except WorkflowNotPausedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except WorkflowResumeInProgressError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get(
    "/claims/{thread_id}",
    response_model=ThreadResponse,
    dependencies=[Depends(require_api_key)],
)
async def get_claim_status(
    thread_id: str,
    workflow: ClaimProcessorWorkflow = Depends(get_claim_workflow),
) -> dict:
    try:
        return await workflow.get_state(thread_id)
    except WorkflowNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get(
    "/claims/{thread_id}/history",
    dependencies=[Depends(require_api_key)],
)
async def get_claim_history(
    thread_id: str,
    workflow: ClaimProcessorWorkflow = Depends(get_claim_workflow),
) -> list[dict]:
    return await workflow.get_state_history(thread_id)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
