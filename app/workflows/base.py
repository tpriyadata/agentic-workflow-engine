"""
BaseAgentWorkflow: the engine. Domain workflows subclass this and implement
only `build_graph()` + a Pydantic state schema. Everything about
persistence, pausing, resuming, and failure handling lives here so
subclasses can't accidentally get it wrong.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Generic, TypeVar

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import StateGraph
from langgraph.types import Command, StateSnapshot
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

StateT = TypeVar("StateT", bound="BaseWorkflowState")


class WorkflowStatus(str, Enum):
    RUNNING = "running"
    PAUSED_FOR_APPROVAL = "paused_for_approval"
    PAUSED_FOR_RETRY = "paused_for_retry"  # post-approval step failed; awaiting operator retry/abandon
    COMPLETED = "completed"
    FAILED = "failed"


_PAUSED_STATUSES = {
    WorkflowStatus.PAUSED_FOR_APPROVAL.value,
    WorkflowStatus.PAUSED_FOR_RETRY.value,
}


def _is_paused(status: str | None) -> bool:
    return status in _PAUSED_STATUSES


class BaseWorkflowState(BaseModel):
    """Every domain state schema must extend this. Gives every workflow a
    consistent status/audit surface regardless of business logic, so the
    API layer never needs to know about domain-specific fields."""

    thread_id: str
    status: WorkflowStatus = WorkflowStatus.RUNNING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: str | None = None
    error_count: int = 0
    last_completed_node: str | None = None


class ApprovalRequiredError(Exception):
    """Raised (and caught internally) to signal a node wants human input.
    Prefer using interrupt() from langgraph.types directly in node code --
    this exists for nodes that want a typed, catchable signal instead."""


class WorkflowNotFoundError(Exception):
    pass


class WorkflowNotPausedError(Exception):
    """Raised on resume() if the thread isn't actually waiting at an
    interrupt. Prevents a stray resume call from corrupting a thread that's
    still running or already completed."""


class WorkflowResumeInProgressError(Exception):
    """Raised when resume() is called for a thread that's already being
    resumed by another concurrent request. Distinct from
    WorkflowNotPausedError: this is a timing conflict (try again once the
    in-flight resume finishes), not an invalid-state error."""


def _advisory_lock_key(thread_id: str) -> int:
    """Deterministic 63-bit int from thread_id, for pg_advisory_xact_lock.
    Postgres advisory locks take a bigint; hashing keeps this safe for any
    thread_id string instead of restricting IDs to be integers."""
    digest = hashlib.sha256(thread_id.encode()).digest()[:8]
    return int.from_bytes(digest, "big", signed=True) >> 1  # keep it in bigint range


class BaseAgentWorkflow(ABC, Generic[StateT]):
    """Subclass this per domain (claims, content review, ...). Subclasses
    implement `build_graph` and set `state_schema`. Everything else --
    submit/resume/history/error-recovery -- is inherited and shared.
    """

    state_schema: type[StateT]
    interrupt_before: list[str] = []  # node names where execution pauses for approval

    def __init__(self, checkpointer: AsyncPostgresSaver) -> None:
        self._checkpointer = checkpointer
        self._graph = self.build_graph().compile(
            checkpointer=checkpointer,
            interrupt_before=self.interrupt_before or None,
        )

    @abstractmethod
    def build_graph(self) -> StateGraph:
        """Return an uncompiled StateGraph wired with domain nodes/edges.
        Do not call .compile() here -- the base class compiles with the
        shared checkpointer and interrupt config."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def submit(self, initial_state: StateT) -> dict[str, Any]:
        """Start a new thread and run until completion or the first
        interrupt. `initial_state.thread_id` must be unique -- callers are
        responsible for generating it (e.g. uuid4), so retried submissions
        can be made idempotent by the caller reusing the same ID."""
        config = {"configurable": {"thread_id": initial_state.thread_id}}
        try:
            await self._graph.ainvoke(initial_state.model_dump(), config=config)
        except Exception:
            logger.exception("workflow.submit.failed thread_id=%s", initial_state.thread_id)
            raise
        return await self._describe(initial_state.thread_id)

    async def resume(
        self,
        thread_id: str,
        resume_value: Any,
        approver_id: str | None = None,
        invoke_timeout_seconds: float = 60.0,
        expected_kind: str | None = None,
    ) -> dict[str, Any]:
        """Resume a paused thread. Guarded per-thread by a Postgres session
        advisory lock, acquired non-blocking (pg_try_advisory_lock) so a
        second concurrent resume call for the same thread_id fails fast
        with WorkflowResumeInProgressError instead of queueing behind the
        first.

        `expected_kind`, when given, must match the pending interrupt's
        "kind" (see _pending_interrupt_kind) or this raises
        WorkflowNotPausedError instead of applying resume_value. Without
        this check, calling the wrong endpoint against the wrong gate --
        e.g. POST /retry against a thread actually paused for customer
        approval -- silently injects a shape it wasn't expecting: the
        target node reads whatever keys it recognizes from resume_value
        and quietly treats the rest as absent, which is a corrupted
        decision, not an error. API callers should always pass this.

        Deliberately NOT wrapped in a SQL transaction held for the full
        ainvoke() duration -- doing that would hold a Postgres transaction
        open for as long as the graph takes to run (including any external
        LLM/API calls), which bloats Postgres and blocks other resumes with
        no bound. Instead: acquire the lock on a dedicated connection,
        release it explicitly in `finally`, and bound the invoke itself
        with a timeout so a hung (not crashed) worker can't hold the lock
        indefinitely. Postgres also auto-releases the lock if this
        connection dies outright (crash, network drop) -- that path was
        already safe; this fixes the "alive but slow" path, which wasn't.
        """
        lock_key = _advisory_lock_key(thread_id)
        pool = self._checkpointer.conn  # AsyncConnectionPool
        async with pool.connection() as lock_conn:
            acquired = (
                await (
                    await lock_conn.execute("SELECT pg_try_advisory_lock(%s)", (lock_key,))
                ).fetchone()
            )[0]
            if not acquired:
                raise WorkflowResumeInProgressError(
                    f"thread_id={thread_id!r} is already being resumed by "
                    f"another request; refusing to run concurrently."
                )
            try:
                snapshot = await self._get_snapshot(thread_id)
                if snapshot is None:
                    raise WorkflowNotFoundError(f"No thread found for id={thread_id!r}")
                current_status = self._effective_status(snapshot)
                if not _is_paused(current_status):
                    raise WorkflowNotPausedError(
                        f"thread_id={thread_id!r} is not paused "
                        f"(status={current_status!r}); refusing to resume."
                    )
                if expected_kind is not None:
                    actual_kind = self._pending_interrupt_kind(snapshot) or "approval"
                    if actual_kind != expected_kind:
                        raise WorkflowNotPausedError(
                            f"thread_id={thread_id!r} is paused for "
                            f"{actual_kind!r}, not {expected_kind!r}; "
                            f"refusing to resume via the wrong gate."
                        )

                config = {"configurable": {"thread_id": thread_id}}
                try:
                    await asyncio.wait_for(
                        self._graph.ainvoke(Command(resume=resume_value), config=config),
                        timeout=invoke_timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "workflow.resume.timed_out thread_id=%s timeout=%.0fs",
                        thread_id, invoke_timeout_seconds,
                    )
                    raise
                except Exception:
                    logger.exception("workflow.resume.failed thread_id=%s", thread_id)
                    raise
            finally:
                await lock_conn.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))

        logger.info(
            "workflow.resumed thread_id=%s approver_id=%s", thread_id, approver_id
        )
        return await self._describe(thread_id)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    async def _get_snapshot(self, thread_id: str) -> StateSnapshot | None:
        config = {"configurable": {"thread_id": thread_id}}
        snapshot = await self._graph.aget_state(config)
        if snapshot is None or not snapshot.values:
            return None
        return snapshot

    @staticmethod
    def _has_pending_interrupt(snapshot: StateSnapshot) -> bool:
        """True if any pending task is parked on interrupt(). This is the
        only reliable signal that a thread is paused for human input:
        interrupt() suspends execution *inside* a node, before that node's
        return value is applied, so a status field the node itself was
        going to set (e.g. `status: PAUSED_FOR_APPROVAL`) never gets
        written. Derive it from the graph's own task state instead of
        trusting application state to reflect a transition it never
        completed."""
        return any(task.interrupts for task in snapshot.tasks)

    @staticmethod
    def _pending_interrupt_kind(snapshot: StateSnapshot) -> str | None:
        """Reads the domain-supplied 'kind' from the interrupt() payload
        (e.g. interrupt({"kind": "approval", ...})) rather than hardcoding
        node names here -- keeps this base class domain-agnostic across
        subclasses that each define their own gate nodes."""
        for task in snapshot.tasks:
            for i in task.interrupts:
                if isinstance(i.value, dict) and "kind" in i.value:
                    return i.value["kind"]
        return None

    def _effective_status(self, snapshot: StateSnapshot) -> str:
        if self._has_pending_interrupt(snapshot):
            kind = self._pending_interrupt_kind(snapshot)
            if kind == "retry":
                return WorkflowStatus.PAUSED_FOR_RETRY.value
            return WorkflowStatus.PAUSED_FOR_APPROVAL.value  # default for legacy/unlabeled interrupts
        return snapshot.values.get("status")

    async def _describe(self, thread_id: str) -> dict[str, Any]:
        snapshot = await self._get_snapshot(thread_id)
        if snapshot is None:
            raise WorkflowNotFoundError(f"No thread found for id={thread_id!r}")
        return {
            "thread_id": thread_id,
            "status": self._effective_status(snapshot),
            "next": list(snapshot.next),
            "values": snapshot.values,
        }

    async def get_state(self, thread_id: str) -> dict[str, Any]:
        return await self._describe(thread_id)

    async def get_state_history(self, thread_id: str) -> list[dict[str, Any]]:
        """Full checkpoint history for a thread, most recent first. This is
        the debugging/audit surface -- every intermediate state is
        individually inspectable, not just the current one."""
        config = {"configurable": {"thread_id": thread_id}}
        history = []
        async for snapshot in self._graph.aget_state_history(config):
            history.append(
                {
                    "checkpoint_id": snapshot.config["configurable"].get("checkpoint_id"),
                    "status": self._effective_status(snapshot) if snapshot.values else None,
                    "next": list(snapshot.next),
                    "updated_at": snapshot.values.get("updated_at") if snapshot.values else None,
                }
            )
        return history

    async def rollback_to(self, thread_id: str, checkpoint_id: str) -> dict[str, Any]:
        """Fault recovery: reset a thread's current pointer back to a prior
        checkpoint. Does not delete newer checkpoints -- they remain in
        history for audit -- it just makes the given checkpoint the new
        'current' state to resume execution from."""
        config = {
            "configurable": {"thread_id": thread_id, "checkpoint_id": checkpoint_id}
        }
        snapshot = await self._graph.aget_state(config)
        if snapshot is None:
            raise WorkflowNotFoundError(
                f"No checkpoint {checkpoint_id!r} found for thread_id={thread_id!r}"
            )
        await self._graph.aupdate_state(config, snapshot.values)
        logger.warning(
            "workflow.rolled_back thread_id=%s checkpoint_id=%s", thread_id, checkpoint_id
        )
        return await self._describe(thread_id)
