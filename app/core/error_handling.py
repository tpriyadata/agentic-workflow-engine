"""
Fault recovery for graph nodes.

Two layers, on purpose:
1. Transient errors (network timeouts, rate limits) get retried with
   backoff inside the node -- the checkpoint before this node is never
   touched during retries, so a mid-retry crash still rolls back cleanly
   to the last valid checkpoint for free (LangGraph only checkpoints after
   a node returns).
2. Permanent failures (retries exhausted, or a non-retryable exception)
   get converted into a FAILED state update instead of an unhandled
   exception -- so the failure itself is checkpointed and visible in
   get_state_history(), rather than just crashing the process.
"""
import asyncio
import functools
import logging
from typing import Awaitable, Callable, TypeVar

from app.workflows.base import BaseWorkflowState, WorkflowStatus

logger = logging.getLogger(__name__)

StateT = TypeVar("StateT", bound=BaseWorkflowState)
NodeFn = Callable[[StateT], Awaitable[dict]]

# Exceptions worth retrying. Extend per-deployment (e.g. add your LLM
# provider's rate-limit exception type) -- don't retry on ValueError/
# validation errors, those won't fix themselves.
RETRYABLE_EXCEPTIONS = (TimeoutError, ConnectionError, asyncio.TimeoutError)


def with_error_recovery(
    *,
    max_retries: int = 2,
    backoff_seconds: float = 1.0,
    retryable: tuple[type[Exception], ...] = RETRYABLE_EXCEPTIONS,
):
    """Decorator for a LangGraph node coroutine. Wraps it with retry-then-
    fail-safe behavior.

    Usage:
        @with_error_recovery(max_retries=3)
        async def call_external_tool(state: ClaimState) -> dict:
            ...
    """

    def decorator(fn: NodeFn) -> NodeFn:
        @functools.wraps(fn)
        async def wrapper(state: StateT) -> dict:
            node_name = fn.__name__
            attempt = 0
            last_exc: Exception | None = None

            while attempt <= max_retries:
                try:
                    return await fn(state)
                except retryable as exc:
                    attempt += 1
                    last_exc = exc
                    if attempt > max_retries:
                        break
                    wait = backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "node.retry node=%s attempt=%d/%d wait=%.1fs error=%s",
                        node_name, attempt, max_retries, wait, exc,
                    )
                    await asyncio.sleep(wait)
                except Exception as exc:
                    # Non-retryable: fail fast, don't waste attempts.
                    last_exc = exc
                    logger.exception("node.failed_nonretryable node=%s", node_name)
                    break

            # All retries exhausted (or non-retryable). Return a FAILED
            # state update instead of raising, so this becomes a real
            # checkpoint the caller/API can see and act on.
            logger.error(
                "node.failed_permanently node=%s error=%s", node_name, last_exc
            )
            return {
                "status": WorkflowStatus.FAILED,
                "error": f"{node_name} failed after {attempt} attempt(s): {last_exc}",
                "error_count": state.error_count + 1,
            }

        return wrapper

    return decorator
