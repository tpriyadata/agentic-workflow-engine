"""
Checkpointer lifecycle.

One AsyncPostgresSaver, backed by one connection pool, shared across the
whole app. Created once at startup, closed once at shutdown. Never
instantiate a saver per-request -- that leaks connections under load.
"""
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_pool: AsyncConnectionPool | None = None
_saver: AsyncPostgresSaver | None = None


async def init_checkpointer() -> AsyncPostgresSaver:
    """Open the connection pool, run checkpointer migrations, and cache the
    saver instance. Call once at app startup. Idempotent-safe to call twice
    (returns the existing saver) but not designed for concurrent first-calls.
    """
    global _pool, _saver
    if _saver is not None:
        return _saver

    settings = get_settings()
    conninfo = str(settings.database_url)

    _pool = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=10,
        open=False,
        kwargs={
            "autocommit": True,
            "prepare_threshold": 0,
            # Bound how long any single query can wait on a lock or run
            # before Postgres kills it -- prevents one hung connection
            # (not crashed, just stuck) from tying up a pool slot
            # indefinitely. Does not affect advisory locks acquired via
            # pg_try_advisory_lock (non-blocking, see workflows/base.py),
            # only ordinary row/table locks and slow statements.
            "options": "-c lock_timeout=5000 -c statement_timeout=30000",
        },
    )
    await _pool.open()

    _saver = AsyncPostgresSaver(_pool)
    # Creates the checkpoint tables if they don't exist. Safe to run on
    # every startup -- it's a no-op against an already-migrated schema.
    await _saver.setup()

    logger.info("checkpointer.initialized backend=postgres pool_max=10")
    return _saver


async def close_checkpointer() -> None:
    """Close the pool cleanly at app shutdown. Not calling this leaks
    connections that show up as 'idle in transaction' in pg_stat_activity."""
    global _pool, _saver
    if _pool is not None:
        await _pool.close()
        logger.info("checkpointer.closed")
    _pool = None
    _saver = None


def get_checkpointer() -> AsyncPostgresSaver:
    """Fetch the already-initialized saver. Raises if called before startup --
    that's intentional: a workflow silently running without persistence is
    worse than a loud crash."""
    if _saver is None:
        raise RuntimeError(
            "Checkpointer not initialized. Call init_checkpointer() during "
            "app startup before handling any workflow requests."
        )
    return _saver


@asynccontextmanager
async def checkpointer_lifespan() -> AsyncIterator[None]:
    """FastAPI lifespan context: wraps startup/shutdown of the pool."""
    await init_checkpointer()
    try:
        yield
    finally:
        await close_checkpointer()
