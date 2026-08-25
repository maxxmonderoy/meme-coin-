"""Connection pool lifecycle."""
from __future__ import annotations

import asyncpg


async def connect(dsn: str, *, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn, min_size=min_size, max_size=max_size, command_timeout=30,
    )
    if pool is None:
        raise RuntimeError(f"could not create a connection pool for {dsn.split('@')[-1]}")
    return pool
