"""Database connections for both dialects behind one interface."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from .dialect import POSTGRES, SQLITE, Database, detect_dialect, to_postgres


def sqlite_path(dsn: str) -> str:
    """Resolve a SQLite DSN to a filesystem path.

    Follows the usual URI convention, where the slash count is meaningful and
    getting it wrong is silent:

        sqlite:///tmp/x.db   -> /tmp/x.db      (three slashes: ABSOLUTE)
        sqlite://./x.db      -> ./x.db         (relative)
        sqlite://x.db        -> x.db           (relative)
        sqlite://:memory:    -> :memory:
        /tmp/x.db            -> /tmp/x.db      (bare path, no scheme)

    The three-slash form is the one that matters: stripping the leading slash
    turns an absolute path into a relative one, so the database is created
    somewhere other than where it was asked for and everything still appears to
    work -- migrations apply, rows insert -- against a file in the wrong place.
    """
    if "://" not in dsn:
        return dsn
    rest = dsn.split("://", 1)[1]
    if rest == ":memory:" or rest.startswith(":memory:"):
        return ":memory:"
    # `sqlite:///abs` leaves a leading slash here, which IS the absolute path.
    return rest or ":memory:"


class SqliteDatabase(Database):
    """Single-connection SQLite with WAL.

    One writer, serialised behind a lock. That is not a limitation to design
    around yet -- 3.2 says migrate when concurrent writers actually bite, and
    at a few hundred thousand rows a week they do not.
    """

    dialect = SQLITE

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._lock = asyncio.Lock()

    @staticmethod
    async def connect(dsn: str) -> SqliteDatabase:
        import aiosqlite

        path = sqlite_path(dsn)
        if path not in (":memory:",):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(path, isolation_level=None)
        conn.row_factory = __import__("aiosqlite").Row
        # WAL survives a hard kill without losing committed rows, which is the
        # failure that matters for a process meant to run seven days unattended.
        await conn.execute("pragma journal_mode=WAL")
        await conn.execute("pragma synchronous=NORMAL")
        await conn.execute("pragma foreign_keys=ON")
        await conn.execute("pragma busy_timeout=5000")
        return SqliteDatabase(conn)

    async def execute(self, sql: str, *args: Any) -> str:
        async with self._lock:
            cursor = await self._conn.execute(sql, args)
            return f"OK {cursor.rowcount}"

    async def executemany(self, sql: str, rows: list[tuple]) -> None:
        async with self._lock:
            await self._conn.executemany(sql, rows)

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        async with self._lock:
            cursor = await self._conn.execute(sql, args)
            return [dict(r) for r in await cursor.fetchall()]

    async def fetchrow(self, sql: str, *args: Any) -> dict | None:
        rows = await self.fetch(sql, *args)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        row = await self.fetchrow(sql, *args)
        return next(iter(row.values()), None) if row else None

    async def close(self) -> None:
        await self._conn.close()


class PostgresDatabase(Database):
    """Postgres via asyncpg. Reached only when the DSN names postgres."""

    dialect = POSTGRES

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @staticmethod
    async def connect(dsn: str) -> PostgresDatabase:
        import asyncpg

        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10, command_timeout=30)
        if pool is None:
            raise RuntimeError("could not create a Postgres pool")
        return PostgresDatabase(pool)

    async def execute(self, sql: str, *args: Any) -> str:
        return await self._pool.execute(to_postgres(sql), *args)

    async def executemany(self, sql: str, rows: list[tuple]) -> None:
        await self._pool.executemany(to_postgres(sql), rows)

    async def fetch(self, sql: str, *args: Any) -> list[dict]:
        return [dict(r) for r in await self._pool.fetch(to_postgres(sql), *args)]

    async def fetchrow(self, sql: str, *args: Any) -> dict | None:
        row = await self._pool.fetchrow(to_postgres(sql), *args)
        return dict(row) if row else None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._pool.fetchval(to_postgres(sql), *args)

    async def close(self) -> None:
        await self._pool.close()


async def connect(dsn: str) -> Database:
    """The connection-string swap 3.2 asks for."""
    dialect = detect_dialect(dsn)
    if dialect == SQLITE:
        return await SqliteDatabase.connect(dsn)
    return await PostgresDatabase.connect(dsn)
