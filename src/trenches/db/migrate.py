"""Forward-only SQL migrations, applied in filename order.

No down-migrations. At this stage the journal is small enough to rebuild and a
half-applied rollback on the table holding your only evidence is a worse
outcome than recreating it.
"""
from __future__ import annotations

from pathlib import Path

import asyncpg

from ..log import get

log = get(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrations"

_BOOTSTRAP = """
create table if not exists schema_migrations (
    version    text primary key,
    applied_at timestamptz not null default now()
);
"""


def discover(directory: Path | None = None) -> list[Path]:
    return sorted((directory or MIGRATIONS_DIR).glob("*.sql"))


async def applied_versions(conn: asyncpg.Connection) -> set[str]:
    await conn.execute(_BOOTSTRAP)
    rows = await conn.fetch("select version from schema_migrations")
    return {r["version"] for r in rows}


async def migrate(pool: asyncpg.Pool, directory: Path | None = None) -> list[str]:
    """Apply pending migrations. Returns the versions applied this run."""
    files = discover(directory)
    if not files:
        raise FileNotFoundError(f"no .sql migrations found in {directory or MIGRATIONS_DIR}")

    applied: list[str] = []
    async with pool.acquire() as conn:
        done = await applied_versions(conn)
        for path in files:
            version = path.stem
            if version in done:
                continue
            log.info("applying migration %s", version)
            # Each migration runs in its own transaction: a failure leaves the
            # database at the last complete version rather than half-way.
            async with conn.transaction():
                await conn.execute(path.read_text())
                await conn.execute(
                    "insert into schema_migrations(version) values($1) "
                    "on conflict (version) do nothing",
                    version,
                )
            applied.append(version)
    return applied
