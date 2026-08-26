"""Dialect-portable database access.

CLAUDE.md 3.2: SQLite to start, Postgres when it hurts, and the swap must be a
connection-string change. That only holds if no dialect-specific SQL leaks into
the repository layer, so this module owns the two things that actually differ:

  * parameter placeholders -- `?` for SQLite, `$1..$n` for Postgres
  * type and identity syntax -- handled by per-dialect migration files

Everything else in repo.py is written in the portable subset. Two consequences
worth stating because they are easy to get wrong later:

  * No `interval` arithmetic in SQL. Time cutoffs are computed in Python and
    passed as bound parameters, because SQLite has no interval type.
  * No `percentile_disc`. Percentiles are computed in Python over a fetched
    column, because SQLite has no ordered-set aggregates.

Both are cheap at this volume and neither is worth an ORM.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Protocol

SQLITE = "sqlite"
POSTGRES = "postgres"

_PLACEHOLDER = re.compile(r"\?")


def detect_dialect(dsn: str) -> str:
    lowered = dsn.lower()
    if lowered.startswith(("postgres://", "postgresql://")):
        return POSTGRES
    if lowered.startswith("sqlite://") or lowered.endswith((".db", ".sqlite", ".sqlite3")):
        return SQLITE
    raise ValueError(
        f"cannot tell which database {dsn!r} names. Use sqlite:///path/to/file.db "
        "or postgresql://user@host/db"
    )


def to_postgres(sql: str) -> str:
    """Rewrite `?` placeholders to `$1..$n`, left to right."""
    counter = 0

    def bump(_match: re.Match[str]) -> str:
        nonlocal counter
        counter += 1
        return f"${counter}"

    return _PLACEHOLDER.sub(bump, sql)


def dumps(value: Any) -> str | None:
    """JSON for storage. Stored as TEXT on both dialects.

    Postgres jsonb would be faster to query, but using it here would put a
    `::jsonb` cast in every statement and break the portability claim. At this
    volume the difference does not register.
    """
    return None if value is None else json.dumps(value, default=str, separators=(",", ":"))


def loads(value: Any) -> Any:
    if value is None or isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def iso(value: dt.datetime | None) -> str | None:
    """Timestamps are stored as ISO-8601 UTC text on both dialects.

    SQLite has no timestamp type, and storing an integer epoch would make the
    journal unreadable by eye -- which matters, because reading the journal by
    eye is the entire point of week 1.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC).isoformat()


def parse_ts(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.UTC)
    try:
        parsed = dt.datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def base_units(value: int | str | None) -> str | None:
    """Token amounts are stored as TEXT holding an exact integer.

    Not INTEGER: SQLite's is 64-bit *signed*, so a u64 supply above 2^63-1
    silently wraps. Not REAL, ever -- 3.10 forbids floats in any amount path.
    TEXT round-trips exactly on both dialects and int() reads it back.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("bool is not a token amount")
    return str(int(value))


def percentiles(values: list[int], points: tuple[float, ...] = (0.5, 0.9, 0.99)) -> dict[str, int]:
    """Nearest-rank percentiles, computed in Python.

    Matches percentile_disc semantics (returns an observed value, never an
    interpolation) so the numbers do not shift if this moves to Postgres.
    """
    if not values:
        return {}
    ordered = sorted(values)
    out: dict[str, int] = {}
    for point in points:
        index = max(0, min(len(ordered) - 1, int(-(-point * len(ordered) // 1)) - 1))
        out[f"p{int(point * 100)}"] = ordered[index]
    return out


class Database(Protocol):
    """The surface repo.py is allowed to use."""

    dialect: str

    async def execute(self, sql: str, *args: Any) -> str: ...
    async def executemany(self, sql: str, rows: list[tuple]) -> None: ...
    async def fetch(self, sql: str, *args: Any) -> list[dict]: ...
    async def fetchrow(self, sql: str, *args: Any) -> dict | None: ...
    async def fetchval(self, sql: str, *args: Any) -> Any: ...
    async def close(self) -> None: ...
