"""Forward-only migrations, per dialect, applied in filename order."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from ..log import get
from .dialect import Database, iso

log = get(__name__)

MIGRATIONS_ROOT = Path(__file__).resolve().parents[3] / "migrations"

_BOOTSTRAP = """
create table if not exists schema_migrations (
    version    text primary key,
    applied_at text not null
)
"""


def discover(dialect: str, root: Path | None = None) -> list[Path]:
    directory = (root or MIGRATIONS_ROOT) / dialect
    if not directory.is_dir():
        raise FileNotFoundError(f"no migrations directory for dialect {dialect!r} at {directory}")
    return sorted(directory.glob("*.sql"))


def _statements(sql: str) -> list[str]:
    """Split on semicolons at statement level.

    aiosqlite's execute() takes one statement at a time, and executescript()
    commits implicitly, which would defeat the per-migration transaction.
    """
    out, buf = [], []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        buf.append(line)
        if stripped.endswith(";"):
            out.append("\n".join(buf).rstrip().rstrip(";"))
            buf = []
    if buf:
        out.append("\n".join(buf).rstrip().rstrip(";"))
    return [s for s in out if s.strip()]


async def migrate(db: Database, root: Path | None = None) -> list[str]:
    await db.execute(_BOOTSTRAP)
    done = {r["version"] for r in await db.fetch("select version from schema_migrations")}

    applied: list[str] = []
    for path in discover(db.dialect, root):
        version = path.stem
        if version in done:
            continue
        log.info("applying migration %s (%s)", version, db.dialect)
        for statement in _statements(path.read_text()):
            await db.execute(statement)
        await db.execute(
            "insert into schema_migrations (version, applied_at) values (?, ?)",
            version, iso(dt.datetime.now(tz=dt.UTC)),
        )
        applied.append(version)
    return applied
