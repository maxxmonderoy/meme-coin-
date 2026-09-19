"""Back up the collected database.

THE ONE THING IN THIS PROJECT THAT CANNOT BE REBUILT. Every other artefact here
is code, and code is reversible. The journal is a record of tokens that launched
at a particular moment, and nobody else is storing what happened to them -- an
observation not taken today cannot be taken later. Losing the file costs
calendar time, not developer time.

`VACUUM INTO` rather than a file copy: it is atomic, safe while the stream and
labeler are still writing in WAL mode, and produces a compacted copy rather than
a snapshot with a half-applied WAL beside it. Copying a live SQLite file with
`cp` is the classic way to get a backup that restores to a corrupt database.
"""
from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ..log import get
from .pool import sqlite_path

log = get(__name__)

#: Tables whose row counts are compared between source and copy. Not a checksum
#: -- a backup that silently lost the journal is the failure worth catching.
VERIFY_TABLES = ("tokens_seen", "raw_events", "outcomes", "price_path",
                 "structural_events", "decisions", "paper_positions")


class BackupError(RuntimeError):
    pass


@dataclass(slots=True)
class BackupReport:
    path: Path
    bytes_written: int
    counts: dict[str, int]
    verified: bool
    mismatches: dict[str, tuple[int, int]]

    @property
    def megabytes(self) -> float:
        return self.bytes_written / (1024 * 1024)


def _counts(conn: sqlite3.Connection) -> dict[str, int]:
    present = {r[0] for r in conn.execute(
        "select name from sqlite_master where type = 'table'")}
    return {
        table: conn.execute(f"select count(*) from {table}").fetchone()[0]  # noqa: S608
        for table in VERIFY_TABLES if table in present
    }


def backup_sqlite(dsn: str, destination: Path | None = None) -> BackupReport:
    """Write a verified copy. Safe to run while collectors are writing."""
    source = sqlite_path(dsn)
    if source == ":memory:":
        raise BackupError("cannot back up an in-memory database")
    if not Path(source).is_file():
        raise BackupError(f"no database at {source}")

    if destination is None:
        stamp = dt.datetime.now(tz=dt.UTC).strftime("%Y%m%dT%H%M%SZ")
        destination = Path(source).with_name(f"{Path(source).stem}-{stamp}.db")
    destination = Path(destination).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise BackupError(f"{destination} already exists; refusing to overwrite a backup")

    conn = sqlite3.connect(source)
    try:
        # VACUUM INTO takes its own consistent snapshot, so writers may continue.
        conn.execute("VACUUM INTO ?", (str(destination),))
        source_counts = _counts(conn)
    finally:
        conn.close()

    copy = sqlite3.connect(destination)
    try:
        copy_counts = _counts(copy)
        # A backup nobody checks is a belief, not a backup.
        integrity = copy.execute("pragma integrity_check").fetchone()[0]
    finally:
        copy.close()

    mismatches = {
        table: (source_counts[table], copy_counts.get(table, -1))
        for table in source_counts
        if copy_counts.get(table) != source_counts[table]
    }
    if integrity != "ok":
        raise BackupError(f"integrity check on the copy failed: {integrity}")

    return BackupReport(
        path=destination, bytes_written=destination.stat().st_size,
        counts=copy_counts, verified=not mismatches, mismatches=mismatches,
    )


def prune_backups(directory: Path, *, keep: int, prefix: str) -> list[Path]:
    """Delete all but the newest `keep` backups. Never touches the live file."""
    candidates = sorted(
        (p for p in Path(directory).glob(f"{prefix}-*.db") if p.is_file()),
        key=lambda p: p.stat().st_mtime, reverse=True,
    )
    removed = []
    for path in candidates[keep:]:
        path.unlink()
        removed.append(path)
    return removed
