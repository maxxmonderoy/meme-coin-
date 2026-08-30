"""Outcome labeling: what actually happened to every mint we recorded.

Strictly read-only observation. Nothing in this package can construct or sign a
transaction, and it holds no key material. It reads mints from the database and
writes to `outcomes`; it never touches the feed path, the feed queue or the
ingest writer.
"""
from __future__ import annotations

#: The four checkpoints. 15m is not a round number chosen for tidiness: median
#: rugged-token lifespan is ~14 minutes and 85% of the profitable sniper cohort
#: exits within 5 (CLAUDE.md 1.2, 1.5). A first checkpoint at 1h would arrive
#: after the entire lifecycle of the median token had finished.
HORIZONS: tuple[str, ...] = ("15m", "1h", "24h", "7d")

HORIZON_SECONDS: dict[str, int] = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "24h": 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
}
