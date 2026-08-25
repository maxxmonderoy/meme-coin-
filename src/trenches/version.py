"""Code version resolution.

Every stream run and every journalled decision records the git sha it was
produced by. Thresholds and decode logic change; a journal row that cannot be
attributed to a specific revision is not evidence of anything.
"""
from __future__ import annotations

import functools
import subprocess


@functools.lru_cache(maxsize=1)
def code_version() -> str:
    """Return `<sha>` or `<sha>-dirty`, else `unknown`.

    Never raises: a missing git binary must not stop the stream.
    """
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],  # noqa: S607
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],  # noqa: S607
            capture_output=True, text=True, timeout=5, check=True,
        ).stdout.strip()
        return f"{sha}-dirty" if dirty else sha
    except Exception:
        return "unknown"
