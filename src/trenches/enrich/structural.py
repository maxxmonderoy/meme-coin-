"""Fetch the structural half of Part 2, and only that half.

Stage 1 has been recording `unfetched` since it was written because nothing
supplied it. This does, from GoPlus -- free, keyless, and per 3.2 the best
Token-2022 authority coverage among the free options.

SCOPE IS THE POINT. This fetches authorities, upgradable flags, hooks and fees:
the facts Part 2 measured as landing within ~1 second. It does NOT fetch
holders, LP holders, concentration or any score. Those have a cold start, and on
a mint minutes old they come back empty rather than clean. Keeping them out of
this module means stage 1 cannot accidentally start reading them.
"""
from __future__ import annotations

import asyncio
import datetime as dt

from ..db import repo
from ..db.dialect import Database
from ..log import get
from .goplus import GoPlusClient

log = get(__name__)

#: GoPlus is free and keyless, which means it is also rate limited by somebody
#: else's rules. Sequential with a small gap rather than a burst: the batch is
#: not latency sensitive (3.2 puts this class of work on the free tier
#: deliberately) and getting throttled costs more than going slowly.
DEFAULT_GAP_SECONDS = 0.35

#: The flags stage 1 reads. Named here so the fetch and the rule cannot drift.
STRUCTURAL_FLAGS = (
    "mintable", "freezable", "closable", "balance_mutable_authority",
    "transfer_fee_upgradable", "transfer_hook_upgradable", "metadata_mutable",
    "default_account_state_upgradable", "non_transferable",
)


def extract(token: dict) -> dict:
    """Pull stage-1 fields out of a GoPlus token object.

    Vendor booleans arrive as "1"/"0", 1/0, or nested under {"status": ...}.
    A field that is ABSENT stays None -- it is not a zero. That distinction is
    the whole reason this returns None rather than defaulting.
    """
    out: dict = {}
    present = 0
    for flag in STRUCTURAL_FLAGS:
        if flag not in token:
            out[flag] = None
            continue
        value = token[flag]
        if isinstance(value, dict):
            value = value.get("status")
        out[flag] = None if value is None else str(value).strip().lower() in {"1", "true", "yes"}
        present += 1
    hook = token.get("transfer_hook")
    out["transfer_hook"] = str(hook) if hook else None
    present += 1 if "transfer_hook" in token else 0
    fee = token.get("transfer_fee")
    if isinstance(fee, dict):
        fee = fee.get("transfer_fee_percent")
    out["transfer_fee"] = str(fee) if fee not in (None, "") else None
    present += 1 if "transfer_fee" in token else 0
    out["fields_present"] = present
    return out


async def fetch_batch(
    db: Database, *, limit: int = 200, gap_seconds: float = DEFAULT_GAP_SECONDS
) -> dict:
    """Probe mints with no structural row yet. Returns a small report."""
    rows = await repo.mints_needing_structural(db, limit=limit)
    if not rows:
        return {"requested": 0, "probed": 0, "errors": 0, "with_fields": 0}

    client = GoPlusClient()
    probed = errors = with_fields = 0
    for i, row in enumerate(rows):
        mint = row["mint"]
        probe = await client.token_security(mint)
        token = probe.token() if probe.error is None else {}
        fields = extract(token) if token else {"fields_present": 0}
        if probe.error:
            errors += 1
        elif fields.get("fields_present"):
            with_fields += 1
        await repo.record_structural(db, mint=mint, fields={
            **fields,
            "source": "goplus",
            "fetched_at": dt.datetime.now(tz=dt.UTC),
            "status_code": probe.status_code,
            "error": probe.error,
            "payload": probe.payload,
        })
        probed += 1
        if gap_seconds and i + 1 < len(rows):
            await asyncio.sleep(gap_seconds)
    return {"requested": len(rows), "probed": probed, "errors": errors,
            "with_fields": with_fields}
