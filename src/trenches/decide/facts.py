"""Database row -> cascade facts. The one place column names meet rule names.

WHY THIS EXISTS AS A MODULE. The cascade stages are pure `facts -> Verdict`
functions and deliberately know nothing about SQL. Something still has to map a
row onto them, and doing it inline in the CLI is how a column rename silently
turns a rule off: the key stops matching, the stage reads `unknown`, everything
keeps passing, and nothing raises. Here it is one function with tests on it.

IT ALSO DOCUMENTS THE GAPS. Every fact a stage can read is either mapped below
or listed as unsupplied with the reason. That list is the honest state of the
cascade, and it is short on purpose -- Part 0 rule 2 means a field nobody has
verified stays absent rather than becoming a plausible guess.
"""
from __future__ import annotations

from typing import Any

#: Facts the cascade reads that NOTHING in this repo supplies yet, with why.
#: Kept as data so `decide` can print it rather than a comment nobody reads.
UNSUPPLIED: dict[str, str] = {
    "lp_state": "no verified field path for LP burn/lock state",
    "lp_unlock_date": "no verified field path for the locker's unlockDate",
    "dev_percentage": "behavioural, cold start; nothing collects it",
    "snipers_total": "behavioural, cold start; nothing collects it",
    "insiders_total": "behavioural, cold start; nothing collects it",
    "bundlers_total": "behavioural, cold start; nothing collects it",
    "bundlers_count": "behavioural, cold start; nothing collects it",
    "bundlers_total_initial_percentage": "behavioural; nothing collects it",
    "bundlers_total_percentage": "behavioural; nothing collects it",
    "holders": "no normalised holder list is stored",
}


def facts_from_row(row: Any) -> dict:
    """Build the facts dict `Cascade.run` reads from a candidates row.

    Passes the row through unchanged apart from one assembly step: the
    exclusion set for stage 4's top-10 computation. 3.4 is explicit that the
    top 10 must be computed EXCLUDING pool, bonding-curve and locker addresses,
    because on a live pump.fun curve the curve holds essentially the whole
    supply and a naive top-10 reads ~100% for every token that exists.

    Only two of those three are available: the curve address from the launch
    itself and the latest pool address from the price path. Locker addresses
    have no source here, so a locked LP would still count toward the top 10 --
    which is why stage 4 records the exclusion list it actually applied instead
    of asserting the number is clean.
    """
    facts = dict(row)
    excluded = [
        facts.get("bonding_curve"),
        facts.get("pair_address"),
    ]
    facts["excluded_addresses"] = [a for a in excluded if a]
    return facts


def missing_from(facts: dict) -> dict[str, str]:
    """Which documented gaps are still gaps for this particular candidate."""
    return {k: why for k, why in UNSUPPLIED.items() if facts.get(k) in (None, "", [])}
