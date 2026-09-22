"""Derive shared-funder labels from the funding graph. No vendor, no list.

WHAT THIS REPLACES. Stage 5 cannot reject without a CEX label set, because
without one every wallet funded from a single exchange hot wallet unions into
one cluster and the stage rejects the whole market. The obvious fix is to buy a
labelled address set. This is the other fix.

THE LABEL DOES NOT NEED TO SAY "BINANCE". It needs to say "this funder's
presence is not evidence of coordination". That is a property of the graph, not
a fact about a company: infrastructure -- exchanges, bridges, faucets, market
makers -- funds wallets that turn up across many UNRELATED tokens, while a dev's
funding wallet funds wallets inside its own launches. Derived this way the set
never goes stale, costs nothing, and catches bridges and distributors that a CEX
list omits entirely.

THE STATISTIC IS `n_tokens`, NOT OUT-DEGREE, and the difference is the whole
design. A single deployer funding forty sniper wallets inside one launch has an
out-degree of forty and is EXACTLY the actor stage 5 exists to catch; strip it
and the stage is worse than useless. The same wallet appearing as a funder
across four hundred unrelated mints is infrastructure. Counting distinct mints
separates those two; counting edges merges them.

THE FAILURE MODE THIS MODULE IS MOSTLY ABOUT. 1.5: of 178,109 serial deployers
studied, 85.3% were net profitable against buyers, and the most aggressive
operator ran ~353 tokens/day. A serial deployer's funding wallet appears across
hundreds of mints -- it looks EXACTLY like infrastructure by the n_tokens test,
and stripping it would erase the single most extractive actor in the market from
the clustering that exists to find it. So an address is never labelled if it is
a known creator, or funds one. We have that list in-house: `tokens_seen.signer`
and `creators`. It costs one query and it is the reason this is safe.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any

#: What a derived label is called. Deliberately NOT "cex": we have not
#: established that any of these are exchanges, only that unioning through them
#: would merge wallets with nothing else in common. A row that claimed "cex"
#: would be asserting something nobody checked.
SHARED_FUNDER = "shared_funder"

#: Why an address that passed the count test was NOT labelled.
CREATOR_EXCLUSION = "is or funds a known creator"


@dataclass(slots=True)
class FunderStat:
    address: str
    n_tokens: int
    n_funded: int


@dataclass(slots=True)
class Derivation:
    """The result, carrying enough provenance to argue with later."""

    labels: dict[str, FunderStat] = field(default_factory=dict)
    #: Addresses over the cutoff that were spared because they touch a creator.
    #: Reported loudly: each one is a candidate serial deployer, which is a
    #: finding in its own right and not merely a skipped label.
    withheld: dict[str, FunderStat] = field(default_factory=dict)
    cutoff_tokens: int = 0
    n_considered: int = 0

    @property
    def addresses(self) -> frozenset[str]:
        return frozenset(self.labels)


def derive(
    stats: Any,
    *,
    cutoff_tokens: int,
    creators: Any = (),
) -> Derivation:
    """Label funders seen across at least `cutoff_tokens` distinct mints.

    `stats` is an iterable of FunderStat or of mappings with `address`,
    `n_tokens` and `n_funded`. `creators` is the in-house set of known signers
    and declared creators; any funder in it, or funding an address in it, is
    withheld rather than labelled.

    There is NO DEFAULT CUTOFF. Nobody has published one, and picking a
    plausible integer here would be exactly the invented threshold that is
    forbidden -- so the caller has to choose, and `distribution()` exists to let
    them choose from data instead of from a hunch.
    """
    if cutoff_tokens < 2:
        # A funder seen in ONE token is the single-launch case: a dev funding
        # its own snipers. Labelling that would strip the target.
        raise ValueError("cutoff_tokens must be >= 2; a one-token funder is a candidate insider")

    known = {str(c) for c in (creators or ()) if c}
    out = Derivation(cutoff_tokens=cutoff_tokens)
    for stat in _stats(stats):
        out.n_considered += 1
        if stat.n_tokens < cutoff_tokens:
            continue
        if stat.address in known:
            out.withheld[stat.address] = stat
            continue
        out.labels[stat.address] = stat
    return out


def withheld_funding_creators(
    edges: Any, labelled: Any, creators: Any
) -> set[str]:
    """Labelled funders that FUND a known creator. The second half of the guard.

    A serial deployer need not sign with the wallet that pays for things. If a
    funder we were about to call infrastructure is bankrolling an address that
    creates tokens, it is a treasury, not an exchange -- exchanges do not fund
    the deployers whose launches they appear in.
    """
    known = {str(c) for c in (creators or ()) if c}
    candidates = {str(a) for a in (labelled or ()) if a}
    if not known or not candidates:
        return set()
    out: set[str] = set()
    for edge in edges or ():
        funder, funded = _pair(edge)
        if funder in candidates and funded in known:
            out.add(funder)
    return out


def distribution(stats: Any) -> dict[int, int]:
    """n_tokens -> how many funders sit at exactly that count.

    Printed rather than reduced to a suggested cutoff. The shape is the point:
    if the counts are bimodal -- a mass at 1-2 and a tail in the hundreds --
    then almost any line drawn in the empty middle gives the same answer, and
    the choice barely matters. If they are smooth, there is no natural cutoff
    and the operator should know that before trusting one.
    """
    out: dict[int, int] = {}
    for stat in _stats(stats):
        out[stat.n_tokens] = out.get(stat.n_tokens, 0) + 1
    return dict(sorted(out.items()))


def widest_gap(stats: Any) -> tuple[int, int] | None:
    """The sharpest break in the observed n_tokens values, ranked BY RATIO.

    Returned as (below, above): the last populated count before the break and
    the first one after it, so any cutoff in `below+1 .. above` gives the same
    labels. NOT a recommendation and not a default -- it shows the operator
    where their data is already separated, so the number they pick is one they
    can point at.

    RATIO, NOT DIFFERENCE. These counts span orders of magnitude: one-token
    funders in the hundreds, infrastructure in the hundreds of tokens. Ranked by
    difference, a 100 -> 200 step (2x, both plainly infrastructure) beats a
    1 -> 100 step (100x, the actual boundary) purely because it is one integer
    wider. Measured on a synthetic graph, that is exactly what happened.

    The single-token mass IS included, because it is usually one side of the
    real boundary. `derive` still refuses a cutoff below 2, so a returned
    `below` of 1 means "anywhere from 2 up to `above`".

    Returns None when the counts are consecutive, which is itself the answer:
    no natural cutoff exists in this data and any line drawn is arbitrary.
    """
    counts = sorted({s.n_tokens for s in _stats(stats) if s.n_tokens >= 1})
    if len(counts) < 2:
        return None
    best = max(itertools.pairwise(counts), key=lambda pair: pair[1] / pair[0])
    return best if best[1] - best[0] > 1 else None


def _stats(stats: Any):
    for item in stats or ():
        if isinstance(item, FunderStat):
            yield item
            continue
        if isinstance(item, dict):
            address = item.get("address") or item.get("funder")
            n_tokens, n_funded = item.get("n_tokens"), item.get("n_funded")
            if address is None or n_tokens is None:
                continue
            yield FunderStat(str(address), int(n_tokens), int(n_funded or 0))


def _pair(edge: Any) -> tuple[str, str]:
    if isinstance(edge, dict):
        return str(edge.get("funder") or ""), str(edge.get("funded") or "")
    if isinstance(edge, (tuple, list)) and len(edge) >= 2:
        return str(edge[0]), str(edge[1])
    return "", ""
