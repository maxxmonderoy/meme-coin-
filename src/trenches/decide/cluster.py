"""Funding-chain clustering: union-find over a labelled edge set (3.4 stage 5).

WHAT THIS ANSWERS. 1.1: the only questions that decide who gets paid are who
holds what and who is about to sell. 1.5 measures the answer -- 36.5% of supply
that appears independently held is controlled by coordinated accounts. Twenty
wallets holding 2% each looks like distribution and is one wallet if nineteen of
them were funded from the twentieth.

WHAT IT DOES NOT ANSWER. Common funding is EVIDENCE of coordination, not proof
of it. Two strangers who both withdrew from the same exchange share a funder and
nothing else -- which is exactly why the exchange addresses have to come out
before the union runs, and why `strip` is a required argument rather than an
option with a default. See labels.py for what happens when that set is empty.

Everything here is pure and offline. The graph is an input; building it costs
RPC calls (3.4: ~80-100 credits/token) and nothing in this module makes one.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

#: Edge relations. Kept as constants because the relation decides whether a
#: cluster may REJECT or may only be journalled -- 3.4's 15% threshold was
#: stated for funding chains, and applying it to a different relation would be
#: inventing a number for that relation.
FUNDING = "funding"
CO_BUY = "co_buy"

#: 3.4 stage 5: "reject if the largest non-CEX cluster exceeds 15% of supply."
DEFAULT_MAX_CLUSTER_PCT = 15.0


class UnionFind:
    """Disjoint sets with path compression and union by size."""

    __slots__ = ("_parent", "_size")

    def __init__(self, items: Iterable[str] = ()) -> None:
        self._parent: dict[str, str] = {}
        self._size: dict[str, int] = {}
        for item in items:
            self.add(item)

    def add(self, item: str) -> None:
        if item not in self._parent:
            self._parent[item] = item
            self._size[item] = 1

    def find(self, item: str) -> str:
        self.add(item)
        root = item
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression, iterative: a funding chain can be long and this runs
        # per candidate.
        while self._parent[item] != root:
            self._parent[item], item = root, self._parent[item]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._size[ra] < self._size[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        self._size[ra] += self._size[rb]

    def groups(self) -> list[set[str]]:
        out: dict[str, set[str]] = {}
        for item in self._parent:
            out.setdefault(self.find(item), set()).add(item)
        return list(out.values())


@dataclass(slots=True)
class Clustering:
    """The result, with everything needed to read it later without guessing."""

    clusters: list[set[str]] = field(default_factory=list)
    #: Addresses removed before the union ran, and why. Journalled because an
    #: empty list on a token whose buyers all came from an exchange means the
    #: clustering did NOT do the thing its number claims.
    stripped: dict[str, str] = field(default_factory=dict)
    #: Which relations actually contributed an edge. A cluster formed only by
    #: co-buy edges may not be judged against 3.4's funding threshold.
    relations: set[str] = field(default_factory=set)
    #: Edges that were dropped because one endpoint was stripped.
    edges_dropped: int = 0

    def largest(
        self, holdings: dict[str, float] | None, *, min_members: int = 2
    ) -> tuple[float | None, set[str]]:
        """Largest cluster by SUPPLY SHARE, not by member count.

        Ten wallets holding dust are not a risk; two holding 30% are.

        A SINGLE ADDRESS IS NOT A CLUSTER, hence min_members=2. One wallet
        holding 30% is a large holder, and stage 4 already asks about large
        holders. Counting it here would make concentration read as coordination
        in the journal -- the same number, relabelled as evidence of something
        it is not -- and stage 5 exists precisely to see what per-address rules
        cannot: the twenty wallets at 2% each that share a funder.

        Returns (share, members). share is None when no qualifying cluster has a
        known holding, which is not zero concentration, it is no measurement.
        """
        best_share, best_members = None, set()
        for members in self.clusters:
            if len(members) < min_members:
                continue
            share = _share_of(members, holdings)
            if share is None:
                continue
            if best_share is None or share > best_share:
                best_share, best_members = share, members
        return best_share, best_members

    def largest_single(self, holdings: dict[str, float] | None) -> float | None:
        """Biggest lone holder among the clustered addresses, for the journal.

        Recorded but never gated on here, so a row shows both numbers and a
        later reader can see that stage 5 declined to reject on the one that
        belongs to stage 4.
        """
        singles = [_share_of(c, holdings) for c in self.clusters if len(c) == 1]
        known = [s for s in singles if s is not None]
        return max(known) if known else None

    def coverage(self, holdings: dict[str, float] | None) -> float | None:
        """Total supply share we can actually see across every clustered address.

        The number that says whether the answer means anything: a 4% largest
        cluster over addresses covering 6% of supply is not a finding about
        concentration, it is a finding about how little was observed.
        """
        if not holdings:
            return None
        seen = {a for c in self.clusters for a in c}
        return round(sum(v for k, v in holdings.items() if k in seen), 4)


def _share_of(members: set[str], holdings: dict[str, float] | None) -> float | None:
    if not holdings:
        return None
    values = [holdings[a] for a in members if a in holdings]
    if not values:
        return None
    return round(sum(values), 4)


def cluster(
    addresses: Iterable[str],
    edges: Iterable[Any],
    *,
    strip: dict[str, str],
) -> Clustering:
    """Union addresses over the edge set, after removing labelled addresses.

    `strip` maps address -> label ("cex", "dex", "pool", "locker"). It is
    REQUIRED, and passing an empty one is a decision the caller has to make
    deliberately: 3.4 says "strip CEX/DEX/pool/locker via cached label sets"
    before the trace, and skipping it does not merely weaken the result, it
    inverts it. Every wallet funded from one exchange hot wallet becomes one
    cluster, that cluster spans most of the holder set, and the stage rejects
    the entire market while looking like it found something.

    Edges may be (a, b) pairs or (a, b, relation) triples; an unlabelled edge is
    recorded as `funding`, since that is 3.4's trace.
    """
    known = normalise_addresses(addresses)
    uf = UnionFind(a for a in known if a not in strip)
    result = Clustering(stripped={a: strip[a] for a in known if a in strip})

    for edge in edges or ():
        parsed = _edge(edge)
        if parsed is None:
            continue
        a, b, relation = parsed
        if a in strip or b in strip:
            # Not an error and not a cluster: it is the exchange doing its job.
            # An endpoint can be labelled without appearing in `addresses` --
            # a funder two hops out, say -- so record it here too.
            result.edges_dropped += 1
            for endpoint in (a, b):
                if endpoint in strip:
                    result.stripped.setdefault(endpoint, strip[endpoint])
            continue
        uf.union(a, b)
        result.relations.add(relation)

    result.clusters = [c for c in uf.groups() if c]
    return result


def normalise_addresses(addresses: Iterable[str]) -> list[str]:
    """Deduplicate and trim, preserving order. Order matters only for the
    journal -- reading the same address list twice must produce the same row."""
    seen: dict[str, None] = {}
    for a in addresses or ():
        if a is None:
            continue
        text = str(a).strip()
        if text:
            seen.setdefault(text, None)
    return list(seen)


def _edge(edge: Any) -> tuple[str, str, str] | None:
    """Parse an edge, or None. Never raises -- edges come from a fetcher."""
    if isinstance(edge, dict):
        a, b = edge.get("from"), edge.get("to")
        relation = edge.get("relation") or FUNDING
    elif isinstance(edge, (tuple, list)) and len(edge) in (2, 3):
        a, b = edge[0], edge[1]
        relation = edge[2] if len(edge) == 3 else FUNDING
    else:
        return None
    if a is None or b is None:
        return None
    a, b = str(a).strip(), str(b).strip()
    if not a or not b or a == b:
        return None
    return a, b, str(relation)
