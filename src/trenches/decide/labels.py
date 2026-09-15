"""Address label sets for stage 5, and the refusal to guess them.

3.4 stage 5: "strip CEX/DEX/pool/locker via cached label sets". This module
holds those sets. IT SHIPS EMPTY, and that is not an oversight.

WHY THERE ARE NO ADDRESSES IN THIS FILE. Part 0 rule 2 forbids inventing a
program ID or an address, and an exchange hot-wallet address recalled from
memory is exactly that -- it looks right, it cannot be checked by reading it,
and it is wrong often enough to matter. Exchanges rotate hot wallets. A stale
CEX address does not fail loudly; it silently stops stripping, and stage 5's
answer inverts (see `LabelSet.usable`).

WHERE TO GET THEM. A labelled address set is a maintained dataset, not a
constant. Verified 8 Sep 2026: Dune exposes `labels.addresses` with a
`blockchain` column and `label_type = 'cex'`, and public Solana CEX-address
queries exist on top of it -- but note Dune's free tier goes view-only on
10 Sep 2026 for accounts created before 21 Jul 2026, and CSV/API export costs
credits. Vybe Network publishes a Solana labelled-wallets API; Solscan and
SolanaFM show account labels in their UIs with API access on paid tiers. Export
one to JSON in the shape below and point TRENCHES_LABEL_SET_PATH at it.
`labels.example.json` documents the shape with zero addresses in it.

THE VENDOR IS NOT THE ONLY ROUTE, and it may not be the best one. What the
stripping actually needs is not "this address is Binance" but "this address is a
shared funder whose presence is not evidence of coordination". An address that
funded tens of thousands of distinct wallets is structurally that, whatever it
is called, and out-degree is computable from the same funding graph the trace
already builds -- no vendor, no staleness, and it catches bridges and faucets
that a CEX list omits. It needs a cutoff nobody has published, so it would ship
uncalibrated; the vendor list is the cross-check that calibrates it.

    {"cex": ["..."], "dex": ["..."], "pool": ["..."], "locker": ["..."]}
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

KINDS = ("cex", "dex", "pool", "locker", "shared_funder")

#: Kinds whose presence makes stage 5 safe to gate. `dex`, `pool` and `locker`
#: do NOT: the failure the stripping prevents is a shared FUNDER merging
#: unrelated wallets, and only these two identify one.
STRIPS_SHARED_FUNDERS = ("cex", "shared_funder")


@dataclass(slots=True, frozen=True)
class LabelSet:
    """Addresses to strip before clustering, by what they are."""

    cex: frozenset[str] = field(default_factory=frozenset)
    dex: frozenset[str] = field(default_factory=frozenset)
    pool: frozenset[str] = field(default_factory=frozenset)
    locker: frozenset[str] = field(default_factory=frozenset)
    #: Derived from our own funding graph rather than bought. Deliberately not
    #: merged into `cex`: we have not established these are exchanges, only that
    #: unioning through them would merge wallets with nothing else in common.
    shared_funder: frozenset[str] = field(default_factory=frozenset)
    source: str = "empty"

    @property
    def usable(self) -> bool:
        """Whether stage 5 may REJECT on a cluster computed with these labels.

        Gated on the sets that identify a SHARED FUNDER -- a bought CEX list, a
        set derived from our own graph, or both -- not on the total. Without
        one, every wallet funded from a single exchange hot wallet unions into a
        cluster spanning most of the holder set, and stage 5 rejects the entire
        market while appearing to have found coordination. That failure is worse
        than the stage not running, because it produces a confident wrong answer
        instead of a gap -- so the stage computes and journals, but does not
        gate, until one exists.

        A DEX, pool or locker set does not satisfy this. Those addresses are not
        funders and stripping them does nothing about the false cluster.
        """
        return any(getattr(self, kind) for kind in STRIPS_SHARED_FUNDERS)

    def with_shared_funders(self, addresses, source: str) -> LabelSet:
        """Return a copy carrying a derived set alongside whatever was loaded.

        The two are complementary rather than alternatives: the bought list is
        authoritative on names, the derived one never goes stale and covers
        bridges and distributors a CEX list omits.
        """
        kept = frozenset(str(a) for a in (addresses or ()) if a)
        if not kept:
            return self
        merged = f"{self.source}+{source}" if self.source != "empty" else source
        return LabelSet(
            cex=self.cex, dex=self.dex, pool=self.pool, locker=self.locker,
            shared_funder=self.shared_funder | kept, source=merged,
        )

    def strip_map(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """address -> label, for `cluster(strip=...)`. Per-token pool and curve
        addresses come in via `extra`; they are not a maintained set."""
        out: dict[str, str] = {}
        for kind in KINDS:
            for address in getattr(self, kind):
                out[address] = kind
        out.update(extra or {})
        return out

    def counts(self) -> dict[str, int]:
        return {kind: len(getattr(self, kind)) for kind in KINDS}


EMPTY = LabelSet()


def load_labels(path: str | Path | None) -> LabelSet:
    """Read a label set from JSON. A missing path returns EMPTY, not an error.

    Unknown keys are ignored rather than rejected: these files come from
    exports that carry extra columns, and refusing to load one over a spare
    field would push people toward hand-editing it.
    """
    if not path:
        return EMPTY
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"TRENCHES_LABEL_SET_PATH points at {path!r}, which does not exist")
    data = json.loads(p.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a JSON object keyed by {KINDS}")
    return LabelSet(
        **{kind: frozenset(str(a) for a in (data.get(kind) or []) if a) for kind in KINDS},
        source=str(p),
    )
