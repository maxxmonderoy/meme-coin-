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
constant: Solana FM / Solscan account labels, Dune's `solana_utils.labels`, or
Helius's address-label endpoint. Export one to JSON in the shape below and
point TRENCHES_LABEL_SET_PATH at it. `labels.example.json` documents the shape
with zero addresses in it.

    {"cex": ["..."], "dex": ["..."], "pool": ["..."], "locker": ["..."]}
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

KINDS = ("cex", "dex", "pool", "locker")


@dataclass(slots=True, frozen=True)
class LabelSet:
    """Addresses to strip before clustering, by what they are."""

    cex: frozenset[str] = field(default_factory=frozenset)
    dex: frozenset[str] = field(default_factory=frozenset)
    pool: frozenset[str] = field(default_factory=frozenset)
    locker: frozenset[str] = field(default_factory=frozenset)
    source: str = "empty"

    @property
    def usable(self) -> bool:
        """Whether stage 5 may REJECT on a cluster computed with these labels.

        Gated on the CEX set specifically, not on the total. Without it, every
        wallet funded from one exchange hot wallet unions into a single cluster
        that spans most of the holder set, and stage 5 rejects the entire market
        while appearing to have found coordination. That failure is worse than
        the stage not running, because it produces a confident wrong answer
        instead of a gap -- so the stage computes and journals, but does not
        gate, until a CEX set exists.
        """
        return bool(self.cex)

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
