"""The decision layer. Stages 0-5, PAPER only.

Nothing in this package can construct or sign a transaction, and there is no
`execute()`. `mode` is a column on `decisions`, never a fork in the code
(3.9.1) -- the identical cascade runs in paper and, eventually, live, because a
decide path that branches on mode is one that was never really tested.

WHAT IS DELIBERATELY NOT HERE: fetching. Every stage is a pure function from a
facts dict to a verdict. That is what makes the cascade testable offline, and it
keeps 3.4's cost ordering an explicit caller decision rather than something
buried inside a predicate that quietly makes a network call.
"""
from __future__ import annotations

from .cascade import (
    Cascade,
    Verdict,
    stage0_ingest,
    stage1_structural,
    stage2_creator,
    stage3_liquidity,
    stage4_concentration,
    stage5_clustering,
    top10_share,
)
from .cluster import CO_BUY, FUNDING, Clustering, UnionFind, cluster
from .facts import UNSUPPLIED, attach_first_buyers, facts_from_row, missing_from
from .labels import EMPTY, LabelSet, load_labels

__all__ = [
    "CO_BUY",
    "EMPTY",
    "FUNDING",
    "UNSUPPLIED",
    "Cascade",
    "Clustering",
    "LabelSet",
    "UnionFind",
    "Verdict",
    "attach_first_buyers",
    "cluster",
    "facts_from_row",
    "load_labels",
    "missing_from",
    "stage0_ingest",
    "stage1_structural",
    "stage2_creator",
    "stage3_liquidity",
    "stage4_concentration",
    "stage5_clustering",
    "top10_share",
]
