"""Diff the vendored IDL against upstream.

3.8.8: a program layout change produces corrupt output, not errors. This turns
that silent class of failure into a visible diff. Run it on a schedule and
before any change to the decode path.

Nothing here runs in the ingest path -- the decoder always reads the vendored
copy, never the network.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import httpx

from . import idl

UPSTREAM = {
    "pump": "https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump.json",
    "pump_amm": (
        "https://raw.githubusercontent.com/pump-fun/pump-public-docs/main/idl/pump_amm.json"
    ),
}


@dataclass(slots=True)
class IdlDiff:
    name: str
    reachable: bool
    address_changed: bool = False
    added_instructions: list[str] = field(default_factory=list)
    removed_instructions: list[str] = field(default_factory=list)
    changed_discriminators: list[str] = field(default_factory=list)
    changed_events: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def clean(self) -> bool:
        return self.reachable and not (
            self.address_changed
            or self.added_instructions
            or self.removed_instructions
            or self.changed_discriminators
            or self.changed_events
        )

    @property
    def breaking(self) -> bool:
        """Changes that can silently corrupt decoding.

        A new instruction is informational. A changed discriminator or a
        changed event layout means the bytes on the wire no longer mean what
        the vendored copy says they mean.
        """
        return bool(
            self.address_changed or self.changed_discriminators
            or self.changed_events or self.removed_instructions
        )


def _event_layouts(doc: dict) -> dict[str, list[tuple[str, str]]]:
    events = {e["name"] for e in doc.get("events", [])}
    out: dict[str, list[tuple[str, str]]] = {}
    for t in doc.get("types", []):
        if t["name"] in events and t["type"].get("kind") == "struct":
            out[t["name"]] = [
                (f["name"], json.dumps(f["type"], sort_keys=True))
                for f in t["type"]["fields"]
            ]
    return out


def compare(name: str, remote: dict) -> IdlDiff:
    local = idl.load(name)
    diff = IdlDiff(name=name, reachable=True)
    diff.address_changed = local.get("address") != remote.get("address")

    local_ix = {i["name"]: bytes(i["discriminator"]).hex() for i in local["instructions"]}
    remote_ix = {i["name"]: bytes(i["discriminator"]).hex() for i in remote["instructions"]}
    diff.added_instructions = sorted(set(remote_ix) - set(local_ix))
    diff.removed_instructions = sorted(set(local_ix) - set(remote_ix))
    diff.changed_discriminators = sorted(
        n for n in set(local_ix) & set(remote_ix) if local_ix[n] != remote_ix[n]
    )

    local_ev, remote_ev = _event_layouts(local), _event_layouts(remote)
    diff.changed_events = sorted(
        n for n in set(local_ev) & set(remote_ev) if local_ev[n] != remote_ev[n]
    )
    diff.changed_events += sorted(set(local_ev) - set(remote_ev))
    return diff


# ASYNC109: the timeout is handed to httpx, which enforces it on the request
# itself. An asyncio.timeout wrapper here would cancel mid-request and lose the
# distinction between "upstream is slow" and "upstream changed".
async def fetch_and_compare(name: str, *, timeout: float = 25.0) -> IdlDiff:  # noqa: ASYNC109
    url = UPSTREAM[name]
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            remote = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return IdlDiff(name=name, reachable=False, error=str(exc))
    return compare(name, remote)


async def check_all() -> list[IdlDiff]:
    return [await fetch_and_compare(name) for name in UPSTREAM]
