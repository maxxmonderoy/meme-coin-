"""Vendored IDL access.

The IDL is vendored under vendor/idl and checksummed. Nothing in the decode
path reaches the network: a decoder that silently re-fetches its own layout at
runtime can be changed underneath you between restarts.
"""
from __future__ import annotations

import functools
import json
from pathlib import Path

VENDOR_DIR = Path(__file__).resolve().parents[3] / "vendor" / "idl"

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PUMP_FEE_CONFIG_PROGRAM_ID = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
#: Resolved from the IDL rather than hardcoded -- see metaplex_program_id().
#: The strict filter in 3.3 depends on this address being exactly right, and a
#: constant typed from memory is precisely the class of error CLAUDE.md rule 2
#: is about.
_METAPLEX_ACCOUNT_NAME = "mpl_token_metadata"


class IdlError(RuntimeError):
    pass


@functools.lru_cache(maxsize=4)
def load(name: str) -> dict:
    path = VENDOR_DIR / f"{name}.json"
    if not path.is_file():
        raise IdlError(
            f"vendored IDL {path} is missing. Run `trenches idl-check --update` "
            "and review the diff before trusting any decode."
        )
    idl = json.loads(path.read_text())
    if "instructions" not in idl or "address" not in idl:
        raise IdlError(f"{path} does not look like an Anchor IDL")
    return idl


def _disc_hex(raw: list[int]) -> str:
    return "0x" + bytes(raw).hex()


@functools.lru_cache(maxsize=4)
def instruction_discriminators(name: str) -> dict[str, str]:
    return {i["name"]: _disc_hex(i["discriminator"]) for i in load(name)["instructions"]}


@functools.lru_cache(maxsize=4)
def event_discriminators(name: str) -> dict[str, bytes]:
    return {e["name"]: bytes(e["discriminator"]) for e in load(name).get("events", [])}


@functools.lru_cache(maxsize=32)
def event_fields(idl_name: str, event_name: str) -> tuple[dict, ...]:
    """Field list for an event, taken from the IDL `types` section."""
    for t in load(idl_name).get("types", []):
        if t["name"] == event_name:
            kind = t["type"]
            if kind.get("kind") != "struct":
                raise IdlError(f"{event_name} is not a struct")
            return tuple(kind["fields"])
    raise IdlError(f"{event_name} not found in {idl_name} IDL types")


def colliding_discriminators() -> dict[str, tuple[str, str]]:
    """Sighashes present in BOTH pump and pump_amm.

    CLAUDE.md 3.5 warns that the AMM and the bonding curve share
    discriminators. Measured against the vendored IDLs the overlap is wider
    than buy/sell alone, which is why every decode path keys on program id
    first and sighash second, never the reverse.
    """
    pump = instruction_discriminators("pump")
    amm = instruction_discriminators("pump_amm")
    by_amm = {v: k for k, v in amm.items()}
    return {
        disc: (name, by_amm[disc])
        for name, disc in pump.items()
        if disc in by_amm
    }


@functools.lru_cache(maxsize=1)
def metaplex_program_id() -> str:
    """Metaplex Token Metadata program id, read out of the pump IDL.

    `create` pins this account by address while `buy`, `sell`, `buy_v2`,
    `sell_v2` and `migrate` do not reference it at all -- which is the entire
    basis for the `account_required` discrimination in 3.3. Reading it from the
    IDL means the filter and the program agree by construction.
    """
    create = next(i for i in load("pump")["instructions"] if i["name"] == "create")
    for account in create["accounts"]:
        if account["name"] == _METAPLEX_ACCOUNT_NAME:
            address = account.get("address")
            if not address:
                raise IdlError(
                    f"{_METAPLEX_ACCOUNT_NAME} is present in `create` but carries no pinned "
                    "address; the strict filter cannot be derived safely"
                )
            return address
    raise IdlError(
        f"`create` no longer references {_METAPLEX_ACCOUNT_NAME}. The 3.3 strict filter "
        "premise is broken -- fall back to FILTER_MODE=naive and re-verify."
    )


#: Backwards-compatible alias used by the stream layer.
METAPLEX_TOKEN_METADATA_PROGRAM_ID = metaplex_program_id()
