"""GoPlus token security -- free and keyless (3.2).

Best Token-2022 authority coverage of the free options, which is what stage 1
needs. Part 2 measured that for a mint under 60 seconds old GoPlus returns
authority flags but OMITS `holders`, `lp_holders`, `dex` and `holder_count` --
so the structural half is usable immediately and the behavioural half is not
merely empty, it is absent. Those are different things and the client reports
which it got.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx

SOLANA_CHAIN = "solana"
DEFAULT_BASE = "https://api.gopluslabs.io/api/v1"

#: Present within ~1s of a launch.
STRUCTURAL_FIELDS = (
    "mintable", "freezable", "closable", "balance_mutable_authority",
    "transfer_fee", "transfer_fee_upgradable", "transfer_hook",
    "transfer_hook_upgradable", "metadata_mutable", "non_transferable",
    "default_account_state_upgradable",
)

#: Absent at t=0 -- not empty, ABSENT. Reading absence as "clean" is the
#: expensive Part 2 mistake.
COLD_START_FIELDS = ("holders", "lp_holders", "dex", "holder_count", "total_supply")


@dataclass(slots=True)
class Probe:
    mint: str
    status_code: int | None
    latency_ms: int
    payload: Any
    error: str | None = None

    def token(self) -> dict:
        result = (self.payload or {}).get("result") or {}
        return result.get(self.mint) or (next(iter(result.values()), {}) if result else {})

    def field_status(self) -> dict[str, str]:
        token = self.token()
        out: dict[str, str] = {}
        for name in STRUCTURAL_FIELDS:
            out[name] = "populated" if name in token else "absent"
        for name in COLD_START_FIELDS:
            if name not in token:
                out[name] = "ABSENT (cold start)"
            elif token[name] in (None, [], {}, "", "0"):
                out[name] = "empty"
            else:
                out[name] = "populated"
        return out

    def structural_rejects(self) -> list[str]:
        """Stage-1 reject reasons present in the payload.

        Reads the underlying fields, never a composite score -- Part 2's first
        non-negotiable.
        """
        token = self.token()
        reasons: list[str] = []
        for flag in ("mintable", "freezable", "closable", "balance_mutable_authority"):
            value = token.get(flag)
            status = value.get("status") if isinstance(value, dict) else value
            if str(status) == "1":
                reasons.append(f"{flag}=1")
        for flag in ("transfer_fee_upgradable", "transfer_hook_upgradable",
                     "metadata_mutable", "default_account_state_upgradable"):
            value = token.get(flag)
            status = value.get("status") if isinstance(value, dict) else value
            if str(status) == "1":
                reasons.append(f"{flag}=1")
        if token.get("transfer_hook"):
            reasons.append("transfer_hook non-empty")
        fee = token.get("transfer_fee")
        if isinstance(fee, dict) and str(fee.get("transfer_fee_percent", "0")) not in ("0", ""):
            reasons.append(f"transfer_fee={fee.get('transfer_fee_percent')}")
        return reasons


class GoPlusClient:
    def __init__(self, base_url: str = DEFAULT_BASE, *, timeout: float = 15.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    async def token_security(self, mint: str) -> Probe:
        url = f"{self._base}/solana/token_security"
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, params={"contract_addresses": mint})
            latency = int((time.monotonic() - started) * 1000)
            try:
                payload = response.json()
            except ValueError:
                payload = {"_raw": response.text[:2000]}
            return Probe(mint, response.status_code, latency, payload)
        except httpx.HTTPError as exc:
            return Probe(mint, None, int((time.monotonic() - started) * 1000), None, str(exc))
