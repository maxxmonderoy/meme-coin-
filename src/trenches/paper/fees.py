"""The cost model.

3.9.2: model costs honestly in paper mode or the exercise is theatre. That is
the whole load-bearing sentence for this file, so every component below is
either a figure from 1.6 or a deliberately pessimistic choice, and each one says
which it is.

1.6's explicit friction is ~4% per round trip and that is the number this
targets. It is NOT a single 4% constant, because the components behave
differently: the launchpad fee scales with notional, the priority fee and tip
are roughly fixed in SOL regardless of trade size, and slippage depends on pool
depth. At the 1-2% position sizes 1.9 allows, the fixed costs dominate and a
flat percentage would flatter small trades badly.

WHAT IS DELIBERATELY MISSING. Snipers, bundlers and the deployer are not a fee
(1.6) -- they are embedded in the entry price, and the entry price here comes
from the real tape, which already contains them. Modelling them again would be
double-counting.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal

#: 1.6: 0.95% per side at the sub-$300k tier, which is exactly where these
#: trades live. Applied to notional on entry and exit.
LAUNCHPAD_FEE_PCT = Decimal("0.0095")

#: 3.6: PumpPortal Local is 0.5%. Local is the documented choice there -- half
#: Lightning's fee and unambiguous custody.
PLATFORM_FEE_PCT = Decimal("0.005")

#: Fixed SOL costs per transaction. 1.6 puts priority fees plus tips at
#: 0.05-0.2% of a round trip; expressed absolutely because they do not scale
#: with a trade this small. Jito's minimum tip is 1,000 lamports and 3.6 calls
#: that a floor rather than a price, so this sits above it.
PRIORITY_FEE_SOL = Decimal("0.00015")
JITO_TIP_SOL = Decimal("0.00003")

#: The haircut, and the most important number in the file.
#:
#: 3.9.2 requires a slippage haircut WIDER than the quote, because thin pools do
#: not fill at quote. A tick tells us a trade happened at some price; it does
#: not tell us our order would have filled there. On a bonding curve a
#: same-slot-or-later fill is ARITHMETICALLY GUARANTEED worse than the one being
#: copied (1.8), so a symmetric haircut is not pessimism, it is the mechanism.
#:
#: 250 bps each way is a choice, not a measurement, and it is flagged as such in
#: every report. Calibrating it needs realised fills we do not have and will not
#: have until live -- which is precisely why it is a parameter.
ENTRY_SLIPPAGE_PCT = Decimal("0.025")
EXIT_SLIPPAGE_PCT = Decimal("0.025")

#: An exit taken because the token is dying does not fill at the last quoted
#: price. Applied on top of the normal exit haircut for rug and timeout exits.
DISTRESS_EXTRA_SLIPPAGE_PCT = Decimal("0.05")


@dataclass(frozen=True, slots=True)
class FeeModel:
    launchpad_pct: Decimal = LAUNCHPAD_FEE_PCT
    platform_pct: Decimal = PLATFORM_FEE_PCT
    priority_sol: Decimal = PRIORITY_FEE_SOL
    tip_sol: Decimal = JITO_TIP_SOL
    entry_slippage_pct: Decimal = ENTRY_SLIPPAGE_PCT
    exit_slippage_pct: Decimal = EXIT_SLIPPAGE_PCT
    distress_extra_pct: Decimal = DISTRESS_EXTRA_SLIPPAGE_PCT

    def as_dict(self) -> dict[str, str]:
        """Snapshotted onto every position, so changing the model later does not
        silently rewrite what earlier results meant."""
        return {
            "launchpad_pct": str(self.launchpad_pct),
            "platform_pct": str(self.platform_pct),
            "priority_sol": str(self.priority_sol),
            "tip_sol": str(self.tip_sol),
            "entry_slippage_pct": str(self.entry_slippage_pct),
            "exit_slippage_pct": str(self.exit_slippage_pct),
            "distress_extra_pct": str(self.distress_extra_pct),
            "note": "slippage is a chosen pessimistic parameter, not a measurement",
        }

    def pessimistic(self, factor: Decimal = Decimal("2")) -> FeeModel:
        """A harsher variant, for asking whether an edge survives being wrong.

        3.9.6 wants net positive after modelled fees AND pessimistic slippage.
        Running the journal through this is how that second clause gets tested
        rather than assumed.
        """
        return replace(
            self,
            entry_slippage_pct=self.entry_slippage_pct * factor,
            exit_slippage_pct=self.exit_slippage_pct * factor,
            distress_extra_pct=self.distress_extra_pct * factor,
        )


@dataclass(frozen=True, slots=True)
class Fill:
    """One simulated side of a trade, with fees itemised rather than netted."""

    quote_price_sol: Decimal
    fill_price_sol: Decimal
    slippage_pct: Decimal
    tokens: Decimal
    gross_sol: Decimal
    fee_launchpad_sol: Decimal
    fee_priority_sol: Decimal
    fee_tip_sol: Decimal

    @property
    def fee_total_sol(self) -> Decimal:
        return self.fee_launchpad_sol + self.fee_priority_sol + self.fee_tip_sol

    @property
    def net_sol(self) -> Decimal:
        """Signed as cash: negative on entry (money out), positive on exit."""
        return self.gross_sol - self.fee_total_sol


def buy(model: FeeModel, quote_price_sol: Decimal, size_sol: Decimal) -> Fill:
    """Spend `size_sol` at a price worse than quote, and pay to do it.

    The haircut raises the price paid, so the same SOL buys FEWER tokens. That
    direction matters: applying it to the notional instead would understate the
    damage, because the position also exits from a worse token count.
    """
    if quote_price_sol <= 0:
        raise ValueError("cannot fill at a non-positive price")
    fill_price = quote_price_sol * (Decimal(1) + model.entry_slippage_pct)
    fee_launchpad = size_sol * (model.launchpad_pct + model.platform_pct)
    spendable = size_sol - fee_launchpad - model.priority_sol - model.tip_sol
    if spendable <= 0:
        raise ValueError(
            f"position size {size_sol} SOL does not cover fixed costs; at 1.9's "
            "1-2% sizing the fixed fees dominate and the trade is uneconomic"
        )
    tokens = spendable / fill_price
    return Fill(
        quote_price_sol=quote_price_sol,
        fill_price_sol=fill_price,
        slippage_pct=model.entry_slippage_pct,
        tokens=tokens,
        gross_sol=-size_sol,
        fee_launchpad_sol=fee_launchpad,
        fee_priority_sol=model.priority_sol,
        fee_tip_sol=model.tip_sol,
    )


def sell(
    model: FeeModel, quote_price_sol: Decimal, tokens: Decimal, *, distressed: bool = False
) -> Fill:
    """Sell `tokens` at a price worse than quote.

    `distressed` widens the haircut for exits taken because something is going
    wrong. A rug exit does not fill at the last printed price, and pretending
    otherwise is the single easiest way to make a losing system look profitable.
    """
    if quote_price_sol < 0:
        raise ValueError("negative price")
    haircut = model.exit_slippage_pct + (model.distress_extra_pct if distressed else Decimal(0))
    haircut = min(haircut, Decimal("0.99"))
    fill_price = quote_price_sol * (Decimal(1) - haircut)
    gross = tokens * fill_price
    fee_launchpad = gross * (model.launchpad_pct + model.platform_pct)
    return Fill(
        quote_price_sol=quote_price_sol,
        fill_price_sol=fill_price,
        slippage_pct=haircut,
        tokens=tokens,
        gross_sol=gross,
        fee_launchpad_sol=fee_launchpad,
        fee_priority_sol=model.priority_sol,
        fee_tip_sol=model.tip_sol,
    )


def round_trip_cost_pct(
    model: FeeModel, size_sol: Decimal, *, include_slippage: bool = True
) -> Decimal:
    """Friction on a flat round trip, as a fraction of size.

    Both halves are reportable because they answer different questions and
    conflating them is how a cost model quietly stops matching 1.6.

    `include_slippage=False` is the EXPLICIT FRICTION of 1.6's table: launchpad
    plus platform plus priority and tip. Expect roughly 3%, slightly under
    1.6's ~4%, and the gap is real rather than an error -- 1.6 budgets ~2.0% for
    a trading terminal's take, and 3.6 routes through PumpPortal Local at 0.5%
    per side instead, so that line is genuinely smaller here. If this figure
    ever drifts far below 3%, the model has lost a fee.

    `include_slippage=True` is what a position actually costs. It lands near 8%,
    roughly double the explicit figure, because 3.9.2 demands a haircut WIDER
    than the quote and 2.5% per side is what this model chose. That number is a
    judgement, not a measurement -- see ENTRY_SLIPPAGE_PCT. If it is too harsh,
    the journal will understate a real edge; if it is too kind, it will invent
    one. Understating is the survivable direction.
    """
    working = (
        model if include_slippage
        else replace(
            model,
            entry_slippage_pct=Decimal(0),
            exit_slippage_pct=Decimal(0),
            distress_extra_pct=Decimal(0),
        )
    )
    entry = buy(working, Decimal(1), size_sol)
    exit_ = sell(working, Decimal(1), entry.tokens)
    return (size_sol - exit_.net_sol) / size_sol
