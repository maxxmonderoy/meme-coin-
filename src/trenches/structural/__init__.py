"""Structural event timeline: state CHANGES, not prices.

WHY THIS EXISTS. A price stop assumes you can fill at your stop price. On a
token with a couple of thousand dollars of liquidity mid-rug there is no bid --
the stop triggers at -20% and fills far below it. The stop that actually works
on an illiquid token is a STRUCTURAL trigger: LP pulled, bundlers distributing,
dev selling, locker unlocking. Those are observable before the price move
completes and free to monitor.

The question this package exists to answer is therefore an ORDERING question --
did the structural signal fire before the price did? -- so every event carries a
timestamp at the same precision as the price path, and the source observation it
was derived from.
"""
