"""Paper trading: simulated positions over the real trade tape.

Nothing here can construct or sign a transaction and it holds no key material.
`mode` is a column on every row it writes (3.9.1) -- going live would reuse this
exact logic with real fills, never a second code path.
"""
