"""Ingest pipeline: receive -> bounded queue -> worker pool.

Nothing is processed on the receive path (CLAUDE.md 3.8.2). Failing to drain
fills the server's channel and gets the connection dropped, which then looks
like an upstream problem rather than a self-inflicted one.
"""
