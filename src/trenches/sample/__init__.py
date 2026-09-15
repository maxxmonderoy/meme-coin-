"""Dense price-path sampling.

Strictly read-only observation, and structurally independent of ingest: it reads
mints from the database and writes its own tables. If the sampler dies the
stream must not notice, which is why it shares no queue, no socket and no task
group with the feed path.
"""
