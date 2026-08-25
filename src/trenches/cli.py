"""Command line entry point.

Week 1 commands only: migrate, stream, stats, inspect, idl-check, prune.
There is no `buy`, no `sell`, and no `decide`. That is not an oversight.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
from pathlib import Path

from . import log as logmod
from .config import Config, ConfigError
from .db import migrate as migrate_mod
from .db import pool as pool_mod
from .db import repo
from .decode import idl, idl_check
from .pipeline.worker import Ingest
from .stream.base import Consumer
from .stream.recorder import Recorder
from .stream.replay import ReplayConsumer
from .stream.supervisor import Supervisor
from .version import code_version

log = logmod.get("trenches.cli")


def _build_consumer(cfg: Config, record_dir: Path | None) -> Consumer:
    if cfg.feed == "yellowstone":
        from .stream.yellowstone import YellowstoneConsumer

        return YellowstoneConsumer(
            cfg.yellowstone_endpoint, cfg.yellowstone_token, tls=cfg.yellowstone_tls,
            filter_mode=cfg.filter_mode, keepalive_seconds=cfg.keepalive_seconds,
        )
    if cfg.feed == "pumpportal":
        from .stream.pumpportal import PumpPortalConsumer

        return PumpPortalConsumer(
            cfg.pumpportal_url, record_dir=record_dir,
            keepalive_seconds=cfg.keepalive_seconds,
        )
    return ReplayConsumer(cfg.replay_path)


# -- commands --------------------------------------------------------------

async def cmd_migrate(cfg: Config, args: argparse.Namespace) -> int:
    pool = await pool_mod.connect(cfg.dsn)
    try:
        applied = await migrate_mod.migrate(pool)
        print(f"applied {len(applied)} migration(s): {', '.join(applied) or 'none pending'}")
    finally:
        await pool.close()
    return 0


async def cmd_stream(cfg: Config, args: argparse.Namespace) -> int:
    record_dir = Path(args.record) if args.record else None
    consumer = _build_consumer(cfg, record_dir)
    pool = await pool_mod.connect(cfg.dsn)
    recorder = Recorder(record_dir) if record_dir and cfg.feed != "pumpportal" else None

    stream_id = await repo.open_stream(
        pool, provider=consumer.provider, filter_mode=cfg.filter_mode,
        subscription=consumer.subscription_descriptor(), code_version=code_version(),
    )
    logmod.kv(log, 20, "stream opened", stream_id=stream_id, provider=consumer.provider,
              feed=cfg.feed, filter_mode=cfg.filter_mode, retention=cfg.retention,
              code_version=code_version())

    ingest = Ingest(pool, cfg, stream_id, recorder=recorder)
    supervisor = Supervisor(
        lambda: _build_consumer(cfg, record_dir),
        watchdog_seconds=cfg.slot_watchdog_seconds,
        backoff_min=cfg.backoff_min_seconds,
        backoff_max=cfg.backoff_max_seconds,
        on_reconnect=lambda n, reason: ingest.health.record_reconnect(),
    )

    stop_reason = "clean shutdown"
    workers = [
        asyncio.create_task(ingest.worker(f"w{i}"), name=f"worker-{i}")
        for i in range(cfg.workers)
    ]
    flusher = asyncio.create_task(ingest.health_flusher(), name="health")
    consume = asyncio.create_task(ingest.consume(supervisor.run()), name="consume")

    loop = asyncio.get_running_loop()
    shutdown = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, shutdown.set)

    try:
        done, _ = await asyncio.wait(
            [consume, asyncio.create_task(shutdown.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if consume in done and (exc := consume.exception()):
            stop_reason = f"{type(exc).__name__}: {exc}"
            log.error("stream failed: %s", stop_reason)
    finally:
        # Drain in order: stop the reader, let workers finish the queue, then
        # flush counters. An open position is never left unwatched because the
        # process exited mid-queue -- there are no positions yet, and this is
        # the shutdown path that has to still be correct when there are.
        consume.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await consume
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(ingest.queue.join(), timeout=10)
        ingest.stop()
        for w in workers:
            w.cancel()
        flusher.cancel()
        await asyncio.gather(*workers, flusher, return_exceptions=True)
        await ingest.final_flush()
        if recorder:
            recorder.close()
        await repo.close_stream(pool, stream_id, stop_reason)
        await pool.close()
        logmod.kv(log, 20, "stream closed", stream_id=stream_id, reason=stop_reason,
                  dropped=ingest.dropped, reconnects=supervisor.reconnects,
                  dedupe_hits=ingest.dedupe.hits)
    return 0


async def cmd_stats(cfg: Config, args: argparse.Namespace) -> int:
    pool = await pool_mod.connect(cfg.dsn)
    try:
        data = await repo.stats(pool, hours=args.hours)
        rows = await repo.filter_diff(pool, hours=args.hours)
    finally:
        await pool.close()

    minutes = data.get("minutes_observed") or 0
    events = data.get("events") or 0
    creates = data.get("creates") or 0

    print(f"window: last {args.hours}h   minutes observed: {minutes}")
    print(f"  events            {events:>10,}")
    print(f"  creates           {creates:>10,}")
    print(f"  unique mints      {data.get('unique_mints', 0):>10,}")
    print(f"  dupes             {data.get('dupes', 0):>10,}")
    print(f"  decode failures   {data.get('decode_failures', 0):>10,}")
    print(f"  slot gaps         {data.get('slot_gaps', 0):>10,}")
    print(f"  reconnects        {data.get('reconnects', 0):>10,}")
    print(f"  queue high water  {data.get('queue_high_water', 0):>10,}")
    # The 3.8.8 canary. Rate, not total: a decoder that broke an hour ago has
    # a healthy-looking total and a detection rate of zero.
    if minutes:
        print(f"\n  DETECTION RATE    {creates / minutes:>10.2f} creates/min"
              f"   ({events / minutes:,.1f} events/min)")
        if creates == 0:
            print("  ** zero creates over the whole window -- check the decoder, "
                  "not just the error count (3.8.8)")
    lat = data.get("detect_latency_ms") or {}
    if lat.get("p50") is not None:
        print(f"\n  detect latency    p50 {lat['p50']}ms  p90 {lat['p90']}ms  p99 {lat['p99']}ms")

    if rows:
        print("\n  by filter (3.3 strict-vs-naive diff):")
        for r in rows:
            print(f"    {r['filter_source']:<24} events={r['events']:>8,} "
                  f"creates={r['creates']:>7,}")
    return 0


async def cmd_inspect(cfg: Config, args: argparse.Namespace) -> int:
    pool = await pool_mod.connect(cfg.dsn)
    try:
        row = await repo.get_token(pool, args.mint)
        if row:
            print("-- tokens_seen --")
            for key, value in dict(row).items():
                print(f"  {key:24s} {value}")
            addresses = [a for a in (row["signer"], row["declared_creator"]) if a]
            if addresses:
                history = await repo.creator_history(pool, addresses)
                print("\n-- creator history (stage-2 cache, both identities) --")
                if not history:
                    print("  unknown -- proceed on unknown, do not block (3.4 stage 2)")
                for h in history:
                    print(f"  {h['role']:<9} {h['address']}  mints={h['n_mints']} "
                          f"rugged={h['n_rugged']} rug_rate={h['rug_rate']}")
                if row["signer"] != row["declared_creator"]:
                    print("  ** signer != declared_creator: reputation must consider both")
        else:
            print(f"{args.mint} not in tokens_seen")

        if args.enrich:
            from .enrich.rugcheck import RugCheckClient

            client = RugCheckClient(cfg.rugcheck_base)
            probe = await client.report(args.mint, fresh=args.fresh)
            print(f"\n-- rugcheck report (status={probe.status_code} "
                  f"{probe.latency_ms}ms bypass_requested={probe.bypassed_cache}) --")
            if probe.error:
                print(f"  error: {probe.error}")
            else:
                await repo.store_enrichment(
                    pool, provider="rugcheck", endpoint="report", key=args.mint,
                    status_code=probe.status_code, payload=probe.payload,
                    latency_ms=probe.latency_ms, bypassed_cache=probe.bypassed_cache,
                )
                report = probe.cold_start_report()
                if report:
                    print("  cold-start field status (Part 2):")
                    for f, state in report.items():
                        mark = "  " if state == "populated" else "!!"
                        print(f"   {mark} {f:24s} {state}")
                    print("\n  'empty' is NOT 'clean'. An empty risks array on a fresh mint"
                          "\n  is indistinguishable from a token nobody has traded yet.")
                if args.raw:
                    print(json.dumps(probe.payload, indent=2)[:8000])
    finally:
        await pool.close()
    return 0


async def cmd_idl_check(cfg: Config, args: argparse.Namespace) -> int:
    diffs = await idl_check.check_all()
    worst = 0
    for d in diffs:
        if not d.reachable:
            print(f"{d.name}: UNREACHABLE ({d.error})")
            worst = max(worst, 1)
            continue
        if d.clean:
            print(f"{d.name}: clean -- vendored copy matches upstream")
            continue
        print(f"{d.name}: DIFFERS{'  [BREAKING]' if d.breaking else '  [additive]'}")
        if d.address_changed:
            print("   program address changed")
        for label, items in (
            ("added instructions", d.added_instructions),
            ("removed instructions", d.removed_instructions),
            ("CHANGED DISCRIMINATORS", d.changed_discriminators),
            ("CHANGED EVENT LAYOUTS", d.changed_events),
        ):
            if items:
                print(f"   {label}: {', '.join(items)}")
        worst = max(worst, 2 if d.breaking else 1)

    collisions = idl.colliding_discriminators()
    print(f"\n{len(collisions)} discriminator(s) shared between pump and pump_amm. "
          "Always disambiguate by program id, never by sighash:")
    for disc, (a, b) in sorted(collisions.items()):
        print(f"   {disc}  pump.{a} == pump_amm.{b}")
    return worst


async def cmd_prune(cfg: Config, args: argparse.Namespace) -> int:
    pool = await pool_mod.connect(cfg.dsn)
    try:
        deleted = await repo.prune_raw_events(pool, cfg.raw_retention_days)
        print(f"deleted {deleted:,} raw_events older than {cfg.raw_retention_days} days")
    finally:
        await pool.close()
    return 0


# -- entry point -----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trenches",
        description="Solana launch ingest and journal. Week 1: no decisions, no trades.",
    )
    parser.add_argument("--log-level", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply pending SQL migrations")

    stream = sub.add_parser("stream", help="run the ingest loop")
    stream.add_argument("--record", metavar="DIR",
                        help="also write observed events to JSONL for replay")

    stats = sub.add_parser("stats", help="events/hour, uniques, dupes, detection rate")
    stats.add_argument("--hours", type=int, default=24)

    inspect = sub.add_parser("inspect", help="show what is known about a mint")
    inspect.add_argument("mint")
    inspect.add_argument("--enrich", action="store_true",
                         help="call the vendor API (manual only; never in the ingest path)")
    inspect.add_argument("--fresh", action="store_true",
                         help="request a cache bypass -- mandatory for held positions (3.4.6)")
    inspect.add_argument("--raw", action="store_true", help="dump the raw payload")

    sub.add_parser("idl-check", help="diff the vendored IDL against upstream")
    sub.add_parser("prune", help="delete raw_events past the retention window")
    return parser


HANDLERS = {
    "migrate": cmd_migrate,
    "stream": cmd_stream,
    "stats": cmd_stats,
    "inspect": cmd_inspect,
    "idl-check": cmd_idl_check,
    "prune": cmd_prune,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        cfg = Config.from_env()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    logmod.setup(args.log_level or cfg.log_level)
    try:
        return asyncio.run(HANDLERS[args.command](cfg, args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
