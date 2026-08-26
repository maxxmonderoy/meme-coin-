"""Command line entry point.

Week 1 commands only. There is no `buy`, no `sell` and no `decide`, and nothing
in this package can construct or sign a transaction. That is not an oversight.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
from collections.abc import Callable
from pathlib import Path

from . import log as logmod
from .config import Config, ConfigError
from .db import migrate as migrate_mod
from .db import pool as pool_mod
from .db import repo
from .db.dialect import detect_dialect
from .pipeline.worker import Ingest
from .stream.base import Consumer
from .stream.multiplex import FeedMultiplexer
from .stream.recorder import Recorder
from .version import code_version

log = logmod.get("trenches.cli")


def _factories(cfg: Config, record_dir: Path | None) -> dict[str, Callable[[], Consumer]]:
    out: dict[str, Callable[[], Consumer]] = {}
    for feed in cfg.feeds:
        if feed == "pumpportal":
            from .stream.pumpportal import PumpPortalConsumer

            out[feed] = lambda c=cfg, r=record_dir: PumpPortalConsumer(
                c.pumpportal_url, record_dir=r, keepalive_seconds=c.keepalive_seconds,
                strict=c.pumpportal_strict,
            )
        elif feed == "rugcheck":
            from .stream.rugcheck_feed import RugCheckNewTokensConsumer

            out[feed] = lambda c=cfg: RugCheckNewTokensConsumer(
                c.rugcheck_base, interval_seconds=c.rugcheck_interval_seconds,
                startup_max_age_seconds=c.rugcheck_startup_max_age_seconds,
            )
        elif feed == "replay":
            from .stream.replay import ReplayConsumer

            out[feed] = lambda c=cfg: ReplayConsumer(c.replay_path)
    return out


# -- commands --------------------------------------------------------------

async def cmd_migrate(cfg: Config, args: argparse.Namespace) -> int:
    db = await pool_mod.connect(cfg.dsn)
    try:
        applied = await migrate_mod.migrate(db)
        print(f"dialect: {db.dialect}")
        print(f"applied {len(applied)} migration(s): {', '.join(applied) or 'none pending'}")
    finally:
        await db.close()
    return 0


async def cmd_stream(cfg: Config, args: argparse.Namespace) -> int:
    record_dir = Path(args.record) if args.record else None
    factories = _factories(cfg, record_dir)
    if not factories:
        print("no feeds configured; set TRENCHES_FEEDS", file=sys.stderr)
        return 2

    db = await pool_mod.connect(cfg.dsn)
    recorder = Recorder(record_dir) if record_dir else None
    probes = {name: factory() for name, factory in factories.items()}
    stream_id = await repo.open_stream(
        db, feeds=list(factories),
        subscription={n: c.subscription_descriptor() for n, c in probes.items()},
        code_version=code_version(),
    )
    logmod.kv(log, 20, "stream opened", stream_id=stream_id, feeds=list(factories),
              dialect=db.dialect, retention=cfg.retention, code_version=code_version())

    ingest = Ingest(db, cfg, stream_id, recorder=recorder)
    mux = FeedMultiplexer(
        factories, backoff_min=cfg.backoff_min_seconds, backoff_max=cfg.backoff_max_seconds,
        on_reconnect=lambda feed, n, reason: ingest.health.record_reconnect(feed),
    )

    workers = [asyncio.create_task(ingest.worker(f"w{i}"), name=f"worker-{i}")
               for i in range(cfg.workers)]
    flusher = asyncio.create_task(ingest.health_flusher(), name="health")
    consume = asyncio.create_task(ingest.consume(mux.run()), name="consume")

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, shutdown.set)

    stop_reason = "clean shutdown"
    try:
        waiter = asyncio.create_task(shutdown.wait())
        done, _ = await asyncio.wait([consume, waiter], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
        if consume in done and (exc := consume.exception()):
            stop_reason = f"{type(exc).__name__}: {exc}"
            log.error("stream failed: %s", stop_reason)
    finally:
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
        await repo.close_stream(db, stream_id, stop_reason)
        await db.close()
        logmod.kv(log, 20, "stream closed", stream_id=stream_id, reason=stop_reason,
                  dropped=ingest.dropped, dedupe_hits=ingest.dedupe.hits)
    return 0


async def cmd_stats(cfg: Config, args: argparse.Namespace) -> int:
    db = await pool_mod.connect(cfg.dsn)
    try:
        data = await repo.stats(db, hours=args.hours)
    finally:
        await db.close()

    minutes = data.get("minutes_observed") or 0
    events = int(data.get("events") or 0)
    creates = int(data.get("creates") or 0)

    print(f"window: last {args.hours}h   minutes observed: {minutes}")
    for label, key in (("events", "events"), ("creates", "creates"),
                       ("dupes", "dupes"), ("decode failures", "decode_failures"),
                       ("schema mismatches", "schema_mismatches"),
                       ("stale responses", "stale_responses"),
                       ("reconnects", "reconnects"), ("queue drops", "queue_drops"),
                       ("queue high water", "queue_high_water")):
        print(f"  {label:<19}{int(data.get(key) or 0):>10,}")
    print(f"  {'unique mints':<19}{int(data.get('unique_mints') or 0):>10,}")

    # The 3.8.8 canary: a rate, not a total. A feed that broke an hour ago has
    # a healthy-looking total and a current rate of zero.
    if minutes:
        print(f"\n  DETECTION RATE     {creates / minutes:>9.2f} creates/min"
              f"   ({events / minutes:,.1f} events/min)")
        if creates == 0:
            print("  ** zero creates across the whole window -- check the feeds and the")
            print("     schema mismatch count, not just the error count (3.8.8)")
    if data.get("stale_responses"):
        print("  ** stale responses seen: a feed served cached data. This looks like a")
        print("     working feed while being useless (3.3).")

    lat = data.get("detect_latency_ms") or {}
    if lat:
        print(f"\n  detect latency     p50 {lat.get('p50')}ms  p90 {lat.get('p90')}ms  "
              f"p99 {lat.get('p99')}ms")

    per_feed = data.get("per_feed") or []
    if per_feed:
        print("\n  per feed:")
        for row in per_feed:
            print(f"    {row['feed']:<14} events={int(row['events']):>8,} "
                  f"creates={int(row['creates']):>7,} stale={int(row['stale']):>4} "
                  f"mismatch={int(row['mismatches']):>4} reconnects={int(row['reconnects']):>4}")

    race = data.get("feed_race") or []
    if race:
        total = sum(int(r["wins"]) for r in race) or 1
        print("\n  FEED RACE -- who saw each mint first (3.2 upgrade trigger 2 evidence):")
        for row in race:
            wins = int(row["wins"])
            d = row.get("delta_ms") or {}
            lead = (f"lead over the other feed: p50 {d.get('p50')}ms p90 {d.get('p90')}ms"
                    if d else "no head-to-head samples yet")
            print(f"    {row['first_feed']:<14} {wins:>7,} wins ({wins / total:>5.1%})  {lead}")
        print("\n  Read this before ever paying for a faster feed: a feed that never wins,")
        print("  or wins by milliseconds you cannot act on, is not worth upgrading.")
    return 0


async def cmd_inspect(cfg: Config, args: argparse.Namespace) -> int:
    db = await pool_mod.connect(cfg.dsn)
    try:
        row = await repo.get_token(db, args.mint)
        if row:
            print("-- tokens_seen --")
            for key, value in row.items():
                print(f"  {key:24s} {value}")
            addresses = [a for a in (row.get("signer"), row.get("declared_creator")) if a]
            history = await repo.creator_history(db, addresses)
            print("\n-- creator history (stage-2 cache, both identities) --")
            if not history:
                print("  unknown -- proceed on unknown, do not block (3.4 stage 2)")
            for h in history:
                print(f"  {h['role']:<9} {h['address']}  mints={h['n_mints']} "
                      f"rugged={h['n_rugged']} rug_rate={h['rug_rate']}")
            if row.get("signer") and row.get("signer") != row.get("declared_creator"):
                print("  ** signer != declared_creator: reputation must consider both")
        else:
            print(f"{args.mint} not in tokens_seen")

        if args.enrich:
            from .enrich.goplus import GoPlusClient
            from .enrich.rugcheck import RugCheckClient

            rc = await RugCheckClient(cfg.rugcheck_base).report(args.mint, fresh=args.fresh)
            print(f"\n-- rugcheck report (status={rc.status_code} {rc.latency_ms}ms "
                  f"bypass_requested={rc.bypassed_cache}) --")
            if rc.error:
                print(f"  error: {rc.error}")
            else:
                await repo.store_enrichment(
                    db, provider="rugcheck", endpoint="report", key=args.mint,
                    status_code=rc.status_code, payload=rc.payload,
                    latency_ms=rc.latency_ms, bypassed_cache=rc.bypassed_cache)
                for f, state in rc.cold_start_report().items():
                    print(f"   {'  ' if state == 'populated' else '!!'} {f:26s} {state}")

            gp = await GoPlusClient().token_security(args.mint)
            print(f"\n-- goplus token_security (status={gp.status_code} {gp.latency_ms}ms) --")
            if gp.error:
                print(f"  error: {gp.error}")
            else:
                await repo.store_enrichment(
                    db, provider="goplus", endpoint="token_security", key=args.mint,
                    status_code=gp.status_code, payload=gp.payload,
                    latency_ms=gp.latency_ms, bypassed_cache=False)
                for f, state in gp.field_status().items():
                    print(f"   {'  ' if state == 'populated' else '!!'} {f:34s} {state}")
                rejects = gp.structural_rejects()
                print(f"\n  stage-1 structural rejects: {rejects or 'none'}")

            print("\n  'empty' and 'ABSENT' are NOT 'clean'. On a mint under a minute old the")
            print("  behavioural fields have not been computed yet (Part 2). Never gate on a")
            print("  composite score; read the underlying fields.")
            if args.raw:
                print(json.dumps({"rugcheck": rc.payload, "goplus": gp.payload}, indent=2)[:8000])
    finally:
        await db.close()
    return 0


async def cmd_verify_capture(cfg: Config, args: argparse.Namespace) -> int:
    """Check recorded PumpPortal frames against the declared schema.

    The reason this command exists: PumpPortal does not publish its response
    payload shape, so the field names the consumer maps are a hypothesis. This
    turns that hypothesis into a measurement.
    """
    from .stream.pumpportal import NEW_TOKEN_SCHEMA, ValidationReport

    # ASYNC240: these are local filesystem reads of a capture the user just
    # recorded, not a hot path. An async filesystem shim would add a dependency
    # to save microseconds on a command that runs once.
    path = Path(args.path)
    files = [path] if path.is_file() else sorted(path.glob("*.jsonl"))  # noqa: ASYNC240
    if not files:
        print(f"no .jsonl files under {path}", file=sys.stderr)
        return 2

    report = ValidationReport()
    for file in files:
        for line in file.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            frame = record.get("frame", record)
            if isinstance(frame, dict) and not frame.keys() <= {"message", "method"}:
                report.observe(frame, NEW_TOKEN_SCHEMA)

    print(f"frames examined: {report.frames}")
    if not report.frames:
        print("no data frames found (only subscription acknowledgements?)")
        return 1
    print(f"matched declared schema: {report.matched} "
          f"({report.matched / report.frames:.1%})")
    if report.missing_required:
        print("\nMISSING REQUIRED FIELDS -- the declared schema is WRONG:")
        for name, count in sorted(report.missing_required.items(), key=lambda kv: -kv[1]):
            print(f"   {name:24s} absent in {count}/{report.frames} frames")
    if report.unknown_fields:
        print("\nfields present that the schema does not declare (add to optional):")
        for name, count in sorted(report.unknown_fields.items(), key=lambda kv: -kv[1]):
            print(f"   {name:24s} seen in {count}/{report.frames} frames")
    if report.absent_optional:
        print("\ndeclared optional fields never seen (remove or rename):")
        for name, count in sorted(report.absent_optional.items(), key=lambda kv: -kv[1]):
            if count == report.frames:
                print(f"   {name:24s} absent in ALL frames")
    if report.matched == report.frames and not report.unknown_fields:
        print("\nSchema confirmed. Set NEW_TOKEN_SCHEMA.verified = True in "
              "src/trenches/stream/pumpportal.py")
        return 0
    print("\nSchema needs correction before it can be marked verified.")
    return 1


async def cmd_idl_check(cfg: Config, args: argparse.Namespace) -> int:
    from .decode import idl, idl_check

    worst = 0
    for d in await idl_check.check_all():
        if not d.reachable:
            print(f"{d.name}: UNREACHABLE ({d.error})")
            worst = max(worst, 1)
        elif d.clean:
            print(f"{d.name}: clean -- vendored copy matches upstream")
        else:
            print(f"{d.name}: DIFFERS{'  [BREAKING]' if d.breaking else '  [additive]'}")
            for label, items in (("added", d.added_instructions),
                                 ("removed", d.removed_instructions),
                                 ("CHANGED DISCRIMINATORS", d.changed_discriminators),
                                 ("CHANGED EVENT LAYOUTS", d.changed_events)):
                if items:
                    print(f"   {label}: {', '.join(items)}")
            worst = max(worst, 2 if d.breaking else 1)

    collisions = idl.colliding_discriminators()
    print(f"\n{len(collisions)} discriminator(s) shared between pump and pump_amm. "
          "Disambiguate by program id, never by sighash:")
    for disc, (a, b) in sorted(collisions.items()):
        print(f"   {disc}  pump.{a} == pump_amm.{b}")
    return worst


async def cmd_prune(cfg: Config, args: argparse.Namespace) -> int:
    db = await pool_mod.connect(cfg.dsn)
    try:
        deleted = await repo.prune_raw_events(db, cfg.raw_retention_days)
        print(f"deleted {deleted:,} raw_events older than {cfg.raw_retention_days} days")
    finally:
        await db.close()
    return 0


# -- entry point -----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trenches",
        description="Solana launch ingest and journal. Week 1: no decisions, no trades, $0.",
    )
    parser.add_argument("--log-level", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply pending migrations for the configured dialect")

    stream = sub.add_parser("stream", help="run the ingest loop across all configured feeds")
    stream.add_argument("--record", metavar="DIR", help="also write events to JSONL for replay")

    stats = sub.add_parser("stats", help="events/hour, uniques, dupes, feed race, detection rate")
    stats.add_argument("--hours", type=float, default=24)

    inspect = sub.add_parser("inspect", help="show what is known about a mint")
    inspect.add_argument("mint")
    inspect.add_argument("--enrich", action="store_true",
                         help="call RugCheck and GoPlus (manual only, never in the ingest path)")
    inspect.add_argument("--fresh", action="store_true",
                         help="request a cache bypass -- mandatory for held positions (3.4.6)")
    inspect.add_argument("--raw", action="store_true", help="dump raw payloads")

    verify = sub.add_parser("verify-capture",
                            help="check recorded PumpPortal frames against the declared schema")
    verify.add_argument("path")

    sub.add_parser("idl-check", help="diff the vendored IDL against upstream")
    sub.add_parser("prune", help="delete raw_events past the retention window")
    return parser


HANDLERS = {
    "migrate": cmd_migrate, "stream": cmd_stream, "stats": cmd_stats,
    "inspect": cmd_inspect, "verify-capture": cmd_verify_capture,
    "idl-check": cmd_idl_check, "prune": cmd_prune,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        cfg = Config.from_env()
        detect_dialect(cfg.dsn)
    except (ConfigError, ValueError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    logmod.setup(args.log_level or cfg.log_level)
    try:
        return asyncio.run(HANDLERS[args.command](cfg, args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
