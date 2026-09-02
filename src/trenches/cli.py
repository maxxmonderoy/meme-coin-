"""Command line entry point.

Week 1 commands only. There is no `buy`, no `sell` and no `decide`, and nothing
in this package can construct or sign a transaction. That is not an oversight.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
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

    paper_runner = cascade = None
    if args.paper:
        from decimal import Decimal

        from .decide import Cascade
        from .paper.fees import FeeModel
        from .paper.runner import PaperRunner
        from .paper.simulator import ExitPlan

        cascade = Cascade(min_mints=cfg.decide_creator_min_mints,
                          max_rug_rate=cfg.decide_creator_max_rug_rate)
        # The runner holds the PumpPortal consumer so opening a position
        # subscribes to that mint's trades on the SHARED socket (3.2).
        tracker = next(
            (c for c in probes.values() if c.provider == "pumpportal"), None
        )
        paper_runner = PaperRunner(
            db, bankroll_sol=Decimal(str(args.bankroll)),
            size_pct=Decimal(str(args.size_pct)),
            plan=ExitPlan(), model=FeeModel(), tracker=tracker,
        )
        if tracker is None:
            log.warning(
                "paper is on but PumpPortal is not among the feeds; positions will be "
                "armed and never priced, because nothing subscribes to their trades"
            )

    ingest = Ingest(db, cfg, stream_id, recorder=recorder,
                    paper=paper_runner, cascade=cascade)
    mux = FeedMultiplexer(
        factories, backoff_min=cfg.backoff_min_seconds, backoff_max=cfg.backoff_max_seconds,
        on_reconnect=lambda feed, n, reason: ingest.health.record_reconnect(feed),
    )

    workers = [asyncio.create_task(ingest.worker(f"w{i}", i), name=f"worker-{i}")
               for i in range(cfg.workers)]
    flusher = asyncio.create_task(ingest.health_flusher(), name="health")

    async def _safety_sweep() -> None:
        """In-process backstop only.

        3.1 wants the exit loop in its own process and `trenches exits` is that.
        This exists so a single-process run does not leave bags open, and it is
        deliberately NOT a substitute: if this process stalls, so does this
        sweep, which is the whole failure 3.1 is describing.
        """
        while paper_runner is not None:
            try:
                await asyncio.wait_for(asyncio.shield(asyncio.sleep(30)), timeout=31)
            except (TimeoutError, asyncio.CancelledError):
                return
            with contextlib.suppress(Exception):
                await paper_runner.sweep_timeouts()

    sweeper = asyncio.create_task(_safety_sweep(), name="safety-sweep")
    consume = asyncio.create_task(ingest.consume(mux.run()), name="consume")

    # The labeler runs as a SIBLING with its own database connection, never
    # sharing ingest's. run_forever() swallows every exception, so a labeler
    # that crashes in a loop costs log lines and nothing else -- ingest is
    # collecting data that cannot be recovered later and must not be at risk
    # from a reporting job.
    label_db = None
    label_task = None
    if getattr(args, "label", False):
        from .label.labeler import run_forever

        label_db = await pool_mod.connect(cfg.dsn)
        label_task = asyncio.create_task(
            run_forever(_labeler(cfg, label_db), interval_seconds=cfg.label_interval_seconds),
            name="labeler",
        )
        logmod.kv(log, 20, "labeler started alongside ingest", isolated_connection=True)

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
            await asyncio.wait_for(ingest.join(), timeout=10)
        ingest.stop()
        for w in workers:
            w.cancel()
        flusher.cancel()
        sweeper.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await sweeper
        if label_task is not None:
            label_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await label_task
        if label_db is not None:
            await label_db.close()
        await asyncio.gather(*workers, flusher, return_exceptions=True)
        await ingest.final_flush()
        if recorder:
            recorder.close()
        await repo.close_stream(db, stream_id, stop_reason)
        await db.close()
        logmod.kv(log, 20, "stream closed", stream_id=stream_id, reason=stop_reason,
                  dropped=ingest.dropped, dedupe_hits=ingest.dedupe.hits,
                  ticks_stored=ingest.ticks_stored, decisions=ingest.decisions_made,
                  armed=ingest.armed)
    return 0


async def cmd_stats(cfg: Config, args: argparse.Namespace) -> int:
    from .label import HORIZON_SECONDS, HORIZONS

    db = await pool_mod.connect(cfg.dsn)
    try:
        data = await repo.stats(db, hours=args.hours)
        coverage = await repo.outcome_coverage(db, HORIZONS)
        due = {h: await repo.due_count(db, h, HORIZON_SECONDS[h]) for h in HORIZONS}
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

    sampler = data.get("sampler")
    if sampler and sampler.get("watch_set"):
        from .label.dexscreener import BATCH_SIZE, RATE_LIMIT_PER_MINUTE
        from .sample.cadence import compute_budget

        budget = compute_budget(batch_size=BATCH_SIZE,
                                requests_per_minute=RATE_LIMIT_PER_MINUTE)
        watching = int(sampler["watch_set"])
        obs_min = float(sampler.get("observations_per_min") or 0)
        print("\n  SAMPLER")
        print(f"    watch set          {watching:,} / {budget.watch_set_max:,} "
              f"({watching / budget.watch_set_max:.0%} of derived capacity)")
        for cohort, n in sorted((sampler.get("cohorts") or {}).items()):
            print(f"      {cohort:<16} {int(n):,}")
        print(f"    observations/min   {obs_min:,.1f} / "
              f"{budget.observations_per_minute:,} "
              f"({obs_min / budget.observations_per_minute:.0%} of budget)")
        print(f"    backlog due now    {int(sampler.get('backlog') or 0):,}")
        by_status = sampler.get("by_status") or {}
        if by_status:
            print(f"    by status          {by_status}")
        if by_status.get("not_yet_indexed"):
            print("      not_yet_indexed is the cold start, NOT a death signal")
        lateness = sampler.get("lateness_ms") or {}
        if lateness:
            print(f"    lateness           p50 {lateness.get('p50')}ms "
                  f"p90 {lateness.get('p90')}ms")

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

    total_rows = sum(v["observed"] for v in coverage["horizons"].values())
    print(f"\n  OUTCOME LABELING -- {total_rows:,} observations over "
          f"{coverage['total_tokens']:,} mints")
    for horizon in HORIZONS:
        bucket = coverage["horizons"][horizon]
        split = " ".join(f"{k}={v:,}" for k, v in sorted(bucket["status"].items())) or "-"
        venue = " ".join(f"{k}={v:,}" for k, v in sorted(bucket["venue"].items())) or "-"
        print(f"    {horizon:<4} observed={bucket['observed']:>7,} "
              f"backfilled={bucket['backfilled']:>7,} "
              f"due_unobserved={due[horizon]:>7,}   {split}")
        print(f"         venue: {venue}")
    print("    no_pool is a LABEL, not a collection failure: most launches never "
          "become tradeable.")
    print("    venue: bonding_curve is NOT graduation -- every launch has a curve "
          "from birth (1.3).")
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

            outcomes = await repo.outcomes_for_mint(db, args.mint)
            peak = await repo.peak_for_mint(db, args.mint)
            print("\n-- outcomes --")
            if not outcomes:
                print("  none recorded yet (horizon not due, or labeler has not run)")
            for o in outcomes:
                late = int(o["lateness_seconds"] or 0)
                flags = []
                if o["ambiguous_no_pool"]:
                    flags.append("AMBIGUOUS(not-yet-indexed vs never-had-a-pool)")
                if o["backfilled"]:
                    flags.append("backfilled")
                if late > 60:
                    flags.append(f"late by {late // 60}m")
                print(f"  {o['horizon']:<4} {o['status']:<8} "
                      f"liq={o['liquidity_usd'] or '-':>12} price={o['price_usd'] or '-':>14} "
                      f"{' '.join(flags)}")
            if peak:
                print(f"  peak  max_price_seen={peak['max_price_usd_seen']} "
                      f"over {peak['observations']} observations")
                print("        LOWER BOUND on the true peak -- we sample, we do not stream.")
        else:
            print(f"{args.mint} not in tokens_seen")

        path, events = await repo.load_path_and_events(db, args.mint)
        if path:
            print(f"\n-- price path ({len(path)} observations) --")
            for r in path[:args.path_limit]:
                print(f"  {r['observed_at']}  {r['status']:<16} "
                      f"px={r.get('price_usd') or '-':<14} "
                      f"liq={r.get('liquidity_usd') or '-':<12} age={r['age_seconds']}s")
            if len(path) > args.path_limit:
                print(f"  ... {len(path) - args.path_limit} more")
        if events:
            # Interleaved with the path on purpose: the question is whether the
            # structural signal fired BEFORE the price move completed.
            print(f"\n-- structural events ({len(events)}) --")
            for e in events:
                print(f"  {e['detected_at']}  {e['event_type']:<22} "
                      f"{e['severity'] or '':<5} {e.get('before_value')} -> "
                      f"{e.get('after_value')}")

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
    if path.is_file():  # noqa: ASYNC240
        files = [path]
        skipped = []
    else:
        # ONLY the raw-wire captures. `--record` writes two different things
        # into the same directory: `pumpportal-frames-*.jsonl` (what came off
        # the socket, which is what a schema claim is about) and
        # `events-*.jsonl` (RawEvent records this code already normalised).
        # Globbing `*.jsonl` reads both, so the report scores our own output
        # against PumpPortal's schema and reports `provider`, `slot` and
        # `commitment` as undeclared PumpPortal fields. It inflates the frame
        # count and dilutes the match rate with rows that were never frames.
        files = sorted(path.glob("pumpportal-frames-*.jsonl"))  # noqa: ASYNC240
        skipped = sorted(path.glob("events-*.jsonl"))  # noqa: ASYNC240
    if not files:
        print(f"no pumpportal-frames-*.jsonl under {path}", file=sys.stderr)
        if skipped:
            print(f"({len(skipped)} events-*.jsonl found -- those are normalised "
                  f"RawEvents, not raw frames, and prove nothing about the "
                  f"upstream schema)", file=sys.stderr)
        return 2
    if skipped:
        print(f"reading {len(files)} raw-frame file(s); skipping {len(skipped)} "
              f"events-*.jsonl (normalised, not raw)")

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
    # Only fields absent from EVERY frame are wrong. A field missing from some
    # frames is normal here: the shape varies by `pool`, so a pump-only field is
    # legitimately absent from every bonk frame and vice versa.
    never_seen = {n: c for n, c in report.absent_optional.items() if c == report.frames}
    if never_seen:
        print("\ndeclared optional fields never seen (remove or rename):")
        for name, count in sorted(never_seen.items(), key=lambda kv: -kv[1]):
            print(f"   {name:24s} absent in ALL {count} frames")
    if report.matched == report.frames and not report.unknown_fields:
        print("\nSchema confirmed. Set NEW_TOKEN_SCHEMA.verified = True in "
              "src/trenches/stream/pumpportal.py")
        return 0
    print("\nSchema needs correction before it can be marked verified.")
    return 1


async def cmd_paper(cfg: Config, args: argparse.Namespace) -> int:
    """Report the paper journal, or replay stored ticks through the simulator.

    Reports what the journal says without softening it (part 0 rule 8). A paper
    run that loses money is a result, not a bug -- 1.9 puts the expected value
    of an unfiltered trade at -7.85% after costs, so a cascade that rejects
    nothing SHOULD lose here, and seeing that is the point.
    """
    from decimal import Decimal

    from .paper.fees import FeeModel, round_trip_cost_pct
    from .paper.runner import PaperRunner
    from .paper.simulator import ExitPlan, Tick

    db = await pool_mod.connect(cfg.dsn)
    try:
        model = FeeModel()
        if args.pessimistic:
            model = model.pessimistic()

        if args.replay:
            plan = ExitPlan()
            runner = PaperRunner(
                db, bankroll_sol=Decimal(str(args.bankroll)),
                size_pct=Decimal(str(args.size_pct)), plan=plan, model=model,
            )
            mints = [r["mint"] for r in await db.fetch(
                "select distinct mint from trade_ticks limit ?", args.limit)]
            if not mints:
                print("no trade_ticks stored. Run `trenches stream` with paper enabled "
                      "so positions subscribe to their own trade tape first.")
                return 1
            for mint in mints:
                runner.arm(mint)
                for row in await repo.ticks_for_mint(db, mint):
                    price = row.get("price_sol")
                    await runner.on_tick(mint, Tick(
                        at=dt.datetime.fromisoformat(row["observed_at"]),
                        price_sol=Decimal(price) if price else None,
                        signature=row["signature"],
                        is_buy=bool(row["is_buy"]) if row["is_buy"] is not None else None,
                    ))
                await runner.sweep_timeouts()
            print(f"replayed {len(mints)} mint(s) through the simulator")

        size_sol = Decimal(str(args.bankroll)) * Decimal(str(args.size_pct)) / Decimal(100)
        explicit = round_trip_cost_pct(model, size_sol, include_slippage=False) * 100
        total = round_trip_cost_pct(model, size_sol) * 100
        print(f"\ncost model{' (PESSIMISTIC: 2x slippage)' if args.pessimistic else ''}")
        print(f"  position size          {size_sol} SOL "
              f"({args.size_pct}% of {args.bankroll})")
        print(f"  explicit friction      {explicit:>6.2f}%   (1.6 budgets ~4%)")
        print(f"  with slippage haircut  {total:>6.2f}%   (3.9.2: wider than quote)")
        print("  the haircut is a chosen parameter, not a measurement")

        s = await repo.paper_summary(db)
        print(f"\npositions   {int(s.get('positions') or 0):>6}   "
              f"closed {int(s.get('closed') or 0)}   open {int(s.get('still_open') or 0)}")
        if int(s.get("closed") or 0) == 0:
            print("  nothing closed yet -- no result to report")
            return 0
        print(f"  staked      {Decimal(s['total_staked_sol']):>12.6f} SOL")
        print(f"  net PnL     {Decimal(s['net_pnl_sol']):>12.6f} SOL")
        print(f"  fees        {Decimal(s['total_fees_sol']):>12.6f} SOL   "
              "(logged separately from PnL, 1.10)")
        if s.get("win_rate") is not None:
            print(f"  win rate    {s['win_rate']:>12.1%}")
        print(f"  exits       {s.get('exit_reasons')}")

        print("\n3.9.6 promotion criteria")
        closed = int(s.get("closed") or 0)
        net = Decimal(s["net_pnl_sol"])
        share = s.get("largest_win_share")
        checks = [
            (f"300+ logged decisions ({closed})", closed >= 300),
            (f"net positive after fees ({net:.6f} SOL)", net > 0),
            (
                "no single trade > 20% of profit"
                + (f" ({share:.0%})" if share is not None else " (n/a)"),
                share is None or share <= 0.20,
            ),
        ]
        for label, ok in checks:
            print(f"  [{'x' if ok else ' '}] {label}")
        print("  [ ] spans >= 3 weeks  -- and the clock only counts once the")
        print("      cascade actually rejects things; a filter that accepts")
        print("      everything is measuring the market, not the filter")
    finally:
        await db.close()
    return 0


async def cmd_structural(cfg: Config, args: argparse.Namespace) -> int:
    """Fetch the structural half of Part 2 so stage 1 stops reading `unfetched`.

    Free and keyless. Reports how many probes came back with any fields at all,
    because GoPlus omitting a field is a different fact from the token being
    clean, and stage 1 depends on that distinction.
    """
    from .enrich.structural import fetch_batch

    db = await pool_mod.connect(cfg.dsn)
    try:
        before = await repo.structural_coverage(db)
        report = await fetch_batch(db, limit=args.limit)
        after = await repo.structural_coverage(db)
        print(f"probed {report['probed']} mint(s), {report['errors']} error(s), "
              f"{report['with_fields']} returned usable fields")
        print(f"coverage    {after['probed']:,} / {after['tokens']:,} tokens "
              f"(+{after['probed'] - before['probed']:,})")
        print(f"with fields {after['with_fields']:,}")
        print(f"would fail stage 1: {after['would_reject']:,}")
        if after["probed"] and not after["with_fields"]:
            print("\n** every probe came back with no usable fields. That is a vendor")
            print("   or endpoint problem, NOT a clean market -- stage 1 will keep")
            print("   reporting `unfetched`, which is the honest answer (Part 2).")
    finally:
        await db.close()
    return 0


async def cmd_exits(cfg: Config, args: argparse.Namespace) -> int:
    """The exit loop, as its own process (3.1).

    Reads open positions from the shared store, applies any ticks that arrived,
    and closes whatever the plan says is over. Run this ALONGSIDE `trenches
    stream`, not inside it: an entry-side crash or rate-limit stall leaving an
    open bag unwatched is the most common way a homemade bot dies, and that only
    stays true while both halves share a process.
    """
    from .paper import exits as exit_mod
    from .paper.fees import FeeModel

    db = await pool_mod.connect(cfg.dsn)
    model = FeeModel().pessimistic() if args.pessimistic else FeeModel()
    try:
        while True:
            report = await exit_mod.sweep(db, model=model)
            if report["open"] or args.once:
                print(f"open={report['open']} closed={report['closed']} "
                      f"still_open={report['still_open']} unusable={report['unusable']}")
            if args.once:
                return 0
            await asyncio.sleep(args.interval)
    except KeyboardInterrupt:
        return 130
    finally:
        await db.close()


async def cmd_sample(cfg: Config, args: argparse.Namespace) -> int:
    """Run the dense price-path sampler. Its own process; ingest never notices."""
    from .sample.cadence import DEFAULT_CADENCE, describe
    from .sample.sampler import Sampler

    db = await pool_mod.connect(cfg.dsn)
    try:
        sampler = Sampler(
            db, cadence=DEFAULT_CADENCE,
            max_age_seconds=int(args.max_age_hours * 3600),
            control_share=args.control_share,
        )
        for line in describe(sampler.budget):
            print(f"  {line}")
        if args.compact:
            from .sample.compaction import compact

            report = await compact(
                db, older_than_seconds=int(args.compact_after_hours * 3600))
            print(f"\ncompacted {report.mints} path(s): "
                  f"{report.rows_before:,} -> {report.rows_after:,} rows "
                  f"({report.removed:,} removed)")
            print("  extremes are always kept: a trailing stop is a function of peaks")
            print("  and the falls from them, so dropping one would change a replay")
            return 0
        if args.once:
            print(f"\n{await sampler.tick()}")
            return 0
        await sampler.run(interval_seconds=args.interval)
    except KeyboardInterrupt:
        return 130
    finally:
        await db.close()
    return 0


async def cmd_replay_exits(cfg: Config, args: argparse.Namespace) -> int:
    """Replay named rulesets over collected paths, split by cohort.

    The comparison this whole change exists to produce. Fees and price impact
    are applied; a rule that only wins before them has not won.
    """
    from decimal import Decimal

    from .exits import report as report_mod
    from .exits.cursor import Event, Observation
    from .exits.engine import ExitEngine
    from .exits.rules import library

    rules = library()
    wanted = args.rules.split(",") if args.rules else list(rules)
    unknown = [r for r in wanted if r not in rules]
    if unknown:
        print(f"unknown ruleset(s): {unknown}. Available: {sorted(rules)}", file=sys.stderr)
        return 2

    db = await pool_mod.connect(cfg.dsn)
    try:
        mints = await repo.mints_with_paths(db, cohort=args.cohort, limit=args.limit)
        if not mints:
            print("no collected paths yet -- run `trenches sample` first")
            return 1

        engine = ExitEngine(
            model=__import__("trenches.paper.fees", fromlist=["FeeModel"]).FeeModel()
        )
        loaded: dict[str, tuple[str, list, list]] = {}
        for row in mints:
            path_rows, event_rows = await repo.load_path_and_events(db, row["mint"])
            observations = [Observation(
                at=dt.datetime.fromisoformat(r["observed_at"]), status=r["status"],
                price_usd=Decimal(r["price_usd"]) if r.get("price_usd") else None,
                liquidity_usd=Decimal(r["liquidity_usd"]) if r.get("liquidity_usd") else None,
                age_seconds=int(r["age_seconds"] or 0), venue_kind=r.get("venue_kind"),
            ) for r in path_rows]
            events = [Event(
                at=dt.datetime.fromisoformat(e["detected_at"]),
                event_type=e["event_type"], severity=e["severity"] or "warn",
                delta_pct=e.get("delta_pct"),
            ) for e in event_rows]
            loaded[row["mint"]] = (row["cohort"], observations, events)

        size = Decimal(str(args.size_usd))
        rows: list = []
        per_rule: dict[str, dict] = {}
        for name in wanted:
            replays_by_cohort: dict[str, list] = {}
            per_rule[name] = {}
            for mint, (cohort, obs, events) in loaded.items():
                replay = engine.replay(mint=mint, cohort=cohort, rule=rules[name],
                                       observations=obs, events=events, size_usd=size)
                replays_by_cohort.setdefault(cohort, []).append(replay)
                per_rule[name][mint] = replay
            for cohort, replays in sorted(replays_by_cohort.items()):
                rows.append(report_mod.summarise(replays, rule=name, cohort=cohort))

        print(f"\nreplayed {len(loaded)} mint(s), position ${size} "
              f"(size is compared against pool liquidity, so it must be USD)\n")
        for line in report_mod.format_summary(rows):
            print(line)

        if "structural_first" in per_rule and "price_stop_only" in per_rule:
            cmp_ = report_mod.compare_timing(
                per_rule["structural_first"], per_rule["price_stop_only"])
            print("\n  STRUCTURAL vs PRICE STOP -- did the structural signal fire first?")
            print("    (only mints where BOTH fired; comparing across mints where only")
            print("     one fired would compare different populations)")
            print(f"    mints where both fired   {cmp_.mints_compared}")
            if cmp_.mints_compared:
                print(f"    structural first         {cmp_.structural_first} "
                      f"({cmp_.structural_first_rate:.0%})")
                print(f"    price stop first         {cmp_.price_first}")
                print(f"    same observation         {cmp_.simultaneous}")
                if cmp_.median_lead_seconds is not None:
                    print(f"    median lead              {cmp_.median_lead_seconds:.0f}s")
                print(f"    better outcome           {cmp_.better_fill_count}")
                if cmp_.median_fill_gain is not None:
                    print(f"    median gain              "
                          f"{float(cmp_.median_fill_gain):+.3f}x")
            else:
                print("    nothing to compare yet -- needs paths where both rules fire")

        print("\n  Impact assumes a balanced pool and uses liquidity from the LAST")
        print("  observation, so mid-rug the real depth is lower than modelled.")
        print("  These returns are therefore OPTIMISTIC, not conservative.")
    finally:
        await db.close()
    return 0


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


def _labeler(cfg: Config, db, *, backfill: bool = False):
    """Build a Labeler. Kept here so cmd_label and cmd_stream agree exactly."""
    from .enrich.rugcheck import RugCheckClient
    from .label.dexscreener import DexScreenerClient
    from .label.labeler import Labeler

    return Labeler(
        db,
        client=DexScreenerClient(),
        rugcheck=RugCheckClient(cfg.rugcheck_base),
        dead_liquidity_usd=cfg.label_dead_liquidity_usd,
        batch_limit=cfg.label_batch_limit,
        fallback_budget=cfg.label_fallback_budget,
        backfill=backfill,
    )


async def cmd_label(cfg: Config, args: argparse.Namespace) -> int:
    """Run the outcome labeler on its own. Never touches the feed path."""
    from .label.labeler import run_forever

    db = await pool_mod.connect(cfg.dsn)
    labeler = _labeler(cfg, db, backfill=args.backfill)
    try:
        backlog = await labeler.backlog()
        print("backlog by horizon: " + ", ".join(f"{h}={n:,}" for h, n in backlog.items()))
        if args.once:
            observed = await labeler.tick()
            print(f"observed {observed:,} horizons "
                  f"({labeler.stats.requests} requests, "
                  f"no_pool={labeler.stats.no_pool} alive={labeler.stats.alive} "
                  f"dead={labeler.stats.dead})")
            return 0
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)
        runner = asyncio.create_task(
            run_forever(labeler, interval_seconds=cfg.label_interval_seconds, stop=stop)
        )
        await stop.wait()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
        print(f"stopped. observed {labeler.stats.observed:,} horizons over "
              f"{labeler.stats.ticks} ticks")
    finally:
        await db.close()
    return 0


async def cmd_rules_report(cfg: Config, args: argparse.Namespace) -> int:
    """What each hypothetical rejection rule would have cost.

    Runs with zero `decisions` rows on purpose -- week 1 has none, and the
    report has to work the moment stage gating lands rather than after it.
    """
    from decimal import Decimal

    from .label import HORIZON_SECONDS
    from .label.rules import RULES, evaluate, is_late

    db = await pool_mod.connect(cfg.dsn)
    try:
        rows = await repo.rule_evaluation_rows(db)
        all_labeled = [r for r in rows if r.get("status_24h") or r.get("status_7d")
                       or r.get("status_15m")]
        if not args.include_stale:
            # A 15m label observed later than 15m after the fact is a
            # "what does it look like now" reading wearing a 15m name.
            rows = [r for r in rows if not is_late(r, HORIZON_SECONDS["15m"], "lateness_15m")]
        labeled = [r for r in rows if r.get("status_24h") or r.get("status_7d")
                   or r.get("status_15m")]
        dropped = len(all_labeled) - len(labeled)
        print(f"candidates: {len(rows):,}   with at least one outcome: {len(labeled):,}")
        if dropped:
            print(f"excluded as stale: {dropped:,} -- observed later than the horizon itself, "
                  f"so the label is nominal only. --include-stale to keep them.")
        if not labeled:
            if all_labeled:
                print("\nEvery outcome on record was excluded as stale. These are backfilled "
                      "observations of mints whose horizon passed long ago: real data, but a "
                      "15m label taken days late is a 'what does it look like now' reading. "
                      "Re-run with --include-stale to score them anyway, or wait for the "
                      "labeler to observe mints on schedule.")
            else:
                print("\nNo outcomes recorded yet. Run `trenches label` first -- the report is "
                      "structurally fine, it just has nothing to score.")
            return 0
        threshold = Decimal(str(args.peak_multiple or cfg.label_peak_multiple))
        print(f"a rejection is WRONG when the token later reached >= {threshold}x its 15m price\n")
        header = (f"  {'rule':22} {'stage':>5} {'rejects':>9} {'correct':>8} "
                  f"{'wrong':>7} {'?':>7} {'prec':>6}")
        print(header)
        print("  " + "-" * (len(header) - 2))
        for out in evaluate(labeled, rules=RULES, peak_threshold=threshold):
            prec = out.precision()
            print(f"  {out.name:22} {out.stage:>5} {out.rejected:>9,} {out.correct:>8,} "
                  f"{out.wrong:>7,} {out.undeterminable:>7,} "
                  f"{'n/a' if prec is None else f'{prec:.1%}':>6}")
        print("\n  correct = rejected and it ended no_pool/dead")
        print("  wrong   = rejected and it reached the peak multiple")
        print("  ?       = rejected, outcome not determinable -- NOT counted as success")
    finally:
        await db.close()
    return 0


async def cmd_gate(cfg: Config, args: argparse.Namespace) -> int:
    """Week-1 gate, attributed.

    The original formulation counted consecutive stream hours, which scores
    "somebody logged the machine out" and "the collector crashed" as the same
    number. On a shared machine that measures the household, not the software.
    This asks the question the soak was actually for: did anything break that
    was OURS?
    """
    db = await pool_mod.connect(cfg.dsn)
    try:
        report = await repo.gap_attribution(db, days=args.days)
        counters = await db.fetchrow(
            "select coalesce(sum(decode_failures),0) as decode_failures, "
            "coalesce(sum(schema_mismatches),0) as schema_mismatches, "
            "coalesce(sum(queue_drops),0) as queue_drops, "
            "coalesce(sum(stale_responses),0) as stale_responses, "
            "coalesce(sum(reconnects),0) as reconnects from feed_health"
        )
    finally:
        await db.close()

    print(f"window: last {args.days:g} days   sessions: {report['sessions']}")
    print(f"  gaps attributed to the HOST   {report['host_caused']:>4}   "
          f"(clean shutdown -- logout, reboot, someone else using the machine)")
    print(f"  gaps attributed to the SYSTEM {report['system_caused']:>4}   "
          f"(crash, stall, or unexplained -- these are ours)")
    print()
    for name in ("decode_failures", "schema_mismatches", "queue_drops", "stale_responses"):
        print(f"  {name:<18} {int(counters[name]):>6,}")
    print(f"  {'reconnects':<18} {int(counters['reconnects']):>6,}   (recovered, not failures)")

    if args.verbose and report["gaps"]:
        print("\n  every gap:")
        for g in report["gaps"][-25:]:
            secs = "unknown" if g["seconds"] is None else f"{g['seconds'] // 60}m"
            print(f"    after stream #{g['after_stream']:<4} {g['attributed']:<7} "
                  f"{secs:>9}  {g['reason'][:60]}")

    broken = (report["system_caused"] + int(counters["decode_failures"])
              + int(counters["schema_mismatches"]) + int(counters["queue_drops"])
              + int(counters["stale_responses"]))
    print()
    if broken == 0:
        print(f"VERDICT: CLEAN over {args.days:g} days -- no failure attributable to this system.")
        print("  This is NOT the same claim as 'ran unattended for seven days'. It says")
        print("  nothing we wrote broke; it says nothing about whether the host stays up.")
    else:
        print(f"VERDICT: {broken} system-attributable failure(s). Run with -v for the list.")
    return 0 if broken == 0 else 1


async def cmd_decide(cfg: Config, args: argparse.Namespace) -> int:
    """Run the 3.4 cascade over candidates and journal every verdict. PAPER only.

    There is no execute path and nothing here can sign. `mode` is a column
    (3.9.1), so the day live becomes possible this same code runs with a
    different value in that column rather than a different branch.
    """
    import time

    from .decide import Cascade

    cascade = Cascade(
        min_mints=cfg.decide_creator_min_mints,
        max_rug_rate=cfg.decide_creator_max_rug_rate,
    )
    thresholds = cascade.thresholds()
    version = code_version()

    db = await pool_mod.connect(cfg.dsn)
    try:
        rows = await repo.candidates_for_decision(db, limit=args.limit)
        if not rows:
            print("no undecided candidates")
            return 0
        accepted = rejected = 0
        for row in rows:
            started = time.perf_counter()
            verdict, trail = cascade.run(dict(row))
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            await repo.record_decision(db, mint=row["mint"], fields={
                "stream_id": row.get("stream_id"),
                "mode": "PAPER",
                "outcome": "reject" if verdict.rejected else "accept",
                "reject_stage": verdict.stage if verdict.rejected else None,
                "reject_reason": verdict.reason if verdict.rejected else None,
                "inputs": {f"stage{v.stage}": v.inputs for v in trail},
                "cascade_ms": {f"stage{v.stage}": elapsed_ms for v in trail},
                "decision_latency_ms": elapsed_ms,
                "thresholds": thresholds,
                "code_version": version,
            })
            if verdict.rejected:
                rejected += 1
            else:
                accepted += 1
        summary = await repo.decision_summary(db)
    finally:
        await db.close()

    print(f"decided {len(rows):,}  accept={accepted:,}  reject={rejected:,}")
    print(f"\njournal now holds {summary['total']:,} decisions "
          f"({summary['accept']:,} accept / {summary['reject']:,} reject)")
    if summary["by_stage"]:
        print("  rejections by stage:")
        for stage, n in sorted(summary["by_stage"].items()):
            print(f"    stage {stage}: {n:,}")
    if accepted == len(rows):
        print("\n  NOTE every candidate was accepted. Stage 1 has no structural facts to")
        print("  read (nothing fetches them yet) and stage 2 cannot fire because n_rugged")
        print("  is zero everywhere -- nothing labels rugs. That is unlabelled, not clean.")
    return 0


async def cmd_rugs(cfg: Config, args: argparse.Namespace) -> int:
    """Derive rug labels from collected outcomes and fill creators.n_rugged.

    Measures LIQUIDITY COLLAPSE, not proven malice -- see label/rugs.py. The
    distinction is not pedantry: stage 2 rejects a wallet on this number, and a
    confident label on a guess is how a filter ends up rejecting people for
    having launched during a downturn.
    """
    from .db.dialect import parse_ts
    from .label.rugs import Collapse, find_collapse, summarise

    db = await pool_mod.connect(cfg.dsn)
    try:
        by_mint = await repo.observations_by_mint(db)
        hits: list[tuple[str, str, str, float, float]] = []
        for mint, observations in by_mint.items():
            if len(observations) < 2:
                continue
            found = find_collapse(
                observations,
                liquidity_floor=args.liquidity_floor,
                collapse_fraction=args.collapse_fraction,
            )
            if found:
                hits.append((mint, *found))

        identity = await repo.mint_identity(db, [h[0] for h in hits])
        collapses: list[Collapse] = []
        for mint, from_h, to_h, peak, final in hits:
            ident = identity.get(mint) or {}
            observations = by_mint[mint]
            collapsed_at = next(
                (o["scheduled_for"] for o in observations if o["horizon"] == to_h), None
            )
            born = parse_ts(ident.get("detected_at"))
            died = parse_ts(collapsed_at)
            lifetime = int((died - born).total_seconds()) if born and died else None
            collapses.append(Collapse(
                mint=mint, signer=ident.get("signer"), peak_liquidity_usd=peak,
                final_liquidity_usd=final, first_seen_at=str(ident.get("detected_at")),
                collapsed_at=str(collapsed_at), lifetime_seconds=lifetime,
                from_horizon=from_h, to_horizon=to_h,
            ))

        counts: dict[str, int] = {}
        lifetimes: dict[str, list[int]] = {}
        for c in collapses:
            if not c.signer:
                continue
            counts[c.signer] = counts.get(c.signer, 0) + 1
            if c.lifetime_seconds is not None:
                lifetimes.setdefault(c.signer, []).append(c.lifetime_seconds)

        if not args.dry_run:
            await repo.reset_derived_rugs(db)
            updated = await repo.apply_rug_counts(db, counts)
            await repo.apply_median_lifetimes(
                db, {a: sorted(v)[len(v) // 2] for a, v in lifetimes.items()}
            )
        else:
            updated = 0

        stats = summarise(collapses)
        at_risk = await repo.creators_at_risk(
            db, min_mints=cfg.decide_creator_min_mints,
            max_rug_rate=cfg.decide_creator_max_rug_rate,
        )
    finally:
        await db.close()

    print(f"mints examined        {len(by_mint):,}")
    suffix = ("   (DRY RUN, nothing written)" if args.dry_run
              else f"   -> {updated:,} creators updated")
    print(f"liquidity collapses   {stats['collapses']:,}{suffix}")
    print(f"distinct signers      {stats['distinct_signers']:,}")
    if stats["median_lifetime_seconds"] is not None:
        print(f"median lifetime       {stats['median_lifetime_seconds'] // 60:,} min")
    print(f"\ncreators stage 2 would now reject "
          f"(>= {cfg.decide_creator_min_mints} mints, rug rate >= "
          f"{cfg.decide_creator_max_rug_rate:.0%}): {len(at_risk):,}")
    for r in at_risk[:10]:
        print(f"  {r['address'][:16]}..  {r['n_rugged']}/{r['n_mints']} = {r['rug_rate']:.0%}")

    print("\n  This measures liquidity COLLAPSE, not proven malice, and it")
    print("  UNDERCOUNTS: median rugged lifespan is ~14 min (1.2) and the first")
    print("  observation is at 15m, so the fastest rugs are invisible here and")
    print("  look identical to a token that never had a pool.")
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
    stream.add_argument("--paper", action=argparse.BooleanOptionalAction, default=True,
                        help="run the cascade and open paper positions (default on). "
                             "--no-paper is week-1 ingest only.")
    stream.add_argument("--bankroll", default="10", help="speculative bankroll in SOL")
    stream.add_argument("--size-pct", default="1.0", dest="size_pct",
                        help="percent of bankroll per position (1.9 hard cap: 2.0)")
    stream.add_argument("--label", action="store_true",
                        help="also run the outcome labeler, isolated from ingest")

    label = sub.add_parser("label", help="observe recorded mints at fixed horizons")
    label.add_argument("--once", action="store_true", help="a single pass, then exit")
    label.add_argument("--backfill", action="store_true",
                       help="mark rows as observed after the fact")

    rules_report = sub.add_parser(
        "rules-report", help="what each hypothetical rejection rule would have cost")
    rules_report.add_argument("--peak-multiple", type=float, default=None)
    rules_report.add_argument("--include-stale", action="store_true",
                              help="include horizons observed later than the horizon itself")

    stats = sub.add_parser("stats", help="events/hour, uniques, dupes, feed race, detection rate")
    stats.add_argument("--hours", type=float, default=24)

    inspect = sub.add_parser("inspect", help="show what is known about a mint")
    inspect.add_argument("mint")
    inspect.add_argument("--enrich", action="store_true",
                         help="call RugCheck and GoPlus (manual only, never in the ingest path)")
    inspect.add_argument("--fresh", action="store_true",
                         help="request a cache bypass -- mandatory for held positions (3.4.6)")
    inspect.add_argument("--raw", action="store_true", help="dump raw payloads")
    inspect.add_argument("--path-limit", type=int, default=40, dest="path_limit")

    verify = sub.add_parser("verify-capture",
                            help="check recorded PumpPortal frames against the declared schema")
    verify.add_argument("path")

    decide = sub.add_parser("decide", help="run the cascade over candidates (PAPER only)")
    decide.add_argument("--limit", type=int, default=5000)

    rugs = sub.add_parser("rugs", help="derive rug labels from outcomes into creators.n_rugged")
    rugs.add_argument("--liquidity-floor", type=float, default=1000.0)
    rugs.add_argument("--collapse-fraction", type=float, default=0.1)
    rugs.add_argument("--dry-run", action="store_true")

    gate = sub.add_parser("gate", help="week-1 gate: gaps attributed to host vs system")
    gate.add_argument("--days", type=float, default=7)
    gate.add_argument("-v", "--verbose", action="store_true", help="list every gap")

    paper = sub.add_parser("paper", help="paper journal, cost model, and 3.9.6 progress")
    paper.add_argument("--replay", action="store_true",
                       help="replay stored trade_ticks through the simulator")
    paper.add_argument("--bankroll", default="10", help="speculative bankroll in SOL")
    paper.add_argument("--size-pct", default="1.0", dest="size_pct",
                       help="percent of bankroll per position (1.9 hard cap: 2.0)")
    paper.add_argument("--pessimistic", action="store_true",
                       help="double the slippage haircut (3.9.6's pessimistic clause)")
    paper.add_argument("--limit", type=int, default=5000)

    structural = sub.add_parser(
        "structural", help="fetch stage-1 structural facts (free, keyless)")
    structural.add_argument("--limit", type=int, default=200)

    exits = sub.add_parser(
        "exits", help="run the exit loop as its own process (3.1) -- run alongside `stream`")
    exits.add_argument("--interval", type=float, default=15.0)
    exits.add_argument("--once", action="store_true")
    exits.add_argument("--pessimistic", action="store_true")

    sample = sub.add_parser("sample", help="dense price-path sampler (own process)")
    sample.add_argument("--interval", type=float, default=5.0)
    sample.add_argument("--once", action="store_true")
    sample.add_argument("--max-age-hours", type=float, default=24.0,
                        dest="max_age_hours",
                        help="24h default: the 1-7d tail costs 41.6%% of the rate budget")
    sample.add_argument("--control-share", type=float, default=0.5, dest="control_share")
    sample.add_argument("--compact", action="store_true",
                        help="downsample expired paths instead of sampling")
    sample.add_argument("--compact-after-hours", type=float, default=24.0,
                        dest="compact_after_hours")

    replay = sub.add_parser("replay-exits",
                            help="replay exit rulesets over collected paths, by cohort")
    replay.add_argument("--rules", default=None,
                        help="comma-separated ruleset names (default: all)")
    replay.add_argument("--cohort", default=None, choices=["filtered", "control"])
    replay.add_argument("--size-usd", default="500", dest="size_usd",
                        help="position size in USD; compared against pool liquidity")
    replay.add_argument("--limit", type=int, default=5000)

    sub.add_parser("idl-check", help="diff the vendored IDL against upstream")
    sub.add_parser("prune", help="delete raw_events past the retention window")
    return parser


HANDLERS = {
    "migrate": cmd_migrate, "stream": cmd_stream, "stats": cmd_stats,
    "inspect": cmd_inspect, "verify-capture": cmd_verify_capture,
    "paper": cmd_paper,
    "structural": cmd_structural,
    "exits": cmd_exits,
    "sample": cmd_sample,
    "replay-exits": cmd_replay_exits,
    "idl-check": cmd_idl_check, "prune": cmd_prune,
    "label": cmd_label, "rules-report": cmd_rules_report, "gate": cmd_gate,
    "decide": cmd_decide, "rugs": cmd_rugs,
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
