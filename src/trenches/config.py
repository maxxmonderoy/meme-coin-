"""Configuration: environment -> frozen dataclass, validated at startup.

Fails fast and loudly. A stream that starts with a silently-defaulted endpoint
and quietly reads nothing is the CLAUDE.md 3.8.1 failure mode wearing a
different hat.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

VALID_FEEDS = ("pumpportal", "rugcheck", "replay")
VALID_FILTER_MODES = ("strict", "naive", "both")
VALID_RETENTION = ("creates_full", "all", "creates_only")


class ConfigError(RuntimeError):
    pass


def _load_env_file() -> None:
    """Load KEY=VALUE lines from TRENCHES_ENV_FILE if set.

    Deliberately not `.env` by default: CLAUDE.md 3.10.6 keeps key material off
    the filenames credential stealers grep for. Existing environment variables
    always win over the file.
    """
    path = os.environ.get("TRENCHES_ENV_FILE")
    if not path:
        return
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"TRENCHES_ENV_FILE points at {path!r}, which does not exist")
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def _get(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ConfigError(f"{name} is required and unset")
    return value


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc


@dataclass(frozen=True, slots=True)
class Config:
    dsn: str
    feeds: tuple[str, ...]
    feed: str
    filter_mode: str
    retention: str
    replay_path: Path
    pumpportal_url: str
    pumpportal_strict: bool
    rugcheck_base: str
    rugcheck_interval_seconds: float
    rugcheck_startup_max_age_seconds: float
    queue_maxsize: int
    workers: int
    dedupe_ttl_seconds: int
    slot_watchdog_seconds: float
    keepalive_seconds: int
    backoff_min_seconds: float
    backoff_max_seconds: float
    raw_retention_days: int
    rugcheck_base: str
    enrich_cache_ttl_seconds: int
    label_interval_seconds: float
    label_batch_limit: int
    label_fallback_budget: int
    label_dead_liquidity_usd: float
    label_peak_multiple: float
    decide_creator_min_mints: int
    decide_creator_max_rug_rate: float
    decide_min_liquidity_usd: float
    decide_hold_horizon_seconds: int
    log_level: str = field(default="INFO")

    @staticmethod
    def from_env() -> Config:
        _load_env_file()
        feeds = tuple(
            f.strip().lower()
            for f in _get("TRENCHES_FEEDS", "pumpportal,rugcheck").split(",")
            if f.strip()
        )
        if not feeds:
            raise ConfigError("TRENCHES_FEEDS is empty; name at least one feed")
        for f in feeds:
            if f not in VALID_FEEDS:
                raise ConfigError(f"unknown feed {f!r}; valid feeds are {VALID_FEEDS}")
        feed = feeds[0]

        filter_mode = _get("TRENCHES_FILTER_MODE", "both").lower()
        if filter_mode not in VALID_FILTER_MODES:
            raise ConfigError(
                f"TRENCHES_FILTER_MODE must be one of {VALID_FILTER_MODES}, got {filter_mode!r}"
            )

        retention = _get("TRENCHES_RETENTION", "creates_full").lower()
        if retention not in VALID_RETENTION:
            raise ConfigError(
                f"TRENCHES_RETENTION must be one of {VALID_RETENTION}, got {retention!r}"
            )

        cfg = Config(
            dsn=_get("TRENCHES_DSN", "sqlite://./trenches.db"),
            feeds=feeds,
            feed=feed,
            filter_mode=filter_mode,
            retention=retention,
            replay_path=Path(_get("TRENCHES_REPLAY_PATH", "./captures")),
            pumpportal_url=_get("TRENCHES_PUMPPORTAL_URL", "wss://pumpportal.fun/api/data"),
            pumpportal_strict=_get("TRENCHES_PUMPPORTAL_STRICT", "false").lower() == "true",
            rugcheck_interval_seconds=_get_float("TRENCHES_RUGCHECK_INTERVAL_SECONDS", 2.0),
            rugcheck_startup_max_age_seconds=_get_float(
                "TRENCHES_RUGCHECK_STARTUP_MAX_AGE_SECONDS", 600.0
            ),
            queue_maxsize=_get_int("TRENCHES_QUEUE_MAXSIZE", 10_000),
            workers=_get_int("TRENCHES_WORKERS", 4),
            dedupe_ttl_seconds=_get_int("TRENCHES_DEDUPE_TTL_SECONDS", 90),
            slot_watchdog_seconds=_get_float("TRENCHES_SLOT_WATCHDOG_SECONDS", 3.0),
            keepalive_seconds=_get_int("TRENCHES_KEEPALIVE_SECONDS", 30),
            backoff_min_seconds=_get_float("TRENCHES_BACKOFF_MIN_SECONDS", 1.0),
            backoff_max_seconds=_get_float("TRENCHES_BACKOFF_MAX_SECONDS", 30.0),
            raw_retention_days=_get_int("TRENCHES_RAW_RETENTION_DAYS", 7),
            rugcheck_base=_get("TRENCHES_RUGCHECK_BASE", "https://api.rugcheck.xyz/v1"),
            enrich_cache_ttl_seconds=_get_int("TRENCHES_ENRICH_CACHE_TTL_SECONDS", 300),
            label_interval_seconds=_get_float("TRENCHES_LABEL_INTERVAL_SECONDS", 60.0),
            label_batch_limit=_get_int("TRENCHES_LABEL_BATCH_LIMIT", 500),
            label_fallback_budget=_get_int("TRENCHES_LABEL_FALLBACK_BUDGET", 100),
            label_dead_liquidity_usd=_get_float("TRENCHES_LABEL_DEAD_LIQUIDITY_USD", 1000.0),
            label_peak_multiple=_get_float("TRENCHES_LABEL_PEAK_MULTIPLE", 2.0),
            decide_creator_min_mints=_get_int("TRENCHES_DECIDE_CREATOR_MIN_MINTS", 3),
            decide_creator_max_rug_rate=_get_float("TRENCHES_DECIDE_CREATOR_MAX_RUG_RATE", 0.6),
            # UNCALIBRATED (3.4 gives no liquidity number). Overridable from the
            # environment precisely so tuning it is a recorded, deliberate act --
            # `thresholds()` snapshots whatever it ends up being into every row.
            decide_min_liquidity_usd=_get_float("TRENCHES_DECIDE_MIN_LIQUIDITY_USD", 5000.0),
            decide_hold_horizon_seconds=_get_int("TRENCHES_DECIDE_HOLD_HORIZON_SECONDS", 6 * 3600),
            log_level=_get("TRENCHES_LOG_LEVEL", "INFO"),
        )
        if cfg.workers < 1:
            raise ConfigError("TRENCHES_WORKERS must be >= 1")
        if cfg.queue_maxsize < 1:
            raise ConfigError("TRENCHES_QUEUE_MAXSIZE must be >= 1")
        if cfg.backoff_min_seconds > cfg.backoff_max_seconds:
            raise ConfigError("TRENCHES_BACKOFF_MIN_SECONDS exceeds BACKOFF_MAX_SECONDS")
        return cfg

    def redacted(self) -> dict[str, object]:
        """Config safe to write into the journal. Never log the token."""
        out = {
            f: getattr(self, f)
            for f in self.__slots__
            if f not in ("yellowstone_token", "dsn")
        }
        out["dsn"] = "<set>"
        out["feeds"] = list(self.feeds)
        out["replay_path"] = str(self.replay_path)
        return out
