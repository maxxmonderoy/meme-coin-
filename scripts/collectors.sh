#!/usr/bin/env bash
#
# Start, stop and check the three collector processes.
#
# WHY THIS EXISTS. The collectors are three separate long-running processes by
# design (3.1: an entry-side stall must never stop an exit loop), which means
# three terminals, three chances to forget one, and three ways to lose a week of
# collection to a closed laptop lid. Outcome data is time-irreversible -- nobody
# stores what happened to the tokens launching today -- so a week not collected
# is a week that cannot be recovered later.
#
# It calls .venv/bin/trenches DIRECTLY rather than relying on an activated
# shell. `command not found: trenches` is what happens when the venv is not
# active, and it is not worth hitting twice.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TRENCHES="$ROOT/.venv/bin/trenches"
RUN_DIR="$ROOT/.run"
LOG_DIR="$ROOT/logs"

# `exits` is included even though nothing opens paper positions unless --paper
# is on: it costs nothing idle, and discovering it was off is worse than the
# handful of log lines it writes.
PROCS=(stream sample exits)

stream_args=()
sample_args=()
exits_args=()

usage() {
    cat <<'USAGE'
usage: scripts/collectors.sh {start|stop|status|logs|restart}

  start     launch stream, sample and exits in the background
  stop      stop all three
  status    which are alive, how long, and the last line each wrote
  logs      follow all three logs at once (ctrl-C to stop watching;
            this does NOT stop the collectors)
  restart   stop then start

Logs are written to logs/<name>.log and survive closing the terminal.
USAGE
}

require_venv() {
    if [ ! -x "$TRENCHES" ]; then
        echo "error: $TRENCHES is missing or not executable." >&2
        echo >&2
        echo "Install it first, from inside the repository directory:" >&2
        echo >&2
        echo "  python3 -m venv .venv" >&2
        echo "  .venv/bin/pip install -r requirements.txt" >&2
        echo "  .venv/bin/pip install -e . --no-deps" >&2
        echo >&2
        exit 1
    fi
}

load_env() {
    if [ -f "$ROOT/trenches.local.conf" ]; then
        export TRENCHES_ENV_FILE="$ROOT/trenches.local.conf"
    elif [ -z "${TRENCHES_ENV_FILE:-}" ]; then
        echo "note: no trenches.local.conf found; using built-in defaults." >&2
        echo "      cp .env.example trenches.local.conf to change anything." >&2
    fi
}

pid_of() {
    local name="$1" file="$RUN_DIR/$name.pid"
    [ -f "$file" ] || return 1
    local pid
    pid="$(cat "$file" 2>/dev/null || true)"
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    echo "$pid"
}

start_one() {
    local name="$1"
    if pid_of "$name" >/dev/null; then
        echo "  $name already running (pid $(pid_of "$name"))"
        return 0
    fi
    local -n extra="${name}_args"
    # Line-buffered so `logs` shows output as it happens rather than in 4KB
    # bursts -- a collector that looks silent for ten minutes is one you stop
    # and debug for no reason.
    PYTHONUNBUFFERED=1 nohup "$TRENCHES" "$name" "${extra[@]+"${extra[@]}"}" \
        >>"$LOG_DIR/$name.log" 2>&1 &
    echo $! > "$RUN_DIR/$name.pid"
    sleep 1
    if pid_of "$name" >/dev/null; then
        echo "  $name started (pid $(pid_of "$name"))"
    else
        echo "  $name FAILED to start -- last lines of logs/$name.log:" >&2
        tail -n 15 "$LOG_DIR/$name.log" >&2 || true
        return 1
    fi
}

cmd_start() {
    require_venv
    load_env
    mkdir -p "$RUN_DIR" "$LOG_DIR"
    echo "starting collectors:"
    local failed=0
    for name in "${PROCS[@]}"; do
        start_one "$name" || failed=1
    done
    echo
    echo "  logs:   scripts/collectors.sh logs"
    echo "  check:  scripts/collectors.sh status"
    echo "  data:   .venv/bin/trenches stats --hours 24"
    echo
    if [ "$(uname -s)" = "Darwin" ]; then
        echo "  ON A MAC, CLOSING THE LID STILL SLEEPS THE MACHINE and the"
        echo "  collectors stop with it. To leave this running for a week, keep it"
        echo "  plugged in and run, in a terminal you can leave open:"
        echo
        echo "      caffeinate -dimsu"
        echo
    fi
    return $failed
}

cmd_stop() {
    mkdir -p "$RUN_DIR"
    echo "stopping collectors:"
    for name in "${PROCS[@]}"; do
        local pid
        if pid="$(pid_of "$name")"; then
            kill "$pid" 2>/dev/null || true
            for _ in $(seq 1 20); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.5
            done
            if kill -0 "$pid" 2>/dev/null; then
                # Clean shutdown is a documented path (3.8); if it did not take,
                # say so rather than killing quietly.
                echo "  $name did not stop cleanly, sending SIGKILL" >&2
                kill -9 "$pid" 2>/dev/null || true
            fi
            echo "  $name stopped"
        else
            echo "  $name not running"
        fi
        rm -f "$RUN_DIR/$name.pid"
    done
}

cmd_status() {
    local all_up=0
    printf '%-10s %-10s %-10s %s\n' NAME STATE PID "LAST LINE"
    for name in "${PROCS[@]}"; do
        local pid last=""
        [ -f "$LOG_DIR/$name.log" ] && last="$(tail -n 1 "$LOG_DIR/$name.log" 2>/dev/null | cut -c1-70)"
        if pid="$(pid_of "$name")"; then
            printf '%-10s %-10s %-10s %s\n' "$name" "UP" "$pid" "$last"
        else
            printf '%-10s %-10s %-10s %s\n' "$name" "DOWN" "-" "$last"
            all_up=1
        fi
    done
    echo
    if [ "$all_up" -ne 0 ]; then
        echo "  Something is DOWN. Its last log line above is usually the reason;"
        echo "  the full log is in logs/. Restart with:"
        echo "      scripts/collectors.sh start"
    else
        echo "  All three up. Check what they have collected with:"
        echo "      .venv/bin/trenches stats --hours 24"
    fi
    return $all_up
}

cmd_logs() {
    mkdir -p "$LOG_DIR"
    for name in "${PROCS[@]}"; do touch "$LOG_DIR/$name.log"; done
    echo "following logs/. ctrl-C stops watching, NOT the collectors."
    echo
    tail -n 20 -f "$LOG_DIR"/*.log
}

case "${1:-}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    restart) cmd_stop; echo; cmd_start ;;
    *)       usage; exit 2 ;;
esac
