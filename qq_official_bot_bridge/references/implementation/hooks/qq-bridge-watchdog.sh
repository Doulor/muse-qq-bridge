#!/usr/bin/env bash
# qq-bridge watchdog: keep the bridge daemon alive. Separate from the
# inbox hook on purpose — message delivery and process supervision fail
# in different ways and must not share a timeout budget.
# - No credentials file -> nothing to guard, stay silent.
# - Daemon running and heartbeat fresh -> silent.
# - Daemon down or heartbeat stale -> restart via run.sh (the flock
#   supervisor) when present, else launch the daemon directly; log to
#   the bridge's logs/watchdog.log; stay silent unless the restart fails
#   or restarts exceed 3 within an hour (restart loop) -> wake the agent.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE="${QQ_BRIDGE_HOME:-$HOME/workspace/qq-bridge}"
CREDS="$BRIDGE/credentials.env"
STATE_DIR="$HOME/hooks/state"
STATE_FILE="$STATE_DIR/qq-bridge-watchdog.restarts"
WATCHDOG_LOG="$BRIDGE/logs/watchdog.log"
PATTERN="qq-bridge.*daemon.py|daemon.py"
HEARTBEAT="$BRIDGE/state/heartbeat.json"
STALE_SECONDS=300
mkdir -p "$STATE_DIR" "$BRIDGE/logs"

if [ ! -f "$CREDS" ]; then
  silent "qq-bridge credentials.env absent; watchdog idle" '{}'
fi

daemon_alive() {
  pgrep -f "python.*daemon\.py" >/dev/null 2>&1
}

heartbeat_fresh() {
  [ -f "$HEARTBEAT" ] || return 0  # no heartbeat yet: give it the benefit
  local now hb
  now="$(date +%s)"
  hb="$(jq -r '.ts // 0' "$HEARTBEAT" 2>/dev/null || echo 0)"
  # heartbeat ts is epoch seconds (float ok for arithmetic via awk)
  awk -v now="$now" -v hb="$hb" -v stale="$STALE_SECONDS" \
    'BEGIN { exit !(now - hb < stale) }'
}

if daemon_alive && heartbeat_fresh; then
  silent "qq-bridge daemon is running" '{}'
fi

# Daemon is down or wedged (alive but no heartbeat: the liveness loop in
# the daemon normally exits by itself in this state; if it could not,
# kill it so the relaunch is clean).
if daemon_alive; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] daemon alive but heartbeat stale; killing wedged process" >> "$WATCHDOG_LOG"
  pkill -f "python.*daemon\.py" || true
  sleep 2
fi

now="$(date +%s)"
count_last_hour=0
if [ -f "$STATE_FILE" ]; then
  count_last_hour="$(awk -v cutoff="$((now - 3600))" '$1 >= cutoff' "$STATE_FILE" | wc -l)"
fi

if [ "${HATCH_HOOK_DRY_RUN:-0}" = "1" ]; then
  silent "dry run: qq-bridge daemon not healthy (would restart; $count_last_hour restart(s) in the last hour)" '{}'
fi

# Record this restart attempt (prune entries older than one hour first).
if [ -f "$STATE_FILE" ]; then
  awk -v cutoff="$((now - 3600))" '$1 >= cutoff' "$STATE_FILE" > "$STATE_FILE.tmp" || true
  mv "$STATE_FILE.tmp" "$STATE_FILE"
fi
echo "$now" >> "$STATE_FILE"
count_last_hour=$((count_last_hour + 1))

echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] daemon not healthy; restarting (attempt $count_last_hour in the last hour)" >> "$WATCHDOG_LOG"
if [ -x "$BRIDGE/run.sh" ]; then
  setsid bash -c '"$1" >> logs/supervisor.log 2>&1' _ "$BRIDGE/run.sh" < /dev/null & disown || true
else
  setsid bash -c 'cd "$1" && exec .venv/bin/python daemon.py >> logs/daemon-stdout.log 2>&1' _ "$BRIDGE" < /dev/null & disown || true
fi
sleep 5

if daemon_alive; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] restart succeeded" >> "$WATCHDOG_LOG"
  log "qq-bridge daemon restarted by watchdog" "{\"restarts_last_hour\": $count_last_hour}"
  if [ "$count_last_hour" -gt 3 ]; then
    wake "qq-bridge daemon is in a restart loop ($count_last_hour restarts in the last hour)" "{\"restarts_last_hour\": $count_last_hour}"
  fi
  silent "qq-bridge daemon restarted successfully" "{\"restarts_last_hour\": $count_last_hour}"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] restart FAILED: daemon still not running" >> "$WATCHDOG_LOG"
log "qq-bridge daemon restart failed" "{\"restarts_last_hour\": $count_last_hour}"
wake "qq-bridge daemon is down and the watchdog restart attempt failed" "{\"restarts_last_hour\": $count_last_hour}"
