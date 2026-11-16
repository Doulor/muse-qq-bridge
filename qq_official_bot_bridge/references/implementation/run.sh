#!/usr/bin/env bash
# qq-bridge supervisor: keep exactly one daemon running.
#
# flock-guarded: a second run.sh (or a second supervisor) exits at once.
# When the daemon exits — crash, gateway wedge self-exit, OOM — it is
# relaunched after 3 seconds. This is the supervision layer that works
# everywhere, including VMs where the user systemd bus is unreachable;
# the watchdog hook only has to notice that this supervisor itself died.
set -u
cd "$(dirname "$0")"

exec 9> .supervisor.lock
if ! flock -n 9; then
  echo "[run.sh] another supervisor already holds the lock; exiting"
  exit 0
fi

mkdir -p logs
while true; do
  echo "[run.sh] $(date '+%Y-%m-%d %H:%M:%S %z') starting daemon"
  .venv/bin/python daemon.py >> logs/daemon-stdout.log 2>&1
  code=$?
  echo "[run.sh] $(date '+%Y-%m-%d %H:%M:%S %z') daemon exited with code $code; relaunching in 3s"
  sleep 3
done
