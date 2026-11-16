#!/usr/bin/env bash
# qq-bridge inbox wake hook: one-shot poll of the bridge inbox.
#
# Two-stage delivery (peek -> wake -> commit), so a message can never be
# marked seen before the agent has actually been woken for it:
#   1. At start, commit the previous run's pending list into the seen file
#      (that wake was emitted, so those files are handed off).
#   2. Peek: files in inbox/ that are in neither seen nor pending are new.
#   3. Write them to the pending file and wake with their names as payload.
# Worst case after a crash between wake and the next run is a duplicate
# wake; the agent dedupes by msg_id. The daemon archives each inbox file
# to done/ once its reply is sent, so the seen list stays small in
# practice; it is pruned to entries whose files still exist anywhere in
# the bridge tree.
#
# Suggested hook settings: poll_interval_secs 5-10. The script itself is
# a single quick pass — no internal watch loop, nothing to time out.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE="${QQ_BRIDGE_HOME:-$HOME/workspace/qq-bridge}"
INBOX="$BRIDGE/inbox"
STATE_DIR="$HOME/hooks/state"
SEEN_FILE="$STATE_DIR/qq-bridge-inbox.seen"
PENDING_FILE="$STATE_DIR/qq-bridge-inbox.pending"
mkdir -p "$STATE_DIR"
touch "$SEEN_FILE"

# Stage 1: commit the previous run's pending wake, if any.
if [ -f "$PENDING_FILE" ]; then
  if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then
    cat "$PENDING_FILE" >> "$SEEN_FILE"
    rm -f "$PENDING_FILE"
  fi
fi

# Peek: collect inbox files not yet committed as seen.
new_files=()
shopt -s nullglob
for f in "$INBOX"/*.json; do
  base="$(basename "$f")"
  case "$base" in .*) continue ;; esac
  if ! grep -qxF -- "$base" "$SEEN_FILE"; then
    new_files+=("$base")
  fi
done

if [ "${#new_files[@]}" -eq 0 ]; then
  silent "qq-bridge inbox has no new messages" '{}'
fi

# Stage 2: record as pending (not seen) and wake.
if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then
  printf '%s\n' "${new_files[@]}" > "$PENDING_FILE"
fi
payload="$(printf '%s\n' "${new_files[@]}" | jq -R . | jq -s '{files: .}')"
log "new QQ inbox message file(s) detected" "$payload"
wake "new QQ message(s) waiting in the qq-bridge inbox" "$payload"
