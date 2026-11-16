#!/usr/bin/env bash
# Guest inbox watcher for the QQ bot framework (public mode).
#
# One-shot poll, same two-stage delivery as the owner inbox hook:
# commit the previous run's pending list into the seen file, then peek
# at new guest_inbox files, remember them as pending and wake the
# restricted guest worker with their paths.  A message is only marked
# seen after a run that actually woke the worker, so a crashed run
# re-delivers instead of stranding the message.
#
# Set QQ_BRIDGE_HOME to the bridge deployment directory.
set -u
source "$HATCH_HOOK_RUNTIME"

BASE="${QQ_BRIDGE_HOME:-$HOME/workspace/qq-bridge}"
GUEST_INBOX="$BASE/guest_inbox"
STATE_DIR="$HOME/hooks/state"
SEEN="$STATE_DIR/qq-guest-seen.txt"
PENDING="$STATE_DIR/qq-guest-pending.txt"

mkdir -p "$STATE_DIR"
touch "$SEEN"

# No guest inbox (private mode / bridge not deployed): stay silent.
test -d "$GUEST_INBOX" || silent "no guest inbox"

# Stage 2 of the previous run: its pending files were delivered.
if test -s "$PENDING"; then
    cat "$PENDING" >> "$SEEN"
    : > "$PENDING"
fi

# Peek: files not yet seen.
new_files=$(for f in "$GUEST_INBOX"/*.json; do
    test -e "$f" || continue
    name=$(basename "$f")
    case "$name" in .*) continue;; esac
    grep -qx "$name" "$SEEN" || echo "$f"
done)

if test -z "$new_files"; then
    silent "no new guest messages"
fi

# Stage 1 of this run: remember what we are about to deliver.
for f in $new_files; do basename "$f" >> "$PENDING"; done

payload=$(python3 -c '
import json, sys
files = sys.argv[1:]
print(json.dumps({"files": files}, ensure_ascii=False))
' $new_files)

echo "$payload"
wake "new guest QQ messages" "$payload"
