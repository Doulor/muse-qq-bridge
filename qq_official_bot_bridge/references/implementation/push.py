#!/usr/bin/env python3
"""Queue a proactive (non-reply) QQ message for the bridge daemon.

The daemon polls proactive/ every few seconds; this CLI just writes the
payload file atomically. Plain proactive messages need no msg_id and are
delivered as normal proactive sends. With --wakeup the message is sent
as an interaction recall (is_wakeup), which the platform budgets per
user in four periods over 30 days from the last interaction — the
daemon claims/releases that budget itself.

Usage:
  push.py "text" [--target OPENID] [--group GROUP_OPENID] [--wakeup]
          [--markdown]

Default target: the first entry of allow_openids in config.json.
"""
import argparse
import json
import os
import time
import uuid
from pathlib import Path

BASE = Path(os.environ.get("QQ_BRIDGE_HOME", Path(__file__).resolve().parent))


def main() -> int:
    ap = argparse.ArgumentParser(description="Queue a proactive QQ message")
    ap.add_argument("content", help="message text")
    ap.add_argument("--target", help="recipient user openid (C2C)")
    ap.add_argument("--group", help="recipient group openid")
    ap.add_argument("--wakeup", action="store_true",
                    help="send as interaction recall (is_wakeup, C2C only)")
    ap.add_argument("--markdown", action="store_true")
    args = ap.parse_args()

    if args.group:
        scene, target = "group", args.group
    else:
        scene = "c2c"
        target = args.target
        if not target:
            cfg = {}
            cfg_file = BASE / "config.json"
            if cfg_file.is_file():
                try:
                    cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
                except Exception:
                    cfg = {}
            allow = cfg.get("allow_openids") or []
            if not allow:
                ap.error("no --target given and config.json has no allow_openids")
            target = str(allow[0])

    payload = {
        "scene": scene,
        "target": target,
        "content": args.content,
        "markdown": args.markdown,
        "wakeup": bool(args.wakeup and scene == "c2c"),
    }
    proactive = BASE / "proactive"
    proactive.mkdir(parents=True, exist_ok=True)
    name = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}.json"
    tmp = proactive / f".{name}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, proactive / name)
    print(f"queued proactive message {name} -> {scene}/{target[:8]}...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
