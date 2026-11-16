#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline tests for the framework v3 additions: access tiers, guests,
config hot reload, the actions runner and the admin API.

Same style as test_bridge_offline.py: stub API, fabricated events, a
scratch QQ_BRIDGE_HOME.  Run with the bridge venv:

    .venv/bin/python tests/test_framework_offline.py
"""
import asyncio
import json
import os
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SCRATCH = Path(tempfile.mkdtemp(prefix="qqbridge-fw-test-"))
os.environ["QQ_BRIDGE_HOME"] = str(SCRATCH)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import daemon  # noqa: E402
from guests import GuestStore  # noqa: E402
from admin import AdminFacade, AdminServer  # noqa: E402
from qqbot_agent_sdk import InboundEvent  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))


class StubAPI:
    def __init__(self):
        self.sent = []
        self.typing = []
        self.requests = []

    async def post_c2c_message(self, user_id, msg, **kw):
        self.sent.append({"route": "c2c", "target": user_id, "content": msg.content,
                          "msg_id": msg.msg_id, "msg_seq": msg.msg_seq})
        return {"id": "sent"}

    async def post_group_message(self, group_id, msg, **kw):
        self.sent.append({"route": "group", "target": group_id, "content": msg.content})
        return {"id": "sent"}

    async def post_guild_message(self, channel_id, msg, **kw):
        self.sent.append({"route": "guild", "target": channel_id, "content": msg.content})
        return {"id": "sent"}

    async def send_text(self, chat_type, chat_id, content, **kw):
        self.sent.append({"route": f"text:{chat_type}", "target": chat_id,
                          "content": content, "msg_id": kw.get("reply_to")})
        return {"id": "sent"}

    async def send_typing(self, chat_id, msg_id, **kw):
        self.typing.append((chat_id, msg_id))

    async def request(self, method, path, body=None, **kw):
        self.requests.append((method, path))
        return {"ok": True, "path": path}

    async def acknowledge_interaction(self, interaction_id, code=0, **kw):
        self.sent.append({"route": "ack", "target": interaction_id})


def make_bridge(cfg_overrides=None):
    cfg = dict(daemon.DEFAULT_CONFIG)
    cfg.update({
        "allow_openids": ["OWNER1"],
        "reply_window_seconds": 300,
        "max_replies_per_message": 4,
        "proactive_retries": 1,
        "proactive_retry_delay_seconds": 0,
        "typing": True,
    })
    if cfg_overrides:
        cfg.update(cfg_overrides)
    api = StubAPI()
    bridge = daemon.Bridge("1234567890", api, cfg, http_client=None)
    return bridge, api


def fresh_dirs():
    for d in (daemon.INBOX, daemon.OUTBOX, daemon.PROACTIVE, daemon.DONE,
              daemon.STATE, daemon.EVENTS, daemon.GUEST_INBOX, daemon.ACTIONS):
        d.mkdir(parents=True, exist_ok=True)
        for p in d.glob("*"):
            if p.is_file():
                p.unlink()
    for p in daemon.GUESTS_DIR.glob("*"):
        if p.is_file():
            p.unlink()


def fake_event(msg_id="M1", sender="GUEST1", content="hi there", scope="c2c",
               chat_id=None, event_type="C2C_MESSAGE_CREATE"):
    return InboundEvent(
        event_type=event_type, chat_id=chat_id or sender, user_id=sender,
        chat_scope=scope, content=content, message_id=msg_id,
        timestamp="", message_type=0, attachments=[], msg_elements=None, raw={},
    )


def patch_parser(bridge, event):
    bridge.parser = type("P", (), {"parse": staticmethod(lambda et, raw: event)})()


async def t_guest_routing_public():
    fresh_dirs()
    bridge, api = make_bridge({"access_mode": "public"})
    patch_parser(bridge, fake_event(sender="GUEST1"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    rec_file = daemon.GUEST_INBOX / "M1.json"
    check("guest: public stranger lands in guest_inbox", rec_file.exists())
    if rec_file.exists():
        rec = json.loads(rec_file.read_text())
        check("guest: record tier is guest", rec.get("tier") == "guest")
    check("guest: owner inbox untouched", not (daemon.INBOX / "M1.json").exists())
    hist = bridge.guests.history_path("GUEST1")
    check("guest: history has the user turn",
          hist.exists() and '"role": "user"' in hist.read_text())
    patch_parser(bridge, fake_event(msg_id="M2", sender="OWNER1", chat_id="OWNER1"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    check("guest: owner still lands in owner inbox", (daemon.INBOX / "M2.json").exists())


async def t_guest_routing_private_and_block():
    fresh_dirs()
    bridge, api = make_bridge({"access_mode": "private"})
    patch_parser(bridge, fake_event(sender="GUEST2"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    check("private: stranger dropped everywhere",
          not (daemon.GUEST_INBOX / "M1.json").exists()
          and not (daemon.INBOX / "M1.json").exists())
    bridge2, _ = make_bridge({"access_mode": "public", "block_openids": ["BAD1"]})
    patch_parser(bridge2, fake_event(sender="BAD1"))
    await bridge2.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    check("public: blocklisted sender dropped",
          not (daemon.GUEST_INBOX / "M1.json").exists())


async def t_guest_rate_limits():
    store = GuestStore(SCRATCH / "guests-unit")
    ok1, _ = store.check_and_count("U1", daily_limit=2, min_interval=0)
    ok2, _ = store.check_and_count("U1", daily_limit=2, min_interval=0)
    ok3, reason3 = store.check_and_count("U1", daily_limit=2, min_interval=0)
    check("ratelimit: first two pass", ok1 and ok2)
    check("ratelimit: third hits daily limit", not ok3 and reason3 == "daily_limit")
    check("ratelimit: notice allowed once", store.should_notice_limit("U1"))
    store.note_limit_notice("U1")
    check("ratelimit: notice suppressed after send", not store.should_notice_limit("U1"))
    okf, reasonf = store.check_and_count("U2", daily_limit=10, min_interval=3600)
    okf2, reasonf2 = store.check_and_count("U2", daily_limit=10, min_interval=3600)
    check("ratelimit: min interval enforced", okf and not okf2 and reasonf2 == "too_fast")


async def t_guest_reply_flow():
    fresh_dirs()
    bridge, api = make_bridge({"access_mode": "public"})
    patch_parser(bridge, fake_event(msg_id="G1", sender="GUEST9", content="hello bot"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    (daemon.OUTBOX / "G1.json").write_text(
        json.dumps({"replies": ["hi human", "second part"]}), encoding="utf-8")
    await bridge._handle_outbox_file(daemon.OUTBOX / "G1.json")
    markers = list(daemon.DONE.glob("G1.SENT"))
    check("guest reply: archived SENT", len(markers) == 1)
    check("guest reply: both parts sent",
          len([s for s in api.sent if s["route"] == "c2c"]) == 2)
    hist = bridge.guests.history_path("GUEST9").read_text()
    check("guest reply: assistant turns in history",
          '"role": "assistant"' in hist and "second part" in hist)
    check("guest reply: record left guest_inbox",
          not (daemon.GUEST_INBOX / "G1.json").exists()
          and (daemon.DONE / "G1.json").exists())


def t_sanitize_config():
    cfg, errors = daemon.sanitize_config({"access_mode": "public", "guest_daily_limit": 5})
    check("sanitize: valid patch merges", not errors and cfg["access_mode"] == "public"
          and cfg["guest_daily_limit"] == 5)
    _, errors = daemon.sanitize_config({"access_mode": "bogus"})
    check("sanitize: bad access_mode rejected", bool(errors))
    _, errors = daemon.sanitize_config({"nope_key": 1})
    check("sanitize: unknown key rejected", bool(errors))
    _, errors = daemon.sanitize_config({"allow_openids": "notalist"})
    check("sanitize: bad list rejected", bool(errors))


async def t_reload_config():
    fresh_dirs()
    bridge, _ = make_bridge()
    cfg_path = daemon.CONFIG_FILE
    new_cfg = dict(bridge.cfg)
    new_cfg["guest_daily_limit"] = 7
    cfg_path.write_text(json.dumps(new_cfg), encoding="utf-8")
    # force a different mtime than the one captured at Bridge init
    os.utime(cfg_path, (1_900_000_000, 1_900_000_000))
    changed = bridge.reload_config()
    check("reload: changed config swapped in", changed and bridge.cfg["guest_daily_limit"] == 7)
    bad = dict(bridge.cfg)
    bad["access_mode"] = "bogus"
    cfg_path.write_text(json.dumps(bad), encoding="utf-8")
    os.utime(cfg_path, (1_900_000_100, 1_900_000_100))
    changed2 = bridge.reload_config()
    check("reload: invalid config rejected, old kept",
          not changed2 and bridge.cfg["access_mode"] == "private")
    cfg_path.unlink()


async def t_actions():
    fresh_dirs()
    bridge, api = make_bridge()
    (daemon.ACTIONS / "a1.json").write_text(
        json.dumps({"action": "get_bot_info", "args": {}}), encoding="utf-8")
    await bridge._handle_action_file(daemon.ACTIONS / "a1.json")
    result_file = daemon.DONE / "a1.result.json"
    check("actions: result written", result_file.exists())
    if result_file.exists():
        result = json.loads(result_file.read_text())
        check("actions: get_bot_info ok via qqapi",
              result.get("ok") is True and ("GET", "/users/@me") in api.requests)
    check("actions: file consumed", not (daemon.ACTIONS / "a1.json").exists())
    (daemon.ACTIONS / "a2.json").write_text(
        json.dumps({"action": "rm_rf_everything", "args": {}}), encoding="utf-8")
    await bridge._handle_action_file(daemon.ACTIONS / "a2.json")
    result2 = json.loads((daemon.DONE / "a2.result.json").read_text())
    check("actions: unknown action refused", result2.get("ok") is False)


def t_admin_http():
    calls = {"push": [], "put": []}

    def put_config(patch):
        merged, errors = daemon.sanitize_config(patch, dict(daemon.DEFAULT_CONFIG))
        if errors:
            return False, "; ".join(errors)
        calls["put"].append(patch)
        return True, "saved"

    def queue_push(payload):
        calls["push"].append(payload)
        return True, "queued"

    facade = AdminFacade(
        status=lambda: {"pid": 1, "access_mode": "private"},
        get_config=lambda: dict(daemon.DEFAULT_CONFIG),
        put_config=put_config,
        guests=lambda: [],
        events_tail=lambda n: [],
        queue_push=queue_push,
    )
    server = AdminServer(18799, "test-token", facade)
    server.start()
    try:
        def call(method, path, body=None, token=None):
            req = urllib.request.Request(
                f"http://127.0.0.1:18799{path}", method=method,
                data=json.dumps(body).encode() if body is not None else None,
                headers={"Content-Type": "application/json",
                         **({"X-Admin-Token": token} if token else {})})
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as e:
                return e.code, json.loads(e.read())

        code, _ = call("GET", "/api/status")
        check("admin: no token -> 401", code == 401)
        code, body = call("GET", "/api/status", token="test-token")
        check("admin: status ok with token", code == 200 and body.get("pid") == 1)
        code, _ = call("PUT", "/api/config", {"guest_daily_limit": 9}, token="test-token")
        check("admin: valid config put ok", code == 200 and calls["put"])
        code, _ = call("PUT", "/api/config", {"evil_key": 1}, token="test-token")
        check("admin: unknown key -> 400", code == 400)
        code, _ = call("POST", "/api/push",
                       {"scene": "c2c", "target": "X", "content": "hi"}, token="test-token")
        check("admin: push accepted", code == 200 and calls["push"])
    finally:
        server.stop()


async def main():
    await t_guest_routing_public()
    await t_guest_routing_private_and_block()
    await t_guest_rate_limits()
    await t_guest_reply_flow()
    t_sanitize_config()
    await t_reload_config()
    await t_actions()
    t_admin_http()
    failed = [n for n, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
