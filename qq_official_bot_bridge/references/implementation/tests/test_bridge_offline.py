#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline tests for the qq-bridge daemon.

No network, no credentials: the QQ API is replaced by a stub and inbound
events by fabricated InboundEvent objects. Run with the bridge venv:

    .venv/bin/python tests/test_bridge_offline.py

Exits 0 when every test passes, 1 otherwise. Each test prints one line.
"""
import asyncio
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Point the daemon at a scratch home BEFORE importing it (BASE is read at
# import time), and make the implementation dir importable.
SCRATCH = Path(tempfile.mkdtemp(prefix="qqbridge-test-"))
os.environ["QQ_BRIDGE_HOME"] = str(SCRATCH)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import daemon  # noqa: E402
from qqbot_agent_sdk import InboundEvent  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))


class StubAPI:
    """Records sends; can be told to fail the next N calls."""

    def __init__(self):
        self.sent = []          # list of dicts describing each send
        self.typing = []
        self.fail_next = 0
        self.fail_exc = RuntimeError("connection timeout")

    def _maybe_fail(self):
        if self.fail_next > 0:
            self.fail_next -= 1
            raise self.fail_exc

    async def post_c2c_message(self, user_id, msg, **kw):
        self._maybe_fail()
        self.sent.append({
            "route": "c2c", "target": user_id,
            "content": msg.content, "msg_id": msg.msg_id,
            "msg_seq": msg.msg_seq, "msg_type": msg.msg_type,
            "media": bool(msg.media),
            "markdown": bool(msg.markdown),
        })
        return {"id": "sent"}

    async def post_group_message(self, group_id, msg, **kw):
        self._maybe_fail()
        self.sent.append({
            "route": "group", "target": group_id,
            "content": msg.content, "msg_id": msg.msg_id,
            "msg_seq": msg.msg_seq, "msg_type": msg.msg_type,
            "media": bool(msg.media),
            "markdown": bool(msg.markdown),
        })
        return {"id": "sent"}

    async def post_guild_message(self, channel_id, msg, **kw):
        self._maybe_fail()
        self.sent.append({
            "route": "guild", "target": channel_id,
            "content": msg.content, "msg_id": msg.msg_id,
            "msg_seq": msg.msg_seq, "msg_type": msg.msg_type,
            "media": bool(msg.media),
            "markdown": bool(msg.markdown),
        })
        return {"id": "sent"}

    async def send_text(self, chat_type, chat_id, content, **kw):
        self._maybe_fail()
        self.sent.append({"route": f"text:{chat_type}", "target": chat_id,
                          "content": content, "msg_id": kw.get("reply_to")})
        return {"id": "sent"}

    async def send_typing(self, chat_id, msg_id, **kw):
        self.typing.append((chat_id, msg_id))

    async def request(self, route, body=None, **kw):
        self._maybe_fail()
        self.sent.append({"route": "request", "target": str(route), "body": body})
        return {"id": "sent"}

    async def acknowledge_interaction(self, interaction_id, code=0, **kw):
        self.sent.append({"route": "ack", "target": interaction_id})


class StubUploader:
    async def upload(self, scene, chat_id, path, media_type, filename):
        return f"fileinfo:{filename}"


def make_bridge(cfg_overrides=None):
    cfg = dict(daemon.DEFAULT_CONFIG)
    cfg.update({
        "allow_openids": ["OWNER1"],
        "reply_window_seconds": 300,
        "max_replies_per_message": 4,
        "proactive_retries": 3,
        "proactive_retry_delay_seconds": 0,
        "typing": True,
    })
    if cfg_overrides:
        cfg.update(cfg_overrides)
    api = StubAPI()
    bridge = daemon.Bridge("1234567890", api, cfg, http_client=None)
    bridge.uploader = StubUploader()
    return bridge, api


def fresh_dirs():
    for d in (daemon.INBOX, daemon.OUTBOX, daemon.PROACTIVE,
              daemon.DONE, daemon.STATE, daemon.EVENTS):
        d.mkdir(parents=True, exist_ok=True)
        for p in d.glob("*"):
            if p.is_file():
                p.unlink()


def fake_event(msg_id="M1", sender="OWNER1", content="hello", scope="c2c",
               chat_id="OWNER1", attachments=None, event_type="C2C_MESSAGE_CREATE"):
    return InboundEvent(
        event_type=event_type, chat_id=chat_id, user_id=sender,
        chat_scope=scope, content=content, message_id=msg_id,
        timestamp="", message_type=0, attachments=attachments or [],
        msg_elements=None, raw={},
    )


def patch_parser(bridge, event):
    bridge.parser = type("P", (), {"parse": staticmethod(lambda et, raw: event)})()


def write_inbox(msg_id, age_seconds=0, scene="c2c", chat_id="OWNER1"):
    rec = {
        "msg_id": msg_id, "scene": scene, "event_type": "C2C_MESSAGE_CREATE",
        "user_openid": "OWNER1", "group_openid": None, "channel_id": None,
        "chat_id": chat_id, "content": "hi", "message_type": 0,
        "attachments": [], "attachment_only": False,
        "received_at": (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).isoformat(),
    }
    (daemon.INBOX / f"{msg_id}.json").write_text(json.dumps(rec), encoding="utf-8")


def write_outbox(msg_id, payload):
    (daemon.OUTBOX / f"{msg_id}.json").write_text(json.dumps(payload), encoding="utf-8")


def marker(msg_id):
    for p in daemon.DONE.glob(f"{msg_id}.*"):
        if p.suffix.lstrip(".") in ("SENT", "EXPIRED", "INVALID", "ORPHAN", "FAILED", "PARTIAL"):
            return p.suffix.lstrip(".")
    return None


async def t_inbound_allowlist():
    fresh_dirs()
    bridge, api = make_bridge()
    patch_parser(bridge, fake_event(sender="STRANGER"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    check("inbound: stranger C2C dropped", not (daemon.INBOX / "M1.json").exists())
    patch_parser(bridge, fake_event(sender="OWNER1"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    await asyncio.sleep(0.05)  # typing indicator is fired as a task
    check("inbound: owner C2C inboxed", (daemon.INBOX / "M1.json").exists())
    check("inbound: typing sent for owner", api.typing == [("OWNER1", "M1")])
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    files = list(daemon.INBOX.glob("*.json"))
    check("inbound: duplicate delivery deduped", len(files) == 1)


async def t_binding_mode():
    fresh_dirs()
    bridge, api = make_bridge({"allow_openids": []})
    patch_parser(bridge, fake_event(sender="FIRST1", chat_id="FIRST1"))
    await bridge.on_message_event("C2C_MESSAGE_CREATE", {"author": {}})
    pending = daemon.STATE / "pending.json"
    check("binding: first sender recorded", pending.exists()
          and json.loads(pending.read_text())["openid"] == "FIRST1")
    check("binding: message not inboxed", not (daemon.INBOX / "M1.json").exists())
    notices = [s for s in api.sent if s["route"] == "text:c2c"]
    check("binding: not-bound notice sent once", len(notices) == 1)


async def t_split_replies_and_seqs():
    fresh_dirs()
    bridge, api = make_bridge()
    write_inbox("M2")
    write_outbox("M2", {"replies": ["one", "two", "three"]})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M2.json")
    texts = [s for s in api.sent if s["route"] == "c2c"]
    check("split: 3 parts sent", len(texts) == 3)
    check("split: all quote the source msg_id", all(s["msg_id"] == "M2" for s in texts))
    seqs = [s["msg_seq"] for s in texts]
    check("split: msg_seq all distinct", len(set(seqs)) == 3, f"seqs={seqs}")
    check("split: archived SENT", marker("M2") == "SENT")


async def t_seq_collision_bump():
    fresh_dirs()
    bridge, api = make_bridge()
    orig = daemon.QQApiClient.next_msg_seq
    daemon.QQApiClient.next_msg_seq = staticmethod(lambda seed="": 777)
    try:
        write_inbox("M3")
        write_outbox("M3", {"replies": ["a", "b", "c"]})
        await bridge._handle_outbox_file(daemon.OUTBOX / "M3.json")
    finally:
        daemon.QQApiClient.next_msg_seq = orig
    seqs = [s["msg_seq"] for s in api.sent]
    check("seq: constant generator still yields distinct seqs",
          seqs == [777, 778, 779], f"seqs={seqs}")


async def t_budget_partial():
    fresh_dirs()
    bridge, api = make_bridge({"max_replies_per_message": 2})
    bridge.ledger = daemon.ReplyLedger(daemon.STATE, 2)
    write_inbox("M4")
    write_outbox("M4", {"replies": ["a", "b", "c"]})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M4.json")
    check("budget: only 2 parts sent", len(api.sent) == 2)
    check("budget: archived PARTIAL", marker("M4") == "PARTIAL")


async def t_expiry():
    fresh_dirs()
    bridge, api = make_bridge()
    write_inbox("M5", age_seconds=400)
    write_outbox("M5", {"reply": "too late"})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M5.json")
    check("expiry: nothing sent", len(api.sent) == 0)
    check("expiry: archived EXPIRED", marker("M5") == "EXPIRED")


async def t_transient_retry_new_seq():
    fresh_dirs()
    bridge, api = make_bridge()
    api.fail_next = 1
    api.fail_exc = RuntimeError("connection timeout")
    write_inbox("M6")
    write_outbox("M6", {"reply": "hello"})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M6.json")
    check("retry: first attempt failed, file kept", (daemon.OUTBOX / "M6.json").exists())
    await bridge._handle_outbox_file(daemon.OUTBOX / "M6.json")
    check("retry: second attempt sent", len(api.sent) == 1)
    check("retry: archived SENT", marker("M6") == "SENT")


async def t_fatal_no_retry():
    fresh_dirs()
    bridge, api = make_bridge()
    api.fail_next = 99
    api.fail_exc = RuntimeError("400 invalid msg_id")
    write_inbox("M7")
    write_outbox("M7", {"reply": "hello"})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M7.json")
    check("fatal: archived FAILED immediately", marker("M7") == "FAILED")
    check("fatal: outbox file consumed", not (daemon.OUTBOX / "M7.json").exists())


async def t_invalid_media_and_orphan():
    fresh_dirs()
    bridge, api = make_bridge()
    write_inbox("M8")
    write_outbox("M8", {"reply": "x", "media": [{"kind": "image", "path": "/nonexistent/nope.png"}]})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M8.json")
    check("media: missing file -> INVALID, nothing sent",
          marker("M8") == "INVALID" and len(api.sent) == 0)
    write_outbox("M9", {"reply": "orphan"})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M9.json")
    check("orphan: outbox without inbox -> ORPHAN", marker("M9") == "ORPHAN")


async def t_media_send():
    fresh_dirs()
    bridge, api = make_bridge()
    img = SCRATCH / "pic.png"
    img.write_bytes(b"\x89PNG")
    write_inbox("M10")
    write_outbox("M10", {"reply": "look", "media": [{"kind": "image", "path": str(img)}]})
    await bridge._handle_outbox_file(daemon.OUTBOX / "M10.json")
    media_sends = [s for s in api.sent if s["media"]]
    check("media: one rich-media send with text riding along",
          len(media_sends) == 1 and media_sends[0]["content"] == "look")
    check("media: archived SENT", marker("M10") == "SENT")


async def t_proactive():
    fresh_dirs()
    bridge, api = make_bridge()
    p = daemon.PROACTIVE / "p1.json"
    p.write_text(json.dumps({"scene": "c2c", "target": "OWNER1", "content": "morning"}), encoding="utf-8")
    await bridge._handle_proactive_file(p)
    check("proactive: sent", len(api.sent) == 1 and api.sent[0]["content"] == "morning")
    check("proactive: archived SENT", (daemon.DONE / "p1.SENT").exists())

    api.fail_next = 99
    api.fail_exc = RuntimeError("connection timeout")
    p2 = daemon.PROACTIVE / "p2.json"
    p2.write_text(json.dumps({"scene": "c2c", "target": "OWNER1", "content": "again"}), encoding="utf-8")
    for _ in range(3):
        if p2.exists():
            await bridge._handle_proactive_file(p2)
    archived = daemon.DONE / "p2.proactive.json"
    check("proactive: retries exhausted -> FAILED with error recorded",
          (daemon.DONE / "p2.FAILED").exists() and archived.exists()
          and "timeout" in json.loads(archived.read_text()).get("_error", ""))


def t_wakeup_periods():
    day = 86400.0
    now = 100 * day
    check("wakeup: day 0 -> period 0", daemon.wakeup_period(now, now + 0.5 * day) == 0)
    check("wakeup: day 2 -> period 1", daemon.wakeup_period(now, now + 2 * day) == 1)
    check("wakeup: day 5 -> period 2", daemon.wakeup_period(now, now + 5 * day) == 2)
    check("wakeup: day 10 -> period 3", daemon.wakeup_period(now, now + 10 * day) == 3)
    check("wakeup: day 31 -> None", daemon.wakeup_period(now, now + 31 * day) is None)


async def run_all():
    await t_inbound_allowlist()
    await t_binding_mode()
    await t_split_replies_and_seqs()
    await t_seq_collision_bump()
    await t_budget_partial()
    await t_expiry()
    await t_transient_retry_new_seq()
    await t_fatal_no_retry()
    await t_invalid_media_and_orphan()
    await t_media_send()
    await t_proactive()
    t_wakeup_periods()


def main():
    asyncio.run(run_all())
    failed = [n for n, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    if failed:
        print("failed:", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
