#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""qq-bridge daemon: official QQ Bot <-> file inbox/outbox <-> Muse agent.

Design (merged from two independent Muse QQ-bridge builds, Nov 2026):

Inbound : QQ messages received through Tencent's official qqbot-agent-sdk
          (WebSocket Gateway + OpenAPI v2) are written as one JSON file per
          message to inbox/.  Covered scenes: C2C private chat, group @
          messages, group full messages (GROUP_MESSAGE_CREATE, opt-in),
          guild (channel) messages and channel DMs.  Attachments (image /
          video / file / voice) are recorded as metadata; voice attachments
          are run through the SDK's STT pipeline (which falls back to the
          platform-provided ``asr_refer_text``) and the transcript is stored
          as ``asr_text``.  Interaction and lifecycle events are acknowledged
          (interactions) and appended to events/events.jsonl; they never
          enter the inbox.

          An owner allowlist is enforced in the daemon: C2C messages from
          anyone not in ``allow_openids`` never reach the agent.  With an
          empty allowlist the bridge runs in binding mode — the first
          sender is recorded in state/pending.json and told the bot is not
          bound yet; their message content is not processed.

Outbound: a watcher polls outbox/ every ~3s.  outbox/<msg_id>.json carries
          ``{"reply": "..."}`` or ``{"replies": ["...", "..."]}`` (split
          replies: each part is sent as its own passive reply quoting the
          original msg_id), optionally ``"markdown": true`` and/or
          ``"media": [{"kind": ..., "path": ...}]`` (rich media, msg_type
          7, C2C/group only).  A second watcher polls proactive/ for
          proactive (non-reply) messages, with bounded retries.

Safety rails, each learned from a real incident:
  * Passive-reply window: a reply older than ``reply_window_seconds``
    (config.json, default 300) is archived EXPIRED and never sent, instead
    of burning attempts on a request the platform will reject.
  * Per-message reply budget (default 4, the platform cap): split parts
    beyond the budget are not sent.
  * msg_seq values come from the SDK, but are de-duplicated per msg_id in
    a persistent registry: the SDK's generator is pseudo-random over
    0-65535, and a repeated (msg_id, msg_seq) pair is silently dropped by
    the platform as a duplicate.  A failed send never reuses its seq.
  * Fatal platform errors (per the SDK's is_fatal_send_error) archive the
    reply as FAILED at once; transient errors retry until the window
    closes.
  * Gateway liveness: if no heartbeat ACK / connect / ready arrives for
    ``liveness_timeout_seconds`` (default 300), the process exits hard and
    lets the supervisor relaunch it — the SDK's reconnect can wedge
    silently while the process still looks alive.
  * A heartbeat file (state/heartbeat.json) is refreshed every
    ``heartbeat_seconds`` for external watchdogs.

Credentials come from the environment variables QQBOT_APP_ID and
QQBOT_APP_SECRET, optionally loaded from credentials.env in the bridge
directory.  Behaviour options live in config.json next to this file (see
config.example.json).  The secret is never written to any log.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# httpx chokes on bracketed IPv6 entries in NO_PROXY (e.g. "[::1]") when it
# builds proxy mounts; the QQ endpoints go through the egress proxy anyway,
# so trim NO_PROXY down to plain hostnames/IPv4 entries for this process.
for _key in ("NO_PROXY", "no_proxy"):
    _raw = os.environ.get(_key)
    if _raw:
        _kept = [p.strip() for p in _raw.split(",") if p.strip() and ":" not in p and "[" not in p]
        os.environ[_key] = ",".join(_kept) if _kept else "localhost"

import httpx
from qqbot_agent_sdk import (
    EventParser,
    InboundEvent,
    MAX_MESSAGE_LENGTH,
    MEDIA_TYPE_FILE,
    MEDIA_TYPE_IMAGE,
    MEDIA_TYPE_VIDEO,
    MEDIA_TYPE_VOICE,
    QQApiClient,
    QQMessageType,
    QQWebSocket,
    WSCallbacks,
    WSSessionStore,
    is_fatal_send_error,
    is_voice_content_type,
    parse_interaction_event,
    resolve_stt_config,
)
from qqbot_agent_sdk.api_client import Route
from qqbot_agent_sdk.attachment import AttachmentDownloader
from qqbot_agent_sdk.audio import STTPipeline
from qqbot_agent_sdk.dto import (
    GuildMessageToCreate,
    MarkdownContent,
    MediaInfo,
    MessageToCreate,
)
from qqbot_agent_sdk.media_loader import MediaUploader

BASE = Path(os.environ.get("QQ_BRIDGE_HOME", Path(__file__).resolve().parent))
INBOX = BASE / "inbox"
OUTBOX = BASE / "outbox"
PROACTIVE = BASE / "proactive"
EVENTS = BASE / "events"
DONE = BASE / "done"
LOGS = BASE / "logs"
STATE = BASE / "state"
CREDENTIALS_FILE = Path(
    os.environ.get("QQ_BRIDGE_CREDENTIALS", str(BASE / "credentials.env"))
)
CONFIG_FILE = Path(
    os.environ.get("QQ_BRIDGE_CONFIG", str(BASE / "config.json"))
)

OUTBOX_POLL_SECONDS = 3.0
ASR_TIMEOUT_SECONDS = 25.0

MEDIA_KIND_TO_TYPE = {
    "image": MEDIA_TYPE_IMAGE,
    "video": MEDIA_TYPE_VIDEO,
    "voice": MEDIA_TYPE_VOICE,
    "file": MEDIA_TYPE_FILE,
}

# Lifecycle / relation events: logged to events.jsonl, never answered.
LIFECYCLE_EVENT_TYPES = frozenset({
    "FRIEND_ADD",
    "FRIEND_DEL",
    "GROUP_ADD_ROBOT",
    "GROUP_DEL_ROBOT",
    "GROUP_MSG_REJECT",
    "GROUP_MSG_RECEIVE",
    "C2C_MSG_REJECT",
    "C2C_MSG_RECEIVE",
})

UNBOUND_TEXT = (
    "This bot is not bound to its owner yet, so this message was not "
    "forwarded. Please try again after binding is complete."
)

logger = logging.getLogger("qq-bridge")


# ── configuration ──────────────────────────────────────────────────────

DEFAULT_CONFIG: dict = {
    # C2C senders allowed to reach the agent. Empty = binding mode.
    "allow_openids": [],
    # Group senders (member_openid) allowed to reach the agent.
    # Empty = every member of a group the bot sits in may talk to it.
    "allow_group_member_openids": [],
    # Passive replies older than this (seconds since the daemon received
    # the message) are archived EXPIRED instead of sent. The platform's
    # own documentation is inconsistent (5 minutes in one field, 60 in
    # another); 300 is the conservative reading. Raise it only if you
    # have measured later replies succeeding on your bot.
    "reply_window_seconds": 300,
    # Platform cap on passive replies per source message.
    "max_replies_per_message": 4,
    # Proactive sends: attempts before archiving as FAILED.
    "proactive_retries": 3,
    "proactive_retry_delay_seconds": 5,
    # Send the "typing..." indicator when a C2C message lands.
    "typing": True,
    # Also receive group messages that do not @ the bot (needs the
    # platform-side group full-message permission).
    "group_full_messages": False,
    "heartbeat_seconds": 20,
    # Exit hard when the gateway has been silent this long (seconds).
    "liveness_timeout_seconds": 300,
}


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.is_file():
        try:
            user_cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(user_cfg, dict):
                cfg.update(user_cfg)
        except (json.JSONDecodeError, OSError):
            logger.exception("config.json unreadable; using defaults")
    return cfg


def load_credentials() -> Optional[tuple]:
    """Return (app_id, secret) or None when either is missing.

    Values already present in the process environment win; the rest are
    read from the credentials env file (simple KEY=VALUE lines).
    """
    if CREDENTIALS_FILE.is_file():
        for line in CREDENTIALS_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    app_id = os.environ.get("QQBOT_APP_ID", "").strip()
    secret = os.environ.get("QQBOT_APP_SECRET", "").strip()
    if not app_id or not secret:
        return None
    return app_id, secret


# ── small state helpers (daemon is the only writer) ────────────────────

def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def _parse_iso(value: str) -> Optional[float]:
    """Parse an ISO-8601 timestamp to epoch seconds (None when invalid)."""
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class ReplyLedger:
    """Per-message passive-reply budget and msg_seq registry.

    Persisted to state/reply_counts.json so a daemon restart mid-reply
    cannot reissue a (msg_id, msg_seq) pair the platform already saw.
    """

    def __init__(self, state_dir: Path, max_replies: int) -> None:
        self._file = state_dir / "reply_counts.json"
        self._max = max_replies
        self._data: dict = _read_json(self._file, {})

    def _save(self) -> None:
        _write_json_atomic(self._file, self._data)

    def used(self, msg_id: str) -> int:
        return int(self._data.get(msg_id, {}).get("used", 0))

    def claim_slot(self, msg_id: str) -> bool:
        """Reserve one passive-reply slot; False when the budget is spent."""
        rec = self._data.setdefault(msg_id, {"used": 0, "seqs": []})
        if int(rec["used"]) >= self._max:
            return False
        rec["used"] = int(rec["used"]) + 1
        self._save()
        return True

    def next_seq(self, msg_id: str) -> int:
        """A msg_seq never before used with this msg_id.

        The SDK generator is pseudo-random over 0-65535; collisions are
        bumped forward until unique for this message. Call once per send
        attempt — a seq burned by a failed attempt is never reused,
        because the platform may have recorded it before the failure.
        """
        rec = self._data.setdefault(msg_id, {"used": 0, "seqs": []})
        seen = {int(s) for s in rec.get("seqs", [])}
        seq = QQApiClient.next_msg_seq()
        while seq in seen:
            seq = (seq + 1) % 65536
        rec["seqs"] = sorted(seen | {seq})
        self._save()
        return seq


# ── wakeup (interaction-recall) bookkeeping ────────────────────────────
# A proactive message may optionally be sent as an interaction recall
# (is_wakeup). The platform budgets recalls in four periods counted from
# the last interaction with that user: day 0, days 1-3, days 3-7 and
# days 7-30; one recall per period. Plain proactive messages do not use
# this mechanism and are not budgeted here.

def wakeup_period(anchor_ts: float, now: float) -> Optional[int]:
    d = (now - anchor_ts) / 86400.0
    if d < 0 or d >= 30:
        return None
    if d < 1:
        return 0
    if d < 3:
        return 1
    if d < 7:
        return 2
    return 3


class WakeupBook:
    def __init__(self, state_dir: Path) -> None:
        self._seen_file = state_dir / "last_seen.json"
        self._usage_file = state_dir / "wakeup_usage.json"

    def touch(self, openid: str) -> None:
        seen = _read_json(self._seen_file, {})
        seen[openid] = time.time()
        _write_json_atomic(self._seen_file, seen)
        usage = _read_json(self._usage_file, {})
        usage[openid] = {"anchor_ts": time.time(), "used_periods": []}
        _write_json_atomic(self._usage_file, usage)

    def claim(self, openid: str) -> Optional[int]:
        usage = _read_json(self._usage_file, {})
        rec = usage.get(openid)
        if not rec:
            return None
        period = wakeup_period(float(rec["anchor_ts"]), time.time())
        if period is None or period in rec.get("used_periods", []):
            return None
        rec.setdefault("used_periods", []).append(period)
        usage[openid] = rec
        _write_json_atomic(self._usage_file, usage)
        return period

    def release(self, openid: str, period: int) -> None:
        usage = _read_json(self._usage_file, {})
        rec = usage.get(openid)
        if rec and period in rec.get("used_periods", []):
            rec["used_periods"].remove(period)
            _write_json_atomic(self._usage_file, usage)


# ── GROUP_MESSAGE_CREATE support ─────────────────────────────────────
#
# The SDK's EventParser has no handler for GROUP_MESSAGE_CREATE (group
# full messages) and its dto.EventType does not list the event at all, so
# we register a handler into EventParser._EVENT_HANDLERS from here (the
# SDK files themselves are not modified).  The payload shape matches
# GROUP_AT_MESSAGE_CREATE (group_openid / author.member_openid /
# attachments); the content is kept as delivered — a full message was
# not necessarily addressed to the bot, so no leading @mention is
# stripped.

def _parse_group_full_message(
    event_type: str,
    msg: Any,
    content: str,
    raw: dict,
) -> Optional[InboundEvent]:
    if not msg.group_openid:
        return None
    return InboundEvent(
        event_type=event_type,
        chat_id=msg.group_openid,
        user_id=msg.author.member_openid,
        chat_scope="group",
        content=content,
        message_id=msg.id,
        timestamp=msg.timestamp,
        message_type=msg.message_type,
        attachments=msg.attachments,
        msg_elements=msg.msg_elements,
        raw=raw,
    )


EventParser._EVENT_HANDLERS["GROUP_MESSAGE_CREATE"] = _parse_group_full_message


class BridgeWebSocket(QQWebSocket):
    """QQWebSocket plus dispatch for events the SDK drops.

    The SDK's ``_handle_dispatch`` only routes READY/RESUMED, the five
    MESSAGE_EVENT_TYPES and INTERACTION_CREATE; every other dispatch is
    logged at debug level and discarded.  GROUP_MESSAGE_CREATE and the
    lifecycle events therefore never reach the host callbacks.  This
    subclass (SDK files untouched) routes them through the same
    ``_dispatch_to_main`` mechanism the SDK itself uses.
    """

    lifecycle_handler: Optional[Any] = None  # async (event_type, data)

    def _handle_dispatch(self, event_type: str, data: dict) -> None:
        if event_type == "GROUP_MESSAGE_CREATE":
            msg_id = str(data.get("id", ""))
            if msg_id and self._is_duplicate(msg_id):
                logger.debug("duplicate group full message dropped: %s", msg_id)
                return
            self._dispatch_to_main(self._cb.on_message_event(event_type, data))
            return
        if event_type in LIFECYCLE_EVENT_TYPES and self.lifecycle_handler is not None:
            self._dispatch_to_main(self.lifecycle_handler(event_type, data))
            return
        super()._handle_dispatch(event_type, data)


def setup_logging() -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(LOGS / "daemon.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])


def _attachment_dict(att: Any) -> dict:
    """Serialize an SDK MessageAttachment for the inbox record."""
    d: dict = {
        "filename": att.filename or "",
        "content_type": att.content_type or "",
        "url": att.resolved_url or "",
        "size": att.size or 0,
    }
    if att.width:
        d["width"] = att.width
    if att.height:
        d["height"] = att.height
    if att.voice_wav_url:
        d["voice_wav_url"] = att.voice_wav_url
    return d


class Bridge:
    """Holds bridge state and implements the SDK callbacks."""

    def __init__(
        self,
        app_id: str,
        api: QQApiClient,
        config: dict,
        http_client: Optional[Any] = None,
    ) -> None:
        self.app_id = app_id
        self.api = api
        self.cfg = config
        self.parser = EventParser()
        self.bot_openids: set = set()
        self.session_store = WSSessionStore(
            base_dir=str(STATE), filename="ws_session.json"
        )
        self.ledger = ReplyLedger(
            STATE, int(config["max_replies_per_message"])
        )
        self.wakeup = WakeupBook(STATE)
        self._progress_file = STATE / "send_progress.json"
        # Media upload + voice transcription helpers (SDK-provided).
        self.uploader: Optional[MediaUploader] = None
        self.stt: Optional[STTPipeline] = None
        self.stt_configured = resolve_stt_config({}) is not None
        if http_client is not None:
            self.uploader = MediaUploader(api, http_client, log_tag="QQBridge")
            downloader = AttachmentDownloader(
                http_client,
                cache_dir=str(STATE / "attachments"),
                log_tag="QQBridge",
            )
            self.stt = STTPipeline(
                http_client,
                lambda: resolve_stt_config({}),
                downloader,
                log_tag="QQBridge",
            )

    # -- gateway callbacks ---------------------------------------------

    def on_ready(self, ready: Any) -> None:
        user = getattr(ready, "user", None)
        if user is not None:
            for value in (getattr(user, "id", ""), getattr(user, "user_openid", "")):
                if value:
                    self.bot_openids.add(str(value))
        logger.info(
            "gateway READY; bot identity captured (%d id(s))", len(self.bot_openids)
        )

    def get_session(self):
        session = self.session_store.get(self.app_id)
        if session.is_resumable and session.is_fresh():
            return (session.session_id, session.seq)
        return (None, None)

    def set_session(self, session_id: Optional[str], seq: Optional[int]) -> None:
        if session_id:
            self.session_store.save(self.app_id, session_id, seq)

    # -- event log (interactions + lifecycle) ---------------------------

    def _append_event(self, entry: dict) -> None:
        EVENTS.mkdir(parents=True, exist_ok=True)
        with (EVENTS / "events.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    @staticmethod
    def _extract_ids(raw: dict) -> dict:
        ids: dict = {}
        for key in (
            "id",
            "user_openid",
            "member_openid",
            "group_openid",
            "group_member_openid",
            "operator_openid",
            "channel_id",
            "guild_id",
        ):
            value = raw.get(key)
            if value:
                ids[key] = str(value)
        return ids

    async def on_interaction_event(self, event_type: str, raw: dict) -> None:
        """Acknowledge a button interaction and log it; never answered."""
        try:
            event = parse_interaction_event(raw)
        except Exception:
            logger.exception("failed to parse interaction event")
            return
        if event.id:
            try:
                await self.api.acknowledge_interaction(event.id, code=0)
            except Exception:
                logger.exception("failed to acknowledge interaction %s", event.id)
        self._append_event({
            "type": event_type,
            "interaction_id": event.id,
            "scene": event.scene,
            "ids": self._extract_ids(raw),
            "timestamp": raw.get("timestamp"),
            "received_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("interaction %s acknowledged (scene %s)", event.id, event.scene)

    async def on_lifecycle_event(self, event_type: str, raw: dict) -> None:
        """Append a lifecycle/relation event to events.jsonl."""
        self._append_event({
            "type": event_type,
            "ids": self._extract_ids(raw),
            "timestamp": raw.get("timestamp"),
            "received_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("lifecycle event logged: %s", event_type)

    # -- inbound ---------------------------------------------------------

    async def _transcribe_voice(self, att: Any) -> tuple:
        """Return (asr_text, asr_note) for a voice attachment.

        Uses the SDK STTPipeline: a configured STT engine when available,
        otherwise the platform-provided ``asr_refer_text`` carried by the
        event itself.  Nothing is invented — when neither source yields
        text, asr_text is None and the note says why.
        """
        if self.stt is None:
            return None, "asr unavailable: transcription pipeline not initialised"
        try:
            text = await asyncio.wait_for(
                self.stt.transcribe(att), timeout=ASR_TIMEOUT_SECONDS
            )
        except Exception:
            logger.exception("voice transcription failed")
            return None, "asr failed: transcription pipeline error (see daemon log)"
        if text:
            return text, None
        if self.stt_configured:
            return None, "asr produced no text: STT engine and platform asr_refer_text both empty"
        return None, (
            "asr produced no text: no STT engine configured "
            "(set QQ_STT_API_KEY to enable one) and platform asr_refer_text was empty"
        )

    def _record_pending_binding(self, openid: str, content: str) -> bool:
        """Binding mode: remember the first sender; True when newly recorded."""
        pending_file = STATE / "pending.json"
        existing = _read_json(pending_file, None)
        if existing:
            return False
        _write_json_atomic(pending_file, {
            "openid": openid,
            "sample": content[:80],
            "ts": time.time(),
        })
        return True

    async def _handle_unbound(self, openid: str, msg_id: str, content: str) -> None:
        """Binding mode: record the sender and send the not-bound notice."""
        newly = self._record_pending_binding(openid, content)
        logger.info("binding mode: candidate sender %s... recorded", openid[:8])
        if not newly:
            return  # notice once per binding candidate, not per message
        try:
            await self.api.send_text(
                "c2c", openid, UNBOUND_TEXT, reply_to=msg_id, markdown=False
            )
        except Exception:
            logger.exception("failed to send not-bound notice")

    async def on_message_event(self, event_type: str, raw: dict) -> None:
        try:
            event = self.parser.parse(event_type, raw)
        except Exception:
            logger.exception("failed to parse event %s", event_type)
            return
        if event is None or event.chat_scope not in ("c2c", "group", "guild", "dm"):
            return
        author = raw.get("author") or {}
        if author.get("bot"):
            return  # never react to bots, including ourselves
        sender = str(event.user_id or "")
        if sender and sender in self.bot_openids:
            return  # our own message echoed back
        if event_type == "GROUP_MESSAGE_CREATE" and not self.cfg["group_full_messages"]:
            return  # full messages are opt-in
        content = (event.content or "").strip()
        attachments = [_attachment_dict(att) for att in event.attachments]
        if not content and not attachments and not event.msg_elements:
            return  # genuinely empty message — nothing to hand to the agent
        msg_id = event.message_id
        if not msg_id or "/" in msg_id or "\\" in msg_id or msg_id in (".", ".."):
            logger.warning("dropping message with unsafe id %r", msg_id)
            return

        # Owner allowlist, enforced here so strangers never wake the agent.
        if event.chat_scope == "c2c":
            self.wakeup.touch(sender)
            allow = [str(x) for x in self.cfg.get("allow_openids", [])]
            if not allow:
                await self._handle_unbound(sender, msg_id, content)
                return
            if sender not in allow:
                logger.info("c2c sender %s... not in allowlist; dropped", sender[:8])
                return
        elif event.chat_scope == "group":
            allow_g = [str(x) for x in self.cfg.get("allow_group_member_openids", [])]
            if allow_g and sender not in allow_g:
                logger.info("group sender %s... not in allowlist; dropped", sender[:8])
                return

        target = INBOX / f"{msg_id}.json"
        if target.exists() or (DONE / f"{msg_id}.json").exists():
            return  # duplicate delivery
        record: dict = {
            "msg_id": msg_id,
            "scene": event.chat_scope,
            "event_type": event_type,
            "user_openid": sender,
            "group_openid": event.chat_id if event.chat_scope == "group" else None,
            "channel_id": event.chat_id if event.chat_scope == "guild" else None,
            "chat_id": event.chat_id,
            "content": content,
            "message_type": event.message_type,
            "attachments": attachments,
            "attachment_only": not content and bool(attachments),
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        if event_type == "GROUP_MESSAGE_CREATE":
            record["full_message"] = True
        if event.msg_elements:
            record["msg_elements"] = [
                {
                    "content": el.content or "",
                    "attachments": [_attachment_dict(a) for a in el.attachments],
                }
                for el in event.msg_elements
            ]
        reference = raw.get("message_reference")
        if isinstance(reference, dict) and reference.get("message_id"):
            record["message_reference"] = {
                "message_id": str(reference["message_id"])
            }
        # Voice: transcribe the first voice attachment, if any.
        for att in event.attachments:
            if is_voice_content_type(att.content_type or "", att.filename or ""):
                asr_text, asr_note = await self._transcribe_voice(att)
                record["asr_text"] = asr_text
                if asr_note:
                    record["asr_note"] = asr_note
                break
        tmp = INBOX / f".{msg_id}.tmp"
        tmp.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.rename(target)
        logger.info(
            "inbox: %s message %s from %s (%d chars, %d attachment(s))",
            record["scene"],
            msg_id,
            sender,
            len(content),
            len(attachments),
        )
        if event.chat_scope == "c2c" and self.cfg["typing"]:
            asyncio.create_task(self._send_typing(sender, msg_id))

    async def _send_typing(self, user_openid: str, msg_id: str) -> None:
        """Best-effort typing indicator for C2C conversations."""
        try:
            await self.api.send_typing(user_openid, msg_id)
        except Exception:
            logger.debug("send_typing failed for %s", msg_id, exc_info=True)


    # -- outbound --------------------------------------------------------

    def _progress(self) -> dict:
        return _read_json(self._progress_file, {})

    def _set_progress(self, key: str, value: Optional[dict]) -> None:
        data = self._progress()
        if value is None:
            data.pop(key, None)
        else:
            data[key] = value
        _write_json_atomic(self._progress_file, data)

    async def _send_text_part(
        self,
        scene: str,
        chat_id: str,
        text: str,
        *,
        reply_to: Optional[str],
        markdown: bool,
    ) -> None:
        """Send one text part. C2C/group/guild go through MessageToCreate
        so the daemon controls msg_seq; channel DMs have no seq concept
        and use the SDK's generic request with the official /dms route."""
        text = text[:MAX_MESSAGE_LENGTH]
        if scene == "dm":
            body = GuildMessageToCreate(
                content=text,
                msg_id=reply_to or "",
            )
            route = Route(
                "POST", "/dms/{guild_id}/messages", guild_id=chat_id
            )
            await self.api.request(route, body.to_dict())
            return
        msg = MessageToCreate(
            content="" if markdown else text,
            msg_type=int(QQMessageType.TEXT),
            msg_id=reply_to or "",
            msg_seq=self.ledger.next_seq(reply_to) if reply_to else QQApiClient.next_msg_seq(),
            markdown=MarkdownContent(content=text) if markdown else None,
        )
        if scene == "c2c":
            await self.api.post_c2c_message(chat_id, msg)
        elif scene == "group":
            await self.api.post_group_message(chat_id, msg)
        elif scene == "guild":
            await self.api.post_guild_message(chat_id, msg)
        else:
            raise RuntimeError(f"text send not supported for scene {scene!r}")

    async def _upload_media(self, scene: str, chat_id: str, item: dict) -> str:
        """Upload one local media file via the SDK; returns file_info."""
        kind = str(item.get("kind") or "")
        if kind not in MEDIA_KIND_TO_TYPE:
            raise ValueError(f"unknown media kind: {kind!r}")
        path = Path(str(item.get("path") or ""))
        if self.uploader is None:
            raise RuntimeError("media uploader unavailable (no http client)")
        return await self.uploader.upload(
            scene, chat_id, str(path), MEDIA_KIND_TO_TYPE[kind], path.name
        )

    async def _send_media_part(
        self,
        scene: str,
        chat_id: str,
        item: dict,
        *,
        text: str,
        reply_to: Optional[str],
        markdown: bool,
    ) -> None:
        """Send one rich-media (msg_type 7) message for C2C/group."""
        if scene not in ("c2c", "group"):
            raise RuntimeError(f"rich media not supported for scene {scene!r}")
        file_info = await self._upload_media(scene, chat_id, item)
        msg = MessageToCreate(
            content=text[:MAX_MESSAGE_LENGTH] if text and not markdown else "",
            msg_type=int(QQMessageType.RICH_MEDIA),
            msg_seq=self.ledger.next_seq(reply_to) if reply_to else QQApiClient.next_msg_seq(),
            msg_id=reply_to or "",
            media=MediaInfo(file_info=file_info),
            markdown=MarkdownContent(content=text[:MAX_MESSAGE_LENGTH]) if (text and markdown) else None,
        )
        if scene == "c2c":
            await self.api.post_c2c_message(chat_id, msg)
        else:
            await self.api.post_group_message(chat_id, msg)

    @staticmethod
    def _validated_media(payload: dict) -> Optional[list]:
        """Return the payload's media list, or None if any entry is bad.

        Every entry must name a known kind and a local file that exists;
        a bad entry makes the whole payload unsendable (archived INVALID
        by the caller) rather than retrying forever.
        """
        media = payload.get("media") or []
        if not isinstance(media, list):
            return None
        for item in media:
            if not isinstance(item, dict):
                return None
            if str(item.get("kind") or "") not in MEDIA_KIND_TO_TYPE:
                return None
            path = Path(str(item.get("path") or ""))
            if not path.is_file():
                return None
        return media

    @staticmethod
    def _build_parts(payload: dict, media: list) -> list:
        """Flatten a reply payload into an ordered list of send parts.

        Each part is {"kind": "text", "text": ...} or
        {"kind": "media", "media": {...}, "text": ...}.  When media is
        present, the first text part rides along with the first media
        part (one platform message), the rest follow in order.
        """
        texts: list = []
        for key in ("reply", "content"):
            single = payload.get(key)
            if isinstance(single, str) and single.strip():
                texts.append(single.strip())
                break
        multi = payload.get("replies")
        if isinstance(multi, list):
            for item in multi:
                if isinstance(item, str) and item.strip():
                    texts.append(item.strip())
        parts: list = []
        if media:
            for idx, item in enumerate(media):
                parts.append({
                    "kind": "media",
                    "media": item,
                    "text": texts[0] if idx == 0 and texts else "",
                })
            for extra in texts[1:]:
                parts.append({"kind": "text", "text": extra})
        else:
            for text in texts:
                parts.append({"kind": "text", "text": text})
        return parts

    async def _deliver_part(
        self,
        scene: str,
        chat_id: str,
        part: dict,
        *,
        reply_to: Optional[str],
        markdown: bool,
    ) -> None:
        if part["kind"] == "media":
            await self._send_media_part(
                scene,
                chat_id,
                part["media"],
                text=part.get("text", ""),
                reply_to=reply_to,
                markdown=markdown,
            )
        else:
            await self._send_text_part(
                scene,
                chat_id,
                part["text"],
                reply_to=reply_to,
                markdown=markdown,
            )

    async def outbox_loop(self) -> None:
        while True:
            try:
                for path in sorted(p for p in OUTBOX.glob("*.json") if not p.name.startswith(".")):
                    await self._handle_outbox_file(path)
            except Exception:
                logger.exception("outbox scan failed")
            await asyncio.sleep(OUTBOX_POLL_SECONDS)

    async def _handle_outbox_file(self, path: Path) -> None:
        msg_id = path.stem
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return  # still being written; retry on the next poll
        inbox_file = INBOX / f"{msg_id}.json"
        if not inbox_file.exists():
            logger.warning(
                "outbox %s has no matching inbox record; archiving as orphan",
                msg_id,
            )
            self._archive(msg_id, "ORPHAN")
            return
        try:
            record = json.loads(inbox_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.exception("inbox record %s unreadable", msg_id)
            return
        window = float(self.cfg["reply_window_seconds"])
        received_ts = _parse_iso(str(record.get("received_at", "")))
        age = time.time() - received_ts if received_ts is not None else float("inf")
        progress = self._progress().get(f"outbox:{msg_id}", {})
        sent_parts = int(progress.get("sent_parts", 0))

        if age > window:
            marker = "PARTIAL" if sent_parts else "EXPIRED"
            logger.warning(
                "reply for %s closed (age %.0fs > %.0fs, %d part(s) sent); archiving %s",
                msg_id, age, window, sent_parts, marker,
            )
            self._archive(msg_id, marker)
            return

        media = self._validated_media(payload)
        if media is None:
            logger.warning("outbox %s has invalid media entries; archiving", msg_id)
            self._archive(msg_id, "INVALID")
            return
        parts = self._build_parts(payload, media)
        if not parts:
            logger.warning("outbox %s has an empty reply; archiving as invalid", msg_id)
            self._archive(msg_id, "INVALID")
            return
        scene = str(record.get("scene") or "c2c")
        if scene in ("guild", "dm"):
            # Rich media cannot be uploaded for guild/dm; drop media parts'
            # attachments but keep any text riding on them.
            adjusted: list = []
            for part in parts:
                if part["kind"] == "media":
                    logger.warning("media skipped for scene %s (upload supports c2c/group only)", scene)
                    if part.get("text"):
                        adjusted.append({"kind": "text", "text": part["text"]})
                else:
                    adjusted.append(part)
            parts = adjusted
            if not parts:
                self._archive(msg_id, "INVALID")
                return
        chat_id = (
            record.get("chat_id")
            or record.get("group_openid")
            or record.get("user_openid")
        )
        markdown = bool(payload.get("markdown"))

        while sent_parts < len(parts):
            if not self.ledger.claim_slot(msg_id):
                logger.warning(
                    "reply budget for %s exhausted at %d/%d parts; archiving PARTIAL",
                    msg_id, sent_parts, len(parts),
                )
                self._archive(msg_id, "PARTIAL", note="reply budget exhausted")
                return
            part = parts[sent_parts]
            try:
                await self._deliver_part(
                    scene, str(chat_id), part, reply_to=msg_id, markdown=markdown
                )
            except Exception as exc:
                if is_fatal_send_error(str(exc)):
                    logger.error(
                        "fatal send error for %s part %d: %s; archiving FAILED",
                        msg_id, sent_parts, exc,
                    )
                    self._archive(msg_id, "FAILED", note=str(exc)[:300])
                    return
                logger.exception(
                    "send failed for %s part %d; will retry until the window closes",
                    msg_id, sent_parts,
                )
                return  # slot stays claimed: the platform may have recorded the seq
            sent_parts += 1
            self._set_progress(f"outbox:{msg_id}", {"sent_parts": sent_parts})
        logger.info("sent reply for %s (%d part(s))", msg_id, len(parts))
        self._archive(msg_id, "SENT")

    def _archive(self, msg_id: str, marker: str, note: str = "") -> None:
        DONE.mkdir(parents=True, exist_ok=True)
        inbox_file = INBOX / f"{msg_id}.json"
        outbox_file = OUTBOX / f"{msg_id}.json"
        if inbox_file.exists():
            inbox_file.rename(DONE / f"{msg_id}.json")
        if outbox_file.exists():
            outbox_file.rename(DONE / f"{msg_id}.reply.json")
        marker_body = {
            "marker": marker,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        if note:
            marker_body["note"] = note
        (DONE / f"{msg_id}.{marker}").write_text(
            json.dumps(marker_body, ensure_ascii=False), encoding="utf-8"
        )
        self._set_progress(f"outbox:{msg_id}", None)


    # -- proactive messages ----------------------------------------------

    async def proactive_loop(self) -> None:
        while True:
            try:
                for path in sorted(p for p in PROACTIVE.glob("*.json") if not p.name.startswith(".")):
                    await self._handle_proactive_file(path)
            except Exception:
                logger.exception("proactive scan failed")
            await asyncio.sleep(OUTBOX_POLL_SECONDS)

    async def _handle_proactive_file(self, path: Path) -> None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return  # still being written; retry on the next poll
        key = f"proactive:{path.name}"
        progress = self._progress().get(key, {})
        attempts = int(progress.get("attempts", 0))
        last_attempt = float(progress.get("last_attempt_ts", 0))
        delay = float(self.cfg["proactive_retry_delay_seconds"])
        if attempts and (time.time() - last_attempt) < delay:
            return  # still backing off between attempts

        scene = str(payload.get("scene") or "")
        target = str(payload.get("target") or "")
        markdown = bool(payload.get("markdown"))
        media = self._validated_media(payload)
        parts = self._build_parts(payload, media or [])
        wakeup = bool(payload.get("wakeup", False))
        if scene not in ("c2c", "group") or not target or media is None or not parts:
            logger.warning("proactive file %s is invalid; archiving as FAILED", path.name)
            self._archive_proactive(path, "FAILED", note="invalid payload")
            return

        # Interaction-recall sends claim their period before the first
        # attempt and release it only on a final failure, so a delivered
        # recall is never double-spent and a failed one is not lost.
        period: Optional[int] = progress.get("wakeup_period")
        if wakeup and scene == "c2c" and period is None and not attempts:
            period = self.wakeup.claim(target)
            if period is None:
                logger.warning(
                    "proactive file %s: wakeup period unavailable for target; archiving FAILED",
                    path.name,
                )
                self._archive_proactive(path, "FAILED", note="wakeup period unavailable")
                return
            progress["wakeup_period"] = period

        sent_parts = int(progress.get("sent_parts", 0))
        try:
            while sent_parts < len(parts):
                part = parts[sent_parts]
                if wakeup and scene == "c2c":
                    await self._send_wakeup_part(target, part, markdown=markdown)
                else:
                    await self._deliver_part(
                        scene, target, part, reply_to=None, markdown=markdown
                    )
                sent_parts += 1
        except Exception as exc:
            attempts += 1
            progress.update({
                "attempts": attempts,
                "sent_parts": sent_parts,
                "last_attempt_ts": time.time(),
                "last_error": str(exc)[:300],
            })
            self._set_progress(key, progress)
            if attempts >= int(self.cfg["proactive_retries"]) or is_fatal_send_error(str(exc)):
                # Log the platform error verbatim (HTTP status / biz code +
                # message) so quota problems are visible exactly as reported.
                logger.error(
                    "proactive send failed for %s after %d attempt(s): %s",
                    path.name, attempts, exc,
                )
                if period is not None:
                    self.wakeup.release(target, int(period))
                self._archive_proactive(path, "FAILED", note=str(exc)[:300])
            else:
                logger.warning(
                    "proactive send attempt %d failed for %s: %s",
                    attempts, path.name, exc,
                )
            return
        logger.info("proactive message sent from %s to %s/%s", path.name, scene, target)
        self._archive_proactive(path, "SENT")

    async def _send_wakeup_part(self, target: str, part: dict, *, markdown: bool) -> None:
        """Send one proactive part as an interaction recall (is_wakeup).

        Text only: the recall route is the plain message endpoint with
        the is_wakeup flag; media parts fall back to a normal proactive
        send for that part.
        """
        if part["kind"] != "text":
            await self._deliver_part(
                "c2c", target, part, reply_to=None, markdown=markdown
            )
            return
        route = Route("POST", "/v2/users/{openid}/messages", openid=target)
        body: dict = {
            "content": part["text"][:MAX_MESSAGE_LENGTH],
            "msg_type": 0,
            "is_wakeup": True,
        }
        res = await self.api.request(route, body)
        if not res:
            raise RuntimeError("wakeup send returned no result")

    def _archive_proactive(self, path: Path, marker: str, note: str = "") -> None:
        DONE.mkdir(parents=True, exist_ok=True)
        archived = DONE / f"{path.stem}.proactive.json"
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if note:
                payload["_error"] = note
            payload["_result"] = marker
            _write_json_atomic(archived, payload)
            path.unlink()
        marker_body = {
            "marker": marker,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        if note:
            marker_body["note"] = note
        (DONE / f"{path.stem}.{marker}").write_text(
            json.dumps(marker_body, ensure_ascii=False), encoding="utf-8"
        )
        self._set_progress(f"proactive:{path.name}", None)

    # -- heartbeat file ----------------------------------------------------

    async def heartbeat_loop(self) -> None:
        interval = float(self.cfg["heartbeat_seconds"])
        while True:
            try:
                pending = sum(1 for p in INBOX.glob("*.json") if not p.name.startswith("."))
                _write_json_atomic(STATE / "heartbeat.json", {
                    "ts": time.time(),
                    "pid": os.getpid(),
                    "inbox_pending": pending,
                })
            except Exception:
                logger.exception("heartbeat write failed")
            await asyncio.sleep(interval)


async def main() -> int:
    for directory in (INBOX, OUTBOX, PROACTIVE, EVENTS, DONE, LOGS, STATE):
        directory.mkdir(parents=True, exist_ok=True)
    setup_logging()
    config = load_config()

    creds = load_credentials()
    if creds is None:
        logger.error(
            "credentials missing: QQBOT_APP_ID / QQBOT_APP_SECRET are not set. "
            "Put them in %s (KEY=VALUE lines) or export them in the environment. "
            "Exiting.",
            CREDENTIALS_FILE,
        )
        return 2
    app_id, secret = creds
    masked = "****" + app_id[-4:] if len(app_id) >= 4 else "****"
    logger.info("qq-bridge starting (app_id %s)", masked)

    http_client = httpx.AsyncClient(timeout=30.0)
    api = QQApiClient(app_id=app_id, client_secret=secret, log_tag="QQBridge")
    api.setup(http_client)
    bridge = Bridge(app_id, api, config, http_client=http_client)

    # Gateway liveness: heartbeat ACKs (and connect/ready events) refresh
    # this timestamp. See liveness_loop below.
    liveness = {"t": time.monotonic()}

    def _touch() -> None:
        liveness["t"] = time.monotonic()

    def _on_connected() -> None:
        _touch()
        logger.info("gateway connected")

    def _on_ready(ready: Any) -> None:
        _touch()
        bridge.on_ready(ready)

    try:
        await api.ensure_token()
    except Exception:
        logger.exception("failed to obtain access token; check credentials/network")
        return 3
    try:
        gateway_url = await api.get_gateway_url()
    except Exception:
        logger.exception("failed to obtain gateway URL")
        return 3

    callbacks = WSCallbacks(
        on_message_event=bridge.on_message_event,
        on_interaction_event=bridge.on_interaction_event,
        on_connected=_on_connected,
        on_disconnected=lambda: logger.warning("gateway disconnected"),
        on_fatal_error=lambda code, msg: logger.error(
            "gateway fatal error [%s]: %s", code, msg
        ),
        get_token=api.ensure_token_sync,
        get_gateway_url=api.get_gateway_url_sync,
        get_session=bridge.get_session,
        set_session=bridge.set_session,
        set_heartbeat_interval=lambda interval: logger.debug(
            "heartbeat interval %.0fs", interval
        ),
        clear_token=api.clear_token,
        fail_pending=lambda reason: logger.warning(
            "pending requests failed: %s", reason
        ),
        on_ready=_on_ready,
        on_heartbeat_ack=_touch,
    )
    ws = BridgeWebSocket(callbacks=callbacks, log_tag="QQBridge")
    ws.lifecycle_handler = bridge.on_lifecycle_event

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    ws.start(gateway_url, loop)

    async def liveness_loop() -> None:
        # The SDK's reconnect can wedge after a dropped gateway connection
        # (observed in production: overnight stalls of tens of minutes
        # after a single "Reconnecting" log line, bridge effectively
        # offline while the process looked alive). Heartbeat ACKs arrive
        # every heartbeat interval while the gateway is healthy; if none
        # (and no connect/ready) for the configured timeout, exit hard
        # and let the supervisor relaunch fresh.
        timeout = float(config["liveness_timeout_seconds"])
        while True:
            await asyncio.sleep(30)
            silent_for = time.monotonic() - liveness["t"]
            if silent_for > timeout:
                logger.error(
                    "gateway silent for %.0fs; exiting for supervisor restart",
                    silent_for,
                )
                os._exit(4)

    watchers = [
        asyncio.create_task(bridge.outbox_loop()),
        asyncio.create_task(bridge.proactive_loop()),
        asyncio.create_task(bridge.heartbeat_loop()),
        asyncio.create_task(liveness_loop()),
    ]
    logger.info("qq-bridge running; watching %s and %s", OUTBOX, PROACTIVE)
    try:
        await stop_event.wait()
    finally:
        for watcher in watchers:
            watcher.cancel()
        ws.stop()
        logger.info("qq-bridge stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
