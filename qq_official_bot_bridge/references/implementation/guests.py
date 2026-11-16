"""Guest access for the QQ bot framework: rate limits, history, memory.

When the bridge runs in public mode (``access_mode: "public"``), C2C
senders who are not on the owner allowlist become *guests*: their
messages are written to ``guest_inbox/`` for a restricted worker (no
tools, no owner data — see references/guest-hook-prompt.txt), and this
module keeps the per-guest state that makes that safe and useful:

- a daily message budget and a minimum interval, enforced in the
  daemon so an over-limit guest never wakes the worker at all;
- an append-only conversation history per guest
  (``guests/<openid>.history.jsonl``), written by the daemon for both
  sides of the conversation, so guest memory does not depend on the
  worker remembering to save it;
- a free-form notes file per guest (``guests/<openid>.notes.md``) that
  the guest worker maintains with durable facts about that guest.

All state lives in one JSON file updated atomically; the daemon is the
only writer, so no locking is needed.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

MAX_INDEX = 2000


def _today(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(
        ts if ts is not None else time.time(), timezone.utc
    ).strftime("%Y-%m-%d")


class GuestStore:
    def __init__(self, base: Path) -> None:
        self.base = base
        self.base.mkdir(parents=True, exist_ok=True)
        self._state_file = self.base / "state.json"
        self._state = self._load()

    # -- persistence ----------------------------------------------------

    def _load(self) -> dict:
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("guests", {})
                data.setdefault("msg_index", {})
                return data
        except (OSError, json.JSONDecodeError):
            pass
        return {"guests": {}, "msg_index": {}}

    def _save(self) -> None:
        tmp = self._state_file.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=1),
            encoding="utf-8",
        )
        tmp.replace(self._state_file)

    # -- rate limiting ----------------------------------------------------

    def check_and_count(
        self, openid: str, daily_limit: int, min_interval: float
    ) -> tuple:
        """Count one inbound guest message.

        Returns ``(True, "")`` when the message may proceed, otherwise
        ``(False, reason)`` with reason ``"daily_limit"`` or
        ``"too_fast"``.  A rejected message does not consume budget
        (except that a too-fast message still refreshes nothing).
        """
        now = time.time()
        guests = self._state["guests"]
        entry = guests.setdefault(
            openid,
            {
                "day": _today(now),
                "count": 0,
                "last_ts": 0.0,
                "notice_day": "",
                "first_seen": datetime.now(timezone.utc).isoformat(),
                "last_seen": "",
            },
        )
        if entry.get("day") != _today(now):
            entry["day"] = _today(now)
            entry["count"] = 0
        if daily_limit > 0 and entry["count"] >= daily_limit:
            return False, "daily_limit"
        if min_interval > 0 and now - float(entry.get("last_ts", 0)) < min_interval:
            return False, "too_fast"
        entry["count"] += 1
        entry["last_ts"] = now
        entry["last_seen"] = datetime.now(timezone.utc).isoformat()
        self._save()
        return True, ""

    def should_notice_limit(self, openid: str) -> bool:
        """The over-limit notice is sent at most once per guest per day."""
        entry = self._state["guests"].get(openid) or {}
        return entry.get("notice_day") != _today()

    def note_limit_notice(self, openid: str) -> None:
        entry = self._state["guests"].setdefault(openid, {})
        entry["notice_day"] = _today()
        self._save()

    # -- message index (which guest a msg_id belongs to) -----------------

    def mark_message(self, msg_id: str, openid: str) -> None:
        index = self._state["msg_index"]
        index[msg_id] = openid
        while len(index) > MAX_INDEX:
            index.pop(next(iter(index)))
        self._save()

    def guest_of(self, msg_id: str) -> Optional[str]:
        return self._state["msg_index"].get(msg_id)

    # -- history -----------------------------------------------------------

    def history_path(self, openid: str) -> Path:
        return self.base / f"{openid}.history.jsonl"

    def notes_path(self, openid: str) -> Path:
        return self.base / f"{openid}.notes.md"

    def append_history(
        self, openid: str, role: str, text: str, msg_id: Optional[str] = None
    ) -> None:
        if not text:
            return
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "role": role,
            "text": text,
        }
        if msg_id:
            entry["msg_id"] = msg_id
        with self.history_path(openid).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # -- reporting (admin API / status) -------------------------------------

    def list_guests(self) -> list:
        out = []
        for openid, entry in self._state["guests"].items():
            out.append(
                {
                    "openid": openid,
                    "first_seen": entry.get("first_seen", ""),
                    "last_seen": entry.get("last_seen", ""),
                    "today_count": entry.get("count", 0)
                    if entry.get("day") == _today()
                    else 0,
                }
            )
        out.sort(key=lambda g: g["last_seen"], reverse=True)
        return out

    def guest_count(self) -> int:
        return len(self._state["guests"])
