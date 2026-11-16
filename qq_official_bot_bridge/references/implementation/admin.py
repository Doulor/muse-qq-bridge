"""Localhost admin API for the QQ bot framework.

A tiny stdlib-only HTTP server bound to 127.0.0.1.  It exists so the
owner's agent can inspect status and change configuration without
editing files by hand:

    GET  /api/status    daemon + gateway status, counters
    GET  /api/config    effective config (credentials are never in it)
    PUT  /api/config    validate + persist a config patch; the daemon
                        hot-reloads it within seconds
    GET  /api/events?n= last N lifecycle/interaction events
    POST /api/push      queue a proactive message {scene,target,content}

Every request needs the header ``X-Admin-Token`` matching the token in
``state/admin.token`` (created 0600 on first start).  The server is
off unless ``admin_port`` > 0 in the config.
"""

from __future__ import annotations

import json
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Optional


class AdminFacade:
    """Callables the daemon hands to the admin server."""

    def __init__(
        self,
        status: Callable[[], dict],
        get_config: Callable[[], dict],
        put_config: Callable[[dict], tuple],
        events_tail: Callable[[int], list],
        queue_push: Callable[[dict], tuple],
    ) -> None:
        self.status = status
        self.get_config = get_config
        self.put_config = put_config
        self.events_tail = events_tail
        self.queue_push = queue_push


def ensure_token(state_dir: Path) -> str:
    token_file = state_dir / "admin.token"
    if token_file.is_file():
        token = token_file.read_text(encoding="utf-8").strip()
        if token:
            return token
    token = secrets.token_urlsafe(24)
    state_dir.mkdir(parents=True, exist_ok=True)
    tmp = token_file.with_suffix(".tmp")
    tmp.write_text(token, encoding="utf-8")
    tmp.chmod(0o600)
    tmp.replace(token_file)
    return token


class AdminServer(threading.Thread):
    def __init__(self, port: int, token: str, facade: AdminFacade) -> None:
        super().__init__(daemon=True, name="qq-bridge-admin")
        self._facade = facade
        self._token = token

        facade_ref = facade
        token_ref = token

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:  # keep stdout clean
                pass

            def _send(self, code: int, obj: Any) -> None:
                body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _authed(self) -> bool:
                return self.headers.get("X-Admin-Token", "") == token_ref

            def _body(self) -> Optional[dict]:
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    return None
                raw = self.rfile.read(max(0, min(length, 1_000_000)))
                try:
                    data = json.loads(raw.decode("utf-8")) if raw else {}
                except (ValueError, UnicodeDecodeError):
                    return None
                return data if isinstance(data, dict) else None

            def do_GET(self) -> None:  # noqa: N802 (http.server API)
                if not self._authed():
                    self._send(401, {"error": "bad admin token"})
                    return
                if self.path == "/api/status":
                    self._send(200, facade_ref.status())
                elif self.path == "/api/config":
                    self._send(200, facade_ref.get_config())
                elif self.path.startswith("/api/events"):
                    n = 50
                    if "n=" in self.path:
                        try:
                            n = max(1, min(500, int(self.path.split("n=", 1)[1].split("&")[0])))
                        except ValueError:
                            pass
                    self._send(200, {"events": facade_ref.events_tail(n)})
                else:
                    self._send(404, {"error": "not found"})

            def do_PUT(self) -> None:  # noqa: N802
                if not self._authed():
                    self._send(401, {"error": "bad admin token"})
                    return
                if self.path == "/api/config":
                    patch = self._body()
                    if patch is None:
                        self._send(400, {"error": "invalid JSON body"})
                        return
                    ok, message = facade_ref.put_config(patch)
                    self._send(200 if ok else 400, {"ok": ok, "message": message})
                else:
                    self._send(404, {"error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                if not self._authed():
                    self._send(401, {"error": "bad admin token"})
                    return
                if self.path == "/api/push":
                    payload = self._body()
                    if payload is None:
                        self._send(400, {"error": "invalid JSON body"})
                        return
                    ok, message = facade_ref.queue_push(payload)
                    self._send(200 if ok else 400, {"ok": ok, "message": message})
                else:
                    self._send(404, {"error": "not found"})

        self._server = ThreadingHTTPServer(("127.0.0.1", port), Handler)

    def run(self) -> None:
        self._server.serve_forever()

    def stop(self) -> None:
        self._server.shutdown()
