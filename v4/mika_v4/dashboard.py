from __future__ import annotations

import json
import mimetypes
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .config import ConfigError, policy_payload, update_editable_policy
from .domain import RuntimeMode
from .runtime import LendingRuntime


class DashboardServer:
    def __init__(self, runtime: LendingRuntime, host: str = "127.0.0.1", port: int | None = None) -> None:
        self.runtime = runtime
        self.host = host
        self.port = int(runtime.policy.dashboard_port if port is None else port)
        self.web_root = Path(__file__).resolve().parents[1] / "www"
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_: object) -> None:
                return

            def _json(self, status: int, payload: object) -> None:
                encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(encoded)

            def _body(self) -> dict:
                size = min(int(self.headers.get("Content-Length", 0)), 1_000_000)
                raw = self.rfile.read(size)
                value = json.loads(raw or b"{}")
                if not isinstance(value, dict):
                    raise ValueError("request body must be an object")
                return value

            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                if path == "/api/status":
                    self._json(HTTPStatus.OK, outer.runtime.status_payload())
                    return
                if path == "/api/config":
                    self._json(HTTPStatus.OK, policy_payload(outer.runtime.policy))
                    return
                relative = "index.html" if path in {"", "/"} else path.lstrip("/")
                target = (outer.web_root / relative).resolve()
                if outer.web_root.resolve() not in target.parents and target != outer.web_root.resolve():
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                if not target.is_file():
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                data = target.read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", mimetypes.guess_type(target.name)[0] or "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self) -> None:  # noqa: N802
                try:
                    body = self._body()
                    path = urlparse(self.path).path
                    if path == "/api/mode":
                        requested = RuntimeMode(str(body.get("mode", "")).upper())
                        if requested == RuntimeMode.LIVE:
                            outer.runtime.enable_live(str(body.get("confirmation", "")))
                        elif requested in {RuntimeMode.SHADOW, RuntimeMode.PAUSED}:
                            outer.runtime.disable_live(requested)
                        else:
                            raise ValueError("SAFE 只能由安全状态机进入")
                        self._json(HTTPStatus.OK, {"mode": outer.runtime.store.mode().value})
                        return
                    if path == "/api/config":
                        candidate = update_editable_policy(outer.runtime.settings, body)
                        self._json(HTTPStatus.OK, {"saved": policy_payload(candidate), "restart_required": True})
                        return
                    if path == "/api/adopt":
                        count = outer.runtime.adopt_external(
                            [int(item) for item in body.get("offer_ids", [])],
                            dict(body.get("confirmations", {})),
                        )
                        self._json(HTTPStatus.OK, {"adopted": count})
                        return
                    self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                except (ValueError, ConfigError, RuntimeError, json.JSONDecodeError) as exc:
                    self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

        self.httpd = ThreadingHTTPServer((host, self.port), Handler)
        self.port = int(self.httpd.server_address[1])
        self._thread: threading.Thread | None = None

    def serve_forever(self) -> None:
        self.httpd.serve_forever(poll_interval=0.5)

    def start(self) -> None:
        self._thread = threading.Thread(target=self.serve_forever, daemon=True, name="v4-dashboard")
        self._thread.start()

    def close(self) -> None:
        if self._thread and self._thread.is_alive():
            self.httpd.shutdown()
        self.httpd.server_close()
        if self._thread:
            self._thread.join(timeout=5)
