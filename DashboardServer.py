"""Local-only Dashboard HTTP transport and routing.

The application layer is injected as typed callbacks so this module owns HTTP
security and rendering without importing the CLI composition root.
"""

import json
import mimetypes
import os
import secrets
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler
from typing import Any, Callable
from urllib.parse import urlparse


MAX_BODY_BYTES = 64 * 1024


class ApiRequestError(Exception):
    def __init__(self, message, code="BAD_REQUEST", status=400, details=None):
        super().__init__(message)
        self.code = code
        self.status = int(status)
        self.details = details


@dataclass(frozen=True)
class DashboardApplication:
    project_root: str
    service_id: str
    timestamp: Callable[[], str]
    status_payload: Callable[..., dict]
    config_payload: Callable[[str], dict]
    controlled_status: Callable[..., dict]
    runtime_payload: Callable[..., dict]
    store_for_config: Callable[..., tuple]
    stats_payload: Callable[[Any], dict]
    strategy_preview: Callable[..., dict]
    save_strategy_draft: Callable[..., dict]
    apply_strategy_draft: Callable[..., dict]
    discard_strategy_draft: Callable[..., dict]
    controlled_running: Callable[..., bool]
    stop_controlled: Callable[..., dict]
    replay_from_store: Callable[..., dict]
    create_preflight: Callable[..., dict]
    start_controlled: Callable[..., dict]


def load_static_snapshot(directory, build_id, csrf_token, build_placeholder, csrf_placeholder, app_version):
    assets = {}
    root = os.path.abspath(directory)
    for current, _, files in os.walk(root):
        for name in files:
            path = os.path.join(current, name)
            relative = os.path.relpath(path, root).replace("\\", "/")
            if relative in {"botlog.json", "bot-process.log"}:
                continue
            try:
                with open(path, "rb") as file:
                    data = file.read()
            except OSError:
                continue
            if relative == "lendingbot.html":
                data = data.replace(build_placeholder.encode("utf-8"), build_id.encode("ascii"))
                data = data.replace(csrf_placeholder.encode("utf-8"), csrf_token.encode("ascii"))
                data = data.replace(b"__MIKA_APPLICATION_VERSION__", app_version.encode("ascii"))
            assets[relative] = data
    return assets


class DashboardRequestHandler(SimpleHTTPRequestHandler):
    config_path = "default.cfg"
    status_path = os.path.join("www", "botlog.json")
    build_id = ""
    static_assets = {}
    dashboard_started_at = ""
    csrf_token = ""
    app_context = None
    application: DashboardApplication | None = None

    def log_message(self, format, *args):
        return

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self'; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        super().end_headers()

    def _send_json(self, payload, status=200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Mika-Dashboard-Build", self.build_id)
        self.end_headers()
        self.wfile.write(data)

    def _send_api_error(self, code, error, status=400, details=None):
        self._send_json(
            {"ok": False, "code": str(code), "error": str(error), "details": details},
            status=status,
        )

    def _send_static(self, path):
        relative = path.lstrip("/") or "lendingbot.html"
        data = self.static_assets.get(relative)
        if data is None:
            self.send_error(404, "Not found")
            return
        content_type = mimetypes.guess_type(relative)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            content_type += "; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-Mika-Dashboard-Build", self.build_id)
        self.end_headers()
        self.wfile.write(data)

    def _validate_json_envelope(self):
        content_type = str(self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ApiRequestError("写接口只接受 application/json", "CONTENT_TYPE_REQUIRED", 415)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ApiRequestError("Content-Length 无效", "INVALID_CONTENT_LENGTH", 400) from exc
        if length < 0:
            raise ApiRequestError("Content-Length 无效", "INVALID_CONTENT_LENGTH", 400)
        if length > MAX_BODY_BYTES:
            raise ApiRequestError("请求体不能超过 64 KiB", "REQUEST_TOO_LARGE", 413)
        return length

    def _read_json_body(self):
        length = self._validate_json_envelope()
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _validate_write_request(self):
        host = str(self.headers.get("Host") or "").lower()
        if host not in {"127.0.0.1:8000", "localhost:8000"}:
            raise ApiRequestError("写请求 Host 不受信任", "INVALID_HOST", 403)
        origin = str(self.headers.get("Origin") or "").lower()
        if origin not in {"http://127.0.0.1:8000", "http://localhost:8000"}:
            raise ApiRequestError("写请求必须来自本地控制台", "INVALID_ORIGIN", 403)
        supplied = str(self.headers.get("X-Mika-CSRF") or "")
        if not supplied or not secrets.compare_digest(supplied, self.csrf_token):
            raise ApiRequestError("控制台安全令牌无效，请刷新页面", "INVALID_CSRF", 403)
        self._validate_json_envelope()

    def do_GET(self):
        path = urlparse(self.path).path
        app = self.application
        try:
            if app is None:
                raise RuntimeError("Dashboard application is not configured")
            if path == "/api/health":
                self._send_json(
                    {
                        "ok": True,
                        "service": app.service_id,
                        "buildId": self.build_id,
                        "pid": os.getpid(),
                        "startedAt": self.dashboard_started_at,
                        "projectRoot": app.project_root,
                        "configPath": os.path.abspath(self.config_path),
                        "time": app.timestamp(),
                    }
                )
                return
            if path == "/api/status":
                self._send_json(app.status_payload(self.status_path, self.config_path, self.app_context))
                return
            if path == "/api/config":
                self._send_json(app.config_payload(self.config_path))
                return
            if path == "/api/control/status":
                self._send_json(app.controlled_status(self.config_path, self.app_context))
                return
            if path == "/api/runtime/v3":
                self._send_json({"ok": True, **app.runtime_payload(self.config_path, self.app_context)})
                return
            if path == "/api/stats/v3":
                store, _ = app.store_for_config(self.config_path)
                self._send_json({"ok": True, **app.stats_payload(store)})
                return
        except Exception as exc:
            self._send_api_error("INTERNAL_ERROR", exc, status=500)
            return
        self._send_static(path)

    def do_POST(self):
        path = urlparse(self.path).path
        app = self.application
        try:
            if app is None:
                raise RuntimeError("Dashboard application is not configured")
            self._validate_write_request()
            if path == "/api/config":
                self._read_json_body()
                self._send_json(
                    {
                        "ok": False,
                        "code": "V2_STRATEGY_DISABLED",
                        "details": None,
                        "error": "V2 配置写入已永久禁用；请使用 /api/strategy/v3/*",
                    },
                    status=410,
                )
                return
            if path == "/api/strategy/v3/preview":
                payload = self._read_json_body()
                result = app.strategy_preview(self.config_path, payload, app_context=self.app_context)
                self._send_json({"ok": True, **result})
                return
            if path == "/api/strategy/v3/draft":
                payload = self._read_json_body()
                result = app.save_strategy_draft(self.config_path, payload, app_context=self.app_context)
                self._send_json({"ok": True, **result})
                return
            if path == "/api/strategy/v3/apply":
                payload = self._read_json_body()
                result = app.apply_strategy_draft(self.config_path, payload, app_context=self.app_context)
                self._send_json({"ok": True, **result})
                return
            if path == "/api/strategy/v3/discard":
                self._read_json_body()
                result = app.discard_strategy_draft(self.config_path)
                self._send_json({"ok": True, **result})
                return
            if path == "/api/runtime/v3/mode":
                self._handle_mode(app, self._read_json_body())
                return
            if path == "/api/runtime/v3/resolve-ambiguous":
                payload = self._read_json_body()
                store, _ = app.store_for_config(self.config_path)
                result = store.resolve_ambiguous_intent(
                    payload.get("intentId"),
                    exchange_offer_id=payload.get("exchangeOfferId"),
                    close=bool(payload.get("confirmAbsent", False)),
                )
                self._send_json({"ok": True, **result})
                return
            if path == "/api/control/preflight":
                self._read_json_body()
                result = app.create_preflight(self.config_path, context=self.app_context)
                self._send_json({"ok": True, **result})
                return
            if path == "/api/control/start":
                payload = self._read_json_body()
                result = app.start_controlled(
                    self.config_path,
                    self.status_path,
                    str(payload.get("preflightId", "")),
                    context=self.app_context,
                )
                self._send_json({"ok": True, "bot": result})
                return
            if path == "/api/control/stop":
                result = app.stop_controlled(self.config_path, context=self.app_context)
                store, _ = app.store_for_config(self.config_path)
                runtime = store.runtime()
                if not runtime.get("safe_manual"):
                    runtime = store.set_mode("PAUSED", "dashboard_stop")
                self._send_json({"ok": True, "bot": result, "runtime": runtime})
                return
            self._send_api_error("NOT_FOUND", "Not found", status=404)
        except ApiRequestError as exc:
            self._send_api_error(exc.code, exc, status=exc.status, details=exc.details)
        except Exception as exc:
            self._send_api_error("REQUEST_REJECTED", exc, status=400)

    def _handle_mode(self, app, payload):
        target = str(payload.get("mode", "")).strip().upper()
        store, _ = app.store_for_config(self.config_path)
        if target == "PAUSED":
            if app.controlled_running(self.config_path, self.app_context):
                app.stop_controlled(self.config_path, context=self.app_context)
            runtime = store.set_mode("PAUSED", "dashboard_pause")
            self._send_json({"ok": True, "runtime": runtime})
            return
        if target == "REPLAY":
            replay = app.replay_from_store(self.config_path, context=self.app_context)
            self._send_json({"ok": True, "runtime": store.runtime(), "replay": replay})
            return
        if target == "LIVE":
            raise ApiRequestError(
                "LIVE 必须通过 /api/control/preflight 和 /api/control/start 启动",
                "LIVE_PREFLIGHT_REQUIRED",
                400,
            )
        raise ApiRequestError("仅允许切换到 PAUSED、REPLAY", "INVALID_MODE", 400)
