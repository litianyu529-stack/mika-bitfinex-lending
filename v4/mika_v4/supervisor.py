from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .config import V4Settings, atomic_write, policy_payload
from .domain import RuntimeMode
from .recovery import HEARTBEAT_TIMEOUT_MS
from .runtime import LendingRuntime
from .store import V4Store


class V4Supervisor:
    """Dashboard-owned supervisor for the independent V4 worker process."""

    def __init__(self, settings: V4Settings) -> None:
        self.settings = settings
        self.policy = settings.policy
        self.store = V4Store(settings.state_db)
        self.runtime = LendingRuntime(settings, store=self.store)
        self.session_id = uuid.uuid4().hex
        self.process: subprocess.Popen[bytes] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._requested_stop = False
        self._restart_pending = False
        self._live_authorized = False
        self._started_at_ms = 0
        self._build_fingerprint = self._fingerprint_build()
        self._config_fingerprint = self._fingerprint_file(settings.config_file)
        if self.store.mode() == RuntimeMode.LIVE:
            # A Dashboard restart creates a new session, so the previous
            # session's temporary LIVE recovery authorization is invalid.
            self.store.set_mode(RuntimeMode.PAUSED)
            self.store.record_event("WARNING", "DASHBOARD_SESSION_INVALIDATED_LIVE", {})

    @staticmethod
    def _fingerprint_file(path: Path) -> str:
        try:
            return hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            return "missing"

    def _fingerprint_build(self) -> str:
        root = Path(__file__).resolve().parents[1]
        paths = [root / "main.py", *sorted((root / "mika_v4").glob("*.py"))]
        digest = hashlib.sha256()
        for path in paths:
            digest.update(str(path.relative_to(root)).encode())
            try:
                digest.update(path.read_bytes())
            except OSError:
                digest.update(b"missing")
        return digest.hexdigest()

    @property
    def stop_request_file(self) -> Path:
        return self.settings.state_db.parent / "v4-worker-stop.json"

    def start_worker(self) -> None:
        if self.process and self.process.poll() is None:
            return
        self._requested_stop = False
        self._restart_pending = False
        try:
            self.stop_request_file.unlink(missing_ok=True)
        except OSError:
            pass
        main_path = Path(__file__).resolve().parents[1] / "main.py"
        command = [
            sys.executable,
            str(main_path),
            "--config",
            str(self.settings.config_file),
            "worker",
            "--session",
            self.session_id,
        ]
        creationflags = 0x08000000 if os.name == "nt" else 0
        self.process = subprocess.Popen(
            command,
            cwd=str(main_path.parent),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        self._started_at_ms = int(time.time() * 1000)
        self.store.record_event(
            "INFO",
            "WORKER_STARTED",
            {"pid": self.process.pid, "session": self.session_id},
        )

    def stop_worker(self, *, preserve_authorization: bool = False) -> None:
        process = self.process
        if process is None or process.poll() is not None:
            self.process = None
            if not preserve_authorization:
                self._live_authorized = False
                self._restart_pending = False
            return
        self._requested_stop = True
        atomic_write(
            self.stop_request_file,
            json.dumps({"pid": process.pid, "session": self.session_id, "requestedAt": int(time.time() * 1000)}),
        )
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        finally:
            self.store.record_event("INFO", "WORKER_STOPPED", {"pid": process.pid})
            self.process = None
            try:
                self.stop_request_file.unlink(missing_ok=True)
            except OSError:
                pass
            if not preserve_authorization:
                self._live_authorized = False

    def _authorization_valid(self) -> bool:
        if self._fingerprint_build() != self._build_fingerprint:
            return False
        return self._fingerprint_file(self.settings.config_file) == self._config_fingerprint

    def _watch(self) -> None:
        while not self._stop.wait(5):
            self._watch_once()

    def _watch_once(self, now_ms: int | None = None) -> None:
        process = self.process
        if process is None:
            if not self._restart_pending:
                return
            if not self._authorization_valid():
                self._restart_pending = False
                self._live_authorized = False
                self.store.set_mode(RuntimeMode.PAUSED)
                self.store.begin_recovery(
                    "BUILD_CONFIG_CHANGED",
                    "build or configuration changed; manual preflight is required",
                    origin_mode=RuntimeMode.PAUSED,
                    target_mode=RuntimeMode.PAUSED,
                    manual_required=True,
                )
                return
            recovery = self.store.recovery_status()
            now = int(now_ms if now_ms is not None else time.time() * 1000)
            next_probe = recovery.get("nextProbeAt")
            target = recovery.get("targetMode")
            authorized = target != RuntimeMode.LIVE.value or self._live_authorized
            if (
                recovery["active"]
                and not recovery["manualRequired"]
                and authorized
                and (next_probe is None or now >= int(next_probe))
            ):
                self.start_worker()
            return
        if not self._authorization_valid():
            self.stop_worker()
            self.store.set_mode(RuntimeMode.PAUSED)
            self.store.begin_recovery(
                "BUILD_CONFIG_CHANGED",
                "build or configuration changed; manual preflight is required",
                origin_mode=RuntimeMode.PAUSED,
                target_mode=RuntimeMode.PAUSED,
                manual_required=True,
            )
            return
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        recovery = self.store.recovery_status()
        heartbeat = recovery.get("heartbeatAt")
        stale = (
            process.poll() is None
            and now - self._started_at_ms >= HEARTBEAT_TIMEOUT_MS
            and (heartbeat is None or now - int(heartbeat) >= HEARTBEAT_TIMEOUT_MS)
        )
        if stale:
            self.store.enter_safe("worker heartbeat has been missing for five minutes", category="WORKER_HEARTBEAT")
            self.stop_worker(preserve_authorization=True)
            self._restart_pending = True
            return
        if process.poll() is None:
            return
        exit_code = process.returncode
        self.process = None
        if self._requested_stop:
            self._requested_stop = False
            return
        recovery = self.store.recovery_status()
        target = recovery.get("targetMode")
        authorized = target != RuntimeMode.LIVE.value or self._live_authorized
        if recovery["active"] and not recovery["manualRequired"] and authorized:
            self.store.record_event("WARNING", "WORKER_RECOVERABLE_EXIT", {"exit_code": exit_code})
            next_probe = recovery.get("nextProbeAt")
            if next_probe is None or now >= int(next_probe):
                self.start_worker()
            else:
                self._restart_pending = True
        else:
            self._live_authorized = False
            self.store.enter_safe(
                f"worker exited unexpectedly with code {exit_code}",
                category="WORKER_UNEXPECTED_EXIT",
                manual_required=True,
            )

    def start(self) -> None:
        self.start_worker()
        self._thread = threading.Thread(target=self._watch, daemon=True, name="v4-worker-supervisor")
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self.stop_worker()
        if self._thread:
            self._thread.join(timeout=10)

    def status_payload(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.settings.status_file.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            payload = self.runtime.status_payload()
        payload["runtime"] = self.store.status_payload()["runtime"]
        payload["recovery"] = self.store.recovery_status()
        payload["worker"] = {
            "pid": self.process.pid if self.process and self.process.poll() is None else None,
            "running": bool(self.process and self.process.poll() is None),
            "session": self.session_id,
            "liveRecoveryAuthorized": self._live_authorized,
        }
        return payload

    def enable_live(self, confirmation: str) -> None:
        self.stop_worker(preserve_authorization=True)
        try:
            self.runtime.enable_live(confirmation, acquire_lock=False)
        except Exception:
            self.start_worker()
            raise
        self._live_authorized = True
        self.start_worker()

    def disable_live(self, mode: RuntimeMode = RuntimeMode.SHADOW) -> None:
        self.stop_worker()
        self.runtime.disable_live(mode)
        self.start_worker()

    def adopt_external(self, offer_ids: list[int], confirmations: dict[str, str]) -> int:
        self.runtime.last_account = self.runtime.client.account_snapshot(self.policy.currency)
        return self.runtime.adopt_external(offer_ids, confirmations)

    def config_payload(self) -> dict[str, Any]:
        return policy_payload(self.policy)
