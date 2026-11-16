"""Bounded, local diagnostics for exchange connections and worker recovery."""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from urllib.error import URLError


def error_fields(error):
    """Keep structured exception names and codes only, never error text or URLs."""
    details = {}
    kinds = []
    while isinstance(error, BaseException) and len(kinds) < 5:
        kinds.append(type(error).__name__)
        for attribute in ("errno", "winerror", "status_code", "code"):
            value = getattr(error, attribute, None)
            if type(value) is int:
                details.setdefault(attribute, value)
        response_status = getattr(getattr(error, "response", None), "status_code", None)
        if type(response_status) is int:
            details.setdefault("status_code", response_status)
        close_code = getattr(getattr(error, "rcvd", None), "code", None)
        if isinstance(close_code, int):
            details.setdefault("close_code", int(close_code))
        if isinstance(error, URLError) and isinstance(error.reason, BaseException):
            error = error.reason
        else:
            error = error.__cause__
    return {"errors": kinds, **details}


class _DiagnosticFile(RotatingFileHandler):
    def handleError(self, record):
        # Diagnostics must not change the outcome of exchange operations.
        return


class ConnectionDiagnostics:
    def __init__(self, path, clock=time.time):
        self.clock = clock
        self._lock = threading.Lock()
        self._outages = {}
        self._file = None
        try:
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            self._file = _DiagnosticFile(path, maxBytes=1_000_000, backupCount=3, encoding="utf-8", delay=True)
        except OSError:
            pass

    def _emit(self, channel, event, now, **fields):
        if self._file is None:
            return
        payload = {"time": datetime.fromtimestamp(now, timezone.utc).isoformat(), "pid": os.getpid(),
                   "channel": channel, "event": event, **fields}
        record = logging.LogRecord("connection", logging.INFO, "", 0, json.dumps(payload), (), None)
        self._file.handle(record)

    def failed(self, channel, error, *, code=None, rest=None):
        rest = rest or {}
        key = (channel, rest.get("endpoint"))
        with self._lock:
            now = self.clock()
            started, attempts = self._outages.get(key, (now, 0))
            attempts += 1
            self._outages[key] = (started, attempts)
            fields = error_fields(error)
            if type(code) is int:
                fields["protocol_code"] = code
            self._emit(channel, "failure", now, attempts=attempts, **fields, **rest)

    def connected(self, channel, *, rest=None):
        rest = rest or {}
        key = (channel, rest.get("endpoint"))
        with self._lock:
            outage = self._outages.pop(key, None)
            if outage is not None:
                now = self.clock()
                started, attempts = outage
                self._emit(channel, "recovered", now, attempts=attempts,
                           elapsed_ms=max(0, int((now - started) * 1000)), **rest)

    def worker_rest_failure(self, error, category, *, previous_mode, recovering=False):
        rest = getattr(error, "rest_request", None)
        if rest is not None:
            if previous_mode != "PAUSED":
                effect = "paused"
            else:
                effect = "recovery_probe_failed" if recovering else "already_paused"
            with self._lock:
                self._emit("worker", "rest_failure", self.clock(),
                           category=category, effect=effect, **error_fields(error), **rest)

    def trade_rest_fallback(self, error):
        rest = getattr(error, "rest_request", None)
        if rest is not None:
            with self._lock:
                self._emit("worker", "rest_fallback", self.clock(), **error_fields(error), **rest)

    def worker_recovered(self, started_at_ms, attempts, now_ms):
        with self._lock:
            self._emit("worker", "recovered", now_ms / 1000, attempts=int(attempts or 0),
                       elapsed_ms=max(0, now_ms - int(started_at_ms)))

    def worker_recovery_probe(self, before, after, started_at_ms, finished_at_ms):
        previous = before["lastProbeAt"] or before["startedAt"] or started_at_ms
        resumed = not after["active"] and after["startedAt"] == before["startedAt"]
        with self._lock:
            self._emit("worker", "recovery_probe", finished_at_ms / 1000,
                       category=before["category"], wait_ms=max(0, started_at_ms - previous),
                       probe_elapsed_ms=max(0, finished_at_ms - started_at_ms),
                       attempts=after["attempts"], resumed=resumed,
                       snapshots=after["requiredSnapshots"] if resumed else after["successfulSnapshots"])

    def worker_started(self):
        with self._lock:
            self._emit("worker", "started", self.clock())
