from __future__ import annotations

import http.client
import socket
import sqlite3
from dataclasses import dataclass
from urllib import error

from .bitfinex import BitfinexError


BACKOFF_SECONDS = (30, 60, 120, 300)
REQUIRED_SNAPSHOTS = 2
MINIMUM_GAP_MS = 30_000
HEARTBEAT_TIMEOUT_MS = 300_000


@dataclass(frozen=True)
class RecoveryDecision:
    category: str
    retryable: bool
    manual_required: bool = False


def delay_seconds(attempts: int) -> int:
    return BACKOFF_SECONDS[max(0, min(int(attempts), len(BACKOFF_SECONDS) - 1))]


def classify_error(exc: BaseException) -> RecoveryDecision:
    if isinstance(exc, BitfinexError):
        return RecoveryDecision(exc.category, exc.retryable, exc.manual_required)
    if isinstance(
        exc,
        (
            error.URLError,
            TimeoutError,
            socket.timeout,
            ConnectionError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
        ),
    ):
        return RecoveryDecision("NETWORK_TRANSPORT", True)
    if isinstance(exc, sqlite3.OperationalError):
        if any(value in str(exc).lower() for value in ("locked", "busy")):
            return RecoveryDecision("DATABASE_BUSY", True)
        return RecoveryDecision("DATABASE_ERROR", False, True)
    if isinstance(exc, (TypeError, AssertionError, ValueError)):
        return RecoveryDecision("PROGRAM_ERROR", False, True)
    return RecoveryDecision("UNEXPECTED_RUNTIME_ERROR", False, True)
