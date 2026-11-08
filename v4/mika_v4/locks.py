from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from typing import IO


class LiveLockError(RuntimeError):
    pass


class CrossVersionLiveLock:
    """Advisory lock shared with V3 at repository/.state/lendingbot-live.lock."""

    def __init__(self, repository_root: Path, owner: str = "v4") -> None:
        self.path = Path(repository_root) / ".state" / "lendingbot-live.lock"
        self.owner = owner
        self._handle: IO[bytes] | None = None

    @property
    def held(self) -> bool:
        return self._handle is not None

    def acquire(self) -> None:
        if self.held:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b", buffering=0)
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise LiveLockError(f"lock is already held: {self.path.name}") from exc
        handle.seek(1)
        handle.truncate()
        handle.write(
            json.dumps(
                {
                    "owner": self.owner,
                    "pid": os.getpid(),
                    "host": socket.gethostname(),
                    "acquired_at_ms": int(time.time() * 1000),
                },
                ensure_ascii=False,
            ).encode("utf-8")
        )
        handle.flush()
        os.fsync(handle.fileno())
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None

    def __enter__(self) -> CrossVersionLiveLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


class ProcessLock(CrossVersionLiveLock):
    def __init__(self, path: Path, owner: str) -> None:
        self.path = Path(path)
        self.owner = owner
        self._handle = None
