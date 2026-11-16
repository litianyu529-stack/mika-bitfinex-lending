"""Measure Bitfinex connectivity through a local proxy without authentication."""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener, proxy_bypass

from websockets.sync.client import connect

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ConnectionDiagnostics import error_fields  # noqa: E402
from bitfinex import APP_VERSION  # noqa: E402


def probe_ws(host, proxy):
    with connect(f"wss://{host}/ws/2", proxy=proxy, open_timeout=8, close_timeout=2) as socket:
        info = json.loads(socket.recv(timeout=8))
        if info.get("event") != "info":
            raise ValueError("unexpected Bitfinex WebSocket greeting")


def probe_rest(opener):
    request = Request(
        "https://api-pub.bitfinex.com/v2/ticker/fUSD",
        headers={"User-Agent": f"Bitfinex-lendingbot/{APP_VERSION} Python", "Accept": "application/json"},
    )
    with opener.open(request, timeout=8) as response:
        result = json.load(response)
        if not isinstance(result, list) or len(result) < 13:
            raise ValueError("unexpected Bitfinex funding ticker")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--duration-seconds", type=int, default=86_400)
    parser.add_argument("--interval-seconds", type=int, default=120)
    args = parser.parse_args()
    proxy = urlsplit(args.proxy)
    if proxy.scheme != "http" or proxy.hostname not in {"127.0.0.1", "::1", "localhost"} or not proxy.port:
        parser.error("provide a local HTTP proxy with a port")
    if args.duration_seconds < 1 or args.interval_seconds < 60:
        parser.error("duration must be positive and interval at least 60 seconds")
    if proxy_bypass("api-pub.bitfinex.com"):
        parser.error("the current proxy bypass list excludes api-pub.bitfinex.com")
    opener = build_opener(ProxyHandler({"https": args.proxy}))
    probes = (
        ("rest_public", lambda: probe_rest(opener)),
        ("ws_public", lambda: probe_ws("api-pub.bitfinex.com", args.proxy)),
        ("ws_account_host", lambda: probe_ws("api.bitfinex.com", args.proxy)),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration_seconds
    with args.output.open("x", encoding="utf-8", buffering=1) as log:
        while time.monotonic() < deadline:
            round_started = time.monotonic()
            for channel, check in probes:
                started = time.monotonic()
                row = {"time": datetime.now(timezone.utc).isoformat(), "channel": channel}
                try:
                    check()
                    row["ok"] = True
                except Exception as exc:
                    row.update(ok=False, **error_fields(exc))
                row["elapsed_ms"] = int((time.monotonic() - started) * 1000)
                log.write(json.dumps(row) + "\n")
            time.sleep(max(0, min(round_started + args.interval_seconds, deadline) - time.monotonic()))


if __name__ == "__main__":
    main()
