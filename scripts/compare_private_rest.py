"""Compare authenticated read transports during a planned Worker maintenance window."""

import argparse
import io
import json
import math
import ssl
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import getproxies, proxy_bypass

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bitfinex import Bitfinex, BitfinexApiError  # noqa: E402
from lendingbot import AppContext, LiveProcessLock, build_settings, parse_args, read_config  # noqa: E402
from RuntimeV3 import LendingRuntimeV3  # noqa: E402


class ComparisonClient(Bitfinex):
    def __init__(self, *args, pool):
        super().__init__(*args)
        self.pool = pool
        self.reuse = False
        self.previous_socket = None
        self.reused = 0

    def _open_request(self, req):
        path = urlsplit(req.full_url)
        if f"{path.scheme}://{path.netloc}" != self.AUTH_BASE_URL or not path.path.startswith("/v2/auth/r/"):
            raise RuntimeError("Comparison permits only Bitfinex authenticated reads")
        if not self.reuse:
            return super()._open_request(req)
        try:
            response = self.pool.request(req.get_method(), req.full_url, body=req.data,
                                         headers=dict(req.header_items()), retries=False, redirect=False,
                                         timeout=self.timeout, preload_content=False)
            try:
                connection = response.connection
                sock = connection.sock if connection else None
                if sock is not None and sock is self.previous_socket:
                    self.reused += 1
                self.previous_socket = sock
                body = response.read()
            finally:
                response.close()
                response.release_conn()
        except self.pool_error as exc:
            raise OSError(type(exc).__name__) from exc
        if not 200 <= response.status < 300:
            raise HTTPError(req.full_url, response.status, response.reason, response.headers, io.BytesIO(body))
        return io.BytesIO(body)


def compare(client, pairs):
    # Reuse the runtime's exact read sequence, response validation and matching rule.
    runtime = object.__new__(LendingRuntimeV3)
    runtime.client = client
    measurements = {"short": [], "reuse": []}
    next_round = time.monotonic()
    for pair in range(pairs):
        for mode in (("short", "reuse") if pair % 2 == 0 else ("reuse", "short")):
            time.sleep(max(0, next_round - time.monotonic()))
            client.reuse = mode == "reuse"
            client.pool.clear()
            client.previous_socket = None
            client.reused = 0
            start = time.monotonic()
            next_round = start + 12
            category = endpoint = None
            try:
                runtime._read_rest_account("fUSD")
            except BitfinexApiError as exc:
                category = exc.category
                endpoint = getattr(exc, "rest_request", {}).get("endpoint")
            result = {"pair": pair + 1, "transport": mode, "matched": category is None,
                      "elapsedMs": round((time.monotonic() - start) * 1000),
                      "reusedRequests": client.reused, "category": category, "endpoint": endpoint}
            print(json.dumps(result), flush=True)
            measurements[mode].append(result)
    for mode, results in measurements.items():
        times = sorted(row["elapsedMs"] for row in results if row["matched"])
        print(json.dumps({"transport": mode, "rounds": len(results), "failed": len(results) - len(times),
                          "p95SuccessMs": times[math.ceil(len(times) * .95) - 1] if times else None,
                          "reusedRequests": sum(row["reusedRequests"] for row in results)}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="default.cfg")
    parser.add_argument("--pairs", type=int, default=10)
    args = parser.parse_args()
    try:
        import urllib3
    except ImportError:
        parser.error("Install urllib3 to run this optional comparison")
    if args.pairs < 1:
        parser.error("--pairs must be positive")
    context = AppContext.for_project(Path(__file__).resolve().parents[1], config_path=args.config)
    if not Path(context.config_path).is_file():
        parser.error("Config file does not exist")
    config, _ = read_config(context.config_path)
    settings = build_settings(parse_args(["--config", context.config_path]), config)
    hostname = urlsplit(Bitfinex.AUTH_BASE_URL).hostname
    proxy = None if proxy_bypass(hostname) else getproxies().get("https")
    options = {"ssl_context": ssl.create_default_context(), "maxsize": 1}
    pool = urllib3.ProxyManager(proxy, **options) if proxy else urllib3.PoolManager(**options)
    client = ComparisonClient(settings.api_key, settings.api_secret, pool=pool)
    client.pool_error = urllib3.exceptions.HTTPError
    if not client.has_credentials():
        parser.error("Bitfinex credentials are not configured")
    lock = LiveProcessLock(context.live_lock_path)
    if not lock.acquire(context.config_path, {"role": "private_rest_comparison"}):
        parser.error("The LIVE Worker must be stopped for this comparison to avoid competing nonces")
    try:
        compare(client, args.pairs)
    finally:
        pool.clear()
        lock.release()


if __name__ == "__main__":
    main()
