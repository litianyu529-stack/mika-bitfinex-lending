"""Check coverage, backfill public data, or resume the V3.6 comparison on an isolated copy.

Only the backfill command calls Bitfinex, using public endpoints without credentials.
"""

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from EventReplay import DAY_MS, book_coverage, run_event_replay  # noqa: E402
from StateStore import LendingStateStore  # noqa: E402
from StrategyResearch import (  # noqa: E402
    PublicRateLimiter, ReplaySignalCache, _book_observations, backfill_public_market_data,
    evaluate_strategies, write_research_report,
)
from StrategyV3 import StrategyPolicyV3, policy_v3_with_overrides  # noqa: E402
from V36Validation import validation_status  # noqa: E402
from bitfinex import Bitfinex, BitfinexApiError  # noqa: E402
from lendingbot import LiveProcessLock, worker_build_id  # noqa: E402


def research_end(store, window_path, requested_end):
    if window_path.exists():
        window = json.loads(window_path.read_text(encoding="utf-8"))
        end = window.get("endMs") if isinstance(window, dict) else None
        if type(end) is not int or window.get("startMs") != end - 90 * DAY_MS:
            raise ValueError("The saved research window is invalid.")
        if requested_end is not None and requested_end != end:
            raise ValueError("Use a separate isolated copy to choose a different end time.")
        return end
    if requested_end is None:
        with store.read_connection() as connection:
            last_trade = connection.execute("SELECT MAX(mts) FROM market_trades").fetchone()[0]
        if last_trade is None:
            raise ValueError("The isolated copy has no market trades. Specify --end-ms to backfill an empty copy.")
        requested_end = int(last_trade) + 1
    if requested_end <= 90 * DAY_MS or requested_end > int(time.time() * 1000):
        raise ValueError("--end-ms must define a historical 90-day interval in Unix milliseconds.")
    write_research_report(window_path, {"startMs": requested_end - 90 * DAY_MS, "endMs": requested_end})
    return requested_end


def coverage_summary(store, end):
    start = end - 90 * DAY_MS
    segments = {
        "train": (start, end - 30 * DAY_MS),
        "validation": (end - 30 * DAY_MS, end - 15 * DAY_MS),
        "test": (end - 15 * DAY_MS, end),
    }
    trades = store.market_coverage(start, end)
    books = book_coverage(_book_observations(store, start, end), start, end, segments=segments)
    return {
        "startMs": start, "endMs": end, "tradeCoverageComplete": trades["complete"],
        "verifiedTradeDays": round(trades["coveredDurationMs"] / DAY_MS, 4),
        "missingTradeDays": round(90 - trades["coveredDurationMs"] / DAY_MS, 4),
        "tradeGaps": trades["unresolvedIntervals"], "bookCoverage": books,
        "canReplay": trades["complete"] and all(books[name]["meetsMinimum"] for name in ("validation", "test")),
    }


class ReportingRateLimiter(PublicRateLimiter):
    def __init__(self, interval_seconds, store, start, end, progress):
        super().__init__(interval_seconds)
        self.store = store
        self.start = start
        self.end = end
        self.progress = progress
        self.requests = 0

    def wait(self):
        super().wait()
        self.requests += 1
        if self.requests == 1 or self.requests % 10 == 0:
            coverage = self.store.market_coverage(self.start, self.end)
            self.progress({"phase": "backfill_request", "requests": self.requests,
                           "verifiedTradeDays": round(coverage["coveredDurationMs"] / DAY_MS, 4)})


def replay_identity(path, end, principal, active_version):
    files = {"database": path, "wal": path.with_name(path.name + "-wal")}
    digests = {}
    for name, file in files.items():
        if file.exists() and file.stat().st_size:
            with file.open("rb") as stream:
                digests[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "inputFiles": digests, "endMs": end, "principal": str(principal),
        "activeVersion": active_version, "buildId": worker_build_id(),
        "pythonVersion": sys.version,
    }


def replay_with_checkpoints(store, path, end, principal, progress):
    # Keep the committed input and its WAL stable while hashing and replaying.
    with closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=2)) as guard:
        guard.execute("BEGIN IMMEDIATE")
        coverage = coverage_summary(store, end)
        progress({"phase": "coverage", **coverage})
        if not coverage["canReplay"]:
            raise ValueError("Replay requires verified 90-day trades and enough fresh validation/test books.")
        active = store.strategy("ACTIVE")
        if active is None:
            raise ValueError("The isolated copy must contain the actual baseline policy.")
        policy = policy_v3_with_overrides(StrategyPolicyV3(), active["policy"])
        identity = replay_identity(path, end, principal, active["version_id"])
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        directory = path.parent / "replay-checkpoints" / digest
        write_research_report(directory / "input.json", identity)
        cache = ReplaySignalCache(directory=path.parent)
        reused = 0

        def replay(variant, trades, stats, amount, **kwargs):
            nonlocal reused
            assumptions = asdict(kwargs["assumptions"])
            key = {"assumptions": assumptions, "quickPricingMode": variant.quick_pricing_mode}
            key_digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()
            checkpoint = directory / f"{key_digest}.json"
            if checkpoint.exists():
                result = json.loads(checkpoint.read_text(encoding="utf-8"))
                reused += 1
                progress({"phase": "variant_reused", **key})
                return result
            result = run_event_replay(variant, trades, stats, amount, **kwargs)
            write_research_report(checkpoint, result)
            progress({"phase": "variant_saved", **key})
            return result

        try:
            report = evaluate_strategies(store, policy, principal, now_ms=end, progress=progress,
                                         replay_runner=replay, signal_provider=cache)
            report["data"]["featureCache"] = {
                "hits": cache.hits, "misses": cache.misses, "accountStateShared": False,
            }
        finally:
            cache.close()
    return report, directory, reused, active["version_id"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", help="Report path. Defaults to strategy-validation-report.json beside the copy.")
    parser.add_argument("--principal", type=Decimal, default=Decimal("10000"))
    parser.add_argument("--end-ms", type=int, help="Exclusive UTC end in Unix milliseconds for a new copy.")
    parser.add_argument("--interval-seconds", type=float, default=4.1, help="Minimum interval for public requests.")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check-coverage", action="store_true", help="Print trade gaps and fresh-book coverage.")
    action.add_argument("--backfill", action="store_true", help="Fill trade gaps and verify funding stats.")
    args = parser.parse_args(argv)
    path = Path(args.database).resolve()
    if path.name != "isolated-v36.sqlite3" or not path.parent.name.startswith("validation-v36"):
        parser.error("only isolated-v36.sqlite3 inside validation-v36* may be audited")
    if not path.is_file():
        parser.error("isolated database does not exist")
    if not args.principal.is_finite() or args.principal <= 0:
        parser.error("--principal must be a positive finite amount")
    if not 4.1 <= args.interval_seconds < float("inf"):
        parser.error("--interval-seconds must be at least 4.1")
    output = Path(args.output).resolve() if args.output else path.parent / "strategy-validation-report.json"
    window_path = path.parent / "research-window.json"
    lock_path = path.parent / "research.lock"
    if (output.suffix.lower() != ".json" or output == window_path
            or output.is_relative_to(path.parent / "replay-checkpoints")):
        parser.error("report output must be a separate .json file outside the checkpoint directory")
    started = time.monotonic()

    def progress(item):
        print(json.dumps({"elapsedSeconds": round(time.monotonic() - started, 1), **item},
                         ensure_ascii=False, default=str), flush=True)

    lock = LiveProcessLock(lock_path)
    if not lock.acquire(path, {"role": "isolated_research"}):
        parser.exit(1, "An isolated research command is already using this copy.\n")
    try:
        store = LendingStateStore(path)
        end = research_end(store, window_path, args.end_ms)
        if args.check_coverage or args.backfill:
            progress({"phase": "coverage", **coverage_summary(store, end)})
        if args.check_coverage:
            return 0
        if args.backfill:
            limiter = ReportingRateLimiter(args.interval_seconds, store, end - 90 * DAY_MS, end, progress)
            # Each public retry must pass through the same request limiter.
            result = backfill_public_market_data(Bitfinex(read_retry_delays=()), store, now_ms=end,
                                                 rate_limiter=limiter)
            progress({"phase": "backfill_finished", "tradePages": result["tradePages"],
                      "statsPages": result["statsPages"], "statsCoverageComplete": result["statsCoverage"]["complete"],
                      **coverage_summary(store, end)})
            return 0 if result["complete"] and result["statsCoverage"]["complete"] else 1

        report, checkpoint_dir, reused, active_version = replay_with_checkpoints(
            store, path, end, args.principal, progress)
        report["executionAudit"] = {
            "database": str(path), "scope": "isolated copy; no exchange calls or live activation",
            "elapsedSeconds": round(time.monotonic() - started, 3),
            "baselineVersionId": active_version, "certificateWritten": False,
            "checkpointDirectory": str(checkpoint_dir), "reusedVariants": reused,
        }
        write_research_report(output, report)
        candidate = policy_v3_with_overrides(StrategyPolicyV3(), report["selection"]["candidatePolicy"])
        progress({"phase": "complete", "reportPath": str(output), "selection": report["selection"],
                  "liveCertificate": validation_status(store, candidate)})
        return 0
    except (ValueError, OSError, sqlite3.Error, BitfinexApiError) as error:
        progress({"phase": "error", "error": str(error)})
        return 1
    except KeyboardInterrupt:
        progress({"phase": "interrupted"})
        return 130
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
