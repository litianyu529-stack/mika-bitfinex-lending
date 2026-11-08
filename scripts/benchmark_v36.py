"""Audit an explicitly isolated database and benchmark public market data only.

Never pass a production database: migration is intentionally restricted to a
validation-v36 directory and an isolated-v36.sqlite3 filename. No exchange client,
credentials, account amounts or public network requests are used.
"""

import argparse
import copy
import hashlib
import json
import math
import sqlite3
import sys
import threading
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from MarketHistory import HISTORY_MS, MINUTE_MS, MarketHistoryService  # noqa: E402
from StateStore import LendingStateStore  # noqa: E402
from StrategyV3 import (  # noqa: E402
    EXECUTABLE_QUICK_MODE, StrategyPolicyV3, build_market_signals_v3,
    build_strategy_plan_v3, policy_v3_with_overrides,
)


CRITICAL_TABLES = (
    "order_intents", "offers", "credits", "funding_trades", "credit_closures", "offer_history",
    "credit_history", "reprice_events", "reprice_chains", "ownership_events", "recovery_state",
    "consolidation_state", "external_takeover_state", "strategy_versions", "strategy_rollouts",
)


@contextmanager
def readonly(path):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        yield connection
    finally:
        connection.close()


def row_hashes(path, original_columns=None):
    result, columns = {}, {}
    with readonly(path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in CRITICAL_TABLES:
            if table not in tables:
                continue
            names = ((original_columns or {}).get(table)
                     or [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')])
            columns[table] = names
            selected = ",".join(f'"{name}"' for name in names)
            digest, count = hashlib.sha256(), 0
            for row in connection.execute(f'SELECT {selected} FROM "{table}" ORDER BY rowid'):
                digest.update(json.dumps(list(row), separators=(",", ":"), default=str).encode("utf-8") + b"\n")
                count += 1
            result[table] = {"count": count, "sha256": digest.hexdigest()}
        schema = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
    return str(schema), result, columns


def percentiles(values):
    ordered = sorted(values)
    def at(fraction):
        return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] * 1000, 4)
    return {"samples": len(ordered), "medianMs": at(.5), "p95Ms": at(.95), "p99Ms": at(.99)}


def write_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def concurrent_stress(path, output, burst_count=6):
    """Measure the live reader while a real Python thread builds heavy windows.

    This appends to the original report. Its original single-thread stress result
    is retained. Interval coverage is an in-memory benchmark assumption only.
    """
    report = json.loads(output.read_text(encoding="utf-8"))
    with readonly(path) as connection:
        version = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
        if str(version) != "18":
            raise SystemExit("concurrent-only mode requires an already migrated schema 18 copy")
        asof = int(connection.execute("SELECT MAX(mts) FROM market_trades").fetchone()[0]) + 1
        peak = connection.execute(
            "SELECT mts/60000*60000 AS minute,COUNT(*) AS n FROM market_trades "
            "WHERE mts>=? AND mts<? GROUP BY mts/60000 ORDER BY n DESC,minute DESC LIMIT 1",
            (asof - HISTORY_MS, asof),
        ).fetchone()
        book_row = connection.execute(
            "SELECT book_json FROM book_snapshots WHERE mts<? ORDER BY mts DESC LIMIT 1", (asof,),
        ).fetchone()
        stat_row = connection.execute(
            "SELECT payload_json FROM funding_stats WHERE mts<? ORDER BY mts DESC LIMIT 1", (asof,),
        ).fetchone()
    store = LendingStateStore(path)
    active = store.strategy("ACTIVE")
    policy = policy_v3_with_overrides(StrategyPolicyV3(), active["policy"] if active else {})
    policy = replace(policy, quick_pricing_mode=EXECUTABLE_QUICK_MODE)
    book = [] if book_row is None else json.loads(book_row["book_json"])
    stats = [] if stat_row is None else [json.loads(stat_row["payload_json"])]
    peak_minute = int(peak["minute"])
    template = list(store.iter_market_trades(peak_minute, peak_minute + MINUTE_MS))
    history = MarketHistoryService(store, None, clock=lambda: asof / 1000)
    history.snapshot(policy, asof)
    history.process_once(asof, backfill=False)
    initial = history.snapshot(policy, asof)
    initial.update(usable=True, state="READY", status="READY")
    initial["coverage"]["complete"] = True
    fingerprint = initial["policyFingerprint"]
    with history._lock:
        history._published[fingerprint] = initial
    origin = time.monotonic()
    def observed_now():
        return asof + int((time.monotonic() - origin) * 1000)
    started = threading.Event()
    background_times, errors = [], []

    def producer():
        started.set()
        try:
            for iteration in range(burst_count):
                batch = [
                    {**row, "id": f"concurrent-{iteration}-{copy_index}-{row['id']}",
                     "mts": asof - MINUTE_MS + int(row["mts"]) - peak_minute}
                    for copy_index in range(10) for row in template
                ]
                begin = time.perf_counter()
                history._rolling.add(batch)
                tick = observed_now()
                snapshot = history._rolling.snapshot(
                    policy, tick, coverage_complete=True, usable=True, state="READY", status="READY",
                )
                snapshot["computedAtMs"] = observed_now()
                with history._lock:
                    history._published[fingerprint] = snapshot
                background_times.append(time.perf_counter() - begin)
        except Exception as exc:
            errors.append(str(exc))

    worker = threading.Thread(target=producer, name="isolated-v36-10x-statistics")
    worker.start()
    started.wait(5)
    reads, live, ages, states = [], [], [], {}
    while worker.is_alive() or len(reads) < 200:
        begin = time.perf_counter()
        now = observed_now()
        snapshot = history.snapshot(policy, now)
        reads.append(time.perf_counter() - begin)
        signals = build_market_signals_v3(book, [], stats, policy, now, window_snapshot=snapshot)
        build_strategy_plan_v3(Decimal(10000), Decimal(10000), {}, policy, signals)
        live.append(time.perf_counter() - begin)
        ages.append(max(0, now - int(snapshot["asOfMs"])))
        states[snapshot["state"]] = states.get(snapshot["state"], 0) + 1
        if len(reads) >= 5000:
            break
    worker.join(timeout=60)
    read_metrics, live_metrics = percentiles(reads), percentiles(live)
    result = {
        "method": "real producer thread and continuous live reader; monotonic wall-clock snapshot aging",
        "syntheticPlanningPrincipal": 10000,
        "bursts": burst_count, "inputTradesPerBurst": len(template) * 10,
        "inputTiming": "compressed repeated 10x busiest-minute bursts; not a 30-minute wall-clock sleep",
        "backgroundStatistics": percentiles(background_times) if background_times else None,
        "liveSnapshotRead": read_metrics, "liveSnapshotSignalsAndPlan": live_metrics,
        "snapshotFreshness": {"maxAgeMs": max(ages), "stateCounts": states,
                              "allWithin60Seconds": max(ages) <= 60_000},
        "backgroundStillAlive": worker.is_alive(), "backgroundErrors": errors,
        "coverageAssumption": "memory only; actual persisted coverage remains unverified",
        "acceptance": {"snapshotReadP95Below50Ms": read_metrics["p95Ms"] < 50,
                       "liveSnapshotSignalsAndPlanP95Below1Second": live_metrics["p95Ms"] < 1000,
                       "publishedSnapshotsStayFresh": max(ages) <= 60_000 and set(states) == {"READY"},
                       "backgroundCompleted": not worker.is_alive() and not errors},
    }
    report["performance"]["concurrentStress"] = result
    write_report(output, report)
    print(json.dumps({"phase": "concurrent_complete", "concurrentStress": result}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", default=".state/research/v3.6-performance-and-migration.json")
    parser.add_argument("--stress-minutes", type=int, default=30)
    parser.add_argument("--concurrent-only", action="store_true")
    args = parser.parse_args()
    path, output = Path(args.database).resolve(), Path(args.output).resolve()
    if path.name != "isolated-v36.sqlite3" or not path.parent.name.startswith("validation-v36"):
        parser.error("only an isolated-v36.sqlite3 inside validation-v36* may be migrated")
    if not path.is_file():
        parser.error("isolated database does not exist")
    if args.concurrent_only:
        concurrent_stress(path, output)
        return
    if not 1 <= args.stress_minutes <= 120:
        parser.error("stress minutes must be between 1 and 120")
    before_schema, before_hashes, columns = row_hashes(path)
    print(json.dumps({"phase": "migration_start", "schema": before_schema}), flush=True)
    started = time.perf_counter()
    store = LendingStateStore(path)
    migration_seconds = time.perf_counter() - started
    after_schema, after_hashes, _ = row_hashes(path, columns)
    with readonly(path) as connection:
        quick_check = [row[0] for row in connection.execute("PRAGMA quick_check")]
        asof = int(connection.execute("SELECT MAX(mts) FROM market_trades").fetchone()[0]) + 1
        start = asof - HISTORY_MS
        row_count = connection.execute(
            "SELECT COUNT(*) FROM market_trades WHERE mts>=? AND mts<?", (start, asof),
        ).fetchone()[0]
        peak = connection.execute(
            "SELECT mts/60000*60000 AS minute,COUNT(*) AS n FROM market_trades "
            "WHERE mts>=? AND mts<? GROUP BY mts/60000 ORDER BY n DESC,minute DESC LIMIT 1", (start, asof),
        ).fetchone()
        book_row = connection.execute(
            "SELECT book_json FROM book_snapshots WHERE mts<? ORDER BY mts DESC LIMIT 1", (asof,),
        ).fetchone()
        stat_row = connection.execute(
            "SELECT payload_json FROM funding_stats WHERE mts<? ORDER BY mts DESC LIMIT 1", (asof,),
        ).fetchone()
    report = {
        "schemaVersion": 1, "scope": "isolated copy only; no exchange calls or live process changes",
        "database": str(path), "migration": {
            "fromSchema": before_schema, "toSchema": after_schema, "seconds": round(migration_seconds, 3),
            "backupPath": store.migration_backup_path, "quickCheck": quick_check,
            "unverifiedEarlierBackupArtifacts": [
                {"path": str(artifact), "bytes": artifact.stat().st_size,
                 "status": "UNVERIFIED_PARTIAL_FROM_INTERRUPTED_BENCHMARK"}
                for artifact in (path.parent / "backups").glob("schema-v16-*.sqlite3")
                if str(artifact) != str(store.migration_backup_path)
            ],
            "originalColumnsCompared": columns, "before": before_hashes, "after": after_hashes,
            "criticalRowsPreserved": before_hashes == after_hashes,
        },
        "marketInput": {"startMs": start, "asOfMs": asof, "sevenDayRawCount": row_count,
                        "peakMinuteRawCount": peak["n"], "coverageProof": store.market_coverage(start, asof)},
        "notes": [
            "Only public market samples and hashed account-state rows are included in this report.",
            "Historical records do not prove REST interval coverage; no COMPLETE coverage was written by this run.",
            "Trusted snapshots used for CPU benchmarking are ephemeral assumptions, never live trading authorization.",
            "Stress is a finite burst of 10 times the busiest observed real minute, not seven days at 10x volume.",
        ],
    }
    write_report(output, report)
    print(json.dumps({"phase": "migration_complete", "schema": after_schema,
                      "criticalRowsPreserved": before_hashes == after_hashes,
                      "quickCheck": quick_check, "sevenDayRawCount": row_count}), flush=True)
    if before_hashes != after_hashes or quick_check != ["ok"]:
        raise SystemExit("isolated migration integrity audit failed")
    active = store.strategy("ACTIVE")
    policy = policy_v3_with_overrides(StrategyPolicyV3(), active["policy"] if active else {})
    policy = replace(policy, quick_pricing_mode=EXECUTABLE_QUICK_MODE)
    book = [] if book_row is None else json.loads(book_row["book_json"])
    stats = [] if stat_row is None else [json.loads(stat_row["payload_json"])]
    history = MarketHistoryService(store, None, clock=lambda: asof / 1000)
    history.snapshot(policy, asof)
    tracemalloc.start()
    started = time.perf_counter()
    history.process_once(asof, backfill=False)
    warm_seconds = time.perf_counter() - started
    actual = history.snapshot(policy, asof)
    _allocated, warm_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    if actual["rawTradeCount"] != row_count:
        raise SystemExit("window count does not match the durable seven-day query")
    trusted = copy.deepcopy(actual)
    trusted.update(usable=True, state="READY", status="READY")
    trusted["coverage"]["complete"] = True
    reads, full_cycle = [], []
    for _ in range(200):
        started = time.perf_counter()
        history.snapshot(policy, asof)
        reads.append(time.perf_counter() - started)
    for _ in range(30):
        started = time.perf_counter()
        signals = build_market_signals_v3(book, [], stats, policy, asof, window_snapshot=trusted)
        build_strategy_plan_v3(Decimal(10000), Decimal(10000), {}, policy, signals)
        full_cycle.append(time.perf_counter() - started)
    peak_minute = int(peak["minute"])
    template = list(store.iter_market_trades(peak_minute, peak_minute + MINUTE_MS))
    stress = []
    for iteration in range(args.stress_minutes):
        tick = asof + (iteration + 1) * MINUTE_MS
        batch = []
        for copy_index in range(10):
            for row in template:
                batch.append({**row, "id": f"stress-{iteration}-{copy_index}-{row['id']}",
                              "mts": tick - MINUTE_MS + int(row["mts"]) - peak_minute})
        started = time.perf_counter()
        history._rolling.add(batch)
        snapshot = history._rolling.snapshot(policy, tick, coverage_complete=True, usable=True)
        signals = build_market_signals_v3(book, [], stats, policy, tick, window_snapshot=snapshot)
        build_strategy_plan_v3(Decimal(10000), Decimal(10000), {}, policy, signals)
        stress.append(time.perf_counter() - started)
        if iteration % 10 == 0:
            print(json.dumps({"phase": "stress", "minute": iteration + 1}), flush=True)
    report["performance"] = {
        "tracemallocUsedForWarmStartOnly": True, "warmStartSeconds": round(warm_seconds, 3),
        "warmPeakAllocatedMiB": round(warm_peak / 1048576, 3),
        "publishedSnapshotRead": percentiles(reads), "fullSignalsAndPlan": percentiles(full_cycle),
        "stress": {"minutes": args.stress_minutes, "inputTradesPerMinute": len(template) * 10,
                   "statisticsPlusSignalsAndPlan": percentiles(stress)},
        "actualHistoryState": actual["state"], "actualCoverageComplete": actual["coverage"]["complete"],
    }
    report["performance"]["acceptance"] = {
        "snapshotReadP95Below50Ms": report["performance"]["publishedSnapshotRead"]["p95Ms"] < 50,
        "signalsAndPlanP95Below1Second": report["performance"]["fullSignalsAndPlan"]["p95Ms"] < 1000,
        "warmPeakBelow256MiB": warm_peak < 256 * 1048576,
        "stressStatisticsAndPlanP95Below1Second":
            report["performance"]["stress"]["statisticsPlusSignalsAndPlan"]["p95Ms"] < 1000,
    }
    write_report(output, report)
    print(json.dumps({"phase": "complete", "performance": report["performance"]}), flush=True)


if __name__ == "__main__":
    main()
