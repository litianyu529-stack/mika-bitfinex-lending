"""Public-history correctness without network, credentials or production files."""

import random
import tempfile
import sqlite3
import threading
from contextlib import ExitStack, closing, contextmanager
from collections import deque
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from ExchangeModels import parse_funding_trades
from MarketDataStream import BitfinexMarketDataHub
from MarketHistory import (
    HISTORY_MS,
    MarketHistoryService,
    RollingMarketWindows,
    SharedPublicRateLimiter,
    compute_window_snapshot,
    policy_fingerprint,
)
from StrategyV3 import StrategyPolicyV3 as PolicyV3, _window_statistics, filter_supported_trades


D = Decimal
NOW = 1_800_000_012_345


def trade(identifier, mts=NOW - 1000, amount="100", rate=".0002", period=2):
    return {"id": str(identifier), "mts": mts, "amount": D(amount), "rate": D(rate), "period": period}


class MemoryStore:
    def __init__(self, rows=()):
        self.rows = {str(row["id"]): dict(row) for row in rows}
        self.sequence = len(self.rows)
        self.covered = []
        self.upserts = []
        self.deferred = False
        self._metadata = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
        self._metadata.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY,value TEXT)")
        self._metadata_lock = threading.RLock()

    @contextmanager
    def read_connection(self):
        with self._metadata_lock:
            yield self._metadata

    @contextmanager
    def transaction(self, immediate=False, timeout_seconds=None):
        with self._metadata_lock:
            self._metadata.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self._metadata
                self._metadata.commit()
            except Exception:
                self._metadata.rollback()
                raise

    def market_source_sequence(self):
        return self.sequence

    def upsert_market_trades(self, rows, *, defer_if_busy=False):
        if self.deferred:
            return {"deferred": True}
        self.upserts.append(list(rows))
        for row in rows:
            identifier = str(row["id"])
            if self.rows.get(identifier) != row:
                self.sequence += 1
                self.rows[identifier] = dict(row)
        return {"sourceSequence": self.sequence}

    def iter_market_trades(self, start_ms, end_ms, batch_size=5000):
        for row in sorted(self.rows.values(), key=lambda row: row["mts"]):
            if start_ms <= row["mts"] < end_ms:
                yield row

    def record_market_coverage(self, start_ms, end_ms, status="COMPLETE", source="REST", details=None,
                               **_kwargs):
        self.covered.append({"startMs": start_ms, "endMs": end_ms, "status": status})

    def market_coverage(self, start_ms, end_ms):
        complete = sorted((row["startMs"], row["endMs"]) for row in self.covered
                          if row["status"] == "COMPLETE")
        cursor, gaps = start_ms, []
        for start, end in complete:
            if end <= cursor:
                continue
            if start > cursor:
                gaps.append({"startMs": cursor, "endMs": min(start, end_ms), "status": "MISSING"})
            cursor = max(cursor, end)
            if cursor >= end_ms:
                break
        if cursor < end_ms:
            gaps.append({"startMs": cursor, "endMs": end_ms, "status": "MISSING"})
        unresolved = [row for row in self.covered if row["status"] == "UNRESOLVED"
                      and row["startMs"] < end_ms and row["endMs"] > start_ms]
        return {"complete": not gaps and not unresolved, "unresolvedIntervals": gaps + unresolved,
                "coveredIntervals": [row for row in self.covered if row["status"] == "COMPLETE"]}


@pytest.fixture
def memory_store():
    with ExitStack() as resources:
        def create(rows=()):
            store = MemoryStore(rows)
            resources.callback(store._metadata.close)
            return store

        yield create


class PublicClient:
    def __init__(self, rows=(), error=None):
        self.rows, self.error, self.calls = list(rows), error, []

    def funding_trades(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        if self.error:
            raise self.error
        selected = [row for row in self.rows if kwargs["start"] <= row[1] <= kwargs["end"]]
        return sorted(selected, key=lambda row: row[1])[:kwargs["limit"]]


def service(store, client=None, page_limit=10000):
    return MarketHistoryService(store, client, clock=lambda: NOW / 1000, page_limit=page_limit,
                                rate_limiter=SharedPublicRateLimiter(0))


def test_public_parser_preserves_direction_without_changing_absolute_amount():
    row = parse_funding_trades([[1, NOW, "-150", ".0002", 120]])[0]
    assert row["amount"] == D(150)
    assert row["raw_signed_amount"] == D(-150)


def test_exact_half_open_windows_ignore_future_and_deduplicate_ids():
    rows = [trade("start", NOW - HISTORY_MS), trade("old", NOW - HISTORY_MS - 1),
            trade("end", NOW), trade("recent"), trade("recent")]
    result = compute_window_snapshot(rows, PolicyV3(), NOW)
    assert result["windows"]["7d"]["count"] == 2
    assert result["windows"]["1m"]["count"] == 1
    assert result["windows"]["7d"]["volume"] == D(200)


def test_sparse_term_retains_valid_high_rate_without_claiming_sample_support():
    rows = [trade(i) for i in range(28)] + [trade("high", rate=".1")]
    row = compute_window_snapshot(rows, PolicyV3(), NOW)["byPeriod"][2]
    assert row["quality"]["cleaningMode"] == "SPARSE_UNCLIPPED"
    assert row["maxRate7d"] == D(".1")


def test_effective_sample_size_prevents_one_large_trade_from_enabling_cleaning():
    rows = [trade(i, amount="1") for i in range(29)] + [trade("large", amount="100000", rate=".1")]
    row = compute_window_snapshot(rows, PolicyV3(), NOW)["byPeriod"][2]
    assert row["quality"]["effectiveSampleSize"] < 10
    assert not row["cleaning"]["enabled"]


def test_terms_do_not_clip_each_others_real_premiums():
    rows = [trade(i) for i in range(100)]
    rows += [trade(f"long-{i}", period=120, rate=".001") for i in range(30)]
    result = compute_window_snapshot(rows, PolicyV3(), NOW)
    assert result["byPeriod"][2]["windows"]["7d"]["median"] == D(".0002")
    assert result["byPeriod"][120]["windows"]["7d"]["median"] == D(".001")


def test_price_outlier_exclusion_keeps_raw_demand_count_and_volume():
    rows = [trade(i) for i in range(30)] + [trade("tail", amount="1", rate=".1")]
    row = compute_window_snapshot(rows, PolicyV3(), NOW)["byPeriod"][2]
    assert row["windows"]["7d"]["count"] == 31
    assert row["windows"]["7d"]["volume"] == D(3001)
    assert row["quality"]["priceSampleCount"] == 30
    assert row["quality"]["filteredCount"] == 1


def test_large_trade_support_exception_survives_price_sample_filter():
    rows = [trade(i) for i in range(30)] + [trade("supported", amount="150", rate=".1")]
    row = compute_window_snapshot(rows, PolicyV3(), NOW)["byPeriod"][2]
    assert row["quality"]["priceSampleCount"] == 31
    assert row["quality"]["filteredCount"] == 0


def test_incremental_statistics_match_batch_after_late_data_and_window_rollover():
    randomizer, rolling, seen = random.Random(36), RollingMarketWindows(), []
    start = NOW - HISTORY_MS - 120_000
    for index in range(80):
        now = start + index * 7_700_001
        batch = [trade(f"{index}-{j}", now - randomizer.randrange(120_000),
                       amount=str(randomizer.randrange(1, 1000)),
                       rate=str(D(randomizer.randrange(10, 30)) / D(100000)),
                       period=(2, 14, 120)[j % 3]) for j in range(6)]
        seen.extend(batch)
        rolling.add(batch)
        actual = rolling.snapshot(PolicyV3(), now)
        expected = compute_window_snapshot(seen, PolicyV3(), now)
        assert actual == expected


def test_warm_start_reads_durable_history_and_ready_requires_verified_coverage(memory_store):
    store = memory_store([trade("old", NOW - HISTORY_MS + 1000), trade("now")])
    history, policy = service(store), PolicyV3()
    assert history.snapshot(policy, NOW)["state"] == "WARMING"
    history.process_once(NOW, backfill=False)
    assert history.snapshot(policy, NOW)["state"] == "BACKFILLING"
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history.process_once(NOW, backfill=False)
    snapshot = history.snapshot(policy, NOW)
    assert snapshot["usable"]
    assert snapshot["rawTradeCount"] == 2
    assert snapshot["policyFingerprint"] == policy_fingerprint(policy)
    assert history.snapshot(policy, NOW + 60_001)["state"] == "STALE"


def test_busy_ingest_is_retained_for_retry_and_never_blocks_snapshot(memory_store):
    store, policy = memory_store(), PolicyV3()
    history = service(store)
    history.snapshot(policy, NOW)
    history.ingest([trade(1)], NOW)
    store.deferred = True
    history.process_once(NOW, backfill=False)
    assert not store.rows
    store.deferred = False
    history.process_once(NOW, backfill=False)
    assert list(store.rows) == ["1"]


def test_backfill_splits_full_pages_and_marks_saturated_millisecond_unresolved(memory_store):
    store = memory_store()
    client = PublicClient([[1, NOW - 1, "-10", ".0002", 2], [2, NOW - 1, "20", ".0002", 2]])
    history = service(store, client, page_limit=2)
    history._jobs.append((NOW - 1, NOW))
    history.process_once(NOW)
    assert store.covered == [{"startMs": NOW - 1, "endMs": NOW, "status": "UNRESOLVED"}]
    assert len(store.rows) == 2
    assert client.calls[0][1]["end"] == NOW - 1


def test_backfill_empty_success_is_coverage_and_is_not_a_missing_sample(memory_store):
    store, history = memory_store(), None
    history = service(store, PublicClient())
    policy = PolicyV3()
    history.snapshot(policy, NOW)
    history.process_once(NOW)
    result = history.snapshot(policy, NOW)
    assert result["usable"]
    assert result["coverage"]["complete"]
    assert result["rawTradeCount"] == 0
    assert result["byPeriod"][2]["quality"]["cleaningMode"] == "SPARSE_UNCLIPPED"


def test_public_backfill_failure_preserves_fallback_and_does_not_raise_into_live(memory_store):
    history = service(memory_store(), PublicClient(error=RuntimeError("HTTP 429")))
    policy = PolicyV3()
    history.snapshot(policy, NOW)
    history.process_once(NOW)
    result = history.snapshot(policy, NOW)
    assert not result["usable"]
    assert result["state"] == "DEGRADED"
    assert result["fallbackState"] == "LEGACY_INPUT"


def test_hub_sends_rest_and_websocket_to_one_history_ingestor(memory_store):
    class History:
        def __init__(self):
            self.calls = []

        def ingest(self, trades, **kwargs):
            self.calls.append((trades, kwargs))

    history, store = History(), memory_store()
    hub = BitfinexMarketDataHub(store=store, history_service=history)
    hub.apply_rest_snapshot(trades=[trade(1)], synced_at_ms=NOW)
    hub.handle_public_message({"event": "subscribed", "chanId": 8, "channel": "trades"})
    hub.handle_public_message([8, "fte", [2, NOW - 1000, -20, ".0002", 2]])
    assert not store.upserts
    assert [call[1]["source"] for call in history.calls] == ["REST", "WEBSOCKET"]
    assert history.calls[1][0][0]["raw_signed_amount"] == D(-20)


def test_published_snapshot_is_detached_from_callers(memory_store):
    store, policy = memory_store([trade(1)]), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history = service(store)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    first = history.snapshot(policy, NOW)
    first["windows"]["7d"]["count"] = 999
    assert history.snapshot(policy, NOW)["windows"]["7d"]["count"] == 1


def test_public_read_budget_is_shared_and_nonblocking():
    clock = [0.0]
    limiter = SharedPublicRateLimiter(4.1, clock=lambda: clock[0])
    assert limiter.try_acquire()
    assert not limiter.try_acquire()
    clock[0] = 4.1
    assert limiter.try_acquire()
    limiter.defer(30)
    clock[0] = 10
    assert not limiter.try_acquire()


def test_stop_flushes_accepted_trades_without_starting_worker_or_network(memory_store):
    store = memory_store()
    client = PublicClient(error=AssertionError("shutdown must not perform network reads"))
    history = service(store, client)
    history.ingest([trade(1)], NOW)
    assert history.stop(timeout=1)
    assert set(store.rows) == {"1"}
    assert not client.calls
    assert history._thread is None


def test_stop_reports_busy_flush_and_keeps_batch_for_a_later_retry(memory_store):
    store = memory_store()
    history = service(store)
    history.ingest([trade(1)], NOW)
    store.deferred = True
    assert not history.stop(timeout=1)
    assert history._queued_count == 1
    store.deferred = False
    assert history.stop(timeout=1)
    assert history._queued_count == 0
    assert "1" in store.rows


def test_websocket_burst_is_written_in_batches(memory_store):
    store = memory_store()
    history = service(store)
    for index in range(500):
        assert history.ingest([trade(index)], NOW)
    history.process_once(NOW, backfill=False)
    assert len(store.upserts) == 1
    assert len(store.rows) == 500


def test_raw_parser_and_ingest_reject_nonfinite_public_values(memory_store):
    assert parse_funding_trades([[1, NOW, "10", "NaN", 2], [2, NOW, "Infinity", ".1", 2]]) == []
    store, history = memory_store(), None
    history = service(store)
    history.ingest([trade(1, rate="NaN"), trade(2)], NOW)
    history.process_once(NOW, backfill=False)
    assert set(store.rows) == {"2"}
    assert history._invalid_records == 1


def test_policy_snapshots_remain_bound_to_their_registered_configuration(memory_store):
    store = memory_store([trade(1)])
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history, first = service(store), PolicyV3()
    second = replace(first, outlier_min_volume_share=D(".01"))
    history.snapshot(first, NOW)
    history.process_once(NOW, backfill=False)
    assert history.snapshot(second, NOW)["state"] == "WARMING"
    history.process_once(NOW, backfill=False)
    assert history.snapshot(first, NOW)["policyFingerprint"] != history.snapshot(second, NOW)["policyFingerprint"]


def test_real_sqlite_warm_start_and_external_late_insert_are_incremental():
    from StateStore import LendingStateStore

    with tempfile.TemporaryDirectory() as directory:
        store = LendingStateStore(Path(directory) / "isolated.sqlite3", clock=lambda: NOW / 1000)
        store.upsert_market_trades([trade("original", NOW - HISTORY_MS + 1000)])
        store.record_market_coverage(NOW - HISTORY_MS, NOW)
        history, policy = service(store), PolicyV3()
        history.snapshot(policy, NOW)
        history.process_once(NOW, backfill=False)
        assert history.snapshot(policy, NOW)["windows"]["7d"]["count"] == 1
        store.upsert_market_trades([trade("late", NOW - 3 * 86_400_000, period=120)])
        history.process_once(NOW, backfill=False)
        result = history.snapshot(policy, NOW)
        assert result["windows"]["7d"]["count"] == 2
        assert result["byPeriod"][120]["rawTradeCount"] == 1
        # Durable public IDs are immutable; a conflicting transport duplicate
        # must not make the in-memory window diverge from SQLite.
        history.ingest([trade("late", NOW - 3 * 86_400_000, amount="9999", period=120)], NOW)
        history.process_once(NOW, backfill=False)
        assert history.snapshot(policy, NOW)["byPeriod"][120]["windows"]["7d"]["volume"] == D(100)


def test_malformed_response_never_proves_an_empty_complete_window(memory_store):
    class BrokenClient:
        def funding_trades(self, *_args, **_kwargs):
            return None

    store, history = memory_store(), None
    history = service(store, BrokenClient())
    policy = PolicyV3()
    history.snapshot(policy, NOW)
    history.process_once(NOW)
    assert not store.covered
    assert not history.snapshot(policy, NOW)["usable"]


def test_snapshot_registration_alone_never_starts_worker_or_writes(memory_store):
    store, history = memory_store(), None
    history = service(store, PublicClient())
    history.snapshot(PolicyV3(), NOW)
    assert history._thread is None
    assert not store.upserts


def test_policy_registry_and_published_snapshots_are_bounded_lru_caches(memory_store):
    store, first = memory_store([trade(1)]), PolicyV3()
    history = service(store)
    policies = [replace(first, outlier_min_volume_share=D(index + 1) / D(1000)) for index in range(9)]
    for policy in policies[:8]:
        history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    history.snapshot(policies[0], NOW)  # Refresh the first entry, not the second.
    history.snapshot(policies[8], NOW)
    history.process_once(NOW, backfill=False)
    assert len(history._policies) == len(history._published) == 8
    assert policy_fingerprint(policies[0]) in history._published
    assert policy_fingerprint(policies[1]) not in history._policies


def test_incremental_legacy_global_matches_v35_filter_and_every_window():
    randomizer, rolling, seen = random.Random(3500), RollingMarketWindows(cleaning_mode="legacy_global"), []
    for step in range(12):
        now = NOW + step * 3_600_000
        batch = [trade(f"{step}-{index}", now - randomizer.randrange(3_600_000),
                       amount=str(randomizer.randrange(1, 400)), rate=str(D(index % 20 + 1) / D(10000)),
                       period=(2, 7, 14, 30, 120)[index % 5]) for index in range(70)]
        batch.append(trade(f"same-timestamp-{step}", now, amount="150", rate=".1", period=120))
        seen.extend(batch)
        rolling.add(batch)
        snapshot = rolling.snapshot(PolicyV3(), now, coverage_complete=False)
        filtered = filter_supported_trades(seen, PolicyV3())
        expected = _window_statistics(filtered, now)
        for name, values in expected.items():
            assert {key: snapshot["windows"][name][key] for key in values} == values
        for period, actual in snapshot["byPeriod"].items():
            reference = _window_statistics([row for row in filtered if row["period"] == period], now)
            for name, values in reference.items():
                assert {key: actual["windows"][name][key] for key in values} == values


def test_legacy_20000_cap_has_exact_same_timestamp_id_boundary_as_hub():
    rows = [trade(index, NOW) for index in range(20_005)]
    rows += [trade("older", NOW - 1), trade("supported", NOW, amount="1000000", rate=".1")]
    rolling = RollingMarketWindows(max_trades=20_000, cleaning_mode="legacy_global")
    rolling.add(rows)
    result = rolling.snapshot(PolicyV3(), NOW, coverage_complete=False)
    selected = sorted(rows, key=lambda row: (row["mts"], row["id"]))[-20_000:]
    expected = _window_statistics(filter_supported_trades(selected, PolicyV3()), NOW)
    assert len(rolling._rows) == 20_000
    assert result["snapshotKind"] == "LEGACY_GLOBAL_20K"
    for name, values in expected.items():
        assert {key: result["windows"][name][key] for key in values} == values


def test_migration_backup_busy_timeout_preserves_source_and_partial_artifact(monkeypatch, tmp_path):
    import sqlite3
    import StateStore

    path = tmp_path / "isolated.sqlite3"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY,value TEXT)")
        connection.executemany("INSERT INTO schema_meta VALUES(?,?)",
                               [("schema_version", "16"), ("schema_family", "v3")])
    before = path.read_bytes()
    original_connect = sqlite3.connect

    class BusyBackup:
        def __init__(self, connection):
            self.connection = connection

        def backup(self, target, *, pages, progress, sleep):
            assert pages == 1000
            assert sleep <= .1
            progress(sqlite3.SQLITE_BUSY, 0, 0)
            progress(sqlite3.SQLITE_BUSY, 0, 0)

        def close(self):
            self.connection.close()

    def connect(database, *args, **kwargs):
        connection = original_connect(database, *args, **kwargs)
        return BusyBackup(connection) if str(database) == str(path) else connection

    ticks = iter([0, 0, 11])
    monkeypatch.setattr(StateStore.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(StateStore.sqlite3, "connect", connect)
    try:
        StateStore.LendingStateStore(path)
    except StateStore.StateStoreError as exc:
        assert "status=5" in str(exc)
        assert "source remains unchanged" in str(exc)
    else:
        raise AssertionError("busy backup must prevent migration")
    assert path.read_bytes() == before
    assert list((tmp_path / "backups").glob("schema-v16-*.sqlite3"))


def test_migration_backup_allows_slow_continuous_page_progress(monkeypatch, tmp_path):
    import sqlite3
    import StateStore

    path = tmp_path / "slow-isolated.sqlite3"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY,value TEXT)")
        connection.executemany("INSERT INTO schema_meta VALUES(?,?)",
                               [("schema_version", "16"), ("schema_family", "v3")])
    original_connect = sqlite3.connect

    class SlowBackup:
        def __init__(self, connection):
            self.connection = connection

        def backup(self, target, *, pages, progress, sleep):
            for status, remaining in ((sqlite3.SQLITE_OK, 2), (sqlite3.SQLITE_OK, 1), (sqlite3.SQLITE_DONE, 0)):
                progress(status, remaining, 3)

        def close(self):
            self.connection.close()

    def connect(database, *args, **kwargs):
        connection = original_connect(database, *args, **kwargs)
        return SlowBackup(connection) if str(database) == str(path) else connection

    ticks = iter([0, 200, 400, 600])
    monkeypatch.setattr(StateStore.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(StateStore.sqlite3, "connect", connect)
    state = StateStore.LendingStateStore.__new__(StateStore.LendingStateStore)
    state.path, state.migration_backup_path = str(path), None
    state._backup_before_schema_migration()
    assert state.migration_backup_path is not None


def history_markers(store):
    with store.read_connection() as connection:
        return dict(connection.execute("SELECT key,value FROM schema_meta"))


def test_start_persists_running_marker_before_worker_and_clean_stop_clears_it(memory_store):
    store, history = memory_store(), None
    history = service(store)
    observed = []
    history._run = lambda: observed.append(history_markers(store)[history.UNCLEAN_KEY])
    history.start()
    history._thread.join(timeout=1)
    assert observed == ["1"]
    assert history.stop(timeout=1)
    assert history_markers(store)[history.UNCLEAN_KEY] == "0"


def test_unclean_restart_quarantines_full_week_despite_preexisting_complete_coverage(memory_store):
    store, policy = memory_store([trade("durable")]), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    previous = service(store)
    previous._run = lambda: None
    previous.start()
    previous._thread.join(timeout=1)  # Simulate process loss without clean stop.
    restarted = service(store)
    restarted.snapshot(policy, NOW)
    restarted.process_once(NOW, backfill=False)
    result = restarted.snapshot(policy, NOW)
    assert not result["usable"]
    assert not result["coverage"]["complete"]
    assert result["quarantinedIntervals"] == [[NOW - HISTORY_MS, NOW]]
    assert store.market_coverage(NOW - HISTORY_MS, NOW)["complete"]


def test_overflow_immediately_overrides_ready_and_repairs_missing_interval_first(memory_store):
    stamp = NOW - 100
    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    client = PublicClient([["lost", stamp, "-150", ".0002", 2]])
    history = service(store, client)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    assert history.snapshot(policy, NOW)["usable"]
    history._jobs.append((NOW - HISTORY_MS, NOW - HISTORY_MS + 10))
    history._queued_count = 100_000
    assert not history.ingest([trade("lost", stamp, amount="150")], NOW)
    history._queued_count = 0
    assert not history.snapshot(policy, NOW)["usable"]
    history.process_once(NOW)
    assert client.calls[0][1]["start"] == stamp
    assert client.calls[0][1]["end"] == stamp
    assert history.snapshot(policy, NOW)["usable"]
    assert "lost" in store.rows
    assert history_markers(store)[history.PENDING_KEY] == "[]"


def test_quarantine_saturated_millisecond_never_restores_old_complete_proof(memory_store):
    stamp = NOW - 100
    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    client = PublicClient([["a", stamp, "150", ".0002", 2], ["b", stamp, "150", ".0002", 2]])
    history = service(store, client, page_limit=2)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    history._queued_count = 100_000
    assert not history.ingest([trade("a", stamp), trade("b", stamp)], NOW)
    history._queued_count = 0
    history.process_once(NOW)
    result = history.snapshot(policy, NOW)
    assert not result["usable"]
    assert result["quarantinedIntervals"] == [[stamp, stamp + 1]]
    assert history_markers(store)[history.UNCLEAN_KEY] == "1"


def test_busy_shutdown_keeps_durable_unclean_marker_for_next_process(memory_store):
    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history = service(store)
    history._run = lambda: None
    history.start()
    history._thread.join(timeout=1)
    history.ingest([trade("accepted")], NOW)
    store.deferred = True
    assert not history.stop(timeout=1)
    assert history_markers(store)[history.UNCLEAN_KEY] == "1"
    restarted = service(store)
    restarted.snapshot(policy, NOW)
    restarted.process_once(NOW, backfill=False)
    assert not restarted.snapshot(policy, NOW)["usable"]


def test_full_page_repair_only_releases_quarantine_after_all_short_subpages(memory_store):
    left, right = NOW - 1000, NOW - 1
    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    client = PublicClient([["a", left, "150", ".0002", 2], ["b", right, "150", ".0002", 2]])
    history = service(store, client, page_limit=2)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    history._queued_count = 100_000
    history.ingest([trade("a", left), trade("b", right)], NOW)
    history._queued_count = 0
    history.process_once(NOW)
    assert not history.snapshot(policy, NOW)["usable"]
    for _ in range(30):
        history.process_once(NOW)
        if history.snapshot(policy, NOW)["usable"]:
            break
    assert history.snapshot(policy, NOW)["usable"]
    assert not history._quarantine_intervals


def test_publish_timestamp_uses_actual_clock_and_does_not_replace_data_time(memory_store):
    store, policy = memory_store(), PolicyV3()
    history = service(store)
    history.snapshot(policy, NOW)
    history.process_once(NOW - 1000, backfill=False)
    result = history.snapshot(policy, NOW)
    assert result["asOfMs"] == NOW - 1000
    assert result["publishedAtMs"] == NOW


def test_real_sqlite_unclean_restart_requires_fresh_rest_proof_before_reuse():
    from StateStore import LendingStateStore

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "restart-isolated.sqlite3"
        store = LendingStateStore(path, clock=lambda: NOW / 1000)
        store.upsert_market_trades([trade("durable", NOW - 100)])
        store.record_market_coverage(NOW - HISTORY_MS, NOW)
        previous = service(store)
        previous._run = lambda: None
        previous.start()
        previous._thread.join(timeout=1)
        reopened = LendingStateStore(path, clock=lambda: NOW / 1000)
        client = PublicClient([["durable", NOW - 100, "100", ".0002", 2]])
        history, policy = service(reopened, client), PolicyV3()
        history.snapshot(policy, NOW)
        history.process_once(NOW, backfill=False)
        assert not history.snapshot(policy, NOW)["usable"]
        history.process_once(NOW)
        assert client.calls[0][1]["start"] == NOW - HISTORY_MS
        assert history.snapshot(policy, NOW)["usable"]
        assert history.stop(timeout=1)
        assert history_markers(reopened)[history.UNCLEAN_KEY] == "0"


def test_overflow_during_inflight_repair_cannot_be_cleared_by_older_rest_result(memory_store):
    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history = service(store)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    history._queued_count = 100_000
    history.ingest([trade("first", NOW - 500)], NOW)
    history._queued_count = 0

    class ConcurrentDrop:
        def funding_trades(self, *_args, **_kwargs):
            history._queued_count = 100_000
            history.ingest([trade("second", NOW - 400)], NOW)
            history._queued_count = 0
            return []

    history.public_client = ConcurrentDrop()
    history.process_once(NOW)
    result = history.snapshot(policy, NOW)
    assert not result["usable"]
    assert [NOW - 400, NOW - 399] in result["quarantinedIntervals"]


def test_overflow_is_closed_before_timestamp_scan_and_cannot_clear_during_registration(monkeypatch, memory_store):
    import MarketHistory

    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history = service(store)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    assert history.snapshot(policy, NOW)["usable"]
    original, observed = MarketHistory._normalized_trade, []

    def during_scan(raw):
        if not observed:
            observed.append(history.snapshot(policy, NOW)["usable"])
            history.process_once(NOW, backfill=False)
            observed.append(history.snapshot(policy, NOW)["usable"])
        return original(raw)

    monkeypatch.setattr(MarketHistory, "_normalized_trade", during_scan)
    history._queued_count = 100_000
    assert not history.ingest([trade("dropped")], NOW)
    history._queued_count = 0
    assert observed == [False, False]
    assert history._quarantine_registrations == 0
    assert not history.snapshot(policy, NOW)["usable"]


def test_expired_quarantine_does_not_block_new_window_but_boundary_millisecond_still_does(memory_store):
    store, policy = memory_store(), PolicyV3()
    store.record_market_coverage(NOW - HISTORY_MS, NOW + 1)
    history = service(store)
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    cutoff = NOW - HISTORY_MS
    history._quarantine([[cutoff - 10, cutoff]], "OLD_SATURATED_INTERVAL")
    history.process_once(NOW, backfill=False)
    assert history.snapshot(policy, NOW)["usable"]
    history._quarantine([[cutoff, cutoff + 1]], "BOUNDARY_SATURATED_INTERVAL")
    history.process_once(NOW, backfill=False)
    assert not history.snapshot(policy, NOW)["usable"]
    history.process_once(NOW + 1, backfill=False)
    assert history.snapshot(policy, NOW + 1)["usable"]
    assert history_markers(store)[history.PENDING_KEY] == "[]"


def test_repair_keeps_pagination_progress_with_moving_clock_and_public_read_limit(memory_store):
    wall_ms, monotonic = [NOW], [0.0]
    store, policy = memory_store(), PolicyV3()
    store._metadata.execute("INSERT INTO schema_meta VALUES(?,?)", (MarketHistoryService.UNCLEAN_KEY, "1"))
    call_times = []

    class TimedPublicClient(PublicClient):
        def funding_trades(self, symbol, **kwargs):
            call_times.append(monotonic[0])
            return super().funding_trades(symbol, **kwargs)

    client = TimedPublicClient([
        ["a", NOW - 300_000, "150", ".0002", 2],
        ["b", NOW - 200_000, "150", ".0002", 2],
    ])
    history = MarketHistoryService(
        store, client, clock=lambda: wall_ms[0] / 1000, page_limit=2,
        rate_limiter=SharedPublicRateLimiter(4.1, clock=lambda: monotonic[0]),
    )
    history.snapshot(policy, NOW)
    first_complete_at = None
    for seconds in range(301):
        wall_ms[0], monotonic[0] = NOW + seconds * 1000, float(seconds)
        history.process_once(wall_ms[0])
        if store.covered and first_complete_at is None:
            first_complete_at = seconds
        if history.snapshot(policy, wall_ms[0])["usable"]:
            break
    assert first_complete_at is not None
    assert first_complete_at <= 10
    result = history.snapshot(policy, wall_ms[0])
    assert result["state"] == "READY"
    assert 0 <= wall_ms[0] - result["asOfMs"] <= 60_000
    assert store.market_coverage(result["asOfMs"] - HISTORY_MS, result["asOfMs"])["complete"]
    assert history._quarantine_epoch == 1
    assert all(right - left >= 4.1 for left, right in zip(call_times, call_times[1:]))
    widths = [kwargs["end"] - kwargs["start"] + 1 for _symbol, kwargs in client.calls]
    assert min(widths[1:3]) <= HISTORY_MS // 2


def test_expiry_clips_queued_subpages_in_place_without_invalidating_generation(memory_store):
    history = service(memory_store())
    cutoff = NOW - HISTORY_MS
    history._quarantine([[cutoff - 10_000, cutoff + 20_000]], "REVERIFY")
    epoch = history._quarantine_epoch
    jobs = deque([(cutoff - 10_000, cutoff - 5000), (cutoff - 5000, cutoff + 10_000),
                  (cutoff + 10_000, cutoff + 20_000)])
    history._repair_jobs, history._repair_epoch = jobs, epoch
    history._trim_quarantine(NOW)
    assert history._quarantine_epoch == epoch
    assert history._repair_jobs is jobs
    assert list(jobs) == [(cutoff, cutoff + 10_000), (cutoff + 10_000, cutoff + 20_000)]
    assert history._quarantine_intervals == [[cutoff, cutoff + 20_000]]


def test_expiry_during_rest_request_can_accept_proof_for_the_remaining_interval(memory_store):
    store, policy = memory_store(), PolicyV3()
    history = service(store)
    history.snapshot(policy, NOW)
    history._quarantine([[NOW - HISTORY_MS, NOW]], "REVERIFY")
    epoch = history._quarantine_epoch

    class ExpiringClient:
        def funding_trades(self, *_args, **_kwargs):
            history._trim_quarantine(NOW + 1000)
            return []

    history.public_client = ExpiringClient()
    history.process_once(NOW)
    assert history._quarantine_epoch == epoch
    assert history._quarantine_intervals == []
    assert history.snapshot(policy, NOW)["usable"]


def test_new_gap_during_rest_request_still_invalidates_result_after_expiry(memory_store):
    store, policy = memory_store(), PolicyV3()
    history = service(store)
    history.snapshot(policy, NOW)
    history._quarantine([[NOW - HISTORY_MS, NOW]], "REVERIFY")
    epoch = history._quarantine_epoch

    class NewGapClient:
        def funding_trades(self, *_args, **_kwargs):
            history._trim_quarantine(NOW + 1000)
            history._queued_count = 100_000
            history.ingest([trade("new-gap", NOW - 100)], NOW)
            history._queued_count = 0
            return []

    history.public_client = NewGapClient()
    history.process_once(NOW)
    assert history._quarantine_epoch > epoch
    assert history._quarantine_intervals
    assert not history.snapshot(policy, NOW)["usable"]


def test_default_snapshot_observes_clock_after_waiting_for_publisher_lock(memory_store):
    store, policy, wall = memory_store(), PolicyV3(), [NOW]
    store.record_market_coverage(NOW - HISTORY_MS, NOW)
    history = service(store)
    history.clock = lambda: wall[0] / 1000
    history.snapshot(policy, NOW)
    history.process_once(NOW, backfill=False)
    attempted = threading.Event()

    class ObservedLock:
        def __init__(self):
            self.lock = threading.Lock()

        def __enter__(self):
            attempted.set()
            self.lock.acquire()

        def __exit__(self, *_args):
            self.lock.release()

    gate, result = ObservedLock(), []
    history._lock = gate
    gate.lock.acquire()
    reader = threading.Thread(target=lambda: result.append(history.snapshot(policy)))
    reader.start()
    try:
        assert attempted.wait(1)
        wall[0] = NOW + 1000
        history._published[policy_fingerprint(policy)]["asOfMs"] = wall[0]
    finally:
        gate.lock.release()
    reader.join(timeout=1)
    assert not reader.is_alive()
    assert result[0]["state"] == "READY"
    assert result[0]["usable"]
    # Explicit replay/test timestamps retain their strict future-data guard.
    assert history.snapshot(policy, NOW)["state"] == "STALE"


def test_warming_snapshot_default_time_is_observed_after_lock_acquisition(memory_store):
    history, wall = service(memory_store()), [NOW]
    history.clock = lambda: wall[0] / 1000

    class AdvancingLock:
        def __enter__(self):
            wall[0] += 1000

        def __exit__(self, *_args):
            pass

    history._lock = AdvancingLock()
    result = history.snapshot(PolicyV3())
    assert result["state"] == "WARMING"
    assert result["coverage"]["endMs"] == NOW + 1000
