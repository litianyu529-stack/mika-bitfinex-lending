import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from decimal import Decimal

import pytest

from StateStore import EXACT_TERM_EXPLORATION_CURVE, LendingStateStore, StateStoreError
from StrategyV3 import QUICK_EXECUTION_CURVE


D = Decimal
DAY = 86_400_000


def _trade(identity, mts=100, amount="150", raw=None):
    result = {"id": identity, "mts": mts, "amount": D(amount), "rate": D("0.0003"), "period": 120}
    if raw is not None:
        result["raw_signed_amount"] = D(raw)
    return result


def _downgrade_fixture_to_16(store):
    """Make a real pre-V3.6 layout, not just a mismatched version number."""
    with store.transaction(immediate=True) as connection:
        connection.execute("DROP INDEX market_trades_mts_id_idx")
        connection.execute("DROP INDEX book_snapshots_visible_idx")
        connection.execute("ALTER TABLE market_trades DROP COLUMN raw_signed_amount")
        connection.execute("ALTER TABLE market_trades DROP COLUMN source_sequence")
        connection.execute("ALTER TABLE book_snapshots DROP COLUMN observed_at_ms")
        connection.execute("DROP TABLE market_dirty_minutes")
        connection.execute("DROP TABLE market_coverage_intervals")
        connection.execute("UPDATE schema_meta SET value='16' WHERE key='schema_version'")
        connection.execute(
            "DELETE FROM schema_meta WHERE key IN "
            "('schema_family','v36_initialization_complete','market_source_sequence')"
        )


def test_new_v36_database_is_not_automatically_activated(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: 123)
    with store.read_connection() as connection:
        meta = dict(connection.execute("SELECT key,value FROM schema_meta"))
    assert meta["schema_version"] == "19"
    assert meta["schema_family"] == "v3"
    assert store.migration_backup_path is None
    assert store.release_boundary()["v36ActivatedAtMs"] is None
    assert store.release_boundary()["baselineLabel"] == "V3.5"
    assert store.market_source_sequence() == 0
    assert EXACT_TERM_EXPLORATION_CURVE == "EXACT_TERM_EXPLORATION_V4"

    assert store.record_v36_activation({"validation": "PASS"}, now_ms=150_000) == 150_000
    assert store.record_v36_activation({"validation": "again"}, now_ms=200_000) == 150_000
    assert store.release_boundary()["activatedAtMs"] == 123_000
    assert store.release_boundary()["v36ActivatedAtMs"] == 150_000
    with store.read_connection() as connection:
        rows = connection.execute("SELECT * FROM strategy_events WHERE event_type='V36_ACTIVATED'").fetchall()
    assert len(rows) == 1
    assert json.loads(rows[0]["reason"]) == {"validation": "PASS"}


def test_schema_16_migration_backs_up_and_preserves_account_state_and_chains(tmp_path):
    path = tmp_path / "state.db"
    store = LendingStateStore(path, clock=lambda: 123)
    store.set_mode("LIVE", "fixture")
    store.upsert_market_trades([_trade("legacy")])
    store.record_book_snapshot([["0.0003", 120, 1, -150]], now_ms=90_000)
    with store.transaction(immediate=True) as connection:
        connection.execute(
            """INSERT INTO order_intents(
                fingerprint,currency,slice_key,pool,layer,amount,submitted_rate,effective_rate,
                period,offer_type,flags,strategy_version,state,exchange_offer_id,
                pricing_curve_version,fixed_landing_rate,created_at_ms,updated_at_ms
            ) VALUES('old','USD','v:short:balanced:2','short','balanced','150','.0003','.0003',
                2,'LIMIT',0,'v','OPEN',778,'EXACT_TERM_EXPLORATION_V4','.00025',100,100)"""
        )
        connection.execute(
            """INSERT INTO reprice_chains(
                chain_key,strategy_version,base_slice_key,pool,layer,origin_rate,started_at_ms,
                current_stage,current_offer_id,pending_action,pending_target_rate,pending_source_offer_id,
                fixed_landing_rate,pricing_curve_version,status,updated_at_ms
            ) VALUES('chain','v','v:short:balanced:2','short','balanced','.0003',100,4,
                NULL,'REPRICE','.00026',778,'.00025','EXACT_TERM_EXPLORATION_V4','ACTIVE',200)"""
        )
        connection.execute("UPDATE recovery_state SET active=1, attempts=3, resume_pending_cycle=1")
        connection.execute(
            "INSERT INTO ownership_events(offer_id,event_type,details_json,created_at_ms) VALUES(778,'OWNED','{}',100)"
        )
        # Preserve an already established boundary exactly, rather than inventing a cutover.
        connection.execute("INSERT OR REPLACE INTO schema_meta VALUES('release_0.3.5_boundary_evidence_at_ms','100')")
    _downgrade_fixture_to_16(store)
    tables = ("runtime_state", "recovery_state", "order_intents", "reprice_chains", "ownership_events")
    with store.read_connection() as connection:
        before = {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")] for table in tables}
        boundary = dict(connection.execute("SELECT key,value FROM schema_meta WHERE key LIKE 'release_0.3.5%'")).copy()

    upgraded = LendingStateStore(path, clock=lambda: 999)
    assert upgraded.migration_backup_path is not None
    with closing(sqlite3.connect(upgraded.migration_backup_path)) as backup:
        assert backup.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0] == "16"
        assert "raw_signed_amount" not in {row[1] for row in backup.execute("PRAGMA table_info(market_trades)")}
    with upgraded.read_connection() as connection:
        after = {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")] for table in tables}
        assert dict(connection.execute("SELECT key,value FROM schema_meta WHERE key LIKE 'release_0.3.5%'")) == boundary
    assert before == after
    assert list(upgraded.iter_market_trades())[0]["raw_signed_amount"] is None
    book = list(upgraded.iter_book_snapshots())[0]
    assert book["observed_at_ms"] is None
    assert book["visible_at_ms"] == 120_000


@pytest.mark.parametrize("version,family", [(17, None), (20, "v3"), (18, "v4"), (18, None)])
def test_v36_refuses_foreign_or_newer_schemas_without_mutation(tmp_path, version, family):
    path = tmp_path / "state.db"
    store = LendingStateStore(path)
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE schema_meta SET value=? WHERE key='schema_version'", (str(version),))
        if family is None:
            connection.execute("DELETE FROM schema_meta WHERE key='schema_family'")
        else:
            connection.execute("UPDATE schema_meta SET value=? WHERE key='schema_family'", (family,))
    with pytest.raises(StateStoreError):
        LendingStateStore(path)
    with closing(sqlite3.connect(path)) as connection:
        actual = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
        assert actual == str(version)
    assert not (tmp_path / "backups").exists()


def test_v36_initialized_store_opens_under_live_writer_and_concurrent_readers(tmp_path, monkeypatch):
    path = tmp_path / "state.db"
    store = LendingStateStore(path)
    before = store.runtime()

    def reject_migration(*_args, **_kwargs):
        raise AssertionError("An initialized V3.6 read must not migrate or write")

    monkeypatch.setattr(LendingStateStore, "_migrate_existing_v36", reject_migration)
    monkeypatch.setattr(LendingStateStore, "_ensure_performance_indexes", reject_migration)
    with closing(sqlite3.connect(path, isolation_level=None)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(lambda: LendingStateStore(path).runtime()) for _ in range(8)]
            assert all(future.result(timeout=3) == before for future in futures)
        writer.rollback()
    assert not (tmp_path / "backups").exists()


def test_v36_batch_ingest_is_idempotent_and_enriches_unknown_direction(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    first = store.upsert_market_trades([_trade("a", 60_001), _trade("a", 60_001), _trade("b", 120_001, "-40")])
    assert first == {"inserted": 2, "updated": 0, "sourceSequence": 1, "deferred": False}
    assert store.upsert_market_trades([_trade("a", 60_001)])["sourceSequence"] == 1
    enriched = store.upsert_market_trades([_trade("a", 60_001, raw="150")])
    assert enriched["inserted"] == 0 and enriched["updated"] == 1
    assert enriched["sourceSequence"] == 2
    trades = list(store.iter_market_trades(batch_size=1))
    assert [(row["id"], row["amount"], row["raw_signed_amount"]) for row in trades] == [
        ("a", D("150"), D("150")), ("b", D("40"), D("-40"))
    ]
    assert store.dirty_market_minutes(1) == [{"minuteMs": 60_000, "minute_ms": 60_000, "sourceSequence": 2}]
    assert len(list(store.iter_market_trades(60_001, 120_001))) == 1
    assert len(store.market_trades(60_001, 120_001)) == 2  # Existing inclusive API remains compatible.


def test_v36_ingestion_yields_to_account_write_without_partial_sequence(tmp_path):
    path = tmp_path / "state.db"
    store = LendingStateStore(path)
    with closing(sqlite3.connect(path, isolation_level=None)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        assert store.upsert_market_trades([_trade("blocked")], defer_if_busy=True)["deferred"]
        assert store.record_market_coverage(0, 100, defer_if_busy=True)["deferred"]
        writer.rollback()
    assert store.market_source_sequence() == 0
    assert list(store.iter_market_trades()) == []
    assert store.upsert_market_trades([_trade("blocked")], defer_if_busy=True)["inserted"] == 1


def test_v36_coverage_requires_verified_intervals_and_preserves_empty_spans(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.upsert_market_trades([_trade("first", 0), _trade("last", 500)])
    assert not store.market_coverage(0, 500)["complete"]
    store.record_market_coverage(0, 100, details={"empty": True})
    store.record_market_coverage(100, 300)
    store.record_market_coverage(300, 400, status="AMBIGUOUS_BOUNDARY")
    store.record_market_coverage(400, 500)
    coverage = store.market_coverage(0, 500)
    assert not coverage["complete"]
    assert coverage["coveredDurationMs"] == 400
    assert coverage["intervals"] == coverage["coveredIntervals"]
    assert coverage["unresolved"][0] == {
        "startMs": 300, "endMs": 400, "start_ms": 300, "end_ms": 400, "status": "AMBIGUOUS_BOUNDARY"
    }
    store.record_market_coverage(300, 400)
    assert store.market_coverage(0, 500)["complete"]
    assert store.market_coverage(500, 500)["complete"]
    assert not store.market_coverage(499, 501)["complete"]
    with pytest.raises(ValueError):
        store.record_market_coverage(2, 1)


def test_v36_book_iterator_uses_observation_time_and_legacy_bucket_end(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.record_book_snapshot([[1, 2, 3, 4]], now_ms=70_000)
    with store.transaction(immediate=True) as connection:
        connection.execute("INSERT INTO book_snapshots(mts,source,book_json) VALUES(120000,'OLD','[]')")
    assert list(store.iter_book_snapshots(0, 70_000)) == []
    first = list(store.iter_book_snapshots(70_000, 180_000, batch_size=1))
    assert len(first) == 1
    assert first[0]["mts"] == 60_000
    assert first[0]["observed_at_ms"] == first[0]["visible_at_ms"] == 70_000
    old = list(store.iter_book_snapshots(180_000, 180_001))[0]
    assert old["observed_at_ms"] is None and old["visible_at_ms"] == 180_000


def test_v36_retention_keeps_research_coverage_and_short_dirty_window(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    now = 200 * DAY
    store.upsert_market_trades([_trade("old", 109 * DAY), _trade("research", 111 * DAY)])
    store.record_market_coverage(0, now)
    store.prune_market_data(now_ms=now)
    assert [row["id"] for row in store.iter_market_trades()] == ["research"]
    assert store.market_coverage(111 * DAY, now)["complete"]
    assert not store.market_coverage(109 * DAY, now)["complete"]
    assert store.dirty_market_minutes() == []


def test_v36_derived_bars_and_dirty_bins_keep_eight_days_while_evidence_keeps_ninety(tmp_path):
    now = 200 * DAY
    store = LendingStateStore(tmp_path / "state.db")
    store.upsert_market_trades([_trade("nine-days", now - 9 * DAY), _trade("seven-days", now - 7 * DAY)])
    for stamp in (now - 9 * DAY, now - 7 * DAY):
        store.record_market_bars({"1h": {"median": ".0003", "q25": ".0002", "q75": ".0004"}}, now_ms=stamp)
        store.record_book_snapshot([], now_ms=stamp)
        store.upsert_funding_stats([{"mts": stamp, "utilization": ".5"}])
    store.record_market_coverage(now - 89 * DAY, now)
    deleted = store.prune_market_data(now_ms=now)
    assert deleted == {"trades": 0, "bars": 1, "books": 0, "stats": 0}
    with store.read_connection() as connection:
        assert [row[0] for row in connection.execute("SELECT bucket_ms FROM market_bars")] == [now - 7 * DAY]
    assert [row["minuteMs"] for row in store.dirty_market_minutes()] == [now - 7 * DAY]
    assert store.market_coverage(now - 89 * DAY, now)["complete"]
    assert len(list(store.iter_market_trades())) == 2
    assert len(list(store.iter_book_snapshots())) == 2
    assert len(store.funding_stats()) == 2


def test_v36_repayment_calibration_excludes_future_and_deduplicates_sources(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    with store.transaction(immediate=True) as connection:
        for identity, closed in ((1, 500), (2, 600)):
            connection.execute(
                """INSERT INTO credit_history(credit_id,currency,amount,rate,period,status,mts_opening,mts_updated)
                   VALUES(?,'USD','150','.0003',120,'CLOSED',100,?)""", (identity, closed)
            )
            connection.execute(
                """INSERT INTO credit_closures(credit_id,currency,amount,rate,period,opened_at_ms,closed_at_ms)
                   VALUES(?,'USD','150','.0003',120,100,?)""", (identity, closed)
            )
        connection.execute(
            """INSERT INTO credit_closures(credit_id,currency,amount,rate,period,opened_at_ms,closed_at_ms)
               VALUES(3,'USD','40','.0003',120,100,400)"""
        )
    rows = store.repayment_calibration(0, 600)
    assert [(row["credit_id"], row["source"]) for row in rows] == [(3, "OBSERVED_CLOSE"), (1, "HISTORY_CLOSED")]
    assert all(isinstance(row["amount"], D) for row in rows)


def test_v36_quick_curves_keep_ten_stage_policy_and_old_chain_defaults(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    with store.transaction(immediate=True) as connection:
        connection.execute(
            """INSERT INTO strategy_versions(version_id,status,policy_json,created_at_ms)
               VALUES('old','ARCHIVED','{}',0)"""
        )
        assert store._strategy_reprice_stage_count(connection, "old", "short", "quick", QUICK_EXECUTION_CURVE) == 10
        assert store._strategy_reprice_stage_count(
            connection, "old", "short", "quick", EXACT_TERM_EXPLORATION_CURVE
        ) == 10
        assert store._strategy_reprice_stage_count(connection, "old", "short", "quick", "LEGACY") == 6
        connection.execute(
            "UPDATE strategy_versions SET policy_json=? WHERE version_id='old'",
            (json.dumps({"quick_short_reprice_stages_minutes": list(range(10))}),),
        )
        assert store._strategy_reprice_stage_count(connection, "old", "short", "quick", QUICK_EXECUTION_CURVE) == 10


def test_v36_batch_abort_rolls_back_trades_dirty_bins_and_sequence_together(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    with store.transaction(immediate=True) as connection:
        connection.execute(
            """CREATE TRIGGER reject_bad_trade BEFORE INSERT ON market_trades
               WHEN NEW.trade_id='bad' BEGIN SELECT RAISE(ABORT,'invalid source record'); END"""
        )
    with pytest.raises(sqlite3.IntegrityError, match="invalid source record"):
        store.upsert_market_trades([_trade("good", 60_001), _trade("bad", 120_001)])
    assert list(store.iter_market_trades()) == []
    assert store.dirty_market_minutes() == []
    assert store.market_source_sequence() == 0
    with store.transaction(immediate=True) as connection:
        connection.execute("DROP TRIGGER reject_bad_trade")
    assert store.upsert_market_trades([_trade("good", 60_001)])["inserted"] == 1


def test_v36_stream_is_a_consistent_snapshot_while_late_trades_are_ingested(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.upsert_market_trades([_trade("first", 10), _trade("last", 30)])
    with closing(store.iter_market_trades(0, 40, batch_size=1)) as iterator:
        assert next(iterator)["id"] == "first"
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(store.upsert_market_trades, [_trade("late", 20)]).result(timeout=3)
        assert result["sourceSequence"] == 2
        assert [row["id"] for row in iterator] == ["last"]
    assert [row["id"] for row in store.iter_market_trades(0, 40)] == ["first", "late", "last"]


def test_v36_background_ingestion_yields_to_a_writer_on_the_same_store(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    locked, release = threading.Event(), threading.Event()

    def hold_account_writer():
        with store.transaction(immediate=True):
            locked.set()
            assert release.wait(3)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(hold_account_writer)
        assert locked.wait(3)
        try:
            assert store.upsert_market_trades([_trade("retry")], defer_if_busy=True)["deferred"]
            assert store.record_market_coverage(0, 200, defer_if_busy=True)["deferred"]
        finally:
            release.set()
        future.result(timeout=3)
    assert store.market_source_sequence() == 0
    assert store.upsert_market_trades([_trade("retry")], defer_if_busy=True)["inserted"] == 1


def test_v36_empty_batches_and_direction_enrichment_preserve_original_trade_evidence(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.upsert_market_trades([_trade("immutable", 60_001)])
    duplicate = {**_trade("immutable", 120_001, raw="-150"), "rate": D("0.01"), "period": 2}
    result = store.upsert_market_trades([duplicate])
    assert result["updated"] == 1
    stored = list(store.iter_market_trades())[0]
    assert (stored["mts"], stored["rate"], stored["period"], stored["amount"]) == (
        60_001, D(".0003"), 120, D(150),
    )
    assert stored["raw_signed_amount"] == D(-150)
    assert [row["minuteMs"] for row in store.dirty_market_minutes(1)] == [60_000]
    assert store.upsert_market_trades([_trade("immutable", 60_001, raw="150")])["updated"] == 0
    assert store.upsert_market_trades([]) == {
        "inserted": 0, "updated": 0, "sourceSequence": 2, "deferred": False,
    }


def test_v36_coverage_unions_overlaps_and_splits_latest_failure_without_erasing_proof(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.record_market_coverage(100, 200)
    store.record_market_coverage(80, 150)
    store.record_market_coverage(200, 220)
    store.record_market_coverage(100, 140, status="TIMEOUT")
    store.record_market_coverage(220, 250, status="FAILED")
    store.record_market_coverage(240, 260, status="TIMEOUT")
    store.record_market_coverage(270, 280, status="FAILED")
    store.record_market_coverage(280, 290, status="FAILED")
    coverage = store.market_coverage(80, 300)
    assert coverage["coveredDurationMs"] == 140
    assert [(row["startMs"], row["endMs"], row["status"]) for row in coverage["unresolved"]] == [
        (220, 240, "FAILED"), (240, 260, "TIMEOUT"), (260, 270, "UNVERIFIED"),
        (270, 290, "FAILED"), (290, 300, "UNVERIFIED"),
    ]
    assert store.market_coverage(90, 210)["complete"]
    assert store.market_coverage(90, 210)["coveredDurationMs"] == 120
    with pytest.raises(ValueError, match="end must"):
        store.market_coverage(2, 1)


def test_v36_dirty_queries_and_iterators_respect_half_open_boundaries(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.upsert_market_trades([_trade("old", 59_999)])
    store.upsert_market_trades([_trade("new", 60_001)])
    assert [row["minuteMs"] for row in store.dirty_market_minutes(0, 30_000, 60_000)] == [0]
    assert [row["minuteMs"] for row in store.dirty_market_minutes(1, 60_000, 120_000)] == [60_000]
    assert store.dirty_market_minutes(2) == []
    assert list(store.iter_market_trades(60_001, 60_001)) == []
    with pytest.raises(ValueError, match="batch_size"):
        list(store.iter_market_trades(batch_size=0))
    with pytest.raises(ValueError, match="batch_size"):
        list(store.iter_book_snapshots(batch_size=-1))


def test_v36_same_minute_replacement_book_does_not_become_visible_early(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    store.record_book_snapshot([{"rate": ".0003"}], now_ms=70_000)
    store.record_book_snapshot([{"rate": ".0004"}], now_ms=80_000)
    assert list(store.iter_book_snapshots(0, 80_000)) == []
    book = list(store.iter_book_snapshots(80_000, 80_001))[0]
    assert book["observed_at_ms"] == book["visible_at_ms"] == 80_000
    assert book["book"] == [{"rate": ".0004"}]
    assert store.book_snapshots(60_000, 60_000)[0]["mts"] == 60_000


def test_v36_schema_rejection_closes_the_read_connection(tmp_path, monkeypatch):
    path = tmp_path / "state.db"
    store = LendingStateStore(path)
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE schema_meta SET value='17' WHERE key='schema_version'")
    connections, original_connect = [], sqlite3.connect

    def tracked_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    with pytest.raises(StateStoreError, match="legacy V4"):
        LendingStateStore(path)
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


def test_v36_history_calibration_uses_only_closed_usd_credits_observable_in_training(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    rows = [
        {"id": 1, "currency": "USD", "amount": D(150), "rate": D(".0003"), "rate_real": D(".00031"),
         "period": 120, "status": "CLOSED (returned)", "mts_opening": 100, "mts_updated": 500},
        {"id": 2, "currency": "EUR", "amount": D(150), "rate": D(".0003"), "period": 120,
         "status": "CLOSED", "mts_opening": 100, "mts_updated": 500},
        {"id": 3, "amount": D(150), "rate": D(".0003"), "period": 120,
         "status": "ACTIVE", "mts_opening": 100, "mts_updated": 500},
        {"id": 4, "amount": D(150), "rate": D(".0003"), "period": 120,
         "status": "CLOSED", "mts_opening": 100, "mts_updated": 600},
        {"id": 5, "amount": D(150), "rate": D(".0003"), "period": 120,
         "status": "CLOSED", "mts_opening": 500, "mts_updated": 400},
    ]
    store.upsert_credit_history(rows)
    result = store.repayment_calibration(0, 600)
    assert [(row["credit_id"], row["source"], row["closed_at_ms"]) for row in result] == [
        (1, "HISTORY_CLOSED", 500),
    ]
    assert store.repayment_calibration(501, 600) == []
    assert store.repayment_calibration(600, 601)[0]["credit_id"] == 4


def test_v36_new_quick_chain_preserves_landing_and_stage_through_partial_fill_and_restart(tmp_path):
    path = tmp_path / "state.db"
    store = LendingStateStore(path, clock=lambda: 1)
    version = store.save_strategy({"quick_short_reprice_stages_minutes": list(range(1, 11))}, status="ACTIVE")
    order = {
        "slice_key": "v:short:quick:0", "strategy_version": version, "pool": "short", "layer": "quick",
        "amount": D(300), "submitted_rate": D(".0005"), "effective_rate": D(".0005"), "period": 2,
        "offer_type": "LIMIT", "pricing_curve_version": QUICK_EXECUTION_CURVE,
        "fixed_landing_rate": D(".0003"),
    }
    _, intent = store.reserve_intent(order, D(1000))
    store.confirm_intent(intent["id"], 101)
    offer = {"id": 101, "currency": "USD", "amount": D(300), "rate": D(".0005"), "period": 2,
             "offer_type": "LIMIT", "pool": "short", "layer": "quick", "mts_created": 100}
    store.reconcile_offers([offer], 1000)
    chain = store.ensure_reprice_chain(offer, version, 1000)
    key = chain["chain_key"]
    assert store.set_fixed_landing_rate(key, D(".0009"), 1001)["fixed_landing_rate"] == "0.0003"
    store.complete_reprice_stage(key, 4, 1002, market_anchor_rate=D(".00031"))
    store.complete_reprice_stage(key, 1, 1003, market_anchor_rate=D(".0008"))
    store.reconcile_offers([{**offer, "amount": D(260)}], 1004)
    store.mark_reprice_pending(key, "AGE_STAGE", D(".0003"), stage=5, source_offer_id=101, now_ms=1005)
    store.reconcile_offers([], 1006)
    pending = store.pending_reprices(version)[0]
    assert D(pending["source_amount"]) == 260
    assert pending["current_stage"] == 5 and pending["market_anchor_rate"] == "0.00031"
    assert store.pending_reprice_for_base("v:short:quick:0", version)["chain_key"] == key
    replacement = {**order, "slice_key": "v:short:quick:0:r1", "amount": D(260),
                   "submitted_rate": D(".0003"), "effective_rate": D(".0003")}
    _, replacement_intent = store.reserve_intent(replacement, D(1000))
    store.confirm_intent(replacement_intent["id"], 102)
    rebound = store.bind_reprice_replacement_chain(key, 102, D(".0003"), 1007)
    assert rebound["pricing_curve_version"] == QUICK_EXECUTION_CURVE
    restarted = LendingStateStore(path)
    restored = restarted.reprice_chain_for_offer(102)
    assert restored["started_at_ms"] == 100
    assert restored["current_stage"] == 5
    assert restored["fixed_landing_rate"] == "0.0003"
    assert restored["pricing_curve_version"] == QUICK_EXECUTION_CURVE
    assert restored["pending_source_offer_id"] is None
    assert restarted.bind_reprice_replacement_chain(key, 103, D(".0002"), 1008) is None
    assert restarted.bind_consolidation_replacement(102, 104, 1009)["current_offer_id"] == 104
    assert restarted.bind_consolidation_replacement(102, 105, 1010) is None
    assert restarted.bind_consolidation_replacement(104, 104, 1011) is None
    assert restarted.reprice_chain_for_offer(104)["fixed_landing_rate"] == "0.0003"
