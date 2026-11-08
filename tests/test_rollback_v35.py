import json
from contextlib import closing
import sqlite3
from decimal import Decimal

import pytest

from RollbackV35 import migrate
from StateStore import LendingStateStore, StateStoreError
from StrategyV3 import StrategyPolicyV3, policy_v3_to_json


def sources(tmp_path):
    legacy = tmp_path / "legacy.sqlite3"
    source = tmp_path / "current.sqlite3"
    legacy_store = LendingStateStore(legacy)
    with legacy_store.transaction(immediate=True) as db:
        db.execute("DROP INDEX market_trades_mts_id_idx")
        db.execute("DROP INDEX book_snapshots_visible_idx")
        db.execute("ALTER TABLE market_trades DROP COLUMN raw_signed_amount")
        db.execute("ALTER TABLE market_trades DROP COLUMN source_sequence")
        db.execute("ALTER TABLE book_snapshots DROP COLUMN observed_at_ms")
        db.execute("DROP TABLE market_dirty_minutes")
        db.execute("DROP TABLE market_coverage_intervals")
        db.execute("UPDATE schema_meta SET value='16' WHERE key='schema_version'")
        db.execute("DELETE FROM schema_meta WHERE key IN ('schema_family','v36_initialization_complete')")
    store = LendingStateStore(source, clock=lambda: 1_000)
    payload = policy_v3_to_json(
        StrategyPolicyV3(
            max_lend_amount=Decimal("2500"),
            max_lend_percent=Decimal("65"),
            short_floor_apr=Decimal(".0623"),
            medium_floor_apr=Decimal(".0785"),
            long_floor_apr=Decimal(".0853"),
        )
    )
    store.save_strategy(payload, "ACTIVE")
    with store.transaction(immediate=True) as db:
        db.execute("UPDATE schema_meta SET value='17' WHERE key='schema_version'")
        db.execute("UPDATE runtime_state SET mode='LIVE'")
        db.execute("UPDATE recovery_state SET active=1,target_mode='LIVE'")
        db.execute("UPDATE strategy_versions SET status='ARCHIVED'")
        db.execute(
            "INSERT INTO strategy_versions VALUES('adaptive','ACTIVE',?,2000000,2000000)",
            (json.dumps({"strategy_engine": "adaptive_net_yield_v3"}),),
        )
    return source, legacy


def test_migration_preserves_records_and_revokes_live(tmp_path):
    source, legacy = sources(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute(
            "INSERT INTO offers(offer_id,currency,amount,rate,period,offer_type,status,managed,last_seen_ms) "
            "VALUES(42,'USD','150','.0003',2,'LIMIT','ACTIVE',0,1000)"
        )
    result = migrate(source, legacy, tmp_path / "output.sqlite3")
    restored = LendingStateStore(result["output"])
    assert restored.runtime()["mode"] == "PAUSED"
    assert not restored.recovery_status()["active"]
    assert restored.offers()[0]["offer_id"] == 42
    assert restored.offers()[0]["managed"] == 0
    assert result["strategyConfig"]["short_floor_apr"] == "6.23"
    assert result["strategyConfig"]["max_lend_amount"] == "2500"
    assert result["strategyConfig"]["max_lend_percent"] == "65"
    with restored.read_connection() as connection:
        assert connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0] == "19"
    assert restored.strategy("DRAFT") is None
    with closing(sqlite3.connect(source)) as db, db:
        assert db.execute("SELECT mode FROM runtime_state").fetchone()[0] == "LIVE"


def test_migration_rejects_cross_currency(tmp_path):
    source, legacy = sources(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute(
            "INSERT INTO offers(offer_id,currency,amount,rate,period,offer_type,status,managed,last_seen_ms) "
            "VALUES(42,'USDT','150','.0003',2,'LIMIT','ACTIVE',0,1000)"
        )
    with pytest.raises(ValueError, match="non-USD"):
        migrate(source, legacy, tmp_path / "output.sqlite3")
    assert not (tmp_path / "output.sqlite3").exists()


def test_protected_pause_is_not_discarded(tmp_path):
    source, legacy = sources(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("UPDATE runtime_state SET safe_manual=1")
    with pytest.raises(ValueError, match="protected pause"):
        migrate(source, legacy, tmp_path / "output.sqlite3")


def test_refuses_overwrite_or_newer_database(tmp_path):
    source, legacy = sources(tmp_path)
    with pytest.raises(ValueError, match="new, separate"):
        migrate(source, legacy, source)
    with pytest.raises(StateStoreError, match="schema 17 belongs to legacy V4"):
        LendingStateStore(source)


def test_initialized_store_reads_without_acquiring_writer_lock(tmp_path):
    path = tmp_path / "state.sqlite3"
    LendingStateStore(path)
    writer = sqlite3.connect(path)
    writer.execute("BEGIN IMMEDIATE")
    try:
        store = LendingStateStore(path)
        assert store.runtime()["mode"] == "PAUSED"
    finally:
        writer.rollback()
        writer.close()


def insert_intent(db, state="CONFIRMED"):
    db.execute(
        "INSERT INTO order_intents(fingerprint,currency,slice_key,pool,layer,amount,submitted_rate,effective_rate,"
        "period,offer_type,strategy_version,state,exchange_offer_id,created_at_ms,updated_at_ms) "
        "VALUES('test','USD','adaptive:short:balanced:0','short','balanced','150','.0003','.0003',"
        "2,'LIMIT','adaptive',?,42,1234,2000)",
        (state,),
    )


def test_own_offer_keeps_ownership_and_waiting_clock(tmp_path):
    source, legacy = sources(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        insert_intent(db)
        db.execute(
            "INSERT INTO offers(offer_id,currency,amount,rate,period,offer_type,status,managed,"
            "pool,layer,mts_created,last_seen_ms) "
            "VALUES(42,'USD','150','.0003',2,'LIMIT','ACTIVE',1,'short','balanced',1234,2000)"
        )
    result = migrate(source, legacy, tmp_path / "output.sqlite3")
    restored = LendingStateStore(result["output"])
    assert restored.offers()[0]["managed"] == 1
    chain = restored.reprice_chain_for_offer(42)
    assert chain["started_at_ms"] == 1234
    assert Decimal(chain["origin_rate"]) == Decimal(".0003")


def test_unknown_write_blocks_migration(tmp_path):
    source, legacy = sources(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        insert_intent(db, "AMBIGUOUS")
    with pytest.raises(ValueError, match="unresolved writes"):
        migrate(source, legacy, tmp_path / "output.sqlite3")


def test_completed_cancel_chain_cannot_submit_again(tmp_path):
    source, legacy = sources(tmp_path)
    with closing(sqlite3.connect(source)) as db, db:
        insert_intent(db, "CLOSED")
        db.execute(
            "INSERT INTO offers(offer_id,currency,amount,rate,period,offer_type,status,managed,last_seen_ms) "
            "VALUES(42,'USD','150','.0003',2,'LIMIT','CLOSED',1,2000)"
        )
        db.execute(
            "INSERT INTO reprice_chains(chain_key,strategy_version,base_slice_key,pool,layer,origin_rate,"
            "started_at_ms,pending_action,pending_source_offer_id,status,updated_at_ms) "
            "VALUES('old','adaptive','old','short','balanced','.0003',1234,'REPRICE',42,'ACTIVE',2000)"
        )
    result = migrate(source, legacy, tmp_path / "output.sqlite3")
    with closing(sqlite3.connect(result["output"])) as db, db:
        assert db.execute("SELECT status,pending_action FROM reprice_chains WHERE chain_key='old'").fetchone() == (
            "CLOSED",
            "REPRICE",
        )
