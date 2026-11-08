import configparser
import hashlib
import json
from dataclasses import replace
from decimal import Decimal

import pytest

import Configuration
from StateStore import LendingStateStore
from StrategyV3 import LEGACY_QUICK_MODE, StrategyPolicyV3, json_decimal


D = Decimal


def _legacy_active(tmp_path, transform=None):
    config_path, database = tmp_path / "default.cfg", tmp_path / "state.db"
    config = configparser.ConfigParser()
    config.read_dict({
        "BITFINEX": {"apikey": "test", "secret": "test", "currencies": "USD"},
        "BOT": {"statedbfile": str(database)},
        "STRATEGY_V3": {"short_floor_apr": "5", "medium_floor_apr": "7.85", "long_floor_apr": "8.53"},
    })
    with config_path.open("w", encoding="utf-8") as output:
        output.write("# Existing V3.5 configuration must stay intact.\n")
        config.write(output)
    policy = replace(StrategyPolicyV3(), short_floor_apr=D(".05"), medium_floor_apr=D(".0785"),
                     long_floor_apr=D(".0853"), normal_fee_rate=D(".147"))
    payload = json_decimal(vars(policy))
    payload.pop("quick_pricing_mode")
    payload.pop("quick_start_premium_apr_points")
    if transform:
        transform(payload)
    store = LendingStateStore(database, clock=lambda: 123)
    version = store.save_strategy(payload, status="ACTIVE")
    return config_path, store, version


def _seed_legacy_chain(store, version):
    order = {"strategy_version": version, "slice_key": "v:short:quick:0", "pool": "short", "layer": "quick",
             "amount": D(300), "submitted_rate": D(".0003"), "effective_rate": D(".0003"), "period": 2,
             "offer_type": "LIMIT", "pricing_curve_version": "EXACT_TERM_EXPLORATION_V4",
             "fixed_landing_rate": D(".0002")}
    _, intent = store.reserve_intent(order, D(1000))
    store.confirm_intent(intent["id"], 101)
    offer = {"id": 101, "amount": D(260), "rate": D(".0003"), "period": 2, "mts_created": 100}
    store.reconcile_offers([offer], 200)
    chain = store.ensure_reprice_chain(offer, version, 200)
    store.complete_reprice_stage(chain["chain_key"], 3, 201, market_anchor_rate=D(".0002"))
    store.mark_reprice_pending(chain["chain_key"], "AGE_STAGE", D(".00019"), stage=4,
                               source_offer_id=101, now_ms=202)


def _strategy_rows(store):
    with store.read_connection() as connection:
        return {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
                for table in ("strategy_versions", "order_intents", "reprice_chains", "strategy_events")}


@pytest.mark.parametrize("schema", [16, 18])
def test_v35_policy_missing_only_new_defaults_keeps_its_hash_config_and_order_chains(tmp_path, monkeypatch, schema):
    path, store, version = _legacy_active(tmp_path)
    _seed_legacy_chain(store, version)
    original_rows, original_config = _strategy_rows(store), path.read_bytes()
    payload = store.strategy("ACTIVE")["policy"]
    assert version == hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                               separators=(",", ":")).encode()).hexdigest()[:16]
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE schema_meta SET value=? WHERE key='schema_version'", (str(schema),))
    if schema == 16:
        with store.transaction(immediate=True) as connection:
            connection.execute("UPDATE schema_meta SET value='16' WHERE key='schema_version'")
            connection.execute("DELETE FROM schema_meta WHERE key IN ('schema_family','v36_initialization_complete')")

    def reject_redundant_strategy_backup(*_args, **_kwargs):
        raise AssertionError("An inert V3.6 policy extension must not trigger a strategy backup")

    monkeypatch.setattr(Configuration, "backup_strategy_state", reject_redundant_strategy_backup)
    result = Configuration.normalize_current_active_strategy(path)
    assert result == {"changed": False, "versionId": version, "backup": None}
    assert _strategy_rows(store) == original_rows
    assert path.read_bytes() == original_config
    parsed = Configuration.strategy_v3_from_record(store.strategy("ACTIVE"))
    assert parsed.quick_pricing_mode == LEGACY_QUICK_MODE
    assert parsed.quick_start_premium_apr_points == D(".10")
    with store.read_connection() as connection:
        meta = dict(connection.execute("SELECT key,value FROM schema_meta"))
    assert meta["schema_version"] == "19" and meta["schema_family"] == "v3"
    assert "release_0.3.6_activated_at_ms" not in meta
    assert len(list((tmp_path / "backups").glob("schema-v*-*.sqlite3"))) == 1
    assert Configuration.normalize_current_active_strategy(path) == result


@pytest.mark.parametrize("transform", [
    lambda payload: payload.pop("max_lend_percent"),
    lambda payload: payload.update(retired_setting=True),
    lambda payload: payload.update(adopt_external_offers=False),
    lambda payload: payload.update(quick_pricing_mode="EXPLORATION_V4"),
])
def test_other_incomplete_or_noncanonical_active_payloads_still_normalize_and_backup(tmp_path, transform):
    path, store, version = _legacy_active(tmp_path, transform)
    result = Configuration.normalize_current_active_strategy(path)
    assert result["changed"] and result["fromVersion"] == version
    assert result["versionId"] != version
    assert store.strategy("ACTIVE")["policy"]["quick_pricing_mode"] == LEGACY_QUICK_MODE
    assert (tmp_path / "backups").exists()
    assert result["backup"]["config"] and result["backup"]["database"]


def test_legacy_default_compatibility_requires_the_original_payload_hash_to_be_authentic(tmp_path):
    path, store, _ = _legacy_active(tmp_path)
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE strategy_versions SET version_id='incorrect-hash' WHERE status='ACTIVE'")
    result = Configuration.normalize_current_active_strategy(path)
    assert result["changed"] and result["fromVersion"] == "incorrect-hash"
    assert store.strategy("ACTIVE")["version_id"] == result["versionId"]
    assert store.strategy("ACTIVE")["policy"]["quick_pricing_mode"] == LEGACY_QUICK_MODE
