import http.client
import io
import json
import os
import sqlite3
import time
from decimal import Decimal
from email.message import Message
from types import SimpleNamespace
from unittest import mock
from urllib import error

import pytest

import lendingbot
from AppContext import AppContext
from DomainTypes import AccountSnapshot, MarketSnapshot, StrategyPlan, WriteOutcome, WriteResult
from ExchangeModels import parse_credit_rows, parse_loan_rows, parse_offer_rows, parse_wallet_rows
from MarketDataStream import BitfinexMarketDataHub
from RuntimeV3 import LendingRuntimeV3
from StateStore import LendingStateStore, StateStoreError
from StrategyResearch import (
    PublicRateLimiter,
    backfill_public_market_data,
    evaluate_strategies,
    paired_bootstrap_interval,
    research_variants,
    write_research_report,
)
from StrategyV3 import (
    SCORE_MODEL_VERSION,
    V3_RESEARCH_SCORE_WEIGHTS,
    StrategyPolicyV3,
    policy_v3_to_json,
    replay_strategy_v3,
)
from WriteRecovery import can_clear_ambiguous_pause, restart_transition, unique_unbound_candidate
from bitfinex import Bitfinex, BitfinexAmbiguousWriteError, BitfinexApiError, BitfinexTransientError


D = Decimal
DAY_MS = 86_400_000


def policy():
    return StrategyPolicyV3(
        short_floor_apr=D("0.01"),
        medium_floor_apr=D("0.01"),
        long_floor_apr=D("0.01"),
    )


def order(**updates):
    payload = {
        "currency": "USD",
        "slice_key": "v:short:quick:0",
        "slice_index": 0,
        "pool": "short",
        "layer": "quick",
        "amount": D("150"),
        "submitted_rate": D("0.0003"),
        "effective_rate": D("0.0003"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "strategy_version": "v",
        "plan_hash": "plan",
    }
    payload.update(updates)
    return payload


class Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.payload


@pytest.mark.parametrize(
    "failure",
    [
        error.URLError("offline"),
        TimeoutError("late"),
        ConnectionError("reset"),
        http.client.IncompleteRead(b"{"),
        http.client.RemoteDisconnected("closed"),
        UnicodeError("bad encoding"),
    ],
)
def test_write_transport_failures_are_unknown(failure):
    client = Bitfinex("key", "secret")
    with mock.patch("urllib.request.urlopen", side_effect=failure) as urlopen:
        with pytest.raises(BitfinexAmbiguousWriteError):
            client._request_json("https://example.invalid", method="POST", ambiguous_on_failure=True)
    assert urlopen.call_count == 1


def test_read_transport_failure_retries_without_entering_runtime_recovery():
    sleeps = []
    client = Bitfinex("key", "secret", read_retry_delays=(0.25,), retry_sleeper=sleeps.append)
    with mock.patch(
        "urllib.request.urlopen",
        side_effect=[http.client.IncompleteRead(b"{"), Response(b'{"ok":true}')],
    ) as urlopen:
        payload = client._request_json("https://example.invalid", method="POST")

    assert payload == {"ok": True}
    assert urlopen.call_count == 2
    assert sleeps == [0.25]


def test_read_transport_failure_raises_after_retry_budget_is_exhausted():
    client = Bitfinex("key", "secret", read_retry_delays=(0, 0), retry_sleeper=lambda _: None)
    with mock.patch("urllib.request.urlopen", side_effect=http.client.IncompleteRead(b"")) as urlopen:
        with pytest.raises(BitfinexApiError, match="IncompleteRead"):
            client._request_json("https://example.invalid")

    assert urlopen.call_count == 3


def test_authenticated_read_retry_uses_a_fresh_nonce_and_signature():
    sleeps = []
    client = Bitfinex("key", "secret", read_retry_delays=(0.25,), retry_sleeper=sleeps.append)
    with mock.patch.object(
        client,
        "_request_json",
        side_effect=[BitfinexTransientError("truncated"), []],
    ) as request_json:
        assert client._auth_post("v2/auth/r/wallets") == []

    first_headers = request_json.call_args_list[0].kwargs["headers"]
    second_headers = request_json.call_args_list[1].kwargs["headers"]
    assert int(second_headers["bfx-nonce"]) > int(first_headers["bfx-nonce"])
    assert second_headers["bfx-signature"] != first_headers["bfx-signature"]
    assert sleeps == [0.25]


def test_invalid_json_after_write_is_unknown():
    client = Bitfinex("key", "secret")
    with mock.patch("urllib.request.urlopen", return_value=Response(b'{"truncated"')):
        with pytest.raises(BitfinexAmbiguousWriteError):
            client._request_json("https://example.invalid", method="POST", ambiguous_on_failure=True)


def test_http_error_is_a_definite_rejection():
    client = Bitfinex("key", "secret")
    failure = error.HTTPError("https://example.invalid", 400, "bad", {}, io.BytesIO(b'["error","bad"]'))
    with mock.patch("urllib.request.urlopen", side_effect=failure):
        with pytest.raises(BitfinexApiError):
            client._request_json("https://example.invalid", method="POST", ambiguous_on_failure=True)


def test_incomplete_success_notification_is_unknown():
    client = Bitfinex("key", "secret")
    with mock.patch.object(client, "_auth_post", return_value=[1, 2, 3]):
        result = client.submit_funding_offer_result("fUSD", "150", "0.0003", 2)
    assert result.outcome == WriteOutcome.UNKNOWN


def test_domain_types_are_explicit_and_immutable():
    account = AccountSnapshot(D("10"), D("2"), D("3"), D("5"))
    market = MarketSnapshot(123, "REST")
    plan = StrategyPlan("v", "hash", variant="candidate")
    assert (account.total, market.source, plan.variant) == (D("10"), "REST", "candidate")
    with pytest.raises(Exception):
        account.total = D("20")


@pytest.mark.parametrize(
    ("state", "target", "manual"),
    [("PLANNED", "CLOSED", False), ("SUBMITTING", "AMBIGUOUS", True)],
)
def test_restart_transition_is_fail_closed(state, target, manual):
    transition = restart_transition(state)
    assert (transition.state, transition.manual_safe) == (target, manual)


def test_restart_transition_ignores_terminal_states():
    assert restart_transition("CONFIRMED") is None


def test_unique_candidate_requires_exactly_one_unbound_id():
    assert unique_unbound_candidate({1, 2}, {1}) == 2
    assert unique_unbound_candidate({1, 2}, set()) is None


def test_manual_protected_pause_resolution_remains_paused():
    assert can_clear_ambiguous_pause(0, "PAUSED", True) is True
    assert can_clear_ambiguous_pause(1, "PAUSED", True) is False
    assert can_clear_ambiguous_pause(0, "LIVE", False) is False


def test_schema_v16_is_explicit(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    with store.read_connection() as connection:
        version = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(order_intents)")}
        chain_columns = {row["name"] for row in connection.execute("PRAGMA table_info(reprice_chains)")}
    assert version == "19"
    assert {
        "write_phase",
        "resolution",
        "strategy_variant",
        "request_started_at_ms",
        "pricing_curve_version",
        "fixed_landing_rate",
    } <= columns
    assert {"pending_source_offer_id", "fixed_landing_rate"} <= chain_columns


def test_schema_v16_abandons_unsafe_legacy_pending_reprices(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = LendingStateStore(path)
    with store.transaction(immediate=True) as connection:
        connection.execute(
            """INSERT INTO reprice_chains(
                   chain_key, strategy_version, base_slice_key, pool, layer,
                   origin_rate, started_at_ms, current_stage, current_offer_id,
                   pending_action, pending_target_rate, status, updated_at_ms
               ) VALUES('old-chain', 'v3', 'old', 'short', 'high',
                        '0.0003', 1, 3, NULL, 'AGE_STAGE', '0.00025', 'ACTIVE', 2)"""
        )
        connection.execute("UPDATE schema_meta SET value='14' WHERE key='schema_version'")

    migrated = LendingStateStore(path)
    chain = migrated.reprice_chain("old-chain")

    assert chain["status"] == "ABANDONED_LEGACY"
    assert chain["pending_action"] is None
    assert chain["pending_target_rate"] is None
    with store.read_connection() as connection:
        credit_columns = {row["name"] for row in connection.execute("PRAGMA table_info(credits)")}
        ownership_table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ownership_events'"
        ).fetchone()
        reprice_chain_table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='reprice_chains'"
        ).fetchone()
        reprice_columns = {row["name"] for row in connection.execute("PRAGMA table_info(reprice_events)")}
        reprice_chain_columns = {row["name"] for row in connection.execute("PRAGMA table_info(reprice_chains)")}
        offer_history_columns = {row["name"] for row in connection.execute("PRAGMA table_info(offer_history)")}
    assert "attribution_state" in credit_columns
    assert ownership_table is not None
    assert reprice_chain_table is not None
    assert "amount_original" in offer_history_columns
    assert {"chain_key", "stage", "benchmark_rate", "floor_rate"} <= reprice_columns
    assert {"market_anchor_rate", "pricing_curve_version"} <= reprice_chain_columns
    assert store.recovery_status()["requiredSnapshots"] == 2


def test_schema_v16_backs_up_v15_and_abandons_pending_upward_reprices(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = LendingStateStore(path)
    with store.transaction(immediate=True) as connection:
        connection.execute(
            """INSERT INTO reprice_chains(
                   chain_key, strategy_version, base_slice_key, pool, layer,
                   origin_rate, started_at_ms, current_stage, current_offer_id,
                   pending_action, pending_target_rate, pending_source_offer_id,
                   pricing_curve_version, status, updated_at_ms
               ) VALUES('upward-chain', 'v3', 'upward', 'short', 'balanced',
                        '0.0003', 1, 2, NULL, 'MARKET_RISE', '0.0004', 901,
                        'EXACT_TERM_EXPLORATION_V2', 'ACTIVE', 2)"""
        )
        connection.execute("UPDATE schema_meta SET value='15' WHERE key='schema_version'")

    migrated = LendingStateStore(path)
    chain = migrated.reprice_chain("upward-chain")

    assert chain["status"] == "ABANDONED_LEGACY"
    assert chain["pending_action"] is None
    assert list((tmp_path / "backups").glob("schema-v15-*.sqlite3"))


def test_schema_v14_folds_legacy_safe_runtime_into_auto_recovery_paused(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = LendingStateStore(path)
    with store.transaction(immediate=True) as connection:
        connection.execute("UPDATE schema_meta SET value='11' WHERE key='schema_version'")
        connection.execute(
            """UPDATE runtime_state SET mode='SAFE', previous_mode='LIVE',
               safe_reason='AMBIGUOUS_SUBMIT:7', safe_manual=1 WHERE singleton=1"""
        )

    migrated = LendingStateStore(path)

    runtime = migrated.runtime()
    assert runtime["mode"] == "PAUSED"
    assert runtime["previous_mode"] == "LIVE"
    assert runtime["safe_reason"] == "AMBIGUOUS_SUBMIT:7"
    assert runtime["safe_manual"] == 0
    assert list((tmp_path / "backups").glob("schema-v11-*.sqlite3"))


def test_safe_is_no_longer_a_runtime_mode(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    with pytest.raises(StateStoreError):
        store.set_mode("SAFE", "legacy")


def test_schema_v16_migrates_offer_history_without_losing_rows(tmp_path):
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("INSERT INTO schema_meta VALUES('schema_version', '7')")
    connection.execute(
        """CREATE TABLE offer_history(
               offer_id INTEGER PRIMARY KEY, currency TEXT NOT NULL, amount TEXT NOT NULL,
               rate TEXT NOT NULL, rate_real TEXT, period INTEGER NOT NULL,
               offer_type TEXT NOT NULL, flags INTEGER NOT NULL DEFAULT 0,
               status TEXT NOT NULL, mts_created INTEGER, mts_updated INTEGER,
               managed INTEGER NOT NULL DEFAULT 0
           )"""
    )
    connection.execute(
        """INSERT INTO offer_history VALUES(
               9001, 'USD', '150', '0.0003', NULL, 2, 'LIMIT', 0,
               'EXECUTED', 1000, 2000, 1
           )"""
    )
    connection.commit()
    connection.close()

    store = LendingStateStore(path)

    with store.read_connection() as connection:
        version = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
        row = connection.execute("SELECT * FROM offer_history WHERE offer_id=9001").fetchone()
    assert version == "19"
    assert row["amount"] == "150"
    assert row["amount_original"] is None


def test_schema_migration_creates_online_backup(tmp_path):
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("INSERT INTO schema_meta VALUES('schema_version', '3')")
    connection.commit()
    connection.close()
    LendingStateStore(path)
    backups = list((tmp_path / "backups").glob("schema-v3-*.sqlite3"))
    assert len(backups) == 1


def test_schema_migration_failure_rolls_back(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("INSERT INTO schema_meta VALUES('schema_version', '3')")
    connection.commit()
    connection.close()

    def fail(_connection):
        raise RuntimeError("migration failed")

    monkeypatch.setattr(LendingStateStore, "_migrate_v4_columns", staticmethod(fail))
    with pytest.raises(RuntimeError):
        LendingStateStore(path)
    connection = sqlite3.connect(path)
    version = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
    connection.close()
    assert version == "3"


def test_restart_closes_only_never_sent_intent(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    _, intent = store.reserve_intent(order(), D("500"))
    result = store.recover_incomplete_writes()
    recovered = store.intent(intent["id"])
    assert result == {"closedBeforeSend": 1, "ambiguousAfterSend": 0}
    assert (recovered["state"], recovered["resolution"]) == ("CLOSED", "PROCESS_RESTART_BEFORE_SEND")


def test_restart_after_send_enters_auto_recovery_protected_pause(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    _, intent = store.reserve_intent(order(), D("500"))
    store.mark_submitting(intent["id"])
    result = store.recover_incomplete_writes()
    assert result["ambiguousAfterSend"] == 1
    assert store.intent(intent["id"])["state"] == "AMBIGUOUS"
    assert store.runtime()["mode"] == "PAUSED"
    assert store.runtime()["safe_manual"] == 0


class SubmitClient:
    api_key = ""
    api_secret = ""

    def __init__(self, result):
        self.result = result
        self.calls = 0

    def submit_funding_offer_result(self, *_args, **_kwargs):
        self.calls += 1
        return self.result


class NullHub:
    fallback_ms = 0
    rest_stale_ms = 0

    def stop(self):
        return None


def submit_once(tmp_path, write_result):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    client = SubmitClient(write_result)
    runtime = LendingRuntimeV3(client, policy(), store, hub=NullHub())
    result = runtime._submit_plan({"plan": [order()], "plan_hash": "plan"}, D("500"), "v")
    return store, client, result


def test_unknown_submit_is_not_retried_and_enters_protected_pause(tmp_path):
    store, client, submitted = submit_once(tmp_path, WriteResult(WriteOutcome.UNKNOWN, error="timeout"))
    assert client.calls == 1
    assert submitted == []
    assert store.runtime()["mode"] == "PAUSED"
    assert store.intents()[0]["state"] == "AMBIGUOUS"


def test_success_without_offer_id_is_unknown(tmp_path):
    store, client, submitted = submit_once(
        tmp_path, WriteResult(WriteOutcome.CONFIRMED, response=[0, "ok", None, None, None, None, "SUCCESS", "ok"])
    )
    assert client.calls == 1 and submitted == []
    assert store.intents()[0]["resolution"] == "MANUAL_REQUIRED"


def test_definite_submit_rejection_closes_intent_without_safe(tmp_path):
    store, client, submitted = submit_once(tmp_path, WriteResult(WriteOutcome.DEFINITE_REJECT, error="minimum amount"))
    assert client.calls == 1 and submitted == []
    assert store.intents()[0]["resolution"] == "EXCHANGE_REJECTED"
    assert store.runtime()["mode"] == "LIVE"


def matching_offer(offer_id, created=None):
    return {
        "id": offer_id,
        "currency": "USD",
        "amount": D("150"),
        "amount_original": D("150"),
        "rate": D("0.0003"),
        "rate_real": D("0.0003"),
        "period": 2,
        "offer_type": "LIMIT",
        "display_type": "LIMIT",
        "flags": 0,
        "status": "ACTIVE",
        "mts_created": created or int(time.time() * 1000),
        "mts_updated": created or int(time.time() * 1000),
    }


def ambiguous_store(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    _, intent = store.reserve_intent(order(), D("500"))
    store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection reset")
    return store, intent


def test_unique_authoritative_offer_requires_two_clean_snapshots_then_pauses(tmp_path):
    store, intent = ambiguous_store(tmp_path)
    store.reconcile_offers([matching_offer(9001)])
    resolved = store.reconcile_ambiguous_candidates()
    assert resolved == [{"intentId": intent["id"], "offerId": 9001}]
    assert store.runtime()["mode"] == "PAUSED"
    base = store.recovery_status()["lastProbeAt"]
    store.record_consistent_sync(base + 30_000)
    store.record_consistent_sync(base + 60_000)
    assert store.runtime()["mode"] == "PAUSED"
    assert store.offers(active_only=True)[0]["managed"] == 1


def test_partially_filled_offer_matches_ambiguous_intent_by_original_amount(tmp_path):
    store, intent = ambiguous_store(tmp_path)
    partial = matching_offer(9001)
    partial["amount"] = D("75")
    store.reconcile_offers([partial])

    resolved = store.reconcile_ambiguous_candidates()

    assert resolved == [{"intentId": intent["id"], "offerId": 9001}]
    assert store.intent(intent["id"])["resolution"] == "AUTO_UNIQUE_MATCH"


def test_multiple_authoritative_matches_stay_manual_safe(tmp_path):
    store, intent = ambiguous_store(tmp_path)
    store.reconcile_offers([matching_offer(9001), matching_offer(9002)])
    assert store.reconcile_ambiguous_candidates() == []
    assert store.intent(intent["id"])["state"] == "AMBIGUOUS"
    assert store.runtime()["mode"] == "PAUSED"


def test_unique_ambiguous_submit_match_automatically_resumes_live(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    _, intent = store.reserve_intent(order(), D("500"))
    store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection reset")
    store.reconcile_offers([matching_offer(9001)])

    resolved = store.reconcile_ambiguous_candidates(now_ms=1_000_000)

    assert resolved == [{"intentId": intent["id"], "offerId": 9001}]
    assert store.runtime()["mode"] == "PAUSED"
    store.record_consistent_sync(1_030_000)
    store.record_consistent_sync(1_060_000)
    assert store.runtime()["mode"] == "LIVE"
    assert store.intent(intent["id"])["resolution"] == "AUTO_UNIQUE_MATCH"


def test_ambiguous_submit_absence_requires_two_authoritative_history_snapshots(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    _, intent = store.reserve_intent(order(), D("500"))
    store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection reset")
    store.reconcile_offers([])

    store.reconcile_ambiguous_candidates(confirm_absent=False, now_ms=1_000_000)
    store.reconcile_ambiguous_candidates(confirm_absent=True, now_ms=1_030_000)
    assert store.runtime()["mode"] == "PAUSED"
    store.reconcile_ambiguous_candidates(confirm_absent=True, now_ms=1_060_000)

    assert store.runtime()["mode"] == "PAUSED"
    store.record_consistent_sync(1_090_000)
    store.record_consistent_sync(1_120_000)
    assert store.runtime()["mode"] == "LIVE"
    assert store.intent(intent["id"])["state"] == "CLOSED"
    assert store.intent(intent["id"])["resolution"] == "AUTO_CONFIRMED_ABSENT"


def test_unrelated_same_period_history_does_not_block_absence_recovery(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    _, intent = store.reserve_intent(order(), D("500"))
    intent = store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection reset")
    request_ms = int(intent["request_started_at_ms"])
    store.upsert_offer_history(
        [
            {
                **matching_offer(9001, request_ms + 1_000),
                "amount": D("175"),
                "amount_original": D("175"),
            }
        ]
    )
    with store.transaction(immediate=True) as connection:
        connection.execute(
            """INSERT INTO funding_trades(
                   trade_id, currency, offer_id, amount, rate, period, mts, managed
               ) VALUES(1, 'USD', 9002, '150', '0.0004', 2, ?, 0)""",
            (request_ms + 2_000,),
        )

    store.reconcile_ambiguous_candidates(confirm_absent=True, now_ms=request_ms + 60_000)
    store.reconcile_ambiguous_candidates(confirm_absent=True, now_ms=request_ms + 90_000)

    store.record_consistent_sync(request_ms + 120_000)
    store.record_consistent_sync(request_ms + 150_000)
    assert store.runtime()["mode"] == "LIVE"
    assert store.intent(intent["id"])["resolution"] == "AUTO_CONFIRMED_ABSENT"


def test_submit_plan_persists_sixty_attempt_rolling_limit_and_resumes(tmp_path):
    now_seconds = [1_900_000_000]

    def clock():
        return now_seconds[0]

    store = LendingStateStore(tmp_path / "state.sqlite3", clock=clock)

    class ConfirmingClient:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.calls = 0

        def submit_funding_offer_result(self, *_args, **_kwargs):
            self.calls += 1
            return WriteResult(
                WriteOutcome.CONFIRMED,
                response=[0, "ok", None, None, [10_000 + self.calls]],
            )

    client = ConfirmingClient()
    runtime = LendingRuntimeV3(client, policy(), store, hub=NullHub(), clock=clock)
    plan = {
        "plan_hash": "large-plan",
        "plan": [order(slice_index=index) for index in range(61)],
    }

    first = runtime._submit_plan(plan, D("9150"), "v")
    second = runtime._submit_plan(plan, D("9150"), "v")
    assert len(first) == 60
    assert second == []
    assert client.calls == 60

    now_seconds[0] += 60.001
    third = runtime._submit_plan(plan, D("9150"), "v")
    assert len(third) == 1
    assert client.calls == 61
    assert len({intent["slice_key"] for intent in store.intents()}) == 61


def test_ambiguous_cancel_present_automatically_resumes_live_for_retry(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    store.enter_protected_pause("AMBIGUOUS_CANCEL:9001")

    store.observe_ambiguous_cancel({9001}, now_ms=1_000_000)
    store.observe_ambiguous_cancel({9001}, now_ms=1_020_000)
    assert store.runtime()["mode"] == "PAUSED"
    store.observe_ambiguous_cancel({9001}, now_ms=1_030_000)

    assert store.runtime()["mode"] == "PAUSED"
    store.record_consistent_sync(1_060_000)
    store.record_consistent_sync(1_090_000)
    assert store.runtime()["mode"] == "LIVE"
    with store.read_connection() as connection:
        event = connection.execute("SELECT * FROM ownership_events ORDER BY id DESC LIMIT 1").fetchone()
    assert event["offer_id"] == 9001
    assert event["event_type"] == "CANCEL_RECONCILED_PRESENT"


def test_ambiguous_cancel_absent_automatically_resumes_live(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    store.enter_protected_pause("AMBIGUOUS_CANCEL:9001")

    store.observe_ambiguous_cancel(set(), now_ms=1_000_000)
    store.observe_ambiguous_cancel(set(), now_ms=1_030_000)

    assert store.runtime()["mode"] == "PAUSED"
    store.record_consistent_sync(1_060_000)
    store.record_consistent_sync(1_090_000)
    assert store.runtime()["mode"] == "LIVE"
    with store.read_connection() as connection:
        event_type = connection.execute("SELECT event_type FROM ownership_events ORDER BY id DESC LIMIT 1").fetchone()[
            "event_type"
        ]
    assert event_type == "CANCEL_RECONCILED_ABSENT"


def test_ambiguous_submit_history_uses_request_window_not_capped_90_day_window(tmp_path):
    request_ms = 2_000_000

    class HistoryClient:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.calls = []

        def funding_trades_history(self, symbol, **kwargs):
            self.calls.append(("trades", symbol, kwargs))
            return []

        def funding_offers_history(self, symbol, **kwargs):
            self.calls.append(("offers", symbol, kwargs))
            return []

    store = LendingStateStore(tmp_path / "state.sqlite3")
    client = HistoryClient()
    runtime = LendingRuntimeV3(client, policy(), store, hub=NullHub())
    intent = {"request_started_at_ms": request_ms, "updated_at_ms": request_ms}

    assert runtime.sync_ambiguous_write_history([intent], request_ms + 60_000) is True
    assert [call[0] for call in client.calls] == ["trades", "offers"]
    assert all(call[2]["start"] == request_ms - 300_000 for call in client.calls)
    assert all(call[2]["end"] == request_ms + 60_000 for call in client.calls)
    assert all(call[2]["limit"] == 500 for call in client.calls)


def test_ambiguous_submit_history_keeps_offer_evidence_when_trades_endpoint_fails(tmp_path):
    request_ms = 2_000_000

    class PartialHistoryClient:
        api_key = ""
        api_secret = ""

        def funding_trades_history(self, _symbol, **_kwargs):
            raise BitfinexApiError("temporary trades failure")

        def funding_offers_history(self, _symbol, **kwargs):
            return [
                [
                    9001,
                    "fUSD",
                    request_ms + 1_000,
                    request_ms + 2_000,
                    "0",
                    "150",
                    "LIMIT",
                    None,
                    None,
                    0,
                    "EXECUTED",
                    None,
                    None,
                    None,
                    "0.0003",
                    2,
                ]
            ]

    store = LendingStateStore(tmp_path / "state.sqlite3")
    runtime = LendingRuntimeV3(PartialHistoryClient(), policy(), store, hub=NullHub())

    complete = runtime.sync_ambiguous_write_history(
        [{"request_started_at_ms": request_ms, "updated_at_ms": request_ms}],
        request_ms + 60_000,
    )

    assert complete is False
    with store.read_connection() as connection:
        saved = connection.execute("SELECT * FROM offer_history WHERE offer_id=9001").fetchone()
    assert saved["amount_original"] == "150"


def test_regular_authenticated_funding_history_respects_endpoint_limit(tmp_path):
    class HistoryLimitsClient:
        api_key = ""
        api_secret = ""

        def __init__(self):
            self.calls = []

        def funding_trades_history(self, _symbol, **kwargs):
            self.calls.append(("trades", kwargs))
            return []

        def funding_offers_history(self, _symbol, **kwargs):
            self.calls.append(("offers", kwargs))
            return []

        def funding_credits_history(self, _symbol, **kwargs):
            self.calls.append(("credits", kwargs))
            return []

    client = HistoryLimitsClient()
    runtime = LendingRuntimeV3(
        client,
        policy(),
        LendingStateStore(tmp_path / "state.sqlite3"),
        hub=NullHub(),
    )

    assert runtime.sync_history(now_ms=100 * DAY_MS) is True
    assert [name for name, _kwargs in client.calls] == ["trades", "offers", "credits"]
    assert all(kwargs["limit"] == 500 for _name, kwargs in client.calls)


def test_empty_authoritative_histories_automatically_resume_live(tmp_path):
    class EmptyHistoryClient:
        api_key = ""
        api_secret = ""

        def funding_trades_history(self, _symbol, **_kwargs):
            return []

        def funding_offers_history(self, _symbol, **_kwargs):
            return []

    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.set_mode("LIVE", "test")
    _, intent = store.reserve_intent(order(), D("500"))
    intent = store.mark_submitting(intent["id"])
    store.mark_ambiguous(intent["id"], "connection reset")
    request_ms = int(intent["request_started_at_ms"])
    runtime = LendingRuntimeV3(EmptyHistoryClient(), policy(), store, hub=NullHub())

    assert runtime.sync_ambiguous_write_history([intent], request_ms + 60_000) is True
    store.reconcile_ambiguous_candidates(confirm_absent=True, now_ms=request_ms + 60_000)
    assert store.runtime()["mode"] == "PAUSED"
    assert runtime.sync_ambiguous_write_history([intent], request_ms + 90_000) is True
    store.reconcile_ambiguous_candidates(confirm_absent=True, now_ms=request_ms + 90_000)

    store.record_consistent_sync(request_ms + 120_000)
    store.record_consistent_sync(request_ms + 150_000)
    assert store.runtime()["mode"] == "LIVE"
    assert store.intent(intent["id"])["resolution"] == "AUTO_CONFIRMED_ABSENT"


def test_rollout_stages_are_restricted(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    assert store.set_rollout("candidate", 10)["candidate_share"] == 10
    with pytest.raises(StateStoreError):
        store.set_rollout("candidate", 30)


def test_minute_book_snapshot_replaces_same_bucket(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.record_book_snapshot([{"rate": "1"}], now_ms=61_000)
    store.record_book_snapshot([{"rate": "2"}], now_ms=89_000)
    rows = store.book_snapshots()
    assert len(rows) == 1
    assert rows[0]["book"][0]["rate"] == "2"


def test_new_websocket_book_snapshot_clears_old_generation():
    hub = BitfinexMarketDataHub()
    hub.handle_public_message({"event": "subscribed", "chanId": 1, "channel": "book"})
    hub.handle_public_message([1, [["0.0001", 2, 1, "10"], ["0.0002", 2, 1, "20"]]])
    hub.handle_public_message([1, [["0.0003", 2, 1, "30"]]])
    assert [row["rate"] for row in hub.snapshot()["book"]] == [D("0.0003")]


def test_websocket_live_readiness_requires_all_account_snapshots():
    hub = BitfinexMarketDataHub("key", "secret")
    hub.handle_public_message({"event": "subscribed", "chanId": 1, "channel": "book"})
    hub.handle_public_message([1, [["0.0001", 2, 1, "10"]]])
    hub.handle_auth_message({"event": "auth", "status": "OK"})
    hub.handle_auth_message([0, "fos", []])
    hub.handle_auth_message([0, "fcs", []])
    assert hub.snapshot()["source"] != "WEBSOCKET"
    hub.handle_auth_message([0, "fls", []])
    assert hub.snapshot()["source"] != "WEBSOCKET"
    hub.handle_auth_message([0, "ws", [["funding", "USD", "0", "0", None]]])
    assert hub.snapshot()["source"] != "WEBSOCKET"
    hub.handle_auth_message([0, "wu", ["funding", "USD", "0", "0", "0"]])
    snapshot = hub.snapshot()
    assert snapshot["source"] == "WEBSOCKET"
    assert snapshot["accountSnapshotsReady"] is True
    assert snapshot["walletAvailableReady"] is True


def test_websocket_funding_loan_snapshot_and_updates_are_tracked_without_stale_rows():
    hub = BitfinexMarketDataHub("key", "secret")
    first = [11, "fUSD", 1, 1000, 1000, "200", None, "ACTIVE", "FIXED", None, None, "0.0002", 2]
    second = [12, "fUSD", 1, 1000, 1000, "266.5", None, "ACTIVE", "FIXED", None, None, "0.0003", 14]
    updated = [12, "fUSD", 1, 1000, 2000, "250", None, "ACTIVE", "FIXED", None, None, "0.0003", 14]
    replacement = [13, "fUSD", 1, 3000, 3000, "100", None, "ACTIVE", "FIXED", None, None, "0.0004", 30]

    hub.handle_auth_message([0, "fls", [first]])
    hub.handle_auth_message([0, "fln", second])
    hub.handle_auth_message([0, "flu", updated])
    hub.handle_auth_message([0, "flc", first])
    assert [(row["id"], row["amount"]) for row in hub.snapshot()["loans"]] == [(12, D("250"))]

    hub.handle_auth_message([0, "fls", [replacement]])
    assert [row["id"] for row in hub.snapshot()["loans"]] == [13]


def test_rest_fallback_is_explicit():
    hub = BitfinexMarketDataHub(rest_stale_seconds=60)
    hub.apply_rest_snapshot(book=[], synced_at_ms=1000)
    assert hub.snapshot(1500)["source"] == "REST_FALLBACK"


def make_handler(headers=None, body=b"{}"):
    handler = object.__new__(lendingbot.DashboardRequestHandler)
    handler.headers = Message()
    for name, value in (headers or {}).items():
        handler.headers[name] = value
    handler.rfile = io.BytesIO(body)
    handler.csrf_token = "token"
    return handler


def valid_headers(**updates):
    values = {
        "Host": "127.0.0.1:8000",
        "Origin": "http://127.0.0.1:8000",
        "X-Mika-CSRF": "token",
        "Content-Type": "application/json",
        "Content-Length": "2",
    }
    values.update(updates)
    return values


def test_dashboard_accepts_valid_same_origin_csrf_request():
    handler = make_handler(valid_headers())
    handler._validate_write_request()
    assert handler._read_json_body() == {}


@pytest.mark.parametrize(
    ("header", "value", "code"),
    [
        ("Host", "evil.example", "INVALID_HOST"),
        ("Origin", "https://evil.example", "INVALID_ORIGIN"),
        ("X-Mika-CSRF", "wrong", "INVALID_CSRF"),
    ],
)
def test_dashboard_rejects_cross_origin_or_bad_token(header, value, code):
    handler = make_handler(valid_headers(**{header: value}))
    with pytest.raises(lendingbot.ApiRequestError) as raised:
        handler._validate_write_request()
    assert raised.value.code == code


def test_dashboard_requires_json_content_type():
    handler = make_handler(valid_headers(**{"Content-Type": "text/plain"}))
    with pytest.raises(lendingbot.ApiRequestError) as raised:
        handler._validate_write_request()
    assert (raised.value.code, raised.value.status) == ("CONTENT_TYPE_REQUIRED", 415)


def test_dashboard_rejects_request_body_over_64k():
    handler = make_handler(valid_headers(**{"Content-Length": "65537"}))
    with pytest.raises(lendingbot.ApiRequestError) as raised:
        handler._validate_write_request()
    assert (raised.value.code, raised.value.status) == ("REQUEST_TOO_LARGE", 413)


def test_legacy_migration_is_idempotent(tmp_path):
    state_dir = tmp_path / ".state"
    state_dir.mkdir(exist_ok=True)
    source = state_dir / "managed-offers.json"
    source.write_text(json.dumps({"offers": {"7": {"currency": "USD", "bucket": "fast"}}}), encoding="utf-8")
    context = AppContext.for_project(tmp_path)
    store = LendingStateStore(context.state_db_path)
    first = lendingbot.migrate_legacy_state(context, store)
    second = lendingbot.migrate_legacy_state(context, store)
    assert first["importedOffers"] == 1
    assert second["idempotent"] is True
    assert len(store.offers()) == 1


def test_offline_commands_refuse_live_lock(tmp_path):
    context = AppContext.for_project(tmp_path)
    lock = lendingbot.LiveProcessLock(context.live_lock_path)
    assert lock.acquire(context.config_path, {"role": "test"})
    try:
        with pytest.raises(lendingbot.ConfigError):
            lendingbot.ensure_offline_database_access(context)
    finally:
        lock.release()


class CountingLimiter:
    def __init__(self):
        self.calls = 0

    def wait(self):
        self.calls += 1


class PublicPages:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.trade_calls = 0
        self.stats_calls = 0

    def funding_trades(self, _symbol, **kwargs):
        self.trade_calls += 1
        rows = [[1, self.start + 100, "1", "0.0002", 2], [2, self.start + 200, "1", "0.0003", 3],
                [3, self.end - 100, "1", "0.0004", 5]]
        return [row for row in rows if kwargs["start"] <= row[1] <= kwargs["end"]][:kwargs["limit"]]

    def funding_stats(self, _symbol, **kwargs):
        self.stats_calls += 1
        rows = [[self.end - 100, 0, 0, "0.0002", 2, 0, 0, "100", "50"],
                [self.start + 100, 0, 0, "0.0002", 2, 0, 0, "100", "50"]]
        return [row for row in rows if kwargs["start"] <= row[0] <= kwargs["end"]][:kwargs["limit"]]


def test_live_market_context_requests_latest_trades_before_applying_limit():
    now = 200 * DAY_MS

    class LatestClient:
        def __init__(self):
            self.trade_kwargs = None

        def funding_book(self, _symbol, _limit):
            return []

        def funding_trades(self, _symbol, **kwargs):
            self.trade_kwargs = kwargs
            return [
                [2, now - 100, "1", "0.0003", 3],
                [1, now - 200, "1", "0.0002", 2],
            ]

        def funding_stats(self, _symbol, **_kwargs):
            return []

    client = LatestClient()
    _book, trades, _stats, _signals, warnings = lendingbot.load_v3_market_context(client, policy(), now)
    assert warnings == []
    assert client.trade_kwargs["sort"] == -1
    assert [row["id"] for row in trades] == ["1", "2"]


def test_public_backfill_pages_and_upserts_without_auth_writes(tmp_path):
    end = 200 * DAY_MS
    start = end - 90 * DAY_MS
    client = PublicPages(start, end)
    limiter = CountingLimiter()
    store = LendingStateStore(tmp_path / "state.sqlite3")
    result = backfill_public_market_data(client, store, days=90, now_ms=end, page_limit=2, rate_limiter=limiter)
    assert result["complete"] is True
    assert result["bookHistory"]["backfillable"] is False
    assert len(store.market_trades(start, end)) == 3
    assert limiter.calls == client.trade_calls + client.stats_calls


def test_market_retention_prunes_all_market_time_series(tmp_path):
    now = 200 * DAY_MS
    old = now - 91 * DAY_MS
    recent = now - 89 * DAY_MS
    store = LendingStateStore(tmp_path / "state.sqlite3")
    store.upsert_market_trades(
        [
            {"id": "old", "mts": old, "amount": D("1"), "rate": D("0.0002"), "period": 2},
            {"id": "recent", "mts": recent, "amount": D("1"), "rate": D("0.0003"), "period": 3},
        ]
    )
    store.upsert_funding_stats(
        [
            {"mts": old, "frr_daily_rate": D("0.0002"), "utilization": D("0.5")},
            {"mts": recent, "frr_daily_rate": D("0.0003"), "utilization": D("0.6")},
        ]
    )
    store.record_book_snapshot([], now_ms=old)
    store.record_book_snapshot([], now_ms=recent)
    store.record_market_bars({"1h": {"median": 1, "q25": 1, "q75": 1}}, now_ms=old)
    store.record_market_bars({"1h": {"median": 2, "q25": 2, "q75": 2}}, now_ms=recent)

    deleted = store.prune_market_data(retention_days=90, now_ms=now)

    assert deleted == {"trades": 1, "bars": 2, "books": 1, "stats": 1}
    assert [row["id"] for row in store.market_trades()] == ["recent"]
    assert [row["mts"] for row in store.funding_stats()] == [recent]
    assert [row["mts"] for row in store.book_snapshots()] == [recent - (recent % 60_000)]


def test_public_backfill_rejects_short_research_window(tmp_path):
    store = LendingStateStore(tmp_path / "state.sqlite3")
    with pytest.raises(ValueError, match="90 days"):
        backfill_public_market_data(SimpleNamespace(), store, days=89)


def test_public_backfill_retries_rate_limit_and_uses_endpoint_limits(tmp_path):
    end = 200 * DAY_MS
    start = end - 90 * DAY_MS
    calls = {"trades": 0, "stats": 0, "stats_limit": None}

    class RateLimitedPages:
        def funding_trades(self, _symbol, **_kwargs):
            calls["trades"] += 1
            if calls["trades"] == 1:
                raise BitfinexApiError("HTTP 429 Too Many Requests: ratelimit")
            return [[1, start + 100, "1", "0.0002", 2], [2, end - 100, "1", "0.0003", 3]]

        def funding_stats(self, _symbol, **kwargs):
            calls["stats"] += 1
            calls["stats_limit"] = kwargs["limit"]
            return [
                [end - 100, 0, 0, "0.0002", 2, 0, 0, "100", "50"],
                [start + 100, 0, 0, "0.0002", 2, 0, 0, "100", "50"],
            ]

    sleeps = []
    result = backfill_public_market_data(
        RateLimitedPages(),
        LendingStateStore(tmp_path / "state.sqlite3"),
        days=90,
        now_ms=end,
        page_limit=10000,
        rate_limiter=CountingLimiter(),
        retry_sleeper=sleeps.append,
    )
    assert result["complete"] is True
    assert calls == {"trades": 2, "stats": 1, "stats_limit": 250}
    assert sleeps == [30.0]


def synthetic_market(store, now):
    trades = []
    stats = []
    start = now - 90 * DAY_MS
    for day in range(90):
        mts = start + day * DAY_MS + 12 * 3_600_000
        for offset, period in enumerate((2, 3, 14, 30, 120)):
            trades.append(
                {
                    "id": f"{day}-{period}",
                    "mts": mts + offset * 1000,
                    "amount": D("5000"),
                    "rate": D("0.0005") + D(day % 7) * D("0.00001"),
                    "period": period,
                }
            )
        stats.append({"mts": mts, "frr_daily_rate": D("0.00045"), "utilization": D("0.7")})
    store.upsert_market_trades(trades)
    store.upsert_funding_stats(stats)


def test_strategy_evaluation_uses_fixed_chronological_splits(tmp_path):
    now = 200 * DAY_MS
    store = LendingStateStore(tmp_path / "state.sqlite3")
    synthetic_market(store, now)
    report = evaluate_strategies(store, policy(), D("1000"), now_ms=now)
    assert [report["split"][name]["days"] for name in ("train", "validation", "test")] == [60, 15, 15]
    assert report["baselines"] == ["A", "B"]
    assert report["comparison"]["strategyLabels"] == ["A", "B", "C"]
    assert report["selection"]["requiresManualPromotion"] is True
    assert report["selection"]["eligibleForLiveCandidate"] is False
    assert report["selection"]["scenarioCount"] == 12


def test_research_variants_preserve_hard_user_boundaries():
    base = policy()
    for variant in research_variants(base).values():
        assert variant.max_lend_amount == base.max_lend_amount
        assert variant.max_lend_percent == base.max_lend_percent
        assert variant.short_floor_apr == base.short_floor_apr


def test_score_weights_are_versioned_research_data_not_policy_fields():
    assert SCORE_MODEL_VERSION == "v3-score-2026-07-22"
    assert sum(V3_RESEARCH_SCORE_WEIGHTS.values(), D("0")) == D("100")
    assert not any(name.startswith("score_") for name in policy_v3_to_json(policy()))


def test_paired_bootstrap_detects_consistent_positive_gain():
    interval = paired_bootstrap_interval([D("0.2")] * 15, [D("0.1")] * 15, iterations=500)
    assert interval["lower"] > 0
    assert interval["samples"] == 15


def test_research_report_is_stable_json(tmp_path):
    path = tmp_path / "report.json"
    write_research_report(path, {"value": "ok", "eligible": False})
    assert json.loads(path.read_text(encoding="utf-8")) == {"eligible": False, "value": "ok"}


def test_replay_window_excludes_older_trades():
    now = 10 * DAY_MS
    trades = [
        {"id": "old", "mts": now - 3 * DAY_MS, "amount": D("10"), "rate": D("0.0005"), "period": 2},
        {"id": "new", "mts": now - 1000, "amount": D("10"), "rate": D("0.0005"), "period": 2},
    ]
    result = replay_strategy_v3(policy(), trades, [], D("1000"), now_ms=now, window_ms=DAY_MS)
    assert result["sampleCount"] == 1


def test_replay_signals_do_not_read_future_funding_stats(monkeypatch):
    now = 10 * DAY_MS
    start = now - DAY_MS
    trades = [
        {
            "id": str(index),
            "mts": start + index * 15 * 60_000 + 1,
            "amount": D("1000"),
            "rate": D("0.0005"),
            "period": 2,
        }
        for index in range(8)
    ]
    stats = [
        {"mts": start, "frr_daily_rate": D("0.0004"), "utilization": D("0.5")},
        {"mts": start + 90 * 60_000, "frr_daily_rate": D("0.0099"), "utilization": D("0.9")},
    ]
    import EventReplay

    original = EventReplay.build_market_signals_v3
    observed = []

    def checked_signals(book, history, visible_stats, active_policy, now_ms=None, **kwargs):
        observed.append((now_ms, [row["mts"] for row in visible_stats]))
        assert all(int(row["mts"]) <= int(now_ms) for row in visible_stats)
        return original(book, history, visible_stats, active_policy, now_ms, **kwargs)

    monkeypatch.setattr(EventReplay, "build_market_signals_v3", checked_signals)
    books = [{"mts": row["mts"], "observed_at_ms": row["mts"], "book": []} for row in trades]
    replay_strategy_v3(policy(), trades, stats, D("1000"), now_ms=now, window_ms=DAY_MS,
                       book_snapshots=books, assumptions={"require_full_window": False})
    assert observed


def test_app_context_roots_all_runtime_paths(tmp_path):
    context = AppContext.for_project(tmp_path, config_path="config/test.cfg", state_db_path="data/state.sqlite3")
    assert context.config_path == os.path.join(str(tmp_path), "config/test.cfg")
    assert context.state_db_path == os.path.join(str(tmp_path), "data/state.sqlite3")
    assert context.live_lock_path.startswith(str(tmp_path))


def test_exchange_account_parsers_share_symbol_normalization():
    wallet = parse_wallet_rows([["funding", "USD", "10", None, "9"]])[0]
    offer = parse_offer_rows(
        [[1, "fUSD", 1, 2, "3", "4", "LIMIT", None, None, 0, "ACTIVE", None, None, None, "0.1", 2]]
    )[0]
    credit = parse_credit_rows([[2, "fUSD", None, 1, 2, "3", None, "ACTIVE", "FIXED", None, None, "0.1", 2]])[0]
    loan = parse_loan_rows([[3, "fUSD", 1, 1, 2, "4", None, "ACTIVE", "FIXED", None, None, "0.1", 2]])[0]
    assert (wallet["currency"], offer["currency"], credit["currency"], loan["currency"]) == (
        "USD",
        "USD",
        "USD",
        "USD",
    )
    assert loan["side"] == 1
    assert loan["funding_state"] == "loan"


def test_unknown_websocket_available_balance_is_not_promoted_to_wallet_balance():
    wallet = parse_wallet_rows([["funding", "USD", "1000", "0", None]])[0]

    assert wallet["balance"] == D("1000")
    assert wallet["available"] is None

    account = LendingRuntimeV3._account(
        {
            "wallets": [wallet],
            "offers": [{"currency": "USD", "amount": D("600"), "period": 2}],
            "credits": [
                {
                    "id": 2,
                    "currency": "USD",
                    "amount": D("400"),
                    "period": 2,
                    "side": 1,
                    "funding_state": "credit",
                }
            ],
            "loans": [],
        }
    )
    assert account["wallet"] == D("0")
    assert account["walletAvailableKnown"] is False
    assert account["componentTotal"] is None
    assert account["reconciliationStatus"] == "UNAVAILABLE"


def test_account_total_includes_provider_funding_loans_and_matches_wallet_balance():
    snapshot = {
        "wallets": [
            {
                "wallet_type": "funding",
                "currency": "USD",
                "balance": D("13635.96811969"),
                "available": D("0"),
            }
        ],
        "offers": [{"id": 1, "currency": "USD", "amount": D("663.97229357"), "period": 2}],
        "credits": [
            {
                "id": 2,
                "currency": "USD",
                "amount": D("12505.49582612"),
                "period": 14,
                "side": 1,
                "funding_state": "credit",
            }
        ],
        "loans": [
            {
                "id": 3,
                "currency": "USD",
                "amount": D("466.5"),
                "period": 2,
                "side": 1,
                "funding_state": "loan",
            },
            {
                "id": 4,
                "currency": "USD",
                "amount": D("999"),
                "period": 2,
                "side": -1,
                "funding_state": "loan",
            },
        ],
    }

    account = LendingRuntimeV3._account(snapshot)

    assert account["creditPrincipal"] == D("12505.49582612")
    assert account["loanPrincipal"] == D("466.5")
    assert account["credits"] == D("12971.99582612")
    assert account["componentTotal"] == D("13635.96811969")
    assert account["total"] == D("13635.96811969")
    assert account["walletAvailableKnown"] is True
    assert account["reconciliationDifference"] == D("0")
    assert account["reconciliationStatus"] == "MATCHED"


def test_public_rate_limiter_uses_injected_clock_and_sleep():
    times = iter([0.0, 0.2, 1.0])
    sleeps = []
    limiter = PublicRateLimiter(1.0, clock=lambda: next(times), sleeper=sleeps.append)
    limiter.wait()
    limiter.wait()
    assert sleeps == [0.8]
