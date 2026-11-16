import sqlite3
from dataclasses import replace
from decimal import Decimal as D
from contextlib import closing

import pytest

from ManualProtection import MANUAL_PROTECTION_MS as TWO_HOURS
from MarketDataStream import BitfinexMarketDataHub
from RuntimeV3 import LendingRuntimeV3
from StateStore import LendingStateStore
from StrategyV3 import StrategyPolicyV3
from lendingbot import proposed_external_adoption


NOW = 1_900_000_000_000


def offer(**overrides):
    return dict(id=9001, currency="USD", amount=D("175"), amount_original=D("175"),
                rate=D(".0004"), rate_real=None, period=2, offer_type="LIMIT", flags=0,
                status="ACTIVE", pool="short", mts_created=NOW, mts_updated=NOW, **overrides)


def configured():
    return replace(StrategyPolicyV3(), short_floor_apr=D(".04"), medium_floor_apr=D(".05"),
                   long_floor_apr=D(".06"))


def managed(store, item):
    order = dict(currency="USD", slice_key="v3:short:quick:0", pool="short", layer="quick",
                 amount=item["amount_original"], submitted_rate=item["rate"], effective_rate=item["rate"],
                 period=item["period"], offer_type=item["offer_type"], flags=item["flags"], strategy_version="v3")
    _, intent = store.reserve_intent(order, D("1000"))
    store.confirm_intent(intent["id"], item["id"])
    store.reconcile_offers([dict(item, managed=True)], NOW)


class Client:
    api_key = api_secret = ""

    def __init__(self, item):
        self.item = item
        self.canceled = []

    def active_funding_offers(self, symbol):
        o = self.item
        return [[o["id"], "fUSD", o["mts_created"], o["mts_updated"], str(o["amount"]),
                 str(o["amount_original"]), o["offer_type"], None, None, o["flags"], o["status"],
                 None, None, None, str(o["rate"]), o["period"]]] if o else []

    def cancel_funding_offer(self, identity):
        self.canceled.append(identity)
        return [0, "SUCCESS"]


def confirmed(store, item, now=NOW):
    store.observe_external_takeover(item, now)
    store.observe_external_takeover(item, now + 30_000)


def test_manual_offer_boundary_partial_fills_and_restart(tmp_path):
    path = tmp_path / "state.db"
    store = LendingStateStore(path, clock=lambda: NOW / 1000)
    item = offer()
    store.reconcile_offers([item], NOW)
    confirmed(store, item)
    assert store.adopt_external_offers([item], "v3", NOW + TWO_HOURS - 1000) == []
    partial = dict(item, amount=D("40"), status="PARTIALLY FILLED", mts_updated=NOW + 3_600_000)
    store.reconcile_offers([partial], NOW + 3_600_000)
    reloaded = LendingStateStore(path, clock=lambda: (NOW + TWO_HOURS) / 1000)
    protection = reloaded.manual_protection_for_offer(partial)
    assert protection["protectedUntilMs"] == NOW + TWO_HOURS
    assert not protection["protected"]
    confirmed(reloaded, partial, NOW + TWO_HOURS - 30_000)
    assert reloaded.adopt_external_offers([partial], "v3") == [item["id"]]
    client = Client(partial)
    runtime = LendingRuntimeV3(client, configured(), reloaded, hub=object())
    assert runtime._cancel_external_takeovers(NOW + TWO_HOURS, "v3") == [item["id"]]
    assert client.canceled == [9001]
    reloaded.reconcile_offers([], NOW + TWO_HOURS + 1000)
    assert reloaded.manual_protections()[9001]["status"] == "CLOSED"


def test_modified_robot_order_retires_chain_and_resets_timer(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    original = offer()
    managed(store, original)
    stored = store.offers(True)[0]
    chain = store.ensure_reprice_chain(stored, "v3", NOW)
    assert not store.manual_protections()
    first = dict(original, rate=D(".0005"), mts_updated=NOW + 1000)
    store.reconcile_offers([first], NOW + 2000)
    second = dict(first, period=30, pool="medium", mts_updated=NOW + 60_000)
    store.reconcile_offers([second], NOW + 61_000)
    assert store.manual_protection_for_offer(second, NOW + TWO_HOURS)["protected"]
    assert store.manual_protection_for_offer(second, NOW + 61_000)["protectedUntilMs"] == NOW + 60_000 + TWO_HOURS
    with store.read_connection() as db:
        row = db.execute("SELECT status FROM reprice_chains WHERE chain_key=?", (chain["chain_key"],)).fetchone()
        assert row[0] == "ABANDONED"
        assert db.execute("SELECT COUNT(*) FROM order_intents").fetchone()[0] == 1
    assert store.offers(True)[0]["period"] == 30
    assert store.offers(True)[0]["managed"]
    confirmed(store, second, NOW + 60_000)
    assert not store.queue_manual_retake(second, NOW + TWO_HOURS)
    assert store.queue_manual_retake(second, NOW + 60_000 + TWO_HOURS)
    client = Client(second)
    runtime = LendingRuntimeV3(client, configured(), store, hub=object())
    assert runtime._cancel_external_takeovers(NOW + 60_000 + TWO_HOURS, "v3") == [9001]


@pytest.mark.parametrize("changes", [dict(amount=D("40"), mts_updated=NOW + 1000),
                                    dict(rate_real=D(".0006"), mts_updated=NOW + 1000),
                                    dict(mts_updated=NOW + 1000)])
def test_robot_fill_frr_and_time_only_do_not_start_protection(tmp_path, changes):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = offer()
    managed(store, item)
    store.reconcile_offers([dict(item, **changes)], NOW + 1000)
    assert store.manual_protections() == {}


@pytest.mark.parametrize("stamp", [0, NOW + 99_000, "bad"])
def test_missing_or_invalid_exchange_time_falls_back(tmp_path, stamp):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = dict(offer(), mts_created=stamp, mts_updated=stamp)
    protection = store.observe_manual_offer(item, NOW)
    assert protection["timeSource"] == "FIRST_OBSERVED"
    assert protection["protectedUntilMs"] == NOW + TWO_HOURS


def test_preflight_preview_is_read_only_and_skips_protected_manual_offer(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = offer()
    item["manualProtection"] = store.manual_protection_for_offer(item, NOW)
    _, _, candidates = proposed_external_adoption({"offers": [item]}, configured())
    assert not candidates
    assert store.manual_protections() == {}
    assert store.offers() == []


def test_cancel_rechecks_manual_edit_and_safety_override_is_audited(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = offer()
    managed(store, item)
    client = Client(dict(item, rate=D(".000001"), mts_updated=NOW + 1000))
    runtime = LendingRuntimeV3(client, configured(), store, hub=object())
    assert not runtime._manual_cancel_allowed(store.offers(True)[0], NOW + 1000)
    store.reconcile_offers([client.item], NOW + 1000)
    stored = store.offers(True)[0]
    assert not runtime._manual_cancel_allowed(stored, NOW + 1000)
    assert runtime._cancel_hard_adjustments([(stored, None, "below_new_floor")], NOW + 1000) == [9001]
    assert "below_new_floor" in store.manual_protections()[9001]["reason"]
    assert len(client.canceled) == 1


def test_external_safety_override_requires_two_confirmations(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = dict(offer(), rate=D(".000001"))
    store.reconcile_offers([item], NOW)
    assert store.adopt_external_offers([item], "v3", NOW, safety_reason="below_new_floor") == []
    confirmed(store, item)
    client = Client(item)
    runtime = LendingRuntimeV3(client, configured(), store, hub=object())
    result = runtime._pending_adjustments({}, {"existingExposure": {"total": D("175")}},
                                          configured(), {"funding_cap": D("1000")}, NOW + 30_000)
    assert len(result) == 1
    assert runtime._cancel_hard_adjustments(result, NOW + 30_000, strategy_version="v3") == [9001]
    assert store.offers(True)[0]["managed"]


def test_schema18_migration_backs_up_without_modifying_operational_rows(tmp_path):
    path = tmp_path / "state.db"
    store = LendingStateStore(path, clock=lambda: NOW / 1000)
    managed(store, offer())
    store.set_mode("LIVE", "fixture")
    tables = ("offers", "order_intents", "ownership_events", "runtime_state", "recovery_state")
    with store.transaction(immediate=True) as db:
        db.execute("DROP TABLE manual_offer_protection")
        db.execute("UPDATE schema_meta SET value='18' WHERE key='schema_version'")
        before = {t: [tuple(r) for r in db.execute(f"SELECT * FROM {t}")] for t in tables}
    migrated = LendingStateStore(path)
    with migrated.read_connection() as db:
        assert before == {t: [tuple(r) for r in db.execute(f"SELECT * FROM {t}")] for t in tables}
        assert db.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0] == "19"
    with closing(sqlite3.connect(migrated.migration_backup_path)) as backup:
        assert backup.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0] == "18"


@pytest.mark.parametrize("robot", [False, True])
def test_authoritative_runtime_sync_waits_then_adopts_remaining_order(tmp_path, robot):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = offer()
    if robot:
        managed(store, item)
        item = dict(item, rate=D(".0005"), mts_updated=NOW + 1000)
    client = Client(item)
    client.funding_book = lambda *_: [[".0004", 2, 1, "-1000"]]
    client.funding_trades = lambda *_, **__: []
    client.funding_stats = lambda *_, **__: []
    client.wallets = lambda: [["funding", "USD", "175", None, "0"]]
    client.active_funding_credits = lambda *_: []
    client.active_funding_loans = lambda *_: []
    hub = BitfinexMarketDataHub(client)
    runtime = LendingRuntimeV3(client, configured(), store, hub=hub, clock=lambda: NOW / 1000)
    runtime.history = None
    runtime.sync_history = lambda *_: None
    runtime.sync_rest(now_ms=NOW + 2000)
    runtime.sync_rest(now_ms=NOW + 32_000)
    assert store.external_takeovers()[0]["state"] == "CONFIRMED"
    assert runtime._cancel_external_takeovers(NOW + TWO_HOURS - 1000, "v3") == []
    runtime.sync_rest(now_ms=NOW + TWO_HOURS + 1000)
    assert store.external_takeovers()[0]["state"] == "ADOPTED"
    assert runtime._cancel_external_takeovers(NOW + TWO_HOURS + 1000, "v3") == [9001]
    assert client.canceled == [9001]
    with store.read_connection() as db:
        assert db.execute("SELECT COUNT(*) FROM order_intents").fetchone()[0] == 1


def test_ordinary_reprice_and_dust_skip_manually_modified_robot_order(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = offer()
    managed(store, item)
    changed = dict(item, rate=D(".0005"), mts_updated=NOW + 1000)
    store.reconcile_offers([changed], NOW + 1000)
    runtime = LendingRuntimeV3(Client(changed), configured(), store, hub=object(), clock=lambda: NOW / 1000)
    assert runtime._cancel_reprice_candidates({}, {"plan": []}, NOW + 1000, "v3") == []
    assert runtime._dust_consolidation({"wallet": D("50")}, {}, NOW + 1000, "v3")["canceled"] == []
    assert runtime.client.canceled == []


def test_late_robot_confirmation_clears_false_manual_classification(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    item = offer()
    order = dict(currency="USD", slice_key="v3:short:quick:0", pool="short", layer="quick",
                 amount=D("175"), submitted_rate=D(".0004"), effective_rate=D(".0004"),
                 period=2, offer_type="LIMIT", flags=0, strategy_version="v3")
    _, intent = store.reserve_intent(order, D("1000"))
    store.mark_ambiguous(intent["id"], "submit timed out")
    store.reconcile_offers([item], NOW)
    confirmed(store, item)
    assert store.manual_protections(NOW)[9001]["protected"]
    result = store.reconcile_ambiguous_candidates(now_ms=NOW + 30_000)
    assert result == [{"intentId": intent["id"], "offerId": 9001}]
    assert store.manual_protections(NOW + 30_000)[9001]["status"] == "ROBOT_CONFIRMED"
    assert not store.manual_protections(NOW + 30_000)[9001]["needsTakeover"]
    assert store.external_takeovers() == []


def test_new_robot_offer_gets_no_protection_but_manual_cancel_repost_does(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: NOW / 1000)
    original = offer()
    managed(store, original)
    store.reconcile_offers([], NOW + 1000)
    repost = dict(original, id=9002, mts_created=NOW + 2000, mts_updated=NOW + 2000)
    store.reconcile_offers([repost], NOW + 3000)
    assert store.manual_protections(NOW + 3000)[9002]["protected"]
    assert store.manual_protection_for_offer(repost, NOW + TWO_HOURS)["protected"]
