import configparser
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

import RuntimeV3
from Configuration import (
    strategy_v3_api_values, strategy_v3_config_values, strategy_v3_from_api_payload,
    strategy_v3_from_config,
)
from MarketHistory import MarketHistoryService, compute_window_snapshot, policy_fingerprint
from MarketDataStream import BitfinexMarketDataHub
from RuntimeV3 import LendingRuntimeV3, _exploration_age_stage_target
from StateStore import LendingStateStore
from StrategyV3 import (
    EXECUTABLE_QUICK_MODE, LEGACY_QUICK_MODE, QUICK_EXECUTION_CURVE, RATE_TICK,
    StrategyPolicyV3, _candidate_target_rate, build_market_signals_v3,
    build_strategy_plan_v3, ceil_rate_tick, executable_quick_quote, gross_daily_floor,
    json_decimal, net_apr_from_daily, validate_policy_v3,
)


NOW = 1_900_000_000_000


def policy(**values):
    return replace(StrategyPolicyV3(), short_floor_apr=D('.01'), medium_floor_apr=D('.01'),
                   long_floor_apr=D('.01'), enable_limit=True, enable_frr=False,
                   enable_frr_delta_fixed=False, enable_frr_delta_variable=False,
                   quick_pricing_mode=EXECUTABLE_QUICK_MODE, **values)


def signals(levels=(), *, count=30, effective=30, median='.0003', median24=None):
    row = {'period': 2, 'borrowLevels': [{'rate': D(rate), 'amount': D(amount)} for rate, amount in levels],
           'rateWindows': {'1h': {'count': count, 'median': D(median)},
                           '24h': {'count': 30, 'median': D(median24 or median)}},
           'dataQuality': {'effectiveSampleSize': D(effective)}}
    return {'completeWindowUsable': True, 'periodSelection': {'byPool': {'short': {'scores': [row]}}}}


def quote(sig, amount='300', floor='.0001', **changes):
    return executable_quick_quote({'layer': 'quick', 'pool': 'short', 'period': 2, 'amount': D(amount)},
                                  sig, D(floor), policy(**changes))


def test_quick_premium_is_net_annual_percentage_points_after_fee_and_ceils_tick():
    p = policy()
    result = quote(signals([('.0004', '100'), ('.0003', '200'), ('.0002', '500')]))
    expected = ceil_rate_tick(gross_daily_floor(net_apr_from_daily(D('.0003'), p.normal_fee_rate)
                                               + D('.001'), p.normal_fee_rate))
    assert result['referenceRate'] == D('.0003')
    assert result['targetRate'] == expected
    assert result['targetRate'] % RATE_TICK == 0
    assert result['fixedLandingRate'] == D('.0003')
    assert result['premiumAprPoints'] == D('.10')
    assert result['pricingCurveVersion'] == QUICK_EXECUTION_CURVE


def test_fee_specific_quote_applies_premium_after_actual_fee():
    p = policy()
    result = executable_quick_quote({'layer': 'quick', 'pool': 'short', 'period': 2, 'amount': D(150)},
                                    signals([('.0003', '500')]), D('.0001'), p, fee_rate=D('.25'))
    assert result['targetRate'] == ceil_rate_tick(D('.0003') + D('.001') / (D(365) * D('.75')))


@pytest.mark.parametrize('amount,full', [('150', True), ('299', False)])
def test_partial_book_depth_is_usable_only_above_minimum(amount, full):
    result = quote(signals([('.0004', amount)]), amount='300' if not full else '150')
    assert result['depthSufficient'] is full
    assert result['referenceRate'] == D('.0004')


def test_reference_below_floor_is_ignored_and_median_respects_hard_floor():
    result = quote(signals([('.00001', '10000')], median='.00001'), floor='.0003')
    assert result['referenceSource'].startswith('EXACT_TERM_MEDIAN_')
    assert result['targetRate'] == D('.0003')
    assert result['fixedLandingRate'] == D('.0003')


def test_sparse_hour_falls_back_to_day_and_sparse_day_to_legacy():
    sig = signals([('.0005', '149')], count=29, median24='.0002')
    assert quote(sig)['referenceSource'] == 'EXACT_TERM_MEDIAN_24H'
    sig['periodSelection']['byPool']['short']['scores'][0]['rateWindows']['24h']['count'] = 29
    assert quote(sig) is None
    assert quote(signals(effective=9)) is None


@pytest.mark.parametrize('change', ['term', 'disabled', 'old-input', 'legacy'])
def test_no_other_term_reference_and_fail_closed_mode_switches(change):
    sig = signals([('.0004', '1000')])
    values = {}
    if change == 'term':
        sig['periodSelection']['byPool']['short']['scores'][0]['period'] = 7
    elif change == 'disabled':
        sig['quickPricingEnabled'] = False
    elif change == 'old-input':
        sig['completeWindowUsable'] = False
    else:
        values['quick_pricing_mode'] = LEGACY_QUICK_MODE
        return_value = executable_quick_quote(
            {'layer': 'quick', 'pool': 'short', 'period': 2, 'amount': D(300)}, sig, D('.0001'),
            replace(policy(), **values))
        assert return_value is None
        return
    assert quote(sig) is None


def test_new_quick_does_not_add_q75_guard_or_slice_ladder():
    p = policy(balanced_start_premium_percent=D('100'))
    sig = signals([('.0002', '500')])
    sig['periodSelection']['byPool']['short']['scores'][0]['rateWindows']['24h']['q75'] = D('.01')
    targets = [_candidate_target_rate({'pool': 'short', 'period': 2, 'layer': 'quick',
                                      'amount': D(150), 'slice_index': index}, sig, D('.0001'), p)
               for index in range(10)]
    assert len(set(targets)) == 1
    assert targets[0] < D('.00021')


def market(p):
    trades = [{'id': i, 'mts': NOW - 120_000 - i * 1000, 'amount': D(200), 'rate': D('.0003'), 'period': 2}
              for i in range(40)]
    snapshot = compute_window_snapshot(trades, p, NOW, coverage_complete=True)
    snapshot['usable'] = True
    return trades, snapshot


@pytest.mark.parametrize('bad', ['age', 'future', 'fingerprint', 'cleaning', 'coverage', 'schema'])
def test_full_window_rejects_stale_mismatched_or_uncertified_snapshots(bad):
    p = policy()
    trades, snapshot = market(p)
    if bad == 'age':
        snapshot['asOfMs'] = NOW - 60_001
    elif bad == 'future':
        snapshot['asOfMs'] = NOW + 1
    elif bad == 'fingerprint':
        snapshot['policyFingerprint'] = 'other'
    elif bad == 'cleaning':
        snapshot['cleaningVersion'] = 'old'
    elif bad == 'schema':
        snapshot['schemaVersion'] = 0
    else:
        snapshot['coverage']['complete'] = False
    result = build_market_signals_v3([], trades, [], p, NOW, window_snapshot=snapshot)
    assert result['historyMode'] == 'LEGACY_INPUT'


def test_plan_marks_new_curve_with_fixed_reference_and_keeps_balanced_old_formula():
    p = policy(short_share=D(100), medium_share=D(0), long_share=D(0), short_periods=(2,))
    trades, snapshot = market(p)
    book = [{'period': 2, 'rate': D('.0003'), 'amount': D(-10000), 'count': 1}]
    sig = build_market_signals_v3(book, trades, [], p, NOW, window_snapshot=snapshot)
    assert sig['completeWindowUsable']
    plan = build_strategy_plan_v3(D(1500), D(1500), {}, p, sig)
    quick = [r for r in plan['plan'] if r['layer'] == 'quick']
    assert quick
    assert all(r['pricing_curve_version'] == QUICK_EXECUTION_CURVE for r in quick)
    assert all(r['fixed_landing_rate'] == D('.0003') for r in quick)
    old = build_strategy_plan_v3(D(1500), D(1500), {}, replace(p, quick_pricing_mode=LEGACY_QUICK_MODE), sig)
    for layer in ('balanced', 'high'):
        assert [r['target_rate'] for r in plan['plan'] if r['layer'] == layer] == [
            r['target_rate'] for r in old['plan'] if r['layer'] == layer]


def test_new_curve_uses_original_ten_stages_and_fixed_landing_when_market_rises():
    p = policy()
    assert p.reprice_stages('short', 'quick', QUICK_EXECUTION_CURVE) == p.quick_short_reprice_stages_minutes
    chain = {'origin_rate': D('.0004'), 'fixed_landing_rate': D('.0003')}
    values = [_exploration_age_stage_target(stage, chain, D('.001'), D('.0001'), 5, 10)
              for stage in range(1, 11)]
    assert values == sorted(values, reverse=True)
    assert values[4] == D('.0003') and values[-1] == D('.0001')


def test_configuration_round_trip_preserves_point_units_and_legacy_default():
    cfg = configparser.ConfigParser()
    assert strategy_v3_from_config(cfg).quick_pricing_mode == LEGACY_QUICK_MODE
    p = policy()
    cfg['STRATEGY_V3'] = strategy_v3_config_values(p)
    assert strategy_v3_from_config(cfg) == p
    payload = strategy_v3_api_values(p)
    assert payload['quick_start_premium_apr_points'] == '0.10'
    assert strategy_v3_from_api_payload(payload) == p


@pytest.mark.parametrize('mode,premium', [('bad', '.10'), (EXECUTABLE_QUICK_MODE, '-1'),
                                         (EXECUTABLE_QUICK_MODE, 'NaN'), (EXECUTABLE_QUICK_MODE, '1.01')])
def test_invalid_quote_configuration_is_rejected(mode, premium):
    with pytest.raises(ValueError):
        validate_policy_v3(replace(policy(), quick_pricing_mode=mode, quick_start_premium_apr_points=D(premium)))


def test_runtime_certificate_gate_and_quality_fallback(tmp_path, monkeypatch):
    p = policy()
    trades, window = market(p)
    history = SimpleNamespace(snapshot=lambda *args: deepcopy(window))
    runtime = LendingRuntimeV3(SimpleNamespace(), p, LendingStateStore(tmp_path / 'state.db'),
                               hub=SimpleNamespace(), history_service=history)
    snapshot = {'book': [], 'trades': trades}
    sig = runtime._market_signals(snapshot, p, NOW)
    assert not sig['completeWindowUsable']
    assert runtime._history_status['fallbackReason'] == 'VALIDATION_GATE_NOT_PASSED'
    monkeypatch.setattr(RuntimeV3, 'validation_status', lambda *args: {'approved': True})
    assert runtime._market_signals(snapshot, p, NOW)['quickPricingEnabled']
    window['coverage']['complete'] = False
    window['state'] = 'BACKFILLING'
    sig = runtime._market_signals(snapshot, p, NOW)
    assert not sig['quickPricingEnabled']
    assert runtime._history_status['fallbackReason'] == 'BACKFILLING'


def test_rest_book_records_response_time_and_history_endpoint_shares_limiter(tmp_path):
    clock = [NOW / 1000]

    class Client:
        def funding_book(self, *args):
            clock[0] += 5
            return [[.0003, 2, 1, -1000]]

        def funding_trades(self, *args, **kwargs):
            raise AssertionError('history request must respect the shared limiter')

        def wallets(self):
            return [['funding', 'USD', '0', None, '0']]

        def __getattr__(self, name):
            return lambda *args, **kwargs: []

    store = LendingStateStore(tmp_path / 'state.db', clock=lambda: clock[0])
    history = SimpleNamespace(public_read_limiter=SimpleNamespace(try_acquire=lambda: False),
                              ingest=lambda *args, **kwargs: None)
    hub = BitfinexMarketDataHub('', '', store=store)
    runtime = LendingRuntimeV3(Client(), policy(), store, hub=hub, history_service=history,
                               clock=lambda: clock[0])
    runtime._last_history_sync_ms = NOW
    runtime.sync_rest(now_ms=NOW)
    rows = list(store.iter_book_snapshots(NOW - 60_000, NOW + 60_000))
    assert rows[0]['observed_at_ms'] == NOW + 5000
    assert rows[0]['visible_at_ms'] == NOW + 5000


@pytest.mark.parametrize('explicit_time,pending', [(False, False), (True, False), (False, True)])
def test_cycle_observes_post_rest_window_at_decision_time(tmp_path, monkeypatch, explicit_time, pending):
    clock = [NOW / 1000]
    candidate = policy()
    active = replace(candidate, quick_pricing_mode=LEGACY_QUICK_MODE) if pending else candidate
    store = LendingStateStore(tmp_path / 'state.db', clock=lambda: clock[0])
    store.save_strategy(json_decimal(vars(active)), 'ACTIVE')
    if pending:
        store.save_strategy(json_decimal(vars(candidate)), 'PENDING')
    store.set_mode('LIVE', 'test')
    hub = BitfinexMarketDataHub('', '', store=store)
    history = MarketHistoryService(store, SimpleNamespace(), clock=lambda: clock[0])
    runtime = LendingRuntimeV3(SimpleNamespace(), active, store, hub=hub,
                               history_service=history, clock=lambda: clock[0])
    runtime._bootstrapped = True
    trades, _ = market(candidate)

    def slow_rest(**kwargs):
        clock[0] += 20
        window = compute_window_snapshot(trades, candidate, NOW + 15_000, coverage_complete=True)
        window.update(state='READY', status='READY', usable=True)
        history._published[policy_fingerprint(candidate)] = window
        hub.apply_rest_snapshot(
            book=[], trades=trades, wallets=[['funding', 'USD', '0', None, '0']],
            offers=[], credits=[], synced_at_ms=NOW + 20_000,
        )
        return hub.snapshot(NOW)

    monkeypatch.setattr(RuntimeV3, 'validation_status',
                        lambda *_: {'approved': True, 'reportDigest': '0' * 64})
    monkeypatch.setattr(runtime, 'sync_rest', slow_rest)
    monkeypatch.setattr(runtime, '_strategy_status', lambda snapshot, signals, result: signals)
    result = runtime.cycle(NOW) if explicit_time else runtime.cycle()
    assert result['completeWindowUsable'] is (not explicit_time)
    assert result['quickPricingEnabled'] is (not explicit_time)
    assert runtime._history_status['state'] == ('STALE' if explicit_time else 'READY')
    if pending:
        assert store.strategy('PENDING') is None
        assert store.strategy('ACTIVE')['policy']['quick_pricing_mode'] == EXECUTABLE_QUICK_MODE


def test_live_cycle_rechecks_hub_freshness_after_slow_rest(tmp_path, monkeypatch):
    clock = [NOW / 1000]
    p = replace(policy(), quick_pricing_mode=LEGACY_QUICK_MODE)
    store = LendingStateStore(tmp_path / 'state.db', clock=lambda: clock[0])
    store.save_strategy(json_decimal(vars(p)), 'ACTIVE')
    store.set_mode('LIVE', 'test')
    hub = BitfinexMarketDataHub('', '', store=store)
    runtime = LendingRuntimeV3(SimpleNamespace(), p, store, hub=hub, clock=lambda: clock[0])
    runtime._bootstrapped = True

    def slow_rest(**kwargs):
        clock[0] += 70
        hub.apply_rest_snapshot(book=[], wallets=[['funding', 'USD', '0', None, '0']],
                                offers=[], credits=[], synced_at_ms=NOW)
        old_snapshot = hub.snapshot(NOW)
        assert not old_snapshot['safeRequired']
        return old_snapshot

    monkeypatch.setattr(runtime, 'sync_rest', slow_rest)
    monkeypatch.setattr(runtime, '_strategy_status', lambda snapshot, signals, result: signals)
    runtime.cycle()
    assert store.runtime()['mode'] == 'PAUSED'
    assert store.runtime()['safe_reason'] == 'MARKET_DATA_STALE'
