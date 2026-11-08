import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import lendingbot
from AppContext import AppContext
from StateStore import LendingStateStore
from StrategyV3 import LEGACY_QUICK_MODE, json_decimal
from V36Validation import validation_status
from test_v36_validation import _candidate, _valid_report


def test_offline_activation_checks_all_evidence_and_is_idempotent(tmp_path, capsys):
    context = AppContext.for_project(tmp_path)
    Path(context.config_path).write_text('[BOT]\n', encoding='utf-8')
    report_path = tmp_path / 'approved.json'
    p = _candidate()
    report_path.write_text(json.dumps(_valid_report(p)), encoding='utf-8')
    args = lendingbot.parse_args(['--apply-validated-v36', str(report_path)])
    settings = SimpleNamespace(strategy_v3=p)
    assert lendingbot.run_offline_commands(args, settings, context) == 0
    first = json.loads(capsys.readouterr().out)['v36Activation']
    assert lendingbot.run_offline_commands(args, settings, context) == 0
    assert json.loads(capsys.readouterr().out)['v36Activation'] == first
    store = LendingStateStore(context.state_db_path)
    assert validation_status(store, p)['approved']
    assert store.strategy('PENDING') is None
    assert store.runtime()['mode'] == 'PAUSED'
    with store.read_connection() as connection:
        count = connection.execute(
            "SELECT count(*) FROM strategy_events WHERE event_type='V36_ACTIVATED'"
        ).fetchone()[0]
        assert count == 1


def test_failed_evidence_cannot_change_active_or_record_cutover(tmp_path):
    context = AppContext.for_project(tmp_path)
    p = _candidate()
    old = replace(p, quick_pricing_mode=LEGACY_QUICK_MODE)
    store = LendingStateStore(context.state_db_path)
    old_id = store.save_strategy(json_decimal(vars(old)), 'ACTIVE')
    report = _valid_report(p)
    report['comparison']['scenarioResults'][0]['bootstrap']['A']['lower'] = '0'
    path = tmp_path / 'rejected.json'
    path.write_text(json.dumps(report), encoding='utf-8')
    args = lendingbot.parse_args(['--apply-validated-v36', str(path)])
    with pytest.raises(ValueError):
        lendingbot.run_offline_commands(args, SimpleNamespace(strategy_v3=p), context)
    assert store.strategy('ACTIVE')['version_id'] == old_id
    assert store.release_boundary()['v36ActivatedAtMs'] is None
    assert not validation_status(store, p)['approved']


def test_activation_cannot_overwrite_current_hard_boundaries(tmp_path):
    context = AppContext.for_project(tmp_path)
    p = _candidate()
    current = replace(p, short_floor_apr=p.short_floor_apr / 2)
    store = LendingStateStore(context.state_db_path)
    original = store.save_strategy(json_decimal(vars(current)), 'ACTIVE')
    path = tmp_path / 'different-policy.json'
    path.write_text(json.dumps(_valid_report(p)), encoding='utf-8')
    args = lendingbot.parse_args(['--apply-validated-v36', str(path)])
    with pytest.raises(ValueError, match='cannot change allocation'):
        lendingbot.run_offline_commands(args, SimpleNamespace(strategy_v3=p), context)
    assert store.strategy('ACTIVE')['version_id'] == original
    assert store.release_boundary()['v36ActivatedAtMs'] is None

def test_draft_and_pending_versions_cannot_be_replaced_during_activation(tmp_path):
    database = tmp_path / 'strategy.sqlite3'
    store = LendingStateStore(database)
    other = LendingStateStore(database)
    active = store.save_strategy({'policy': 'active'}, 'ACTIVE')
    confirmed = store.save_strategy({'policy': 'confirmed'}, 'DRAFT')
    newer = other.save_strategy({'policy': 'newer'}, 'DRAFT')
    with pytest.raises(lendingbot.StaleStrategyVersion):
        store.apply_confirmed_draft(confirmed, active, queue=False)
    assert store.strategy('ACTIVE')['version_id'] == active
    assert store.strategy('DRAFT')['version_id'] == newer

    store.apply_confirmed_draft(newer, active, queue=True)
    with pytest.raises(lendingbot.StaleStrategyVersion):
        other.save_strategy({'policy': 'newer'}, 'DRAFT', expected_active_id=active)
    latest = other.save_strategy({'policy': 'latest'}, 'DRAFT')
    with pytest.raises(lendingbot.StaleStrategyVersion):
        other.apply_confirmed_draft(latest, active, queue=True)
    with pytest.raises(lendingbot.StaleStrategyVersion):
        store.activate_pending_strategy(latest)
    assert store.strategy('PENDING')['version_id'] == newer
    assert store.activate_pending_strategy(newer)['version_id'] == newer


def test_certified_draft_cannot_apply_without_current_evidence(tmp_path):
    store = LendingStateStore(tmp_path / 'strategy.sqlite3')
    candidate = _candidate()
    active = store.save_strategy(json_decimal(vars(replace(candidate, quick_pricing_mode=LEGACY_QUICK_MODE))), 'ACTIVE')
    draft = store.save_strategy(json_decimal(vars(candidate)), 'DRAFT')
    with pytest.raises(lendingbot.StaleStrategyVersion, match='evidence'):
        store.apply_confirmed_draft(draft, active, queue=False, validated_policy=candidate)
    assert store.strategy('ACTIVE')['version_id'] == active
    assert store.strategy('DRAFT')['version_id'] == draft


def test_offline_certificate_and_strategy_roll_back_together(tmp_path, monkeypatch):
    context = AppContext.for_project(tmp_path)
    candidate = _candidate()
    base = replace(candidate, quick_pricing_mode=LEGACY_QUICK_MODE)
    store = LendingStateStore(context.state_db_path)
    active = store.save_strategy(json_decimal(vars(base)), 'ACTIVE')
    report = tmp_path / 'approved.json'
    report.write_text(json.dumps(_valid_report(candidate)), encoding='utf-8')

    def fail_cutover(*_args):
        assert lendingbot.LiveProcessLock.inspect(context.live_lock_path)['locked']
        raise RuntimeError('simulated failure recording cutover')

    monkeypatch.setattr(LendingStateStore, '_record_v36_activation', staticmethod(fail_cutover))
    with pytest.raises(RuntimeError, match='simulated failure'):
        lendingbot.apply_validated_v36(report, SimpleNamespace(strategy_v3=base), context)
    assert store.strategy('ACTIVE')['version_id'] == active
    assert store.strategy('PENDING') is None
    assert store.release_boundary()['v36ActivatedAtMs'] is None
    assert not validation_status(store, candidate)['approved']
    assert not lendingbot.LiveProcessLock.inspect(context.live_lock_path)['locked']
