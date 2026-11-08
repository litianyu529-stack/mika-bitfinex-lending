import copy
import hashlib
import json
import sqlite3
from dataclasses import replace
from decimal import Decimal
from contextlib import closing

import pytest

from StateStore import LendingStateStore
from StrategyV3 import EXECUTABLE_QUICK_MODE, StrategyPolicyV3, json_decimal
from V36Validation import (
    CERTIFICATE_KEY,
    DAY_MS,
    EXPECTED_SCENARIOS,
    canonical_policy_fingerprint,
    create_validation_certificate,
    validation_status,
)


D = Decimal


def _candidate():
    return replace(StrategyPolicyV3(), quick_pricing_mode=EXECUTABLE_QUICK_MODE,
                   short_floor_apr=D("0.05"), medium_floor_apr=D("0.0785"), long_floor_apr=D("0.0853"))


def _valid_report(policy=None):
    policy = policy or _candidate()
    scenario_results, scenarios = [], {}
    for identity, (queue, delay, repayment) in EXPECTED_SCENARIOS.items():
        bootstrap = {"lower": "0.001", "median": "0.005", "upper": "0.01", "samples": 15,
                     "blockDays": 3, "iterations": 2000, "seed": 307}
        comparison = {
            baseline: {"validationGain": gain, "testGain": gain,
                       "pairedMovingBlock95": copy.deepcopy(bootstrap), "passed": True}
            for baseline, gain in (("A", "0.02"), ("B", "0.01"))
        }
        scenarios[identity] = {
            "assumptions": {"queue_multiplier": queue, "delay_seconds": delay, "repayment_mode": repayment,
                            "control_seconds": 60, "seed": 307},
            "dataUsable": True, "passed": True, "comparisons": comparison,
            "variants": {label: {
                "metrics": {name: {"returnOnPrincipalTime": value} for name in ("validation", "test")},
                "dataQuality": {"historicalBook": True, "repaymentCalibrationUsable": True,
                                "tradeCoverageComplete": True},
                "daily": ["not retained in the certificate"],
            } for label, value in (("A", "0.01"), ("B", "0.02"), ("C", "0.03"))},
        }
        scenario_results.append({
            "id": identity, "available": True, "pass": True,
            "validation": {"A": "0.02", "B": "0.01"}, "test": {"A": "0.02", "B": "0.01"},
            "bootstrap": {baseline: copy.deepcopy(bootstrap) for baseline in ("A", "B")},
        })
    return json_decimal({
        "engineVersion": "event-v36-v1", "generatedAtMs": 90 * DAY_MS,
        "split": {name: {"startMs": start * DAY_MS, "endMs": end * DAY_MS, "days": end - start}
                  for name, start, end in (("train", 0, 60), ("validation", 60, 75), ("test", 75, 90))},
        "data": {"tradeCoverage": {"complete": True, "coveredDurationMs": 90 * DAY_MS,
                                   "coveredIntervals": [{"startMs": 0, "endMs": 90 * DAY_MS,
                                                         "status": "COMPLETE"}]},
                 "bookCoverage": {name: {"meetsMinimum": True, "freshMs": 8 * DAY_MS,
                                         "freshNaturalDays": 8, "freshDays": "8"}
                                  for name in ("validation", "test")}},
        "selection": {"candidate": "C", "candidatePolicy": vars(policy),
                      "candidatePolicyFingerprint": canonical_policy_fingerprint(policy),
                      "eligibleForLiveCandidate": True},
        "comparison": {"strategyLabels": ["A", "B", "C"], "scenarioResults": scenario_results},
        "scenarios": scenarios,
    })


def _stored_certificate(store):
    with store.read_connection() as connection:
        return json.loads(connection.execute("SELECT value FROM schema_meta WHERE key=?", (CERTIFICATE_KEY,))
                          .fetchone()[0])


def _save_certificate(store, certificate):
    payload = json.dumps(certificate) if not isinstance(certificate, str) else certificate
    with store.transaction(immediate=True) as connection:
        connection.execute("INSERT OR REPLACE INTO schema_meta VALUES(?,?)", (CERTIFICATE_KEY, payload))


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def test_certificate_binds_exact_canonical_policy_and_rechecks_read_only(tmp_path):
    store = LendingStateStore(tmp_path / "state.db", clock=lambda: 100)
    policy = _candidate()
    report = _valid_report(policy)
    expected = hashlib.sha256(json.dumps(json_decimal(vars(policy)), sort_keys=True,
                                        separators=(",", ":")).encode()).hexdigest()
    assert canonical_policy_fingerprint(policy) == expected
    assert canonical_policy_fingerprint(json_decimal(vars(policy))) == expected
    certificate = create_validation_certificate(store, report, policy)
    assert certificate["policyFingerprint"] == expected
    assert "daily" not in certificate["evidence"]["scenarios"][next(iter(EXPECTED_SCENARIOS))]["variants"]["A"]
    assert certificate["reportDigest"] == _digest(report)
    with closing(sqlite3.connect(store.path, isolation_level=None)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        status = validation_status(store, policy)
        assert status["approved"] and status["reasons"] == []
        assert status["certifiedAtMs"] == 100_000
        writer.rollback()
    assert store.release_boundary()["v36ActivatedAtMs"] is None  # Certification itself never activates live mode.


def test_certificate_can_derive_policy_from_report_but_rejects_later_policy_changes(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    policy = _candidate()
    create_validation_certificate(store, _valid_report(policy))
    changed = replace(policy, short_floor_apr=policy.short_floor_apr + D("0.0001"))
    assert not validation_status(store, changed)["approved"]
    assert any("policy differs" in reason for reason in validation_status(store, changed)["reasons"])
    with pytest.raises(ValueError, match="active policy"):
        create_validation_certificate(store, _valid_report(policy), changed)
    assert validation_status(store, policy)["approved"]


@pytest.mark.parametrize("change", [
    lambda report: report.update(engineVersion="legacy-daily"),
    lambda report: report["selection"].update(eligibleForLiveCandidate=False),
    lambda report: report["selection"].update(candidatePolicyFingerprint="0" * 64),
    lambda report: report["comparison"].update(strategyLabels=["A", "C"]),
    lambda report: report["comparison"]["scenarioResults"].pop(),
    lambda report: report["comparison"]["scenarioResults"].__setitem__(1, report["comparison"]["scenarioResults"][0]),
    lambda report: report["comparison"]["scenarioResults"][0].update(available=False),
    lambda report: report["comparison"]["scenarioResults"][0].update({"pass": False}),
    lambda report: report["comparison"]["scenarioResults"][0]["validation"].update(A="0"),
    lambda report: report["comparison"]["scenarioResults"][0]["test"].update(B="-0.1"),
    lambda report: report["comparison"]["scenarioResults"][0]["bootstrap"]["A"].update(lower="0"),
    lambda report: report["comparison"]["scenarioResults"][0]["bootstrap"]["A"].update(lower="NaN"),
    lambda report: report["comparison"]["scenarioResults"][0]["bootstrap"]["B"].update(iterations=1999),
    lambda report: report["comparison"]["scenarioResults"][0]["bootstrap"]["B"].update(blockDays=2),
    lambda report: report["comparison"]["scenarioResults"][0]["bootstrap"]["B"].update(seed=42),
    lambda report: report["data"]["bookCoverage"]["test"].update(freshMs=7 * DAY_MS - 1),
    lambda report: report["data"]["bookCoverage"]["validation"].update(freshNaturalDays=6),
    lambda report: report["data"]["bookCoverage"]["test"].update(meetsMinimum=False),
    lambda report: report["data"]["tradeCoverage"].update(complete=False),
    lambda report: report["data"]["tradeCoverage"].update(coveredIntervals=[]),
    lambda report: report["split"]["test"].update(startMs=76 * DAY_MS),
    lambda report: report["scenarios"][next(iter(EXPECTED_SCENARIOS))]["assumptions"].update(delay_seconds=0),
    lambda report: report["scenarios"][next(iter(EXPECTED_SCENARIOS))]["variants"]["C"]["metrics"]["test"]
                  .update(returnOnPrincipalTime="0"),
    lambda report: report["scenarios"][next(iter(EXPECTED_SCENARIOS))]["variants"]["C"]["dataQuality"]
                  .update(repaymentCalibrationUsable=False),
])
def test_certificate_rejects_incomplete_or_optimistic_report(tmp_path, change):
    store = LendingStateStore(tmp_path / "state.db")
    report = _valid_report()
    change(report)
    with pytest.raises(ValueError, match="V3.6 validation rejected"):
        create_validation_certificate(store, report)
    assert not validation_status(store, _candidate())["approved"]


def test_certificate_detects_tampering_even_when_approved_bit_is_added(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    create_validation_certificate(store, _valid_report())
    certificate = _stored_certificate(store)
    certificate["approved"] = True
    certificate["evidence"]["comparison"]["scenarioResults"][0]["bootstrap"]["A"]["lower"] = "0"
    _save_certificate(store, certificate)
    status = validation_status(store, _candidate())
    assert not status["approved"]
    assert any("digest" in reason for reason in status["reasons"])

    # Recomputed checksums cannot make evidence that fails the gate acceptable.
    certificate["evidenceDigest"] = _digest(certificate["evidence"])
    certificate["certificateDigest"] = _digest({key: value for key, value in certificate.items()
                                               if key != "certificateDigest"})
    _save_certificate(store, certificate)
    assert not validation_status(store, _candidate())["approved"]


@pytest.mark.parametrize("certificate", ["{not-json", "null", "[]", "true", "{}", '{"approved":true}'])
def test_empty_or_malformed_certificates_fail_closed(tmp_path, certificate):
    store = LendingStateStore(tmp_path / "state.db")
    assert not validation_status(store, _candidate())["approved"]
    _save_certificate(store, certificate)
    status = validation_status(store, _candidate())
    assert not status["approved"] and status["reasons"]


@pytest.mark.parametrize("mode,premium", [("EXPLORATION_V4", "0.10"), (EXECUTABLE_QUICK_MODE, "0.11")])
def test_certificate_rejects_different_upgrade_mechanism(tmp_path, mode, premium):
    store = LendingStateStore(tmp_path / "state.db")
    candidate = replace(_candidate(), quick_pricing_mode=mode, quick_start_premium_apr_points=D(premium))
    with pytest.raises(ValueError, match="0.10 net APR"):
        create_validation_certificate(store, _valid_report(candidate))


def test_partial_utc_boundary_days_are_counted_without_rejecting_16_intersecting_days(tmp_path):
    store = LendingStateStore(tmp_path / "state.db")
    report = _valid_report()
    report["data"]["bookCoverage"]["test"]["freshNaturalDays"] = 16
    create_validation_certificate(store, report)
    assert validation_status(store, _candidate())["approved"]
