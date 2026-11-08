"""Fail-closed, policy-bound evidence for promoting the V3.6 candidate.

The certificate is a reproducible validation summary, not an authority bit.
Every read rechecks the exact policy, all scenario gates and stored digests.
"""

import hashlib
import json
import sqlite3
from contextlib import nullcontext
from decimal import Decimal, InvalidOperation

from StrategyV3 import (
    EXECUTABLE_QUICK_MODE,
    StrategyPolicyV3,
    json_decimal,
    policy_v3_with_overrides,
    validate_policy_v3,
)


ENGINE_VERSION = "event-v36-v1"
CERTIFICATE_KEY = "v36_validation_certificate"
DAY_MS = 86_400_000
EXPECTED_SCENARIOS = {
    f"q{queue}-delay{delay}-{repayment}": (queue, delay, repayment)
    for queue in (0, 1, 2) for delay in (60, 120) for repayment in ("contract", "calibrated")
}


def _canonical(value):
    return json.dumps(json_decimal(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def canonical_policy_fingerprint(policy):
    """Match the policy fingerprint emitted by the event research report."""
    return _digest(policy if isinstance(policy, dict) else vars(policy))


def _decimal(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return result if result.is_finite() else None


def _number_is(value, expected):
    result = _decimal(value)
    return result is not None and result == expected


def _positive(value):
    result = _decimal(value)
    return result is not None and result > 0


def _object(value):
    return value if isinstance(value, dict) else {}


def _verified_coverage(coverage, start, end):
    """A completeness flag cannot substitute for a traversed interval union."""
    if coverage.get("complete") is not True or start is None or end is None:
        return False
    intervals = coverage.get("coveredIntervals", coverage.get("intervals"))
    if not isinstance(intervals, list):
        return False
    spans = []
    for row in intervals:
        row = _object(row)
        left = _decimal(row.get("startMs", row.get("start_ms")))
        right = _decimal(row.get("endMs", row.get("end_ms")))
        if (left is None or right is None or right < left
                or row.get("status", "COMPLETE") != "COMPLETE"):
            return False
        spans.append((left, right))
    cursor = start
    for left, right in sorted(spans):
        if left > cursor:
            return False
        cursor = max(cursor, right)
        if cursor >= end:
            return True
    return False


def _report_reasons(report, policy=None):
    report = _object(report)
    reasons = []
    if report.get("engineVersion") != ENGINE_VERSION:
        reasons.append("The report must use the event-v36-v1 engine.")
    selection = _object(report.get("selection"))
    if selection.get("eligibleForLiveCandidate") is not True or selection.get("candidate") != "C":
        reasons.append("The report does not approve candidate C.")
    candidate = _object(selection.get("candidatePolicy"))
    if not candidate:
        reasons.append("Candidate policy evidence is missing.")
    fingerprint = canonical_policy_fingerprint(candidate)
    try:
        parsed_candidate = policy_v3_with_overrides(StrategyPolicyV3(), candidate)
        validate_policy_v3(parsed_candidate, require_live_floors=True)
        if canonical_policy_fingerprint(parsed_candidate) != fingerprint:
            reasons.append("Candidate parameters must include the complete canonical policy.")
    except (ValueError, TypeError, AttributeError, ArithmeticError):
        reasons.append("Candidate policy is not valid for LIVE execution.")
    if selection.get("candidatePolicyFingerprint") != fingerprint:
        reasons.append("The candidate policy fingerprint does not match its parameters.")
    if policy is not None and canonical_policy_fingerprint(policy) != fingerprint:
        reasons.append("The active policy differs from the validated candidate.")
    if candidate.get("quick_pricing_mode") != EXECUTABLE_QUICK_MODE or not _number_is(
        candidate.get("quick_start_premium_apr_points"), Decimal("0.10")
    ):
        reasons.append("The candidate must use executable pricing with a 0.10 net APR point premium.")

    splits = _object(report.get("split"))
    previous_end = None
    for name, days in (("train", 60), ("validation", 15), ("test", 15)):
        split = _object(splits.get(name))
        start, end = _decimal(split.get("startMs")), _decimal(split.get("endMs"))
        valid = (start is not None and end is not None and start >= 0 and end - start == days * DAY_MS
                 and start == start.to_integral_value() and end == end.to_integral_value()
                 and _number_is(split.get("days"), days))
        if not valid or (previous_end is not None and start != previous_end):
            reasons.append(f"The {name} interval does not match the continuous 60/15/15 split.")
        previous_end = end

    data = _object(report.get("data"))
    if not _verified_coverage(
        _object(data.get("tradeCoverage")),
        _decimal(_object(splits.get("train")).get("startMs")),
        _decimal(_object(splits.get("test")).get("endMs")),
    ):
        reasons.append("Verified historical trade coverage is incomplete.")
    coverage = _object(data.get("bookCoverage"))
    for name in ("validation", "test"):
        row = _object(coverage.get(name))
        fresh, natural = _decimal(row.get("freshMs")), _decimal(row.get("freshNaturalDays"))
        if (row.get("meetsMinimum") is not True or fresh is None or fresh < 7 * DAY_MS
                or fresh > 15 * DAY_MS or natural is None or natural < 7 or natural > 16
                or natural != natural.to_integral_value()):
            reasons.append(f"The {name} split needs seven fresh-book days across seven natural days.")

    comparison = _object(report.get("comparison"))
    if comparison.get("strategyLabels") != ["A", "B", "C"]:
        reasons.append("The comparison must contain the fixed A/B/C strategies.")
    rows = comparison.get("scenarioResults")
    if not isinstance(rows, list):
        rows = []
    ids = [row.get("id") if isinstance(row, dict) else None for row in rows]
    if len(rows) != 12 or len({str(value) for value in ids}) != 12 or set(map(str, ids)) != set(EXPECTED_SCENARIOS):
        reasons.append("All twelve unique queue/delay/repayment scenarios are required.")
    scenarios = _object(report.get("scenarios"))
    if set(scenarios) != set(EXPECTED_SCENARIOS):
        reasons.append("Detailed evidence for all twelve scenarios is required.")

    for row in rows:
        row = _object(row)
        identity = row.get("id")
        if not isinstance(identity, str) or identity not in EXPECTED_SCENARIOS:
            continue
        evidence = _object(scenarios.get(identity))
        assumptions = _object(evidence.get("assumptions"))
        queue, delay, repayment = EXPECTED_SCENARIOS[identity]
        if (not _number_is(assumptions.get("queue_multiplier"), queue)
                or not _number_is(assumptions.get("delay_seconds"), delay)
                or assumptions.get("repayment_mode") != repayment
                or not _number_is(assumptions.get("control_seconds"), 60)
                or not _number_is(assumptions.get("seed"), 307)):
            reasons.append(f"{identity}: execution assumptions do not match the declared scenario.")
        if (row.get("available") is not True or row.get("pass") is not True
                or evidence.get("dataUsable") is not True or evidence.get("passed") is not True):
            reasons.append(f"{identity}: unavailable or failed scenarios cannot approve promotion.")
        variants = _object(evidence.get("variants"))
        if set(variants) != {"A", "B", "C"}:
            reasons.append(f"{identity}: missing strategy metrics.")
        for label in ("A", "B", "C"):
            quality = _object(_object(variants.get(label)).get("dataQuality"))
            if (quality.get("historicalBook") is not True or quality.get("repaymentCalibrationUsable") is not True
                    or quality.get("tradeCoverageComplete") is not True):
                reasons.append(f"{identity}/{label}: historical book or repayment evidence is unusable.")
        for baseline in ("A", "B"):
            detail = _object(_object(evidence.get("comparisons")).get(baseline))
            if detail.get("passed") is not True:
                reasons.append(f"{identity}/{baseline}: the detailed comparison did not pass.")
            for name in ("validation", "test"):
                gain = _decimal(_object(row.get(name)).get(baseline))
                candidate_return = _decimal(_object(_object(_object(variants.get("C")).get("metrics")).get(name))
                                            .get("returnOnPrincipalTime"))
                baseline_return = _decimal(_object(_object(_object(variants.get(baseline)).get("metrics"))
                                                   .get(name)).get("returnOnPrincipalTime"))
                detailed_gain = _decimal(detail.get(f"{name}Gain"))
                if (gain is None or gain <= 0 or candidate_return is None or baseline_return is None
                        or gain != candidate_return - baseline_return or gain != detailed_gain):
                    reasons.append(f"{identity}/{baseline}: {name} gain must be positive and match strategy metrics.")
            bootstrap = _object(_object(row.get("bootstrap")).get(baseline))
            lower, median, upper = (_decimal(bootstrap.get(key)) for key in ("lower", "median", "upper"))
            if (not _positive(lower) or median is None or upper is None or not lower <= median <= upper
                    or not _number_is(bootstrap.get("blockDays"), 3)
                    or not _number_is(bootstrap.get("iterations"), 2000)
                    or not _number_is(bootstrap.get("seed"), 307)
                    or _decimal(bootstrap.get("samples")) is None
                    or not 7 <= _decimal(bootstrap["samples"]) <= 15):
                reasons.append(f"{identity}/{baseline}: the paired 3-day/2000-draw/seed-307 confidence gate failed.")
            if bootstrap != _object(detail.get("pairedMovingBlock95")):
                reasons.append(f"{identity}/{baseline}: bootstrap summary differs from detailed evidence.")
    return reasons


def _validation_evidence(report):
    """Retain every gate input without storing all replay events and daily state."""
    scenarios = {}
    for identity, row in report["scenarios"].items():
        scenarios[identity] = {
            "assumptions": row["assumptions"], "dataUsable": row["dataUsable"], "passed": row["passed"],
            "comparisons": row["comparisons"],
            "variants": {
                label: {"metrics": {name: {"returnOnPrincipalTime": variant["metrics"][name]["returnOnPrincipalTime"]}
                                     for name in ("validation", "test")},
                        "dataQuality": variant["dataQuality"]}
                for label, variant in row["variants"].items()
            },
        }
    selection = report["selection"]
    return json.loads(_canonical({
        "engineVersion": report["engineVersion"], "split": report["split"],
        "data": {"tradeCoverage": report["data"]["tradeCoverage"],
                 "bookCoverage": {name: report["data"]["bookCoverage"][name]
                                  for name in ("validation", "test")}},
        "selection": {key: selection[key] for key in (
            "candidate", "candidatePolicy", "candidatePolicyFingerprint", "eligibleForLiveCandidate"
        )},
        "comparison": report["comparison"], "scenarios": scenarios,
    }))


def build_validation_certificate(report, policy, now_ms):
    """Prepare a certificate only after all independent promotion gates pass."""
    try:
        normalized = json.loads(_canonical(report))
    except (TypeError, ValueError) as error:
        raise ValueError("V3.6 validation rejected: report is not valid JSON data.") from error
    reasons = _report_reasons(normalized, policy)
    if reasons:
        raise ValueError("V3.6 validation rejected: " + " ".join(reasons))
    evidence = _validation_evidence(normalized)
    certificate = {
        "certificateVersion": 1, "engineVersion": ENGINE_VERSION, "createdAtMs": now_ms,
        "policyFingerprint": normalized["selection"]["candidatePolicyFingerprint"],
        "reportDigest": _digest(normalized), "evidence": evidence, "evidenceDigest": _digest(evidence),
    }
    certificate["certificateDigest"] = _digest(certificate)
    return certificate


def persist_validation_certificate(connection, certificate):
    connection.execute(
        """INSERT INTO schema_meta(key,value) VALUES(?,?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value""", (CERTIFICATE_KEY, _canonical(certificate)),
    )


def create_validation_certificate(store, report, policy=None):
    certificate = build_validation_certificate(report, policy, store._now_ms())
    with store.transaction(immediate=True) as connection:
        persist_validation_certificate(connection, certificate)
    return certificate


def validation_status(store, policy, *, connection=None):
    """Read-only validation; missing, malformed or stale-policy evidence fails closed."""
    result = {"approved": False, "reasons": [], "policyFingerprint": None,
              "reportDigest": None, "certifiedAtMs": None}
    try:
        result["policyFingerprint"] = canonical_policy_fingerprint(policy)
        reader = store.read_connection() if connection is None else nullcontext(connection)
        with reader as connection:
            row = connection.execute("SELECT value FROM schema_meta WHERE key=?", (CERTIFICATE_KEY,)).fetchone()
        if row is None:
            result["reasons"] = ["No V3.6 validation certificate is installed."]
            return result
        certificate = _object(json.loads(row[0]))
        reasons = result["reasons"]
        if type(certificate.get("certificateVersion")) is not int or certificate["certificateVersion"] != 1:
            reasons.append("Unsupported validation certificate version.")
        if certificate.get("engineVersion") != ENGINE_VERSION:
            reasons.append("The certificate does not use the V3.6 event engine.")
        digest_payload = {key: value for key, value in certificate.items() if key != "certificateDigest"}
        if certificate.get("certificateDigest") != _digest(digest_payload):
            reasons.append("The validation certificate digest does not match its contents.")
        evidence = _object(certificate.get("evidence"))
        if certificate.get("evidenceDigest") != _digest(evidence):
            reasons.append("The validation evidence digest does not match its contents.")
        if certificate.get("policyFingerprint") != result["policyFingerprint"]:
            reasons.append("The active policy differs from the validation certificate.")
        if certificate.get("policyFingerprint") != _object(evidence.get("selection")).get("candidatePolicyFingerprint"):
            reasons.append("The certificate policy does not match its evidence.")
        report_digest = certificate.get("reportDigest")
        if not isinstance(report_digest, str) or len(report_digest) != 64 or any(
            char not in "0123456789abcdef" for char in report_digest
        ):
            reasons.append("The source report digest is missing or invalid.")
        reasons.extend(_report_reasons(evidence, policy))
        result.update(approved=not reasons, reportDigest=report_digest, certifiedAtMs=certificate.get("createdAtMs"))
    except (TypeError, ValueError, KeyError, AttributeError, ArithmeticError, sqlite3.Error) as error:
        result["reasons"].append(f"Malformed V3.6 validation evidence: {type(error).__name__}.")
    return result
