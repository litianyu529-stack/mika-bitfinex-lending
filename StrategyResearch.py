"""Offline-only market backfill and chronological V3 strategy evaluation.

Nothing in this module calls an authenticated write endpoint.  It intentionally
depends only on public market reads, SQLite snapshots, and the pure V3 simulator.
"""

import hashlib
import copy
import json
import random
import os
import pickle
import sqlite3
import tempfile
import time
import zlib
from dataclasses import asdict, replace
from decimal import Decimal

from EventReplay import (
    DAY_MS,
    ReplayAssumptions,
    book_coverage,
    calibrate_repayments,
    run_event_replay,
)

from ExchangeModels import parse_funding_stats, parse_funding_trades
from FileUtils import atomic_write_text
from StrategyV3 import (
    SCORE_MODEL_VERSION,
    V3_RESEARCH_SCORE_WEIGHTS,
    build_market_signals_v3,
    json_decimal,
    validate_policy_v3,
)
from bitfinex import BitfinexApiError


D = Decimal
REQUIRED_RESEARCH_DAYS = 90


class PublicRateLimiter:
    def __init__(self, minimum_interval_seconds=4.1, clock=time.monotonic, sleeper=time.sleep):
        self.minimum_interval = max(0.0, float(minimum_interval_seconds))
        self.clock = clock
        self.sleeper = sleeper
        self.last_request = None

    def wait(self):
        if self.last_request is not None:
            remaining = self.minimum_interval - (self.clock() - self.last_request)
            if remaining > 0:
                self.sleeper(remaining)
        self.last_request = self.clock()


def _public_read(action, limiter, retry_sleeper, max_attempts=5):
    """Retry only rate-limited public reads; authenticated writes never use this path."""
    for attempt in range(int(max_attempts)):
        limiter.wait()
        try:
            return action()
        except BitfinexApiError as exc:
            message = str(exc).lower()
            if "http 429" not in message and "ratelimit" not in message:
                raise
            if attempt + 1 >= int(max_attempts):
                raise
            interval = float(getattr(limiter, "minimum_interval", 4.1))
            retry_sleeper(min(120.0, max(30.0, interval * 4) * (2**attempt)))


def _coverage(rows):
    count, earliest, latest = 0, None, None
    for row in rows or ():
        count += 1
        if row.get("mts") is not None:
            stamp = int(row["mts"])
            earliest = stamp if earliest is None else min(earliest, stamp)
            latest = stamp if latest is None else max(latest, stamp)
    return {"count": count, "earliestMs": earliest, "latestMs": latest}


def backfill_public_market_data(
    client,
    store,
    days=REQUIRED_RESEARCH_DAYS,
    now_ms=None,
    page_limit=10000,
    minimum_interval_seconds=4.1,
    rate_limiter=None,
    retry_sleeper=time.sleep,
):
    """Page public fUSD trades/stats without any authenticated or write call."""
    days = int(days)
    if days < REQUIRED_RESEARCH_DAYS:
        raise ValueError("market backfill requires at least 90 days")
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    start = now - days * DAY_MS
    limit = min(10000, max(1, int(page_limit)))
    stats_limit = min(250, limit)
    limiter = rate_limiter or PublicRateLimiter(minimum_interval_seconds)

    # A successful, unsaturated response proves a complete half-open interval,
    # including intervals with no trades. Saturated intervals are bisected, so
    # pagination never advances past unobserved IDs sharing a millisecond.
    def interval_pages(fetch, parse, persist, intervals, page_limit, *, certify=False):
        jobs = list(reversed(intervals))
        pages, complete_ranges, unresolved_ranges = 0, [], []
        while jobs:
            left, right = jobs.pop()
            if right <= left:
                continue
            raw = _public_read(lambda: fetch(left, right - 1, page_limit), limiter, retry_sleeper)
            parsed = parse(raw)
            pages += 1
            valid = [row for row in parsed if left <= int(row["mts"]) < right]
            persist(valid)
            malformed = len(parsed) != len(raw or ()) or len(valid) != len(parsed)
            if malformed:
                reason = "INVALID_OR_OUT_OF_INTERVAL_RESPONSE"
            elif len(raw or ()) < page_limit:
                complete_ranges.append({"startMs": left, "endMs": right})
                if certify:
                    store.record_market_coverage(left, right, status="COMPLETE", source="RESEARCH_REST")
                continue
            elif right - left > 1:
                midpoint = (left + right) // 2
                jobs.append((midpoint, right))
                jobs.append((left, midpoint))
                continue
            else:
                reason = "SATURATED_SINGLE_MILLISECOND"
            unresolved_ranges.append({"startMs": left, "endMs": right, "reason": reason})
            if certify:
                store.record_market_coverage(left, right, status="UNRESOLVED", source="RESEARCH_REST",
                                             details={"reason": reason, "limit": page_limit})
        return pages, complete_ranges, unresolved_ranges

    prior = store.market_coverage(start, now)
    gaps = [(int(row["startMs"]), int(row["endMs"])) for row in prior.get("unresolvedIntervals", [])]
    trade_pages, _complete, _unresolved = interval_pages(
        lambda left, right, count: client.funding_trades("fUSD", start=left, end=right, limit=count, sort=1),
        parse_funding_trades, store.upsert_market_trades, gaps, limit, certify=True)
    # Stats do not have a durable completeness table. Reverify their requested
    # interval rather than inferring continuity from its first and last row.
    stats_pages, stats_complete, stats_unresolved = interval_pages(
        lambda left, right, count: client.funding_stats("fUSD", start=left, end=right, limit=count),
        parse_funding_stats, store.upsert_funding_stats, [(start, now)], stats_limit)
    proven = store.market_coverage(start, now)
    trade_coverage = _coverage(_read_iter(store, "market_trades", start, now))
    stats_coverage = _coverage(_read_iter(store, "funding_stats", start, now))
    complete = bool(proven.get("complete"))

    return {
        "symbol": "fUSD",
        "requestedDays": days,
        "startMs": start,
        "endMs": now,
        "tradePages": trade_pages,
        "statsPages": stats_pages,
        "trades": trade_coverage,
        "stats": stats_coverage,
        "complete": complete,
        "tradeCoverage": proven,
        "statsCoverage": {"complete": not stats_unresolved, "coveredIntervals": stats_complete,
                          "unresolvedIntervals": stats_unresolved},
        "bookHistory": {
            "backfillable": False,
            "note": "Historical order-book snapshots are unavailable; minute snapshots begin at deployment.",
        },
    }


def _policy_id(policy):
    encoded = json.dumps(json_decimal(policy.__dict__), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _safe_variant(base, **changes):
    policy = replace(base, **changes)
    validate_policy_v3(policy)
    hard_fields = (
        "currency",
        "short_floor_apr",
        "medium_floor_apr",
        "long_floor_apr",
        "max_lend_amount",
        "max_lend_percent",
        "normal_fee_rate",
        "hidden_fee_rate",
        "variable_max_share",
        "hidden_max_share",
    )
    if any(getattr(policy, name) != getattr(base, name) for name in hard_fields):
        raise ValueError("research variant changed a hard safety boundary")
    return policy


def research_variants(active_policy):
    """Versioned variants limited to term pools, layers, order types, and repricing."""
    return {
        "current_v3": active_policy,
        "frr_only": _safe_variant(
            active_policy,
            enable_limit=False,
            enable_frr=True,
            enable_frr_delta_fixed=False,
            enable_frr_delta_variable=False,
            enable_hidden=False,
        ),
        "limit_near_fill": _safe_variant(
            active_policy,
            enable_limit=True,
            enable_frr=False,
            enable_frr_delta_fixed=False,
            enable_frr_delta_variable=False,
            enable_hidden=False,
            quick_share=D("60"),
            balanced_share=D("30"),
            high_share=D("10"),
        ),
        "candidate_short_cycle": _safe_variant(
            active_policy,
            quick_share=D("55"),
            balanced_share=D("35"),
            high_share=D("10"),
            minimum_offer_minutes=max(5, active_policy.minimum_offer_minutes),
        ),
        "candidate_term_capture": _safe_variant(
            active_policy,
            short_share=D("40"),
            medium_share=D("35"),
            long_share=D("25"),
            balanced_share=D("45"),
            high_share=D("25"),
            quick_share=D("30"),
            reprice_cooldown_minutes=max(15, active_policy.reprice_cooldown_minutes),
        ),
    }


def paired_bootstrap_interval(candidate, baseline, iterations=2000, seed=307):
    paired = [D(left) - D(right) for left, right in zip(candidate, baseline)]
    if not paired:
        return {"lower": D("0"), "median": D("0"), "upper": D("0"), "samples": 0}
    randomizer = random.Random(seed)
    means = []
    for _ in range(int(iterations)):
        draw = [paired[randomizer.randrange(len(paired))] for _ in paired]
        means.append(sum(draw, D("0")) / D(len(draw)))
    means.sort()
    last = len(means) - 1
    return {
        "lower": means[int(last * 0.025)],
        "median": means[int(last * 0.5)],
        "upper": means[int(last * 0.975)],
        "samples": len(paired),
    }


def paired_moving_block_interval(candidate, baseline, iterations=2000, seed=307, block_days=3):
    """Paired circular blocks preserve short-range dependence between days."""
    if len(candidate) != len(baseline):
        raise ValueError("paired daily returns must have matching dates")
    paired = [D(str(left)) - D(str(right)) for left, right in zip(candidate, baseline)]
    if len(paired) < 7:
        return {"lower": None, "median": None, "upper": None, "samples": len(paired),
                "blockDays": int(block_days), "iterations": int(iterations), "seed": int(seed)}
    if int(iterations) <= 0 or int(block_days) <= 0:
        raise ValueError("bootstrap iterations and block length must be positive")
    randomizer = random.Random(seed)
    means = []
    for _ in range(int(iterations)):
        sample = []
        while len(sample) < len(paired):
            origin = randomizer.randrange(len(paired))
            sample.extend(paired[(origin + offset) % len(paired)] for offset in range(int(block_days)))
        means.append(sum(sample[:len(paired)], D(0)) / D(len(paired)))
    means.sort()
    last = len(means) - 1
    return {"lower": means[int(last * .025)], "median": means[int(last * .5)],
            "upper": means[int(last * .975)], "samples": len(paired),
            "blockDays": int(block_days), "iterations": int(iterations), "seed": int(seed)}


def _research_scenarios():
    return [ReplayAssumptions(queue_multiplier=queue, delay_seconds=delay, repayment_mode=repayment,
                              collect_events=True)
            for queue in (0, 1, 2) for delay in (60, 120) for repayment in ("contract", "calibrated")]


def _scenario_name(value):
    return f"q{value.queue_multiplier}-delay{value.delay_seconds}-{value.repayment_mode}"


def _read_iter(store, kind, start, end):
    method = getattr(store, f"iter_{kind}", None)
    if method is not None:
        return method(start, end)
    return iter(getattr(store, kind)(start, end))


def _book_observations(store, start, end):
    """Read timestamps without parsing historical depth for coverage gates."""
    if hasattr(store, "read_connection"):
        with store.read_connection() as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(book_snapshots)")}
            visible = "COALESCE(observed_at_ms,mts+60000)" if "observed_at_ms" in columns else "mts+60000"
            # The visibility expression and mts live in a covering index. Do
            # not select the appended observed_at_ms column past huge book JSON
            # overflow payloads just to calculate coverage.
            cursor = connection.execute(
                f"SELECT mts, {visible} FROM book_snapshots WHERE {visible} >= ? AND {visible} < ? "
                f"ORDER BY {visible},mts",
                (start - 120_000, end),
            )
            for row in cursor:
                yield {"mts": row[0], "observed_at_ms": row[1]}
    else:
        yield from _read_iter(store, "book_snapshots", start - 120_000, end)


def _daily_split(result, start, end):
    return {int(row["startMs"]): D(row["returnOnPrincipalTime"])
            for row in result.get("daily", [])
            if start <= int(row["startMs"]) and int(row["endMs"]) <= end
            and int(row["endMs"]) - int(row["startMs"]) == DAY_MS}


def _event_evidence(result):
    cancellations = [row for row in result.get("events", []) if row["type"].startswith("CANCEL")]
    return {"orders": result.get("orders", []), "fills": result.get("fills", []),
            "cancellations": cancellations, "returns": result.get("returns", []),
            "counts": {"orders": len(result.get("orders", [])), "fills": len(result.get("fills", [])),
                       "partialFills": sum(row["type"] == "PARTIAL_FILL" for row in result.get("events", [])),
                       "cancelRequested": sum(row["type"] == "CANCEL_REQUEST" for row in cancellations),
                       "cancelConfirmed": sum(row["type"] == "CANCEL_CONFIRMED" for row in cancellations),
                       "returns": len(result.get("returns", []))}}


class ReplaySignalCache:
    """Disk-backed, immutable feature reuse across A/B/C and execution scenarios.

    The temporary database contains public derived market features only. Account
    balances, random state and order state are never cached or shared. Loading a
    pickled value creates a fresh tree for each caller, so planners cannot mutate
    another scenario's signals. No pickle from outside this process is accepted.
    """
    needs_snapshot = True

    def __init__(self, directory=None):
        self._directory = tempfile.TemporaryDirectory(prefix="mika-replay-features-", dir=directory)
        self.connection = sqlite3.connect(os.path.join(self._directory.name, "features.sqlite3"))
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("CREATE TABLE features(key TEXT PRIMARY KEY, payload BLOB NOT NULL)")
        self.hits = self.misses = 0

    def get_signal(self, now, book, history, stats, policy, mode, *, snapshot_factory):
        parameters = dict(vars(policy))
        # These two parameters affect offer pricing, not market statistics.
        parameters.pop("quick_pricing_mode", None)
        parameters.pop("quick_start_premium_apr_points", None)
        fingerprint = hashlib.sha256(json.dumps(json_decimal(parameters), sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
        key = f"{mode}:{now}:{fingerprint}"
        existing = self.connection.execute("SELECT payload FROM features WHERE key=?", (key,)).fetchone()
        if existing is not None:
            self.hits += 1
            cached = pickle.loads(zlib.decompress(existing[0]))
            if cached["kind"] == "SNAPSHOT":
                snapshot = cached["value"]
                snapshot["policyFingerprint"] = hashlib.sha256(json.dumps(
                    json_decimal(vars(policy)), sort_keys=True, separators=(",", ":")
                ).encode()).hexdigest()
                return build_market_signals_v3(book, [], stats, policy, now, window_snapshot=snapshot)
            return cached["value"]
        self.misses += 1
        snapshot = snapshot_factory()
        usable = snapshot is not None and snapshot.get("usable")
        signals = build_market_signals_v3(book, [] if usable else history[-20_000:], stats, policy, now,
                                          window_snapshot=snapshot if usable else None)
        cached = {"kind": "SNAPSHOT" if usable else "SIGNALS", "value": snapshot if usable else signals}
        payload = zlib.compress(pickle.dumps(cached, protocol=pickle.HIGHEST_PROTOCOL), level=1)
        self.connection.execute("INSERT INTO features VALUES(?,?)", (key, payload))
        if self.misses % 1000 == 0:
            self.connection.commit()
        return signals

    def close(self):
        connection = getattr(self, "connection", None)
        try:
            if connection is not None:
                connection.close()
        finally:
            self.connection = None
            directory = getattr(self, "_directory", None)
            if directory is not None:
                directory.cleanup()
                self._directory = None

    def __del__(self):
        # The owner uses finally for deterministic Python-level cleanup. This
        # remains a best-effort fallback, not protection against process kills.
        try:
            self.close()
        except Exception:
            pass


def evaluate_strategies(store, active_policy, principal, days=90, now_ms=None,
                        *, scenarios=None, replay_runner=None, signal_provider=None, progress=None):
    """Own only the internally created cache, including during interruption."""
    feature_cache = None
    try:
        if signal_provider is None and replay_runner is None:
            directory = os.path.dirname(store.path) if getattr(store, "path", None) else None
            feature_cache = ReplaySignalCache(directory=directory)
        provider = signal_provider if signal_provider is not None else feature_cache
        return _evaluate_strategies(
            store, active_policy, principal, days, now_ms, scenarios=scenarios,
            replay_runner=replay_runner, signal_provider=provider, progress=progress,
            feature_cache=feature_cache,
        )
    finally:
        if feature_cache is not None:
            feature_cache.close()


def _evaluate_strategies(store, active_policy, principal, days=90, now_ms=None,
                         *, scenarios=None, replay_runner=None, signal_provider=None,
                         progress=None, feature_cache=None):
    """Fixed A/B/C comparison on twelve explicit execution/repayment scenarios.

    There is no search or validation/test tuning. Every run carries its own
    orders, loans and daily interest through the full 60/15/15 timeline.
    """
    if int(days) < REQUIRED_RESEARCH_DAYS:
        raise ValueError("strategy evaluation requires at least 90 days")
    principal = D(str(principal))
    if principal <= 0:
        raise ValueError("evaluation principal must be positive")
    validate_policy_v3(active_policy)
    end = int(now_ms if now_ms is not None else time.time() * 1000)
    start = end - 90 * DAY_MS
    windows = {"train": (start, start + 60 * DAY_MS),
               "validation": (start + 60 * DAY_MS, start + 75 * DAY_MS),
               "test": (start + 75 * DAY_MS, end)}
    warmup_start = start - 7 * DAY_MS
    market_coverage_reader = getattr(store, "market_coverage", None)
    trade_coverage = market_coverage_reader(start, end) if market_coverage_reader else {
        "complete": False, "coveredIntervals": [], "unresolvedIntervals": [
            {"startMs": start, "endMs": end, "status": "UNVERIFIED"}]}
    trade_coverage = {**trade_coverage, "startMs": start, "endMs": end}
    warmup_coverage = market_coverage_reader(warmup_start, start) if market_coverage_reader else {
        "complete": False, "coveredIntervals": [], "unresolvedIntervals": [
            {"startMs": warmup_start, "endMs": start, "status": "UNVERIFIED"}]}
    combined_intervals = sorted([*warmup_coverage.get("coveredIntervals", []),
                                 *trade_coverage.get("coveredIntervals", [])], key=lambda row: row["startMs"])
    merged_intervals = []
    for interval in combined_intervals:
        left, right = int(interval["startMs"]), int(interval["endMs"])
        if merged_intervals and left <= merged_intervals[-1]["endMs"]:
            merged_intervals[-1]["endMs"] = max(right, merged_intervals[-1]["endMs"])
        else:
            merged_intervals.append({"startMs": left, "endMs": right})
    full_coverage = {"coveredIntervals": merged_intervals}
    legacy_equivalent = not any(row["endMs"] - row["startMs"] >= 7 * DAY_MS
                                and max(start, row["startMs"] + 7 * DAY_MS) < min(end, row["endMs"])
                                for row in merged_intervals)
    coverage = book_coverage(_book_observations(store, start, end), start, end, segments=windows)
    calibration_reader = getattr(store, "repayment_calibration", None)
    calibration = calibrate_repayments(
        calibration_reader(*windows["train"]) if calibration_reader else [], *windows["train"])
    if not hasattr(active_policy, "quick_pricing_mode"):
        raise ValueError("V3.6 policy quick_pricing_mode support is required")
    old = _safe_variant(active_policy, quick_pricing_mode="EXPLORATION_V4")
    upgraded = _safe_variant(active_policy, quick_pricing_mode="EXECUTABLE_PREMIUM_V1",
                             quick_start_premium_apr_points=D("0.10"))
    variants = {"A": (old, "legacy_20k"), "B": (old, "full_7d"), "C": (upgraded, "full_7d")}
    runner = replay_runner or run_event_replay
    shared_provider = signal_provider
    scenario_list = scenarios or _research_scenarios()
    results = {}
    all_pass = True
    reasons = []
    if not all(bool(item["meetsMinimum"]) for item in coverage.values()):
        all_pass = False
        reasons.append("At least one split has fewer than seven fresh-book days or seven observed natural days.")
    for index, cfg in enumerate(scenario_list):
        if not isinstance(cfg, ReplayAssumptions):
            cfg = ReplayAssumptions(**cfg)
        scenario = _scenario_name(cfg)
        scenario_results = {}
        for label, (policy, signal_mode) in variants.items():
            if legacy_equivalent and replay_runner is None and label != "A":
                result = copy.deepcopy(scenario_results["A"])
                result["assumptions"] = {**result["assumptions"], "signal_mode": signal_mode}
                result["dataQuality"]["behaviorReuse"] = "unverified_full_window_forces_legacy"
                result["dataQuality"]["upgradeBehaviorEvaluated"] = False
                scenario_results[label] = result
                continue
            if progress:
                progress({"scenario": scenario, "variant": label, "scenarioIndex": index + 1,
                          "scenarioCount": len(scenario_list), "phase": "started"})
            effective = replace(cfg, signal_mode=signal_mode)
            result = runner(policy, _read_iter(store, "market_trades", warmup_start, end),
                            _read_iter(store, "funding_stats", warmup_start, end), principal,
                            book_snapshots=_read_iter(store, "book_snapshots", warmup_start, end),
                            start_ms=start, now_ms=end, segments=windows,
                            assumptions=effective, repayment_calibration=calibration,
                            market_coverage=full_coverage,
                            signal_provider=shared_provider,
                            progress=(lambda item, s=scenario, v=label: progress({"scenario": s, "variant": v, **item}))
                            if progress else None)
            scenario_results[label] = result
        comparisons = {}
        scenario_pass = True
        for baseline in ("A", "B"):
            gains = {}
            for split in ("validation", "test"):
                candidate_return = D(scenario_results["C"]["segments"][split]["returnOnPrincipalTime"])
                baseline_return = D(scenario_results[baseline]["segments"][split]["returnOnPrincipalTime"])
                gains[split] = candidate_return - baseline_return
            candidate_daily = _daily_split(scenario_results["C"], *windows["test"])
            baseline_daily = _daily_split(scenario_results[baseline], *windows["test"])
            dates = sorted(set(candidate_daily) & set(baseline_daily))
            confidence = paired_moving_block_interval([candidate_daily[date] for date in dates],
                                                      [baseline_daily[date] for date in dates])
            passed = all(value > 0 for value in gains.values()) and confidence["lower"] is not None \
                and confidence["lower"] > 0
            scenario_pass = scenario_pass and passed
            comparisons[baseline] = {"validationGain": gains["validation"], "testGain": gains["test"],
                                     "pairedMovingBlock95": confidence, "passed": passed}
        data_ok = all(result.get("dataQuality", {}).get("historicalBook", False) and
                      result.get("dataQuality", {}).get("repaymentCalibrationUsable", False)
                      and result.get("dataQuality", {}).get("tradeCoverageComplete", False)
                      for result in scenario_results.values())
        scenario_pass = scenario_pass and data_ok
        all_pass = all_pass and scenario_pass
        results[scenario] = {"assumptions": asdict(cfg),
                             "variants": {name: {"versionId": _policy_id(variants[name][0]),
                                                  "metrics": value["segments"], "daily": value["daily"],
                                                  "dataQuality": value["dataQuality"],
                                                  "finalState": value["finalState"],
                                                  **({"eventEvidenceRef":
                                                      f"scenarios.{scenario}.variants.A.eventEvidence",
                                                      "eventEvidenceCounts": _event_evidence(value)["counts"]}
                                                     if label != "A" and value.get("dataQuality", {}).get(
                                                         "behaviorReuse")
                                                     else {"eventEvidence": _event_evidence(value)})}
                                          for name, value in scenario_results.items() for label in (name,)},
                             "comparisons": comparisons, "dataUsable": data_ok, "passed": scenario_pass}
    expected_scenarios = {_scenario_name(cfg) for cfg in _research_scenarios()}
    if set(results) != expected_scenarios:
        all_pass = False
        reasons.append("All twelve predeclared scenarios are required for promotion.")
    if not all(row["passed"] for row in results.values()):
        reasons.append("Candidate did not satisfy every baseline, holdout, uncertainty and calibration gate.")
    result = {
        "schemaVersion": 2, "engineVersion": "event-v36-v1", "generatedAtMs": end,
        "symbol": "fUSD", "principal": principal,
        "methodology": {"split": "continuous 60-day train / 15-day validation / 15-day test",
                        "selectionUses": "fixed A/B/C; no validation or test tuning",
                        "dailyState": "cash, offers, loans and interest carried across all boundaries",
                        "bootstrap": "paired circular moving blocks: 3 days, 2000 draws, seed 307",
                        "promotionGate": ("C strictly beats A/B on validation/test in all 12 scenarios; "
                                          "test 95% lower bound > 0"),
                        "interestSettlement": "daily at configured UTC hour; timing is a declared assumption",
                        "queueModel": ("0/1/2 times visible better-or-equal lender depth; "
                                       "sensitivity, not observable FIFO"),
                        "tradeDirection": "unknown for legacy absolute-amount records",
                        "unmodeledRuntimeControls": ["external offer adoption without initial account evidence",
                                                     "unobserved historical write outages (fault injection supported)"],
                        "scoreModelVersion": SCORE_MODEL_VERSION, "scoreWeights": V3_RESEARCH_SCORE_WEIGHTS},
        "split": {name: {"startMs": bounds[0], "endMs": bounds[1], "days": (bounds[1] - bounds[0]) // DAY_MS}
                  for name, bounds in windows.items()},
        "data": {"bookCoverage": coverage, "historicalBookBackfillable": False,
                 "repaymentCalibration": calibration, "requiredWarmupDays": 7,
                 "tradeCoverage": trade_coverage,
                 "warmupTradeCoverage": warmup_coverage,
                 "behaviorReuse": "unverified_full_window_forces_legacy" if legacy_equivalent else None,
                 "featureCache": {"hits": feature_cache.hits, "misses": feature_cache.misses,
                                  "accountStateShared": False} if feature_cache else None},
        "baselines": ["A", "B"],
        "variants": {label: {"versionId": _policy_id(policy), "policy": policy.__dict__, "signalMode": mode}
                     for label, (policy, mode) in variants.items()},
        "scenarios": results,
        "comparison": {"strategyLabels": ["A", "B", "C"],
                       "scenarioResults": [{"id": name, "available": row["dataUsable"],
                                            "pass": row["passed"],
                                            "validation": {base: comparison["validationGain"]
                                                           for base, comparison in row["comparisons"].items()},
                                            "test": {base: comparison["testGain"]
                                                     for base, comparison in row["comparisons"].items()},
                                            "bootstrap": {base: comparison["pairedMovingBlock95"]
                                                          for base, comparison in row["comparisons"].items()}}
                                           for name, row in results.items()]},
        "selection": {"candidate": "C", "candidateVersionId": _policy_id(upgraded),
                      "candidatePolicy": upgraded.__dict__,
                      "candidatePolicyFingerprint": hashlib.sha256(json.dumps(
                          json_decimal(vars(upgraded)), sort_keys=True, separators=(",", ":")
                      ).encode("utf-8")).hexdigest(),
                      "eligibleForLiveCandidate": bool(all_pass), "requiresManualPromotion": True,
                      "reasons": reasons, "scenarioCount": len(results)},
    }
    return json_decimal(result)


def write_research_report(path, report):
    atomic_write_text(path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return path
