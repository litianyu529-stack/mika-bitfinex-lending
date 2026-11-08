"""Public, durable funding history and exact-term rolling market statistics.

The trading loop only reads published snapshots. Network pagination, SQLite
warm-up and statistical work run on the history worker, independently of orders.
"""

import copy
import hashlib
import heapq
import json
import math
import threading
import time
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, is_dataclass
from decimal import Decimal

from ExchangeModels import parse_funding_trades


D = Decimal
MINUTE_MS = 60_000
DAY_MS = 86_400_000
HISTORY_MS = 7 * DAY_MS
WINDOWS_MS = {
    "1m": MINUTE_MS, "5m": 5 * MINUTE_MS, "15m": 15 * MINUTE_MS,
    "1h": 60 * MINUTE_MS, "6h": 360 * MINUTE_MS,
    "24h": DAY_MS, "7d": HISTORY_MS,
}
CLEANING_VERSION = "exact-term-supported-v1"
LEGACY_CLEANING_VERSION = "legacy-global-v1"


def policy_fingerprint(policy):
    """Fingerprint only public strategy parameters, independently of the store."""
    payload = asdict(policy) if is_dataclass(policy) else dict(vars(policy))
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalized_trade(raw):
    try:
        signed = raw.get("raw_signed_amount")
        row = (
            int(raw["mts"]), abs(D(str(raw["amount"]))), D(str(raw["rate"])),
            int(raw["period"]), None if signed is None else D(str(signed)),
        )
        if (not row[1].is_finite() or not row[2].is_finite()
                or (row[4] is not None and not row[4].is_finite())):
            return None
        if row[0] < 0 or row[1] <= 0 or row[2] <= 0 or not 2 <= row[3] <= 120:
            return None
        return str(raw["id"]), row
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None


def _change(hist, period, rate, count, volume, squares):
    rates = hist.setdefault(period, {})
    old = rates.get(rate, (0, D(0), D(0)))
    value = (old[0] + count, old[1] + volume, old[2] + squares)
    if value[0] <= 0:
        rates.pop(rate, None)
        if not rates:
            hist.pop(period, None)
    else:
        rates[rate] = value


def _quantiles(rates, quantiles):
    total = sum((value[1] for value in rates.values()), D(0))
    if not rates or total <= 0:
        return tuple(D(0) for _ in quantiles)
    ordered = sorted(rates)
    result = []
    for quantile in quantiles:
        target, cumulative = total * D(quantile), D(0)
        selected = ordered[-1]
        for rate in ordered:
            cumulative += rates[rate][1]
            if cumulative >= target:
                selected = rate
                break
        result.append(selected)
    return tuple(result)


def _statistics(price_rates, raw_rates):
    q25, median, q75 = _quantiles(price_rates, (D(".25"), D(".5"), D(".75")))
    price_volume = sum((value[1] for value in price_rates.values()), D(0))
    variance = (
        sum((value[1] * (rate - median) ** 2 for rate, value in price_rates.items()), D(0)) / price_volume
        if price_volume > 0 else D(0)
    )
    return {
        "count": sum(value[0] for value in raw_rates.values()),
        "volume": sum((value[1] for value in raw_rates.values()), D(0)),
        "priceCount": sum(value[0] for value in price_rates.values()),
        "priceVolume": price_volume, "q25": q25, "median": median, "q75": q75,
        "volatility": D(str(math.sqrt(float(max(D(0), variance))))),
    }


class RollingMarketWindows:
    """Exact rolling histograms; complete minute buckets enter/leave incrementally.

    Original timestamps are retained for the two partial boundary minutes and
    the rare supported-outlier exception. No price/amount quantization is used.
    """

    def __init__(self, max_trades=None, cleaning_mode="exact_term", detailed_periods=None):
        if cleaning_mode not in {"exact_term", "legacy_global"}:
            raise ValueError("unknown rolling history cleaning mode")
        if max_trades is not None and int(max_trades) < 1:
            raise ValueError("max_trades must be positive")
        self.max_trades = None if max_trades is None else int(max_trades)
        self.cleaning_mode = cleaning_mode
        # Research may omit unused detailed diagnostics; cleaning, global
        # statistics and demand denominators always retain every public term.
        self.detailed_periods = (None if detailed_periods is None
                                 else frozenset(int(value) for value in detailed_periods))
        self._rows = {}
        self._events = defaultdict(dict)
        self._bins = defaultdict(dict)
        self._expiry = []
        self._minute_expiry = []
        self._histograms = {name: {} for name in WINDOWS_MS}
        self._ranges = {}
        self.as_of_ms = None

    @staticmethod
    def _range(now, duration):
        return ((now - duration + MINUTE_MS - 1) // MINUTE_MS * MINUTE_MS,
                now // MINUTE_MS * MINUTE_MS)

    def _apply_bucket(self, hist, minute, direction):
        for (period, rate, amount), count in self._bins.get(minute, {}).items():
            _change(hist, period, rate, direction * count,
                    direction * count * amount, direction * count * amount * amount)

    def _update(self, trade_id, row, direction):
        mts, amount, rate, period, _signed = row
        minute = mts // MINUTE_MS * MINUTE_MS
        key = (period, rate, amount)
        if minute not in self._bins:
            heapq.heappush(self._minute_expiry, minute)
        bins = self._bins[minute]
        count = bins.get(key, 0) + direction
        if count:
            bins[key] = count
        else:
            bins.pop(key, None)
        if direction > 0:
            self._events[minute][trade_id] = row
        else:
            self._events[minute].pop(trade_id, None)
        for name, (start, end) in self._ranges.items():
            if start <= minute < end:
                _change(self._histograms[name], period, rate, direction,
                        direction * amount, direction * amount * amount)

    def add(self, trades):
        inserted = 0
        for raw in trades or ():
            normalized = _normalized_trade(raw)
            if normalized is None:
                continue
            trade_id, row = normalized
            previous = self._rows.get(trade_id)
            if previous == row:
                continue
            if self.as_of_ms is not None and row[0] < self.as_of_ms - HISTORY_MS - MINUTE_MS:
                continue
            if previous is not None:
                self._update(trade_id, previous, -1)
            self._rows[trade_id] = row
            self._update(trade_id, row, 1)
            heapq.heappush(self._expiry, (row[0], trade_id))
            inserted += 1
            while self.max_trades is not None and len(self._rows) > self.max_trades:
                oldest, old_id = heapq.heappop(self._expiry)
                old_row = self._rows.get(old_id)
                if old_row is not None and old_row[0] == oldest:
                    self._rows.pop(old_id)
                    self._update(old_id, old_row, -1)
        return inserted

    def advance(self, now_ms):
        now = int(now_ms)
        backwards = self.as_of_ms is not None and now < self.as_of_ms
        for name, duration in WINDOWS_MS.items():
            start, end = self._range(now, duration)
            if name not in self._ranges or backwards:
                hist = {}
                for minute in self._bins:
                    if start <= minute < end:
                        self._apply_bucket(hist, minute, 1)
                self._histograms[name] = hist
            else:
                old_start, old_end = self._ranges[name]
                if start >= old_end:
                    self._histograms[name] = {}
                    candidates = (range(start, end, MINUTE_MS)
                                  if (end - start) // MINUTE_MS <= len(self._bins) else self._bins)
                    for minute in candidates:
                        if start <= minute < end:
                            self._apply_bucket(self._histograms[name], minute, 1)
                else:
                    for minute in range(old_start, start, MINUTE_MS):
                        self._apply_bucket(self._histograms[name], minute, -1)
                    for minute in range(old_end, end, MINUTE_MS):
                        self._apply_bucket(self._histograms[name], minute, 1)
            self._ranges[name] = (start, end)
        self.as_of_ms = now
        # The history service may publish the last verified endpoint up to one
        # minute behind wall time. Retain its boundary when coverage catches up.
        oldest_minute = (now - HISTORY_MS - MINUTE_MS) // MINUTE_MS * MINUTE_MS
        while self._expiry and self._expiry[0][0] < oldest_minute:
            mts, trade_id = heapq.heappop(self._expiry)
            row = self._rows.get(trade_id)
            if row is not None and row[0] == mts:
                self._rows.pop(trade_id, None)
        while self._minute_expiry and self._minute_expiry[0] < oldest_minute:
            minute = heapq.heappop(self._minute_expiry)
            self._bins.pop(minute, None)
            self._events.pop(minute, None)

    def _window_histograms(self, now):
        result = {}
        for name, duration in WINDOWS_MS.items():
            hist = {period: dict(rates) for period, rates in self._histograms[name].items()}
            start, end = self._ranges[name]
            threshold = now - duration
            edges = {threshold // MINUTE_MS * MINUTE_MS, now // MINUTE_MS * MINUTE_MS}
            for minute in edges:
                if start <= minute < end:
                    continue
                for mts, amount, rate, period, _signed in self._events.get(minute, {}).values():
                    if threshold <= mts and (mts <= now if self.cleaning_mode == "legacy_global" else mts < now):
                        _change(hist, period, rate, 1, amount, amount * amount)
            result[name] = hist
        return result

    def snapshot(self, policy, now_ms=None, coverage_complete=True, **metadata):
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        self.advance(now)
        raw = self._window_histograms(now)
        configured = {int(period) for pool in ("short", "medium", "long") for period in policy.periods(pool)}
        periods = configured | set(raw["7d"])
        legacy = self.cleaning_mode == "legacy_global"
        global_rates = {}
        if legacy:
            for rates in raw["7d"].values():
                for rate, value in rates.items():
                    _change(global_rates, 0, rate, *value)
            global_rates = global_rates.get(0, {})
            # Every term shares the same legacy cleaning rule. Computing these
            # global quantiles and sums for each of 119 observed funding terms
            # repeats identical work at every replay minute.
            count = sum(value[0] for value in global_rates.values())
            volume = sum((value[1] for value in global_rates.values()), D(0))
            squares = sum((value[2] for value in global_rates.values()), D(0))
            effective = volume * volume / squares if squares > 0 else D(0)
            q95 = _quantiles(global_rates, (D(".95"),))[0]
            support = max(D(150), volume * D(str(getattr(policy, "outlier_min_volume_share", ".005"))))
            high_volume = sum((value[1] for rate, value in global_rates.items() if rate > q95), D(0))
            legacy_context = {
                "effectiveSampleSize": effective, "cleaningMode": "LEGACY_GLOBAL", "enabled": bool(count),
                "q95": q95, "support": support, "removeUnsupported": bool(count and 0 < high_volume < support),
            }
        contexts = {}
        for period in periods:
            term_rates = raw["7d"].get(period, {})
            if legacy:
                term_count = sum(value[0] for value in term_rates.values())
                contexts[period] = {"rawCount": term_count, "validCount": term_count, **legacy_context}
                continue
            rates = global_rates if legacy else term_rates
            term_count = sum(value[0] for value in term_rates.values())
            count = sum(value[0] for value in rates.values())
            volume = sum((value[1] for value in rates.values()), D(0))
            squares = sum((value[2] for value in rates.values()), D(0))
            effective = volume * volume / squares if squares > 0 else D(0)
            enabled = bool(count) if legacy else count >= 30 and effective >= 10
            q95 = _quantiles(rates, (D(".95"),))[0]
            support = max(D(150), volume * D(str(getattr(policy, "outlier_min_volume_share", ".005"))))
            high_volume = sum((value[1] for rate, value in rates.items() if rate > q95), D(0))
            contexts[period] = {
                "rawCount": term_count, "validCount": term_count, "effectiveSampleSize": effective,
                "cleaningMode": "LEGACY_GLOBAL" if legacy else (
                    "EXACT_TERM_SUPPORTED_WINSORIZED" if enabled else "SPARSE_UNCLIPPED"
                ),
                "enabled": enabled, "q95": q95, "support": support,
                "removeUnsupported": bool(enabled and 0 < high_volume < support),
            }
        prices = {name: {period: dict(rates) for period, rates in hist.items()} for name, hist in raw.items()}
        rules = {period: context for period, context in contexts.items() if context["removeUnsupported"]}
        if rules:
            # Usually empty: only the small, unsupported tail needs event-level
            # inspection. Large supported trades retain the existing exception.
            for mts, amount, rate, period, _signed in self._rows.values():
                rule = rules.get(period)
                if rule is None or rate <= rule["q95"] or amount >= rule["support"]:
                    continue
                for name, duration in WINDOWS_MS.items():
                    if now - duration <= mts and (mts <= now if legacy else mts < now):
                        _change(prices[name], period, rate, -1, -amount, -amount * amount)
        legacy_limits = None
        if legacy:
            retained_global = {}
            for rates in prices["7d"].values():
                for rate, value in rates.items():
                    _change(retained_global, 0, rate, *value)
            legacy_limits = _quantiles(retained_global.get(0, {}), (D(".05"), D(".95")))
        for period, context in contexts.items():
            low, high = (legacy_limits if legacy else
                         _quantiles(prices["7d"].get(period, {}), (D(".05"), D(".95"))))
            context.update(low=low, high=high)
            if context["enabled"]:
                for hist in prices.values():
                    clamped = {}
                    for rate, value in hist.get(period, {}).items():
                        _change(clamped, period, max(low, min(high, rate)), *value)
                    if period in clamped:
                        hist[period] = clamped[period]
        by_period = {}
        published_periods = periods if self.detailed_periods is None else periods & self.detailed_periods
        for period in sorted(published_periods):
            context = contexts[period]
            windows = {
                name: _statistics(prices[name].get(period, {}),
                                  (prices if legacy else raw)[name].get(period, {}))
                for name in WINDOWS_MS
            }
            quality = {key: value for key, value in context.items() if key not in {"q95", "support", "low", "high"}}
            quality["priceSampleCount"] = windows["7d"]["priceCount"]
            quality["filteredCount"] = context["validCount"] - quality["priceSampleCount"]
            quality["coverageComplete"] = bool(coverage_complete)
            by_period[period] = {
                "period": period, "windows": windows, "rateWindows": windows,
                "quality": quality, "cleaning": {key: context[key] for key in ("enabled", "low", "high", "support")},
                "maxRate7d": max(prices["7d"].get(period, {}), default=D(0)),
                "rawTradeCount": context["rawCount"], "validTradeCount": context["validCount"],
            }
        global_windows = {}
        for name in WINDOWS_MS:
            price_rates, raw_rates = {}, {}
            for source, dest in ((prices[name], price_rates), ((prices if legacy else raw)[name], raw_rates)):
                for rates in source.values():
                    for rate, value in rates.items():
                        _change(dest, 0, rate, *value)
            global_windows[name] = _statistics(price_rates.get(0, {}), raw_rates.get(0, {}))
        total_count, total_volume = global_windows["7d"]["count"], global_windows["7d"]["volume"]
        demand = {}
        demand_histogram = (prices if legacy else raw)["7d"]
        for period in sorted(periods):
            rates = demand_histogram.get(period, {})
            count = sum(value[0] for value in rates.values())
            volume = sum((value[1] for value in rates.values()), D(0))
            count_share = D(count) / D(total_count) if total_count else D(0)
            volume_share = volume / total_volume if total_volume else D(0)
            demand[period] = {
                "trade_count": count, "volume": volume, "count_share": count_share,
                "volume_share": volume_share, "score": count_share * D(".6") + volume_share * D(".4"),
            }
        payload = {
            "schemaVersion": 1, "cleaningVersion": LEGACY_CLEANING_VERSION if legacy else CLEANING_VERSION,
            "snapshotKind": "LEGACY_GLOBAL_20K" if legacy else "EXACT_TERM_FULL_WINDOW",
            "policyFingerprint": policy_fingerprint(policy), "asOfMs": now,
            "computedAtMs": now, "coverage": {"startMs": now - HISTORY_MS, "endMs": now,
                                                  "complete": bool(coverage_complete)},
            "global": {"windows": global_windows, "periodDemand": demand},
            "windows": global_windows, "periodDemand": demand, "byPeriod": by_period,
            "periodWindows": {period: row["windows"] for period, row in by_period.items()},
            "rawTradeCount": sum(value[0] for rates in raw["7d"].values() for value in rates.values()),
            "validTradeCount": total_count,
            "priceSampleCount": global_windows["7d"]["priceCount"],
            "filteredCount": (sum(context["validCount"] for context in contexts.values())
                              - global_windows["7d"]["priceCount"]),
        }
        payload.update(metadata)
        if self.detailed_periods is not None:
            payload["detailPeriods"] = sorted(self.detailed_periods)
        return payload


def compute_window_snapshot(trades, policy, now_ms, coverage_complete=True, *, max_trades=None,
                            cleaning_mode="exact_term", **metadata):
    """Pure batch reference shared by live verification and historical replay."""
    rolling = RollingMarketWindows(max_trades=max_trades, cleaning_mode=cleaning_mode)
    rolling.add(trades)
    return rolling.snapshot(policy, now_ms, coverage_complete=coverage_complete, **metadata)


class SharedPublicRateLimiter:
    """One nonblocking public-read budget shared by foreground and backfill."""

    def __init__(self, minimum_interval_seconds=4.1, clock=time.monotonic):
        self.minimum_interval = max(0.0, float(minimum_interval_seconds))
        self.clock = clock
        self._next = 0.0
        self._lock = threading.Lock()

    def try_acquire(self):
        with self._lock:
            now = self.clock()
            if now < self._next:
                return False
            self._next = now + self.minimum_interval
            return True

    def defer(self, seconds):
        with self._lock:
            self._next = max(self._next, self.clock() + float(seconds))


class MarketHistoryService:
    UNCLEAN_KEY = "v36_market_history_unclean"
    PENDING_KEY = "v36_market_history_pending_reverify"

    def __init__(self, store, public_client, clock=time.time, *, rate_limiter=None, page_limit=10000):
        self.store, self.public_client, self.clock = store, public_client, clock
        self.public_read_limiter = rate_limiter or SharedPublicRateLimiter()
        self.page_limit = min(10000, max(1, int(page_limit)))
        self._rolling = RollingMarketWindows()
        self._lock, self._processing = threading.Lock(), threading.Lock()
        self._stop, self._wake = threading.Event(), threading.Event()
        self._thread = None
        self._queue, self._jobs = deque(), deque()
        self._queued_count = 0
        self._policies, self._published = OrderedDict(), {}
        self._warmed = False
        self._last_error = None
        self._source_sequence = 0
        self._last_observed_ms = None
        self._dropped_batches = 0
        self._backfill_failures = 0
        self._invalid_records = 0
        self._lifecycle = threading.Lock()
        self._markers_loaded = False
        self._running_marked = False
        self._quarantine_active = False
        self._quarantine_intervals = []
        self._quarantine_epoch = 0
        self._quarantine_dirty = False
        self._quarantine_reason = None
        self._repair_jobs = deque()
        self._repair_epoch = -1
        self._quarantine_registrations = 0

    @staticmethod
    def _merge_intervals(intervals):
        merged = []
        for start, end in sorted((int(start), int(end)) for start, end in intervals if int(start) < int(end)):
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        return merged

    def _quarantine(self, intervals, reason):
        with self._lock:
            self._quarantine_intervals = self._merge_intervals([*self._quarantine_intervals, *intervals])
            self._quarantine_active = True
            self._quarantine_epoch += 1
            self._quarantine_dirty = True
            self._quarantine_reason = reason
            self._last_error = reason
        self._wake.set()

    def _load_markers(self, now):
        if self._markers_loaded:
            return
        with self.store.read_connection() as connection:
            values = dict(connection.execute(
                "SELECT key,value FROM schema_meta WHERE key IN (?,?)", (self.UNCLEAN_KEY, self.PENDING_KEY),
            ))
        pending = json.loads(values.get(self.PENDING_KEY, "[]"))
        if values.get(self.UNCLEAN_KEY) == "1":
            pending = [*pending, [now - HISTORY_MS, now]]
        if pending:
            self._quarantine(pending, "UNCLEAN_HISTORY_REVERIFY_REQUIRED")
        self._markers_loaded = True

    def _write_markers(self, running, intervals):
        try:
            with self.store.transaction(immediate=True, timeout_seconds=0.05) as connection:
                connection.executemany(
                    "INSERT INTO schema_meta(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    [(self.UNCLEAN_KEY, "1" if running else "0"),
                     (self.PENDING_KEY, json.dumps(intervals, separators=(",", ":")))],
                )
            self._running_marked = bool(running)
            return True
        except Exception as exc:
            self._last_error = f"HISTORY_MARKER_WRITE_FAILED:{str(exc)[:200]}"
            return False

    def _persist_quarantine(self, allow_clear=False):
        with self._lock:
            dirty, epoch = self._quarantine_dirty, self._quarantine_epoch
            pending = [list(interval) for interval in self._quarantine_intervals]
        if dirty and not self._write_markers(True, pending):
            return False
        with self._lock:
            if epoch == self._quarantine_epoch:
                self._quarantine_dirty = False
                if (allow_clear and not self._quarantine_intervals and not self._quarantine_dirty
                        and not self._quarantine_registrations):
                    self._quarantine_active = False
                    self._quarantine_reason = None
        return True

    def _trim_quarantine(self, now):
        cutoff = int(now) - HISTORY_MS
        with self._lock:
            pending = self._merge_intervals(
                [[max(left, cutoff), right] for left, right in self._quarantine_intervals if right > cutoff]
            )
            if pending != self._quarantine_intervals:
                self._quarantine_intervals = pending
                self._quarantine_dirty = True
                # Expiry only removes an old prefix; it introduces no new gap.
                # Preserve this generation and its existing binary subdivision.
                # A successful in-flight request for the larger old interval
                # still proves its remaining subset. New drops alone invalidate
                # that request by advancing the epoch in _quarantine/ingest.
                if self._repair_epoch == self._quarantine_epoch:
                    jobs = [(max(left, cutoff), right) for left, right in self._repair_jobs if right > cutoff]
                    self._repair_jobs.clear()
                    self._repair_jobs.extend(jobs)

    def _finish_shutdown(self):
        with self._lock:
            pending = [list(interval) for interval in self._quarantine_intervals]
            quarantined = self._quarantine_active
        return self._write_markers(quarantined, pending)

    def start(self):
        with self._lifecycle:
            if self._thread is not None and self._thread.is_alive():
                return
            self._load_markers(int(self.clock() * 1000))
            with self._lock:
                pending = [list(interval) for interval in self._quarantine_intervals]
            if not self._write_markers(True, pending):
                raise RuntimeError(self._last_error)
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="funding-market-history", daemon=True)
            self._thread.start()

    def stop(self, timeout=3):
        """Stop within the caller's deadline and durably flush accepted batches.

        False means a network operation or busy database prevented a complete
        shutdown; pending rows remain queued for a subsequent stop/restart.
        """
        deadline = time.monotonic() + max(0, float(timeout))
        self._load_markers(int(self.clock() * 1000))
        if not self._running_marked and self._queued_count:
            if not self._write_markers(True, self._quarantine_intervals):
                return False
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        if thread is not None and thread.is_alive():
            return False
        if thread is not None and not thread.is_alive():
            self._thread = None
        if not self._processing.acquire(blocking=False):
            return False
        try:
            while time.monotonic() < deadline:
                pending = self._take_batch()
                if pending is None:
                    return self._finish_shutdown()
                batch, observed, source = pending
                try:
                    persisted = self._persist(batch)
                except Exception as exc:
                    self._last_error = str(exc)[:300]
                    persisted = False
                if not persisted:
                    self._restore_batch(batch, observed, source)
                    return False
            with self._lock:
                empty = not self._queue
            return self._finish_shutdown() if empty else False
        finally:
            self._processing.release()

    def ingest(self, trades, observed_at_ms=None, source="WEBSOCKET"):
        observed = int(observed_at_ms if observed_at_ms is not None else self.clock() * 1000)
        batch = [dict(row) for row in (trades or ())]
        overflow = False
        with self._lock:
            if self._stop.is_set():
                return False
            if self._queued_count + len(batch) > 100_000 or len(self._queue) >= 128:
                self._dropped_batches += 1
                # Publish the fail-closed state before inspecting a potentially
                # large dropped batch outside this lock.
                self._quarantine_active = True
                self._quarantine_epoch += 1
                self._quarantine_dirty = True
                self._quarantine_reason = "INGEST_QUEUE_OVERFLOW_BACKFILL_REQUIRED"
                self._last_error = self._quarantine_reason
                self._quarantine_registrations += 1
                overflow = True
            elif self._queue and self._queue[-1][2] == str(source) and len(self._queue[-1][0]) + len(batch) <= 5000:
                old_batch, old_observed, old_source = self._queue[-1]
                old_batch.extend(batch)
                self._queue[-1] = (old_batch, max(old_observed, observed), old_source)
            else:
                self._queue.append((batch, observed, str(source)))
            if not overflow:
                self._queued_count += len(batch)
        if overflow:
            # Normalization returns (id, row); only timestamps identify the
            # missing public interval, never amounts or account information.
            try:
                timestamps = [int(row[1][0]) for row in (_normalized_trade(raw) for raw in batch) if row is not None]
                left = min(timestamps) if timestamps else observed
                right = max(timestamps) + 1 if timestamps else observed + 1
                self._quarantine([[left, right]], "INGEST_QUEUE_OVERFLOW_BACKFILL_REQUIRED")
            finally:
                with self._lock:
                    self._quarantine_registrations -= 1
                self._wake.set()
            return False
        self._wake.set()
        return True

    def _take_batch(self, maximum=5000):
        with self._lock:
            if not self._queue:
                return None
            result, observed = [], 0
            while self._queue and len(result) < maximum:
                batch, stamp, source = self._queue.popleft()
                room = maximum - len(result)
                result.extend(batch[:room])
                observed = max(observed, stamp)
                if len(batch) > room:
                    self._queue.appendleft((batch[room:], stamp, source))
            self._queued_count -= len(result)
            return result, observed, "BATCH"

    def _restore_batch(self, batch, observed, source):
        with self._lock:
            self._queue.appendleft((batch, observed, source))
            self._queued_count += len(batch)

    def _sequence(self):
        value = self.store.market_source_sequence()
        return int(value.get("sourceSequence", 0) if isinstance(value, dict) else value)

    def _persist(self, trades):
        valid = []
        for raw in trades:
            normalized = _normalized_trade(raw)
            if normalized is None:
                self._invalid_records += 1
                continue
            trade_id, (mts, amount, rate, period, signed) = normalized
            valid.append({"id": trade_id, "mts": mts, "amount": amount, "rate": rate,
                          "period": period, "raw_signed_amount": signed})
        result = self.store.upsert_market_trades(valid, defer_if_busy=True)
        return not (isinstance(result, dict) and result.get("deferred"))

    def _load(self, start, end):
        batch = []
        for row in self.store.iter_market_trades(start, end, batch_size=5000):
            batch.append(row)
            if len(batch) >= 5000:
                self._rolling.add(batch)
                batch = []
        self._rolling.add(batch)

    def _coverage(self, start, end):
        return self.store.market_coverage(start, end)

    def _backfill_once(self, now):
        if self.public_client is None:
            return
        with self._lock:
            repairing, epoch = self._quarantine_active, self._quarantine_epoch
            if repairing and (self._repair_epoch != epoch or not self._repair_jobs):
                self._repair_jobs = deque(tuple(interval) for interval in self._quarantine_intervals)
                self._repair_epoch = epoch
        jobs = self._repair_jobs if repairing else self._jobs
        if not repairing and not jobs:
            coverage = self._coverage(now - HISTORY_MS, now)
            gaps = coverage.get("unresolvedIntervals", coverage.get("unresolved", []))
            if not gaps and not coverage.get("complete"):
                gaps = [{"startMs": now - HISTORY_MS, "endMs": now}]
            for gap in gaps:
                if gap.get("status") == "UNRESOLVED":
                    continue
                start = int(gap.get("startMs", gap.get("start_ms", now - HISTORY_MS)))
                end = int(gap.get("endMs", gap.get("end_ms", now)))
                if start < end:
                    jobs.append((start, end))
        if not jobs:
            return
        if not self.public_read_limiter.try_acquire():
            return
        start, end = jobs.popleft()
        try:
            raw = self.public_client.funding_trades("fUSD", start=start, end=end - 1,
                                                    limit=self.page_limit, sort=1)
            if not isinstance(raw, list):
                raise ValueError("public trade history did not return a list")
            parsed = parse_funding_trades(raw)
            parsed = [row for row in parsed if start <= row["mts"] < end]
            if len(parsed) != len(raw):
                raise ValueError("public trade history contained malformed or out-of-range rows")
            if not self._persist(parsed):
                jobs.appendleft((start, end))
                return
            if len(raw or ()) < self.page_limit:
                result = self.store.record_market_coverage(start, end, status="COMPLETE", source="REST",
                                                          defer_if_busy=True)
                if isinstance(result, dict) and result.get("deferred"):
                    jobs.appendleft((start, end))
                elif repairing:
                    with self._lock:
                        if epoch == self._quarantine_epoch:
                            remaining = []
                            for left, right in self._quarantine_intervals:
                                if left < start:
                                    remaining.append([left, min(right, start)])
                                if right > end:
                                    remaining.append([max(left, end), right])
                            self._quarantine_intervals = self._merge_intervals(remaining)
                            self._quarantine_dirty = True
            elif end - start > 1:
                midpoint = (start + end) // 2
                jobs.appendleft((midpoint, end))
                jobs.appendleft((start, midpoint))
            else:
                result = self.store.record_market_coverage(
                    start, end, status="UNRESOLVED", source="REST",
                    details={"reason": "SATURATED_SINGLE_MILLISECOND", "limit": self.page_limit},
                    defer_if_busy=True,
                )
                if isinstance(result, dict) and result.get("deferred"):
                    jobs.appendleft((start, end))
            self._backfill_failures = 0
            self._last_error = None
        except Exception as exc:
            jobs.appendleft((start, end))
            self._last_error = str(exc)[:300]
            self._backfill_failures += 1
            self.public_read_limiter.defer(min(120, 4.1 * 2 ** min(self._backfill_failures, 5)))

    def process_once(self, now_ms=None, backfill=True):
        now = int(now_ms if now_ms is not None else self.clock() * 1000)
        with self._processing:
            self._load_markers(now)
            self._trim_quarantine(now)
            self._persist_quarantine()
            if not self._warmed:
                self._source_sequence = self._sequence()
                self._load(now - HISTORY_MS - MINUTE_MS, now + 1)
                self._warmed = True
            with self._lock:
                policies = list(self._policies.items())
            for _ in range(8):
                pending = self._take_batch()
                if pending is None:
                    break
                batch, observed, source = pending
                if not self._persist(batch):
                    self._restore_batch(batch, observed, source)
                    break
                self._last_observed_ms = max(self._last_observed_ms or 0, min(now, observed))
            if backfill:
                self._backfill_once(now)
            external_sequence = self._sequence()
            if external_sequence != self._source_sequence:
                dirty_reader = getattr(self.store, "dirty_market_minutes", None)
                if dirty_reader is not None:
                    dirty = dirty_reader(self._source_sequence, start_ms=now - HISTORY_MS - MINUTE_MS,
                                         end_ms=now + 1)
                    for row in dirty:
                        minute = int(row.get("minuteMs", row.get("minute_ms")))
                        self._load(minute, minute + MINUTE_MS)
                else:
                    self._load(now - HISTORY_MS - MINUTE_MS, now + 1)
                # Capture the sequence before reading, so a concurrent late
                # insert cannot disappear behind the next high-water mark.
                self._source_sequence = external_sequence
            self._persist_quarantine(allow_clear=True)
            coverage = self._coverage(now - HISTORY_MS, now)
            asof = now
            if not coverage.get("complete"):
                intervals = coverage.get("coveredIntervals", [])
                ends = [int(row.get("endMs", row.get("end_ms", 0))) for row in intervals]
                recent = [end for end in ends if now - MINUTE_MS <= end <= now]
                if recent:
                    asof = max(recent)
                    coverage = self._coverage(asof - HISTORY_MS, asof)
            with self._lock:
                quarantined = self._quarantine_active
                quarantine_intervals = [list(interval) for interval in self._quarantine_intervals]
            if quarantined:
                coverage = {**coverage, "complete": False, "quarantinedIntervals": quarantine_intervals}
            state = "READY" if coverage.get("complete") and now - asof <= MINUTE_MS else "BACKFILLING"
            if self._last_error and state != "READY":
                state = "DEGRADED"
            self._rolling.advance(asof)
            for fingerprint, policy in policies:
                payload = self._rolling.snapshot(policy, asof, coverage_complete=bool(coverage.get("complete")))
                payload.update(
                    state=state, status=state, usable=state == "READY", computedAtMs=now,
                    sourceSequence=self._source_sequence, fallbackState=None if state == "READY" else "LEGACY_INPUT",
                    lastSuccessfulIngestMs=self._last_observed_ms, lastError=self._last_error,
                    pendingBackfillIntervals=len(self._jobs) + len(self._repair_jobs),
                    droppedBatches=self._dropped_batches,
                    quarantinedIntervals=quarantine_intervals,
                    invalidRecords=self._invalid_records, queuedTradeCount=self._queued_count,
                )
                payload["coverage"].update(coverage)
                payload["publishedAtMs"] = int(self.clock() * 1000)
                with self._lock:
                    if fingerprint in self._policies:
                        self._published[fingerprint] = payload
            return {"state": state, "asOfMs": asof, "coverage": coverage}

    def snapshot(self, policy, now_ms=None):
        fingerprint = policy_fingerprint(policy)
        with self._lock:
            if fingerprint not in self._policies:
                self._policies[fingerprint] = copy.deepcopy(policy)
                while len(self._policies) > 8:
                    removed, _policy = self._policies.popitem(last=False)
                    self._published.pop(removed, None)
            self._policies.move_to_end(fingerprint)
            payload = self._published.get(fingerprint)
            result = copy.deepcopy(payload) if payload is not None else None
            # A publisher may advance asOfMs while this reader waits for the
            # lock. Observe wall time after taking its payload, so fresh data
            # cannot appear to come from the future. Explicit replay times stay
            # fixed and retain the strict future-data check below.
            now = int(now_ms if now_ms is not None else self.clock() * 1000)
            if result is None:
                result = {
                    "policyFingerprint": fingerprint, "cleaningVersion": CLEANING_VERSION,
                    "state": "WARMING", "status": "WARMING", "usable": False,
                    "asOfMs": None, "computedAtMs": None, "fallbackState": "LEGACY_INPUT",
                    "coverage": {"startMs": now - HISTORY_MS, "endMs": now, "complete": False},
                }
            quarantined = self._quarantine_active
            pending = [list(interval) for interval in self._quarantine_intervals]
            reason = self._quarantine_reason
        if quarantined:
            result.update(state="DEGRADED", status="DEGRADED", usable=False, fallbackState="LEGACY_INPUT",
                          quarantinedIntervals=pending, lastError=reason)
            result["coverage"]["complete"] = False
        if result.get("asOfMs") is not None and not 0 <= now - int(result["asOfMs"]) <= MINUTE_MS:
            result.update(state="STALE", status="STALE", usable=False, fallbackState="LEGACY_INPUT")
        self._wake.set()
        return result

    def status(self, policy, now_ms=None):
        payload = self.snapshot(policy, now_ms)
        return {key: value for key, value in payload.items()
                if key not in {"global", "windows", "periodDemand", "byPeriod", "periodWindows"}}

    def _run(self):
        while not self._stop.is_set():
            try:
                self.process_once()
            except Exception as exc:
                self._last_error = str(exc)[:300]
            self._wake.wait(1)
            self._wake.clear()
