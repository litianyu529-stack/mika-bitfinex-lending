from __future__ import annotations

import asyncio
import json
import statistics
import threading
import time
from collections import deque
from decimal import ROUND_CEILING, Decimal
from typing import Callable, Iterable

from .config import V4Policy
from .domain import MarketSnapshot


D = Decimal
RATE_TICK = D("0.0000001")
MINUTE_MS = 60_000


def ceil_tick(value: D) -> D:
    value = D(value)
    if value <= 0:
        return D("0")
    return (value / RATE_TICK).to_integral_value(rounding=ROUND_CEILING) * RATE_TICK


def weighted_quantile(rows: Iterable[dict], quantile: D, value_key: str = "rate", weight_key: str = "amount") -> D:
    valid: list[tuple[D, D]] = []
    for row in rows:
        try:
            value = D(row[value_key])
            weight = abs(D(row[weight_key]))
        except (KeyError, TypeError, ValueError, ArithmeticError):
            continue
        if value > 0 and weight > 0:
            valid.append((value, weight))
    if not valid:
        return D("0")
    valid.sort(key=lambda item: item[0])
    target = sum((item[1] for item in valid), D("0")) * D(quantile)
    seen = D("0")
    for value, weight in valid:
        seen += weight
        if seen >= target:
            return value
    return valid[-1][0]


def _window(rows: Iterable[dict], now_ms: int, minutes: int) -> list[dict]:
    threshold = now_ms - minutes * MINUTE_MS
    return [row for row in rows if threshold <= int(row.get("mts", 0)) <= now_ms]


def _weighted_average(rows: Iterable[dict]) -> D:
    total = D("0")
    weighted = D("0")
    for row in rows:
        amount = abs(D(row.get("amount", 0)))
        rate = D(row.get("rate", 0))
        if amount > 0 and rate > 0:
            total += amount
            weighted += amount * rate
    return D("0") if total <= 0 else weighted / total


def _period_scores(periods: Iterable[int], trades: list[dict], book: list[dict], now_ms: int) -> dict[int, D]:
    recent = _window(trades, now_ms, 24 * 60)
    total_count = len(recent)
    total_volume = sum((abs(D(row.get("amount", 0))) for row in recent), D("0"))
    borrower_depth = {
        period: sum(
            (
                abs(D(row.get("amount", 0)))
                for row in book
                if int(row.get("period", 0)) == period and D(row.get("amount", 0)) < 0
            ),
            D("0"),
        )
        for period in periods
    }
    max_depth = max(borrower_depth.values(), default=D("0"))
    result: dict[int, D] = {}
    for period in periods:
        selected = [row for row in recent if int(row.get("period", 0)) == period]
        count_share = D(len(selected)) / D(total_count) if total_count else D("0")
        volume = sum((abs(D(row.get("amount", 0))) for row in selected), D("0"))
        volume_share = volume / total_volume if total_volume > 0 else D("0")
        demand = count_share * D("0.6") + volume_share * D("0.4")
        depth = borrower_depth[period] / max_depth if max_depth > 0 else D("0")
        result[period] = demand * D("0.5") + depth * D("0.5")
    return result


def build_market_snapshot(
    book: list[dict],
    trades: list[dict],
    policy: V4Policy,
    now_ms: int | None = None,
    last_update_ms: int | None = None,
) -> MarketSnapshot:
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    rows_5m = _window(trades, now, 5)
    rows_1h = _window(trades, now, 60)
    rows_6h = _window(trades, now, 360)
    rows_24h = _window(trades, now, 1440)
    best_borrower = max(
        (D(row.get("rate", 0)) for row in book if D(row.get("amount", 0)) < 0),
        default=D("0"),
    )
    vwap_5m = _weighted_average(rows_5m)
    median_5m = weighted_quantile(rows_5m, D("0.5"))
    median_1h = weighted_quantile(rows_1h, D("0.5"))
    median_6h = weighted_quantile(rows_6h, D("0.5"))
    q25 = weighted_quantile(rows_24h, D("0.25"))
    q75 = weighted_quantile(rows_24h, D("0.75"))
    iqr = max(D("0"), q75 - q25)
    lower = max(D("0"), q25 - iqr * D("1.5"))
    upper = q75 + iqr * D("1.5")
    candidates = [value for value in (best_borrower, vwap_5m, median_1h) if value > 0]
    filtered = [value for value in candidates if upper <= 0 or lower <= value <= upper]
    anchor = D(str(statistics.median(filtered))) if len(filtered) >= 2 else D("0")
    minimum = policy.grid_min_step_percent / D("100")
    maximum = policy.grid_max_step_percent / D("100")
    step = ceil_tick(min(maximum, max(minimum, iqr * policy.grid_iqr_fraction)))
    supported = max(best_borrower, q75 + iqr, anchor)
    periods = tuple(sorted(set(policy.short_periods + policy.medium_periods + (policy.long_period,))))
    freshest = int(last_update_ms or max((int(row.get("mts", 0)) for row in trades), default=0))
    fresh = bool(len(filtered) >= 2 and freshest and now - freshest <= policy.market_stale_seconds * 1000)
    return MarketSnapshot(
        as_of_ms=now,
        best_borrower_rate=best_borrower,
        vwap_5m=vwap_5m,
        median_5m=median_5m,
        median_1h=median_1h,
        median_6h=median_6h,
        q25_24h=q25,
        q75_24h=q75,
        robust_anchor=ceil_tick(anchor),
        grid_step=step,
        supported_ceiling=ceil_tick(supported),
        period_scores=_period_scores(periods, trades, book, now),
        valid_components=len(filtered),
        fresh=fresh,
    )


class MarketBuffer:
    def __init__(self, retention_minutes: int = 24 * 60):
        self._lock = threading.RLock()
        self._book: dict[tuple[D, int, int], dict] = {}
        self._trades: deque[dict] = deque()
        self._trade_ids: set[str] = set()
        self._retention_ms = retention_minutes * MINUTE_MS
        self.last_update_ms = 0

    def replace_book(self, rows: list[dict], now_ms: int | None = None) -> None:
        with self._lock:
            self._book = {}
            for row in rows:
                side = -1 if D(row.get("amount", 0)) < 0 else 1
                self._book[(D(row["rate"]), int(row["period"]), side)] = dict(row)
            self.last_update_ms = int(now_ms if now_ms is not None else time.time() * 1000)

    def update_book(self, row: dict, now_ms: int | None = None) -> None:
        with self._lock:
            side = -1 if D(row.get("amount", 0)) < 0 else 1
            key = (D(row["rate"]), int(row["period"]), side)
            if int(row.get("count", 0)) == 0:
                self._book.pop(key, None)
            else:
                self._book[key] = dict(row)
            self.last_update_ms = int(now_ms if now_ms is not None else time.time() * 1000)

    def add_trade(self, row: dict) -> None:
        with self._lock:
            trade_id = str(row.get("id", ""))
            if trade_id and trade_id in self._trade_ids:
                return
            self._trades.append(dict(row))
            if trade_id:
                self._trade_ids.add(trade_id)
            self.last_update_ms = max(self.last_update_ms, int(row.get("mts", 0)))
            threshold = self.last_update_ms - self._retention_ms
            while self._trades and int(self._trades[0].get("mts", 0)) < threshold:
                removed = self._trades.popleft()
                self._trade_ids.discard(str(removed.get("id", "")))

    def replace_trades(self, rows: list[dict]) -> None:
        with self._lock:
            unique = {str(row.get("id", index)): dict(row) for index, row in enumerate(rows)}
            self._trades = deque(sorted(unique.values(), key=lambda row: int(row["mts"])))
            self._trade_ids = {str(row.get("id", "")) for row in self._trades if str(row.get("id", ""))}
            if rows:
                self.last_update_ms = max(self.last_update_ms, max(int(row["mts"]) for row in rows))

    def snapshot(self) -> tuple[list[dict], list[dict], int]:
        with self._lock:
            return list(self._book.values()), list(self._trades), self.last_update_ms


class PublicMarketStream:
    """Bitfinex public book/trade WebSocket with automatic reconnect."""

    def __init__(self, buffer: MarketBuffer, logger: Callable[[str], None] | None = None):
        self.buffer = buffer
        self.logger = logger or (lambda _message: None)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=lambda: asyncio.run(self._run()), daemon=True, name="v4-market-ws")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    async def _run(self) -> None:
        try:
            import websockets
        except ImportError:
            self.logger("未安装 websockets，V4 将依赖 REST 行情。")
            return
        while not self._stop.is_set():
            try:
                async with websockets.connect("wss://api-pub.bitfinex.com/ws/2", ping_interval=20) as socket:
                    await socket.send(
                        json.dumps(
                            {"event": "subscribe", "channel": "book", "symbol": "fUSD", "prec": "P0", "len": 250}
                        )
                    )
                    await socket.send(json.dumps({"event": "subscribe", "channel": "trades", "symbol": "fUSD"}))
                    channels: dict[int, str] = {}
                    while not self._stop.is_set():
                        message = json.loads(await asyncio.wait_for(socket.recv(), timeout=30))
                        if isinstance(message, dict):
                            if message.get("event") == "subscribed":
                                channels[int(message["chanId"])] = str(message["channel"])
                            continue
                        if not isinstance(message, list) or len(message) < 2 or message[1] == "hb":
                            continue
                        channel = channels.get(int(message[0]))
                        if channel == "book":
                            payload = message[1]
                            rows = payload if payload and isinstance(payload[0], list) else [payload]
                            parsed = [
                                {
                                    "rate": D(str(row[0])),
                                    "period": int(row[1]),
                                    "count": int(row[2]),
                                    "amount": D(str(row[3])),
                                }
                                for row in rows
                                if isinstance(row, list) and len(row) >= 4
                            ]
                            if payload and isinstance(payload[0], list):
                                self.buffer.replace_book(parsed)
                            else:
                                for row in parsed:
                                    self.buffer.update_book(row)
                        elif channel == "trades" and len(message) >= 3 and message[1] in {"te", "tu"}:
                            row = message[2]
                            if isinstance(row, list) and len(row) >= 5:
                                self.buffer.add_trade(
                                    {
                                        "id": str(row[0]),
                                        "mts": int(row[1]),
                                        "amount": abs(D(str(row[2]))),
                                        "rate": D(str(row[3])),
                                        "period": int(row[4]),
                                    }
                                )
            except Exception as exc:
                self.logger(f"V4 WebSocket 重连：{exc}")
                await asyncio.sleep(5)
