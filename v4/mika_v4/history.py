from __future__ import annotations

import json
import time
from decimal import Decimal

from .bitfinex import BitfinexClient
from .runtime import parse_book, parse_trades
from .store import V4Store


D = Decimal


class HistoricalCollector:
    def __init__(self, client: BitfinexClient, store: V4Store) -> None:
        self.client = client
        self.store = store

    def backfill(self, days: int = 90, now_ms: int | None = None) -> dict[str, int]:
        end = int(now_ms if now_ms is not None else time.time() * 1000)
        cursor = end - int(days) * 86_400_000
        counts = {"market_trades": 0, "funding_stats": 0}
        while cursor < end:
            batch = parse_trades(self.client.funding_trades("fUSD", cursor, 10_000, sort=1))
            if not batch:
                break
            with self.store.connect() as db:
                for row in batch:
                    if row["mts"] > end:
                        continue
                    counts["market_trades"] += db.execute(
                        "INSERT OR IGNORE INTO market_trades VALUES(?,?,?,?,?,'v4')",
                        (row["id"], row["mts"], format(row["amount"], "f"), format(row["rate"], "f"), row["period"]),
                    ).rowcount
            next_cursor = max(row["mts"] for row in batch) + 1
            if next_cursor <= cursor or len(batch) < 10_000:
                break
            cursor = next_cursor

        start = end - int(days) * 86_400_000
        end_cursor = end
        while end_cursor >= start:
            batch = self.client.funding_stats("fUSD", start, end_cursor, 250)
            if not batch:
                break
            timestamps: list[int] = []
            with self.store.connect() as db:
                for row in batch:
                    if not isinstance(row, list) or not row:
                        continue
                    mts = int(row[0])
                    timestamps.append(mts)
                    if mts <= end:
                        counts["funding_stats"] += db.execute(
                            "INSERT OR IGNORE INTO funding_stats VALUES(?,?,'v4')",
                            (mts, json.dumps(row, separators=(",", ":"))),
                        ).rowcount
            if not timestamps:
                break
            next_end = min(timestamps) - 1
            if next_end >= end_cursor or len(batch) < 250:
                break
            end_cursor = next_end
        return counts

    def capture_real_book(self, now_ms: int | None = None) -> int:
        mts = int(now_ms if now_ms is not None else time.time() * 1000)
        rows = parse_book(self.client.funding_book("fUSD", 250))
        with self.store.connect() as db:
            for row in rows:
                db.execute(
                    "INSERT INTO book_snapshots(mts,rate,period,count,amount,source) VALUES(?,?,?,?,?,'real')",
                    (mts, format(row["rate"], "f"), row["period"], row["count"], format(row["amount"], "f")),
                )
        return len(rows)
