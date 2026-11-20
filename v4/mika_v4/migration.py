from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import urllib.parse
from contextlib import closing
from pathlib import Path

from .store import V4Store


class MigrationBlocked(RuntimeError):
    pass


def _read_only(path: Path) -> sqlite3.Connection:
    normalized = str(path.resolve()).replace("\\", "/")
    uri = f"file:{urllib.parse.quote(normalized)}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _tables(db: sqlite3.Connection) -> set[str]:
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def import_v3_history(source: Path, target: V4Store) -> dict[str, int]:
    source = Path(source).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    stat = source.stat()
    fingerprint = hashlib.sha256(f"{source}:{stat.st_size}:{stat.st_mtime_ns}".encode()).hexdigest()
    counts = {"market_trades": 0, "funding_stats": 0, "book_snapshots": 0, "account_samples": 0}
    with closing(_read_only(source)) as old:
        tables = _tables(old)
        if "runtime_state" in tables:
            row = old.execute("SELECT mode,safe_reason FROM runtime_state WHERE singleton=1").fetchone()
            if row and str(row[0]).upper() == "SAFE":
                raise MigrationBlocked(f"V3 is SAFE: {row[1] or 'unknown reason'}")
        if "order_intents" in tables:
            unresolved = old.execute(
                "SELECT COUNT(*) FROM order_intents WHERE state IN ('PLANNED','SUBMITTING','AMBIGUOUS')"
            ).fetchone()[0]
            if unresolved:
                raise MigrationBlocked(f"V3 has {unresolved} unresolved write intent(s)")
        with target.connect() as new:
            duplicate = new.execute("SELECT 1 FROM imports WHERE source_fingerprint=?", (fingerprint,)).fetchone()
            if duplicate:
                return counts
            if "market_trades" in tables:
                for row in old.execute("SELECT trade_id,mts,amount,rate,period FROM market_trades"):
                    counts["market_trades"] += new.execute(
                        "INSERT OR IGNORE INTO market_trades VALUES(?,?,?,?,?,'v3')", tuple(row)
                    ).rowcount
            if "funding_stats" in tables:
                for row in old.execute("SELECT mts,payload_json FROM funding_stats"):
                    counts["funding_stats"] += new.execute(
                        "INSERT OR IGNORE INTO funding_stats VALUES(?,?,'v3')", (row[0], row[1])
                    ).rowcount
            if "book_snapshots" in tables:
                for sample in old.execute("SELECT mts,book_json FROM book_snapshots"):
                    try:
                        rows = json.loads(sample[1])
                    except (TypeError, json.JSONDecodeError):
                        continue
                    for row in rows if isinstance(rows, list) else []:
                        if not isinstance(row, dict):
                            continue
                        new.execute(
                            "INSERT INTO book_snapshots(mts,rate,period,count,amount,source) VALUES(?,?,?,?,?,'v3')",
                            (
                                sample[0],
                                str(row.get("rate", 0)),
                                int(row.get("period", 0)),
                                int(row.get("count", 0)),
                                str(row.get("amount", 0)),
                            ),
                        )
                        counts["book_snapshots"] += 1
            if "account_samples" in tables:
                for row in old.execute(
                    "SELECT mts,total_principal,wallet_available,open_offers,active_credits FROM account_samples"
                ):
                    counts["account_samples"] += new.execute(
                        "INSERT INTO account_samples(mts,wallet_available,wallet_total,offers_total,credits_total,loans_total,authoritative) VALUES(?,?,?,?,?,'0',1)",
                        (row[0], row[2], row[1], row[3], row[4]),
                    ).rowcount
            total = sum(counts.values())
            new.execute(
                "INSERT INTO imports VALUES(?,?,?,?,?)",
                (
                    str(source),
                    fingerprint,
                    int(time.time() * 1000),
                    total,
                    "history/statistics only; no V3 active ownership imported",
                ),
            )
    return counts
