from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from .domain import (
    AllocationPlan,
    IntentState,
    LongGateState,
    PeriodChoice,
    PlannedOffer,
    PlannerState,
    RuntimeMode,
)
from .recovery import MINIMUM_GAP_MS, REQUIRED_SNAPSHOTS, delay_seconds


D = Decimal
SCHEMA_VERSION = 2


def _encode(value: Any) -> Any:
    if isinstance(value, D):
        return format(value, "f")
    if hasattr(value, "value"):
        return value.value
    raise TypeError(f"cannot encode {type(value)!r}")


def plan_to_json(plan: AllocationPlan) -> str:
    return json.dumps(asdict(plan), default=_encode, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def plan_from_json(raw: str) -> AllocationPlan:
    data = json.loads(raw)
    state = data["state"]
    planner = PlannerState(
        short_period=PeriodChoice(**state["short_period"]),
        medium_period=PeriodChoice(**state["medium_period"]),
        long_gate=LongGateState(**state["long_gate"]),
    )
    orders = tuple(
        PlannedOffer(
            key=item["key"],
            pool=item["pool"],
            rung_index=int(item["rung_index"]),
            amount=D(item["amount"]),
            rate=D(item["rate"]),
            period=int(item["period"]),
            group_generation=int(item.get("group_generation", 1)),
        )
        for item in data["orders"]
    )
    return AllocationPlan(
        as_of_ms=int(data["as_of_ms"]),
        anchor=D(data["anchor"]),
        step=D(data["step"]),
        deployable=D(data["deployable"]),
        planned_amount=D(data["planned_amount"]),
        idle_amount=D(data["idle_amount"]),
        long_tier=int(data["long_tier"]),
        orders=orders,
        state=planner,
        reasons=tuple(data.get("reasons", [])),
    )


class V4Store:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._backup_before_migration()
        self._initialize()

    def _backup_before_migration(self) -> None:
        if not self.path.exists():
            return
        source = sqlite3.connect(self.path)
        try:
            row = source.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
        except sqlite3.Error:
            source.close()
            return
        version = int(row[0]) if row else 0
        if version >= SCHEMA_VERSION:
            source.close()
            return
        backup_dir = self.path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        target_path = backup_dir / f"schema-v{version}-{stamp}.sqlite3"
        target = sqlite3.connect(target_path)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS schema_info(version INTEGER NOT NULL);
                INSERT INTO schema_info(version) SELECT 2 WHERE NOT EXISTS(SELECT 1 FROM schema_info);
                CREATE TABLE IF NOT EXISTS runtime_state(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), mode TEXT NOT NULL,
                    previous_mode TEXT NOT NULL, safe_reason TEXT, consistent_syncs INTEGER NOT NULL DEFAULT 0,
                    last_authoritative_ms INTEGER, updated_at_ms INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO runtime_state VALUES(1,'SHADOW','SHADOW',NULL,0,NULL,0);
                CREATE TABLE IF NOT EXISTS recovery_state(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), active INTEGER NOT NULL DEFAULT 0,
                    category TEXT, reason TEXT, origin_mode TEXT, target_mode TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, successful_snapshots INTEGER NOT NULL DEFAULT 0,
                    required_snapshots INTEGER NOT NULL DEFAULT 2, started_at_ms INTEGER,
                    last_probe_at_ms INTEGER, next_probe_at_ms INTEGER, last_error TEXT,
                    heartbeat_at_ms INTEGER, manual_required INTEGER NOT NULL DEFAULT 0,
                    resume_pending_cycle INTEGER NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO recovery_state VALUES(1,0,NULL,NULL,NULL,NULL,0,0,2,NULL,NULL,NULL,NULL,NULL,0,0);
                CREATE TABLE IF NOT EXISTS planner_state(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload TEXT NOT NULL, updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS grid_groups(
                    group_id TEXT NOT NULL, generation INTEGER NOT NULL, pool TEXT NOT NULL,
                    anchor TEXT NOT NULL, step TEXT NOT NULL, period INTEGER NOT NULL, status TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL, rebuilt_at_ms INTEGER NOT NULL, rebuild_reason TEXT,
                    PRIMARY KEY(group_id,generation)
                );
                CREATE TABLE IF NOT EXISTS grid_rungs(
                    offer_key TEXT PRIMARY KEY, group_id TEXT NOT NULL, generation INTEGER NOT NULL,
                    pool TEXT NOT NULL, rung_index INTEGER NOT NULL, offer_id INTEGER UNIQUE,
                    amount_original TEXT NOT NULL, amount_remaining TEXT NOT NULL, rate TEXT NOT NULL,
                    period INTEGER NOT NULL, status TEXT NOT NULL, floor_reached_at_ms INTEGER,
                    last_fill_ms INTEGER, updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS execution_intents(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT NOT NULL UNIQUE, action TEXT NOT NULL,
                    offer_key TEXT, offer_id INTEGER, amount TEXT, rate TEXT, period INTEGER,
                    state TEXT NOT NULL, error TEXT, created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pending_plan(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    phase TEXT NOT NULL, reason TEXT, pools TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL, updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rebuild_events(pool TEXT NOT NULL, mts INTEGER NOT NULL, reason TEXT);
                CREATE INDEX IF NOT EXISTS ix_rebuild_events ON rebuild_events(pool,mts);
                CREATE TABLE IF NOT EXISTS confirmations(
                    kind TEXT NOT NULL, candidate TEXT NOT NULL, count INTEGER NOT NULL, mts INTEGER NOT NULL,
                    PRIMARY KEY(kind)
                );
                CREATE TABLE IF NOT EXISTS market_confirmations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mts INTEGER NOT NULL, kind TEXT NOT NULL,
                    current_value TEXT, candidate_value TEXT, confirmations INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mts INTEGER NOT NULL, level TEXT NOT NULL,
                    kind TEXT NOT NULL, payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS shadow_plans(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mts INTEGER NOT NULL, fingerprint TEXT NOT NULL,
                    payload TEXT NOT NULL, UNIQUE(mts,fingerprint)
                );
                CREATE TABLE IF NOT EXISTS market_trades(
                    trade_id INTEGER PRIMARY KEY, mts INTEGER NOT NULL, amount TEXT NOT NULL,
                    rate TEXT NOT NULL, period INTEGER NOT NULL, source TEXT NOT NULL DEFAULT 'v4'
                );
                CREATE TABLE IF NOT EXISTS funding_stats(
                    mts INTEGER PRIMARY KEY, value TEXT NOT NULL, source TEXT NOT NULL DEFAULT 'v4'
                );
                CREATE TABLE IF NOT EXISTS book_snapshots(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mts INTEGER NOT NULL, rate TEXT NOT NULL,
                    period INTEGER NOT NULL, count INTEGER NOT NULL, amount TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'v4'
                );
                CREATE TABLE IF NOT EXISTS account_samples(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mts INTEGER NOT NULL, wallet_available TEXT NOT NULL,
                    wallet_total TEXT NOT NULL, offers_total TEXT NOT NULL, credits_total TEXT NOT NULL,
                    loans_total TEXT NOT NULL, authoritative INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS imports(
                    source_path TEXT PRIMARY KEY, source_fingerprint TEXT NOT NULL, imported_at_ms INTEGER NOT NULL,
                    rows_imported INTEGER NOT NULL, note TEXT
                );
                CREATE TABLE IF NOT EXISTS validation_reports(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, mts INTEGER NOT NULL, evidence_end_ms INTEGER NOT NULL,
                    passed INTEGER NOT NULL, payload TEXT NOT NULL
                );
            """)
            version = int(db.execute("SELECT version FROM schema_info LIMIT 1").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise RuntimeError(f"V4 database schema {version} is newer than supported {SCHEMA_VERSION}")
            db.execute("UPDATE schema_info SET version=?", (SCHEMA_VERSION,))

    @staticmethod
    def fingerprint(plan: AllocationPlan) -> str:
        import hashlib

        structural = [
            {
                "pool": order.pool,
                "rung": order.rung_index,
                "amount": format(order.amount, "f"),
                "rate": format(order.rate, "f"),
                "period": order.period,
            }
            for order in sorted(plan.orders, key=lambda item: (item.pool, item.rung_index))
        ]
        payload = json.dumps(structural, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    def mode(self) -> RuntimeMode:
        with self.connect() as db:
            return RuntimeMode(db.execute("SELECT mode FROM runtime_state WHERE singleton=1").fetchone()[0])

    def set_mode(self, mode: RuntimeMode, *, manual: bool = True) -> None:
        if mode == RuntimeMode.SAFE:
            raise ValueError("use enter_safe")
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute(
                "UPDATE runtime_state SET mode=?, previous_mode=?, safe_reason=NULL, consistent_syncs=0, updated_at_ms=? WHERE singleton=1",
                (mode.value, mode.value, now),
            )
            if manual:
                db.execute(
                    """UPDATE recovery_state SET active=0,category=NULL,reason=NULL,origin_mode=NULL,
                       target_mode=NULL,attempts=0,successful_snapshots=0,started_at_ms=NULL,
                       last_probe_at_ms=NULL,next_probe_at_ms=NULL,last_error=NULL,
                       manual_required=0,resume_pending_cycle=0 WHERE singleton=1"""
                )

    def enter_safe(
        self,
        reason: str,
        *,
        category: str = "ACCOUNT_DATA",
        manual_required: bool = False,
    ) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            row = db.execute("SELECT mode,previous_mode FROM runtime_state WHERE singleton=1").fetchone()
            previous = row[0] if row[0] != RuntimeMode.SAFE.value else row[1]
            db.execute(
                "UPDATE runtime_state SET mode='SAFE', previous_mode=?, safe_reason=?, consistent_syncs=0, last_authoritative_ms=NULL, updated_at_ms=? WHERE singleton=1",
                (previous, reason, now),
            )
            self._event(db, "ERROR", "SAFE_ENTERED", {"reason": reason})
        self.begin_recovery(
            category,
            reason,
            origin_mode=previous,
            target_mode=previous,
            manual_required=manual_required,
            now_ms=now,
        )

    def safe_reason(self) -> str | None:
        with self.connect() as db:
            return db.execute("SELECT safe_reason FROM runtime_state WHERE singleton=1").fetchone()[0]

    @staticmethod
    def _recovery_payload(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        return {
            "active": bool(value["active"]),
            "category": value["category"],
            "reason": value["reason"],
            "originMode": value["origin_mode"],
            "targetMode": value["target_mode"],
            "attempts": int(value["attempts"] or 0),
            "successfulSnapshots": int(value["successful_snapshots"] or 0),
            "requiredSnapshots": int(value["required_snapshots"] or REQUIRED_SNAPSHOTS),
            "startedAt": value["started_at_ms"],
            "lastProbeAt": value["last_probe_at_ms"],
            "nextProbeAt": value["next_probe_at_ms"],
            "lastError": value["last_error"],
            "heartbeatAt": value["heartbeat_at_ms"],
            "manualRequired": bool(value["manual_required"]),
        }

    def recovery_status(self) -> dict[str, Any]:
        with self.connect() as db:
            row = db.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
        return self._recovery_payload(row)

    def begin_recovery(
        self,
        category: str,
        reason: str,
        *,
        origin_mode: str | RuntimeMode | None = None,
        target_mode: str | RuntimeMode | None = None,
        manual_required: bool = False,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        with self.connect() as db:
            runtime = db.execute("SELECT mode,previous_mode FROM runtime_state WHERE singleton=1").fetchone()
            current = db.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
            origin = str(getattr(origin_mode, "value", origin_mode) or runtime[1] or runtime[0])
            target = str(getattr(target_mode, "value", target_mode) or origin)
            if target not in {"LIVE", "SHADOW", "PAUSED"}:
                target = "PAUSED"
            attempts = int(current["attempts"] or 0) if current["active"] else 0
            started = current["started_at_ms"] if current["active"] else now
            db.execute(
                """UPDATE recovery_state SET active=1,category=?,reason=?,origin_mode=?,target_mode=?,
                   attempts=?,successful_snapshots=0,required_snapshots=?,started_at_ms=?,
                   last_probe_at_ms=NULL,next_probe_at_ms=?,last_error=?,manual_required=?,
                   resume_pending_cycle=0 WHERE singleton=1""",
                (
                    category,
                    reason,
                    origin,
                    target,
                    attempts,
                    REQUIRED_SNAPSHOTS,
                    started,
                    None if manual_required else now + delay_seconds(attempts) * 1000,
                    reason,
                    int(manual_required),
                ),
            )
        return self.recovery_status()

    def recovery_probe_due(self, now_ms: int | None = None) -> bool:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        with self.connect() as db:
            row = db.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
        return bool(
            row["active"]
            and not row["manual_required"]
            and (row["next_probe_at_ms"] is None or now >= int(row["next_probe_at_ms"]))
        )

    def record_recovery_failure(self, error: str, category: str | None = None, now_ms: int | None = None) -> None:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        with self.connect() as db:
            row = db.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
            if not row["active"] or row["manual_required"]:
                return
            attempts = int(row["attempts"] or 0) + 1
            db.execute(
                """UPDATE recovery_state SET category=COALESCE(?,category),attempts=?,
                   successful_snapshots=0,last_probe_at_ms=?,next_probe_at_ms=?,last_error=?
                   WHERE singleton=1""",
                (category, attempts, now, now + delay_seconds(attempts - 1) * 1000, error),
            )

    def touch_heartbeat(self, now_ms: int | None = None) -> None:
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        with self.connect() as db:
            db.execute("UPDATE recovery_state SET heartbeat_at_ms=? WHERE singleton=1", (now,))

    def consume_resume_barrier(self) -> bool:
        with self.connect() as db:
            pending = bool(db.execute("SELECT resume_pending_cycle FROM recovery_state WHERE singleton=1").fetchone()[0])
            if pending:
                db.execute("UPDATE recovery_state SET resume_pending_cycle=0 WHERE singleton=1")
        return pending

    def record_consistent_snapshot(self, as_of_ms: int, minimum_gap_ms: int = MINIMUM_GAP_MS) -> bool:
        with self.connect() as db:
            row = db.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
            if not row["active"] or row["manual_required"]:
                return False
            last = row["last_probe_at_ms"]
            count = int(row["successful_snapshots"])
            if last is None or int(as_of_ms) - int(last) >= minimum_gap_ms:
                count += 1
                db.execute(
                    "UPDATE recovery_state SET successful_snapshots=?,last_probe_at_ms=?,next_probe_at_ms=?,last_error=NULL WHERE singleton=1",
                    (count, int(as_of_ms), int(as_of_ms) + minimum_gap_ms),
                )
            if count < 2:
                return False
            unresolved = db.execute(
                "SELECT COUNT(*) FROM execution_intents WHERE state IN ('SUBMITTING','AMBIGUOUS')"
            ).fetchone()[0]
            if unresolved:
                return False
            restored = row["target_mode"] if row["target_mode"] in {"LIVE", "SHADOW", "PAUSED"} else "PAUSED"
            db.execute(
                "UPDATE runtime_state SET mode=?,previous_mode=?,safe_reason=NULL,consistent_syncs=0,updated_at_ms=? WHERE singleton=1",
                (restored, restored, int(time.time() * 1000)),
            )
            db.execute(
                """UPDATE recovery_state SET active=0,successful_snapshots=0,next_probe_at_ms=NULL,
                   last_error=NULL,manual_required=0,resume_pending_cycle=1 WHERE singleton=1"""
            )
            self._event(db, "INFO", "SAFE_RECOVERED", {"mode": restored})
            return True

    def planner_state(self) -> PlannerState:
        with self.connect() as db:
            row = db.execute("SELECT payload FROM planner_state WHERE singleton=1").fetchone()
        if not row:
            return PlannerState()
        data = json.loads(row[0])
        return PlannerState(
            short_period=PeriodChoice(**data["short_period"]),
            medium_period=PeriodChoice(**data["medium_period"]),
            long_gate=LongGateState(**data["long_gate"]),
        )

    def save_planner_state(self, state: PlannerState) -> None:
        payload = json.dumps(asdict(state), separators=(",", ":"), sort_keys=True)
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute(
                "INSERT INTO planner_state VALUES(1,?,?) ON CONFLICT(singleton) DO UPDATE SET payload=excluded.payload,updated_at_ms=excluded.updated_at_ms",
                (payload, now),
            )
            rows = (
                (
                    "short_period",
                    state.short_period.current,
                    state.short_period.candidate,
                    state.short_period.confirmations,
                ),
                (
                    "medium_period",
                    state.medium_period.current,
                    state.medium_period.candidate,
                    state.medium_period.confirmations,
                ),
                ("long_tier", state.long_gate.tier, state.long_gate.candidate_tier, state.long_gate.confirmations),
            )
            db.executemany(
                "INSERT INTO market_confirmations(mts,kind,current_value,candidate_value,confirmations) VALUES(?,?,?,?,?)",
                [(now, kind, current, candidate, count) for kind, current, candidate, count in rows],
            )

    def active_rungs(self) -> list[sqlite3.Row]:
        with self.connect() as db:
            return list(
                db.execute(
                    "SELECT * FROM grid_rungs WHERE status NOT IN ('CLOSED','REJECTED') ORDER BY pool,rung_index"
                )
            )

    def current_offer_shape(self, pools: set[str] | None = None) -> list[dict[str, Any]]:
        return [
            {
                "pool": row["pool"],
                "rung": int(row["rung_index"]),
                "amount": format(D(row["amount_remaining"]), "f"),
                "rate": format(D(row["rate"]), "f"),
                "period": int(row["period"]),
            }
            for row in self.active_rungs()
            if row["offer_id"] is not None and (pools is None or row["pool"] in pools)
        ]

    def managed_offer_ids(self, pools: set[str] | None = None) -> set[int]:
        return {
            int(row["offer_id"])
            for row in self.active_rungs()
            if row["offer_id"] is not None and (pools is None or row["pool"] in pools)
        }

    def save_plan_rungs(self, plan: AllocationPlan, reason: str) -> None:
        now = int(time.time() * 1000)
        by_pool: dict[str, list[PlannedOffer]] = {}
        for offer in plan.orders:
            by_pool.setdefault(offer.pool, []).append(offer)
        with self.connect() as db:
            for pool, offers in by_pool.items():
                group_id = offers[0].key.split(":", 1)[0]
                generation_row = db.execute(
                    "SELECT COALESCE(MAX(generation),0) FROM grid_groups WHERE pool=?", (pool,)
                ).fetchone()
                generation = int(generation_row[0]) + 1
                db.execute(
                    "INSERT INTO grid_groups VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        group_id,
                        generation,
                        pool,
                        format(plan.anchor, "f"),
                        format(plan.step, "f"),
                        offers[0].period,
                        "ACTIVE",
                        now,
                        now,
                        reason,
                    ),
                )
                db.execute("INSERT INTO rebuild_events VALUES(?,?,?)", (pool, now, reason))
                for offer in offers:
                    db.execute(
                        "INSERT OR IGNORE INTO grid_rungs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            offer.key,
                            group_id,
                            generation,
                            pool,
                            offer.rung_index,
                            None,
                            format(offer.amount, "f"),
                            format(offer.amount, "f"),
                            format(offer.rate, "f"),
                            offer.period,
                            "PLANNED",
                            None,
                            None,
                            now,
                        ),
                    )

    def update_rung_offer(self, offer_key: str, offer_id: int, status: str = "OPEN") -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE grid_rungs SET offer_id=?,status=?,updated_at_ms=? WHERE offer_key=?",
                (int(offer_id), status, int(time.time() * 1000), offer_key),
            )

    def mark_rung_rejected(self, offer_key: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE grid_rungs SET status='REJECTED',updated_at_ms=? WHERE offer_key=?",
                (int(time.time() * 1000), offer_key),
            )

    def update_rung_snapshot(self, offer_id: int, remaining: D, status: str, as_of_ms: int) -> None:
        with self.connect() as db:
            row = db.execute("SELECT amount_remaining FROM grid_rungs WHERE offer_id=?", (offer_id,)).fetchone()
            if not row:
                return
            fill_ms = as_of_ms if D(str(row[0])) > D(remaining) else None
            db.execute(
                "UPDATE grid_rungs SET amount_remaining=?,status=?,last_fill_ms=COALESCE(?,last_fill_ms),updated_at_ms=? WHERE offer_id=?",
                (format(D(remaining), "f"), status, fill_ms, as_of_ms, offer_id),
            )

    def mark_floor_reached(self, offer_key: str, as_of_ms: int) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE grid_rungs SET floor_reached_at_ms=COALESCE(floor_reached_at_ms,?),updated_at_ms=? WHERE offer_key=?",
                (as_of_ms, as_of_ms, offer_key),
            )

    def close_missing_rungs(self, open_offer_ids: set[int], as_of_ms: int) -> None:
        with self.connect() as db:
            rows = db.execute(
                "SELECT offer_key,offer_id FROM grid_rungs WHERE offer_id IS NOT NULL AND status NOT IN ('CLOSED','REJECTED')"
            ).fetchall()
            for row in rows:
                if int(row["offer_id"]) not in open_offer_ids:
                    db.execute(
                        "UPDATE grid_rungs SET amount_remaining='0',status='CLOSED',updated_at_ms=? WHERE offer_key=?",
                        (as_of_ms, row["offer_key"]),
                    )

    def rebuild_count(self, pool: str, now_ms: int) -> int:
        with self.connect() as db:
            return int(
                db.execute(
                    "SELECT COUNT(*) FROM rebuild_events WHERE pool=? AND mts>=?", (pool, now_ms - 3_600_000)
                ).fetchone()[0]
            )

    def last_rebuild_ms(self, pool: str) -> int:
        with self.connect() as db:
            return int(
                db.execute("SELECT COALESCE(MAX(mts),0) FROM rebuild_events WHERE pool=?", (pool,)).fetchone()[0]
            )

    def create_intent(
        self,
        fingerprint: str,
        action: str,
        *,
        offer_key: str | None = None,
        offer_id: int | None = None,
        amount: D | None = None,
        rate: D | None = None,
        period: int | None = None,
    ) -> bool:
        now = int(time.time() * 1000)
        with self.connect() as db:
            cursor = db.execute(
                "INSERT OR IGNORE INTO execution_intents(fingerprint,action,offer_key,offer_id,amount,rate,period,state,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    fingerprint,
                    action,
                    offer_key,
                    offer_id,
                    None if amount is None else format(amount, "f"),
                    None if rate is None else format(rate, "f"),
                    period,
                    IntentState.PLANNED.value,
                    now,
                    now,
                ),
            )
            return cursor.rowcount == 1

    def set_intent_state(self, fingerprint: str, state: IntentState, error: str | None = None) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE execution_intents SET state=?,error=?,updated_at_ms=? WHERE fingerprint=?",
                (state.value, error, int(time.time() * 1000), fingerprint),
            )

    def unresolved_intents(self) -> list[sqlite3.Row]:
        with self.connect() as db:
            return list(
                db.execute("SELECT * FROM execution_intents WHERE state IN ('PLANNED','SUBMITTING','AMBIGUOUS')")
            )

    def close_unsent_planned_intents(self) -> int:
        now = int(time.time() * 1000)
        with self.connect() as db:
            cursor = db.execute(
                "UPDATE execution_intents SET state='CLOSED',error='process stopped before send',updated_at_ms=? "
                "WHERE state='PLANNED'",
                (now,),
            )
            if cursor.rowcount:
                self._event(db, "WARNING", "UNSENT_INTENTS_CLOSED", {"count": cursor.rowcount})
            return cursor.rowcount

    def set_pending_plan(self, plan: AllocationPlan, phase: str, reason: str, pools: set[str]) -> None:
        now = int(time.time() * 1000)
        with self.connect() as db:
            db.execute(
                "INSERT INTO pending_plan VALUES(1,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET payload=excluded.payload,fingerprint=excluded.fingerprint,phase=excluded.phase,reason=excluded.reason,pools=excluded.pools,updated_at_ms=excluded.updated_at_ms",
                (plan_to_json(plan), self.fingerprint(plan), phase, reason, json.dumps(sorted(pools)), now, now),
            )

    def update_pending_phase(self, phase: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE pending_plan SET phase=?,updated_at_ms=? WHERE singleton=1",
                (phase, int(time.time() * 1000)),
            )

    def get_pending_plan(self) -> tuple[AllocationPlan, str, str, set[str]] | None:
        with self.connect() as db:
            row = db.execute("SELECT payload,phase,reason,pools FROM pending_plan WHERE singleton=1").fetchone()
        return None if row is None else (plan_from_json(row[0]), row[1], row[2] or "", set(json.loads(row[3])))

    def clear_pending_plan(self) -> None:
        with self.connect() as db:
            db.execute("DELETE FROM pending_plan WHERE singleton=1")

    def record_shadow_plan(self, plan: AllocationPlan) -> None:
        with self.connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO shadow_plans(mts,fingerprint,payload) VALUES(?,?,?)",
                (plan.as_of_ms, self.fingerprint(plan), plan_to_json(plan)),
            )

    def record_account_sample(self, snapshot: Any) -> None:
        with self.connect() as db:
            db.execute(
                "INSERT INTO account_samples(mts,wallet_available,wallet_total,offers_total,credits_total,loans_total,authoritative) VALUES(?,?,?,?,?,?,?)",
                (
                    snapshot.as_of_ms,
                    format(snapshot.wallet_available, "f"),
                    format(snapshot.wallet_total, "f"),
                    format(sum((x.amount for x in snapshot.offers), D("0")), "f"),
                    format(sum((x.amount for x in snapshot.credits), D("0")), "f"),
                    format(sum((x.amount for x in snapshot.loans), D("0")), "f"),
                    int(snapshot.authoritative),
                ),
            )

    def reconcile_ambiguous(
        self,
        snapshot: Any,
        *,
        offer_history: list[list[Any]] | None = None,
        trade_history: list[list[Any]] | None = None,
        history_authoritative: bool = False,
    ) -> bool:
        """Resolve unknown writes only from two matching authoritative observations.

        Active offers and authenticated offer/trade history are considered together.
        More than one possible submit match is deliberately unrecoverable without a
        human, because choosing one could cause a duplicate order.
        """
        if not snapshot.authoritative:
            return False
        offers = {int(item.offer_id): item for item in snapshot.offers}
        historical_offers: dict[int, dict[str, Any]] = {}
        for item in offer_history or []:
            if not isinstance(item, (list, tuple)) or len(item) <= 15:
                continue
            try:
                historical_offers[int(item[0])] = {
                    "offer_id": int(item[0]),
                    "mts": int(item[2] or item[3] or 0),
                    "amount": abs(D(str(item[5]))),
                    "rate": D(str(item[14])),
                    "period": int(item[15]),
                }
            except (TypeError, ValueError, ArithmeticError):
                continue
        historical_trades: list[dict[str, Any]] = []
        for item in trade_history or []:
            if not isinstance(item, (list, tuple)) or len(item) < 7:
                continue
            try:
                historical_trades.append(
                    {
                        "offer_id": int(item[3]),
                        "mts": int(item[2]),
                        "amount": abs(D(str(item[4]))),
                        "rate": D(str(item[5])),
                        "period": int(item[6]),
                    }
                )
            except (TypeError, ValueError, ArithmeticError):
                continue
        manual_reason: str | None = None
        resolved_any = False
        with self.connect() as db:
            rows = db.execute(
                "SELECT * FROM execution_intents WHERE state IN ('SUBMITTING','AMBIGUOUS') ORDER BY id"
            ).fetchall()
            for row in rows:
                candidate: str | None = None
                if row["action"] == "CANCEL" and row["offer_id"] is not None:
                    candidate = "PRESENT" if int(row["offer_id"]) in offers else "ABSENT"
                elif row["action"] == "SUBMIT":
                    created = int(row["created_at_ms"])
                    candidate_ids = {
                        int(item.offer_id)
                        for item in snapshot.offers
                        if D(item.amount_original) == D(row["amount"])
                        and D(item.rate) == D(row["rate"])
                        and int(item.period) == int(row["period"])
                        and created - 5_000 <= int(item.mts_created) <= created + 600_000
                    }
                    candidate_ids.update(
                        item["offer_id"]
                        for item in historical_offers.values()
                        if item["amount"] == D(row["amount"])
                        and item["rate"] == D(row["rate"])
                        and item["period"] == int(row["period"])
                        and created - 5_000 <= item["mts"] <= created + 600_000
                    )
                    candidate_ids.update(
                        item["offer_id"]
                        for item in historical_trades
                        if item["rate"] == D(row["rate"])
                        and item["period"] == int(row["period"])
                        and created - 5_000 <= item["mts"] <= created + 600_000
                    )
                    if len(candidate_ids) > 1:
                        manual_reason = f"multiple possible matches for {row['fingerprint']}"
                        break
                    if len(candidate_ids) == 1:
                        candidate = str(next(iter(candidate_ids)))
                    elif history_authoritative and snapshot.as_of_ms - created >= 60_000:
                        candidate = "ABSENT"
                kind = f"ambiguous:{row['fingerprint']}"
                if candidate is None:
                    db.execute("DELETE FROM confirmations WHERE kind=?", (kind,))
                    continue
                existing = db.execute("SELECT candidate,count,mts FROM confirmations WHERE kind=?", (kind,)).fetchone()
                if existing and existing[0] == candidate and snapshot.as_of_ms - int(existing[2]) >= 30_000:
                    count = int(existing[1]) + 1
                elif existing and existing[0] == candidate:
                    count = int(existing[1])
                else:
                    count = 1
                db.execute(
                    "INSERT INTO confirmations(kind,candidate,count,mts) VALUES(?,?,?,?) ON CONFLICT(kind) DO UPDATE SET candidate=excluded.candidate,count=excluded.count,mts=excluded.mts",
                    (kind, candidate, count, snapshot.as_of_ms),
                )
                if count < 2:
                    continue
                resolved_any = True
                resolved_state = "CONFIRMED"
                resolved_error = None
                if candidate in {"ABSENT", "PRESENT"} and row["action"] == "SUBMIT":
                    resolved_state = "CLOSED"
                    resolved_error = "two authoritative observations confirm no submitted offer"
                elif candidate == "PRESENT" and row["action"] == "CANCEL":
                    resolved_state = "CLOSED"
                    resolved_error = "two authoritative observations confirm cancellation did not apply"
                db.execute(
                    "UPDATE execution_intents SET state=?,error=?,updated_at_ms=? WHERE id=?",
                    (resolved_state, resolved_error, snapshot.as_of_ms, row["id"]),
                )
                if row["action"] == "SUBMIT" and row["offer_key"] and candidate != "ABSENT":
                    db.execute(
                        "UPDATE grid_rungs SET offer_id=?,status='OPEN',updated_at_ms=? WHERE offer_key=?",
                        (int(candidate), snapshot.as_of_ms, row["offer_key"]),
                    )
                elif row["action"] == "SUBMIT" and row["offer_key"]:
                    db.execute(
                        "UPDATE grid_rungs SET status='REJECTED',updated_at_ms=? WHERE offer_key=?",
                        (snapshot.as_of_ms, row["offer_key"]),
                    )
                db.execute("DELETE FROM confirmations WHERE kind=?", (kind,))
        if manual_reason:
            recovery = self.recovery_status()
            self.begin_recovery(
                "AMBIGUOUS_MULTIPLE_MATCHES",
                manual_reason,
                origin_mode=recovery.get("originMode"),
                target_mode=recovery.get("targetMode"),
                manual_required=True,
                now_ms=snapshot.as_of_ms,
            )
            return False
        if resolved_any and not self.unresolved_intents():
            recovery = self.recovery_status()
            self.begin_recovery(
                "POST_AMBIGUOUS_RECONCILIATION",
                "unknown write reconciled; confirming two new clean snapshots",
                origin_mode=recovery.get("originMode"),
                target_mode=recovery.get("targetMode"),
                now_ms=snapshot.as_of_ms,
            )
            with self.connect() as db:
                db.execute(
                    "UPDATE recovery_state SET last_probe_at_ms=?,next_probe_at_ms=? WHERE singleton=1",
                    (snapshot.as_of_ms, snapshot.as_of_ms + MINIMUM_GAP_MS),
                )
        return resolved_any

    def record_event(self, level: str, kind: str, payload: dict[str, Any]) -> None:
        with self.connect() as db:
            self._event(db, level, kind, payload)

    def record_validation_report(self, passed: bool, payload: str, evidence_end_ms: int) -> None:
        with self.connect() as db:
            db.execute(
                "INSERT INTO validation_reports(mts,evidence_end_ms,passed,payload) VALUES(?,?,?,?)",
                (int(time.time() * 1000), int(evidence_end_ms), int(passed), payload),
            )

    def latest_validation(self) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM validation_reports ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def adopt_offers(self, offers: list[Any], as_of_ms: int) -> int:
        adopted = 0
        by_pool: dict[tuple[str, int], list[Any]] = {}
        for offer in offers:
            pool = "short" if offer.period <= 7 else "medium" if offer.period <= 30 else "long"
            by_pool.setdefault((pool, offer.period), []).append(offer)
        with self.connect() as db:
            for (pool, period), selected in by_pool.items():
                group_id = f"adopted-{pool}-{period}-{as_of_ms}"
                generation = int(
                    db.execute(
                        "SELECT COALESCE(MAX(generation),0)+1 FROM grid_groups WHERE pool=?", (pool,)
                    ).fetchone()[0]
                )
                anchor = min((item.rate for item in selected), default=D("0"))
                db.execute(
                    "INSERT INTO grid_groups VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        group_id,
                        generation,
                        pool,
                        format(anchor, "f"),
                        "0",
                        period,
                        "ACTIVE",
                        as_of_ms,
                        as_of_ms,
                        "EXPLICIT_EXTERNAL_ADOPTION",
                    ),
                )
                for index, offer in enumerate(sorted(selected, key=lambda item: (item.rate, item.offer_id))):
                    key = f"adopted:{offer.offer_id}"
                    cursor = db.execute(
                        "INSERT OR IGNORE INTO grid_rungs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            key,
                            group_id,
                            generation,
                            pool,
                            index,
                            offer.offer_id,
                            format(offer.amount_original, "f"),
                            format(offer.amount, "f"),
                            format(offer.rate, "f"),
                            offer.period,
                            "OPEN",
                            None,
                            None,
                            as_of_ms,
                        ),
                    )
                    adopted += cursor.rowcount
            self._event(db, "WARNING", "EXTERNAL_OFFERS_ADOPTED", {"offer_ids": [item.offer_id for item in offers]})
        return adopted

    @staticmethod
    def _event(db: sqlite3.Connection, level: str, kind: str, payload: dict[str, Any]) -> None:
        db.execute(
            "INSERT INTO events(mts,level,kind,payload) VALUES(?,?,?,?)",
            (int(time.time() * 1000), level, kind, json.dumps(payload, default=_encode, ensure_ascii=False)),
        )

    def status_payload(self) -> dict[str, Any]:
        with self.connect() as db:
            state = dict(db.execute("SELECT * FROM runtime_state WHERE singleton=1").fetchone())
            active = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM grid_rungs WHERE status NOT IN ('CLOSED','REJECTED') ORDER BY pool,rung_index"
                )
            ]
            events = [dict(row) for row in db.execute("SELECT * FROM events ORDER BY id DESC LIMIT 30")]
            pending = db.execute(
                "SELECT fingerprint,phase,reason,pools,created_at_ms,updated_at_ms FROM pending_plan WHERE singleton=1"
            ).fetchone()
        return {
            "runtime": state,
            "recovery": self.recovery_status(),
            "active_rungs": active,
            "events": events,
            "pending_plan": dict(pending) if pending else None,
            "validation": self.latest_validation(),
        }
