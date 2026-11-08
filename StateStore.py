import hashlib
import json
import os
import sqlite3
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from decimal import Decimal

from WriteRecovery import can_clear_ambiguous_pause, restart_transition, unique_unbound_candidate
from ExchangeModels import funding_offer_effective_rate
from Recovery import (
    RECOVERY_MINIMUM_GAP_MS,
    RECOVERY_REQUIRED_SNAPSHOTS,
    recovery_category_for_reason,
    recovery_delay_seconds,
)
from StrategyV3 import QUICK_EXECUTION_CURVE
from StrategyState import demand_confirmation_transition, period_selection_transition
from ManualProtection import ManualProtectionStore
from V36Validation import persist_validation_certificate, validation_status


D = Decimal
RUNTIME_MODES = {"PAUSED", "LIVE", "REPLAY", "APPLYING"}
OPEN_INTENT_STATES = {"PLANNED", "SUBMITTING", "AMBIGUOUS"}
AUTO_RECOVERABLE_PAUSE_REASONS = {
    "MARKET_DATA_STALE",
    "ACCOUNT_AVAILABLE_BALANCE_UNKNOWN",
    "ACCOUNT_RECONCILIATION_MISMATCH",
    "AMBIGUOUS_WALLET_TRANSFER",
}
LEGACY_REPRICE_CURVE = "LEGACY"
EXACT_TERM_EXPLORATION_CURVE = "EXACT_TERM_EXPLORATION_V4"
CURRENT_RELEASE = "0.3.6.0"
CURRENT_RELEASE_LABEL = "V3.6"
CURRENT_SCHEMA_VERSION = 19
SCHEMA_FAMILY = "v3"


class StateStoreError(Exception):
    pass


class StaleStrategyVersion(StateStoreError):
    pass


class DuplicateIntent(StateStoreError):
    pass


class InsufficientReservedBalance(StateStoreError):
    pass


def _decimal_text(value):
    return format(D(value), "f")


def order_intent_fingerprint(currency, amount, rate, period, offer_type, flags, strategy_version, slice_key):
    payload = "|".join(
        (
            str(currency).upper(),
            _decimal_text(amount),
            _decimal_text(rate),
            str(int(period)),
            str(offer_type).upper(),
            str(int(flags)),
            str(strategy_version),
            str(slice_key),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def base_slice_key(slice_key):
    value = str(slice_key)
    prefix, marker, suffix = value.rpartition(":r")
    return prefix if marker and suffix.isdigit() else value


def slice_pool_layer(slice_key):
    parts = base_slice_key(slice_key).split(":")
    if len(parts) >= 4 and parts[-3] in {"short", "medium", "long"} and parts[-2] in {"quick", "balanced", "high"}:
        return parts[-3], parts[-2]
    return None, None


class LendingStateStore(ManualProtectionStore):
    def __init__(self, path, clock=time.time):
        self.path = os.path.abspath(path)
        self.clock = clock
        os.makedirs(os.path.dirname(self.path) or os.getcwd(), exist_ok=True)
        self._lock = threading.RLock()
        self.migration_backup_path = None
        self._backup_before_schema_migration()
        self._initialize()

    def _now_ms(self):
        return int(self.clock() * 1000)

    def _backup_before_schema_migration(self):
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
            return
        connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        try:
            row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            family_row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_family'").fetchone()
        except sqlite3.Error:
            return
        finally:
            connection.close()
        version = 0 if row is None else int(row[0])
        family = None if family_row is None else family_row[0]
        self._validate_schema_identity(version, family)
        if version == CURRENT_SCHEMA_VERSION:
            return
        backup_dir = os.path.join(os.path.dirname(self.path), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        backup_path = os.path.join(backup_dir, f"schema-v{version}-{stamp}.sqlite3")
        suffix = 1
        while os.path.exists(backup_path):
            backup_path = os.path.join(backup_dir, f"schema-v{version}-{stamp}-{suffix}.sqlite3")
            suffix += 1
        source = sqlite3.connect(self.path)
        target = sqlite3.connect(backup_path)
        last_progress_at = time.monotonic()
        last_remaining = None
        busy_since = None

        def backup_progress(status, remaining, total):
            nonlocal busy_since, last_progress_at, last_remaining
            now = time.monotonic()
            if status in {sqlite3.SQLITE_OK, sqlite3.SQLITE_DONE} and (
                last_remaining is None or remaining < last_remaining
            ):
                last_progress_at = now
                last_remaining = remaining
            if status in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                busy_since = now if busy_since is None else busy_since
            else:
                busy_since = None
            if now - last_progress_at > 300 or (busy_since is not None and now - busy_since > 10):
                raise StateStoreError(
                    "schema migration backup timed out; source remains unchanged; "
                    f"partial backup={backup_path}; status={status}; remaining={remaining}; total={total}"
                )
        try:
            source.backup(target, pages=1000, progress=backup_progress, sleep=0.05)
        finally:
            target.close()
            source.close()
        self.migration_backup_path = backup_path

    @staticmethod
    def _validate_schema_identity(version, family=None):
        if version == 17:
            raise StateStoreError("schema 17 belongs to legacy V4; use the offline rollback migration")
        if version > CURRENT_SCHEMA_VERSION:
            raise StateStoreError("state database schema is newer than V3.6; use the offline rollback migration")
        if family is not None and family != SCHEMA_FAMILY:
            raise StateStoreError("state database does not belong to the V3 schema family")
        if version >= 18 and family != SCHEMA_FAMILY:
            raise StateStoreError("schema 18+ requires the V3 schema family marker")

    def _connect(self, timeout_seconds=30):
        connection = sqlite3.connect(self.path, timeout=timeout_seconds, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {max(0, int(timeout_seconds * 1000))}")
        return connection

    @contextmanager
    def read_connection(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self, immediate=False, timeout_seconds=None):
        # Background ingestion may yield immediately to the live writer.
        acquired = self._lock.acquire(blocking=timeout_seconds is None)
        if not acquired:
            raise sqlite3.OperationalError("database is busy")
        try:
            connection = self._connect(30 if timeout_seconds is None else timeout_seconds)
            try:
                connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        finally:
            self._lock.release()

    def _initialize(self):
        # Dashboard reads construct stores too. Re-running all migrations under
        # a write lock on every poll can starve the actual lending worker.
        connection = self._connect()
        try:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_meta'"
            ).fetchone()
            if exists:
                ready = dict(connection.execute(
                    "SELECT key,value FROM schema_meta WHERE key IN "
                    "('schema_version','schema_family','v36_initialization_complete','v35_performance_indexes')"
                ).fetchall())
                version = int(ready.get("schema_version", "0"))
                self._validate_schema_identity(version, ready.get("schema_family"))
                if version == CURRENT_SCHEMA_VERSION and ready.get("v36_initialization_complete") == "1":
                    if ready.get("v35_performance_indexes") != "1":
                        self._ensure_performance_indexes()
                    return
                if version in (16, 18, CURRENT_SCHEMA_VERSION):
                    self._migrate_existing_v36()
                    return
        finally:
            connection.close()
        statements = [
            """CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS runtime_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                mode TEXT NOT NULL,
                previous_mode TEXT,
                safe_reason TEXT,
                safe_manual INTEGER NOT NULL DEFAULT 0,
                consistent_syncs INTEGER NOT NULL DEFAULT 0,
                last_consistent_sync_ms INTEGER,
                updated_at_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS recovery_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                active INTEGER NOT NULL DEFAULT 0,
                category TEXT,
                reason TEXT,
                origin_mode TEXT,
                target_mode TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                successful_snapshots INTEGER NOT NULL DEFAULT 0,
                required_snapshots INTEGER NOT NULL DEFAULT 2,
                started_at_ms INTEGER,
                last_probe_at_ms INTEGER,
                next_probe_at_ms INTEGER,
                last_error TEXT,
                heartbeat_at_ms INTEGER,
                manual_required INTEGER NOT NULL DEFAULT 0,
                resume_pending_cycle INTEGER NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS mode_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_mode TEXT,
                to_mode TEXT NOT NULL,
                reason TEXT,
                created_at_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS strategy_versions (
                version_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                policy_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                activated_at_ms INTEGER
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS one_active_strategy
                ON strategy_versions(status) WHERE status = 'ACTIVE'""",
            """CREATE UNIQUE INDEX IF NOT EXISTS one_pending_strategy
                ON strategy_versions(status) WHERE status = 'PENDING'""",
            """CREATE UNIQUE INDEX IF NOT EXISTS one_draft_strategy
                ON strategy_versions(status) WHERE status = 'DRAFT'""",
            """CREATE TABLE IF NOT EXISTS order_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT NOT NULL UNIQUE,
                currency TEXT NOT NULL,
                slice_key TEXT NOT NULL,
                pool TEXT NOT NULL,
                layer TEXT NOT NULL,
                amount TEXT NOT NULL,
                submitted_rate TEXT NOT NULL,
                effective_rate TEXT NOT NULL,
                period INTEGER NOT NULL,
                offer_type TEXT NOT NULL,
                display_type TEXT,
                flags INTEGER NOT NULL DEFAULT 0,
                strategy_version TEXT NOT NULL,
                plan_hash TEXT,
                state TEXT NOT NULL,
                exchange_offer_id INTEGER UNIQUE,
                error_text TEXT,
                pricing_curve_version TEXT NOT NULL DEFAULT 'LEGACY',
                fixed_landing_rate TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS order_intents_state_idx ON order_intents(state)""",
            """CREATE TABLE IF NOT EXISTS offers (
                offer_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                amount TEXT NOT NULL,
                amount_original TEXT,
                rate TEXT NOT NULL,
                rate_real TEXT,
                period INTEGER NOT NULL,
                offer_type TEXT NOT NULL,
                display_type TEXT,
                flags INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                managed INTEGER NOT NULL DEFAULT 0,
                pool TEXT,
                layer TEXT,
                plan_hash TEXT,
                mts_created INTEGER,
                mts_updated INTEGER,
                last_seen_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS credits (
                credit_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                amount TEXT NOT NULL,
                rate TEXT NOT NULL,
                rate_real TEXT,
                period INTEGER NOT NULL,
                rate_type TEXT,
                display_type TEXT,
                hidden INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                managed INTEGER NOT NULL DEFAULT 0,
                pool TEXT,
                layer TEXT,
                offer_id INTEGER,
                mts_opening INTEGER,
                mts_updated INTEGER,
                last_seen_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS funding_trades (
                trade_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                offer_id INTEGER,
                amount TEXT NOT NULL,
                rate TEXT NOT NULL,
                period INTEGER NOT NULL,
                mts INTEGER NOT NULL,
                managed INTEGER NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS credit_closures (
                credit_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                amount TEXT NOT NULL,
                rate TEXT NOT NULL,
                period INTEGER NOT NULL,
                rate_type TEXT,
                hidden INTEGER NOT NULL DEFAULT 0,
                managed INTEGER NOT NULL DEFAULT 0,
                pool TEXT,
                opened_at_ms INTEGER,
                closed_at_ms INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS credit_closures_time_idx ON credit_closures(closed_at_ms)""",
            """CREATE TABLE IF NOT EXISTS offer_history (
                offer_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                amount TEXT NOT NULL,
                amount_original TEXT,
                rate TEXT NOT NULL,
                rate_real TEXT,
                period INTEGER NOT NULL,
                offer_type TEXT NOT NULL,
                flags INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                mts_created INTEGER,
                mts_updated INTEGER,
                managed INTEGER NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS credit_history (
                credit_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                amount TEXT NOT NULL,
                rate TEXT NOT NULL,
                rate_real TEXT,
                period INTEGER NOT NULL,
                rate_type TEXT,
                hidden INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                mts_opening INTEGER,
                mts_updated INTEGER,
                managed INTEGER NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS rate_floor_violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                credit_id INTEGER NOT NULL,
                pool TEXT NOT NULL,
                floor_rate TEXT NOT NULL,
                observed_rate TEXT NOT NULL,
                started_at_ms INTEGER NOT NULL,
                ended_at_ms INTEGER
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS one_open_floor_violation
                ON rate_floor_violations(credit_id) WHERE ended_at_ms IS NULL""",
            """CREATE TABLE IF NOT EXISTS ledger_entries (
                ledger_id INTEGER PRIMARY KEY,
                currency TEXT NOT NULL,
                wallet TEXT,
                amount TEXT NOT NULL,
                balance TEXT,
                description TEXT,
                category INTEGER,
                mts INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS ledger_entries_category_mts_idx
                ON ledger_entries(category, mts)""",
            """CREATE TABLE IF NOT EXISTS income_sync_state (
                currency TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                next_end_ms INTEGER,
                earliest_mts INTEGER,
                last_success_ms INTEGER,
                completed_at_ms INTEGER,
                error TEXT,
                updated_at_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS market_trades (
                trade_id TEXT PRIMARY KEY,
                mts INTEGER NOT NULL,
                amount TEXT NOT NULL,
                rate TEXT NOT NULL,
                period INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS market_trades_mts_idx ON market_trades(mts)""",
            """CREATE TABLE IF NOT EXISTS market_bars (
                interval_name TEXT NOT NULL,
                bucket_ms INTEGER NOT NULL,
                weighted_median_rate TEXT NOT NULL,
                q25_rate TEXT NOT NULL,
                q75_rate TEXT NOT NULL,
                volume TEXT NOT NULL,
                trade_count INTEGER NOT NULL,
                PRIMARY KEY (interval_name, bucket_ms)
            )""",
            """CREATE TABLE IF NOT EXISTS reprice_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_id INTEGER,
                intent_id INTEGER,
                reason TEXT NOT NULL,
                old_rate TEXT,
                new_rate TEXT,
                strategy_version TEXT,
                plan_hash TEXT,
                display_type TEXT,
                chain_key TEXT,
                stage INTEGER,
                benchmark_rate TEXT,
                floor_rate TEXT,
                created_at_ms INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS reprice_events_time_idx ON reprice_events(created_at_ms)""",
            """CREATE TABLE IF NOT EXISTS reprice_chains (
                chain_key TEXT PRIMARY KEY,
                strategy_version TEXT NOT NULL,
                base_slice_key TEXT NOT NULL,
                pool TEXT NOT NULL,
                layer TEXT NOT NULL,
                origin_rate TEXT NOT NULL,
                started_at_ms INTEGER NOT NULL,
                current_stage INTEGER NOT NULL DEFAULT 0,
                current_offer_id INTEGER,
                last_reprice_at_ms INTEGER,
                pending_action TEXT,
                pending_target_rate TEXT,
                pending_source_offer_id INTEGER,
                market_anchor_rate TEXT,
                fixed_landing_rate TEXT,
                pricing_curve_version TEXT NOT NULL DEFAULT 'LEGACY',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                updated_at_ms INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS reprice_chains_offer_idx
                ON reprice_chains(current_offer_id)""",
            """CREATE TABLE IF NOT EXISTS strategy_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                from_version TEXT,
                to_version TEXT,
                plan_hash TEXT,
                reason TEXT,
                created_at_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS account_samples (
                mts INTEGER PRIMARY KEY,
                total_principal TEXT NOT NULL,
                wallet_available TEXT NOT NULL,
                open_offers TEXT NOT NULL,
                active_credits TEXT NOT NULL,
                net_interest_total TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS funding_stats (
                mts INTEGER PRIMARY KEY,
                frr_daily_rate TEXT,
                utilization TEXT,
                payload_json TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS book_snapshots (
                mts INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                book_json TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS legacy_migrations (
                migration_id TEXT PRIMARY KEY,
                source_path TEXT,
                result_json TEXT NOT NULL,
                completed_at_ms INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS strategy_rollouts (
                candidate_version TEXT PRIMARY KEY,
                candidate_share INTEGER NOT NULL,
                stage_started_at_ms INTEGER NOT NULL,
                status TEXT NOT NULL,
                last_reason TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS ownership_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_id INTEGER,
                credit_id INTEGER,
                event_type TEXT NOT NULL,
                details_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            )""",
            """CREATE INDEX IF NOT EXISTS ownership_events_time_idx
                ON ownership_events(created_at_ms)""",
            """CREATE TABLE IF NOT EXISTS period_selection_state (
                strategy_version TEXT NOT NULL,
                pool TEXT NOT NULL,
                selected_period INTEGER,
                selected_since_ms INTEGER,
                scores_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (strategy_version, pool)
            )""",
            """CREATE TABLE IF NOT EXISTS demand_confirmation_state (
                strategy_version TEXT NOT NULL,
                scope TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                consecutive_cycles INTEGER NOT NULL DEFAULT 0,
                confirmed INTEGER NOT NULL DEFAULT 0,
                last_cycle INTEGER,
                last_share TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (strategy_version, scope, signal_key)
            )""",
            """CREATE TABLE IF NOT EXISTS consolidation_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                state TEXT NOT NULL DEFAULT 'IDLE',
                offer_id INTEGER,
                captured_wallet TEXT,
                captured_offer_amount TEXT,
                target_period INTEGER,
                strategy_version TEXT,
                started_at_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS external_takeover_state (
                offer_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                snapshot_digest TEXT,
                first_seen_ms INTEGER NOT NULL,
                confirmed_at_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
            )""",
        ]
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
        finally:
            connection.close()
        with self.transaction(immediate=True) as connection:
            for statement in statements:
                connection.execute(statement)
            self._migrate_v3_audit_columns(connection)
            self._migrate_v4_columns(connection)
            self._migrate_v5_columns(connection)
            self._migrate_v6_reprice_columns(connection)
            self._migrate_v7_reprice_chain_columns(connection)
            self._migrate_v8_recovery_columns(connection)
            self._migrate_v9_period_selection_columns(connection)
            self._migrate_v10_recovery_state(connection)
            self._migrate_v11_allocation_state(connection)
            self._migrate_v12_single_paused_mode(connection)
            self._migrate_v13_unattended_recovery(connection)
            self._migrate_v14_pricing_curve_columns(connection)
            self._migrate_v15_exact_reprice_replacements(connection)
            self._migrate_v16_fixed_landing_rate(connection)
            self._migrate_v18_market_columns(connection)
            self._migrate_v19_manual_protection(connection)
            connection.execute(
                """INSERT INTO schema_meta(key, value) VALUES('schema_version', '19')
                   ON CONFLICT(key) DO UPDATE SET value='19'"""
            )
            self._initialize_v35_release_boundary(connection, self._now_ms())
            connection.execute(
                """INSERT OR IGNORE INTO income_sync_state(
                    currency, status, updated_at_ms
                ) VALUES('USD', 'PENDING', ?)""",
                (self._now_ms(),),
            )
            connection.execute(
                """INSERT OR IGNORE INTO consolidation_state(singleton, state, updated_at_ms)
                   VALUES(1, 'IDLE', ?)""",
                (self._now_ms(),),
            )
            connection.execute(
                """INSERT OR IGNORE INTO recovery_state(
                       singleton, active, required_snapshots, manual_required, resume_pending_cycle
                   ) VALUES(1, 0, 2, 0, 0)"""
            )
            connection.execute(
                """INSERT OR IGNORE INTO runtime_state(
                    singleton, mode, previous_mode, updated_at_ms
                ) VALUES(1, 'PAUSED', NULL, ?)""",
                (self._now_ms(),),
            )
            connection.execute("INSERT OR REPLACE INTO schema_meta VALUES('v35_initialization_complete','1')")
            connection.execute("INSERT OR REPLACE INTO schema_meta VALUES('schema_family','v3')")
            connection.execute("INSERT OR REPLACE INTO schema_meta VALUES('v36_initialization_complete','1')")
        self._ensure_performance_indexes()

    def _migrate_existing_v36(self):
        # Schema 16 already completed the V3.5 migrations.  Re-running them can
        # abandon pending historical chains, so upgrade only the new columns.
        with self.transaction(immediate=True) as connection:
            meta = dict(connection.execute("SELECT key,value FROM schema_meta"))
            version = int(meta.get("schema_version", "0"))
            self._validate_schema_identity(version, meta.get("schema_family"))
            if version == CURRENT_SCHEMA_VERSION and meta.get("v36_initialization_complete") == "1":
                return
            self._migrate_v18_market_columns(connection)
            self._migrate_v19_manual_protection(connection)
            if "release_0.3.5_activated_at_ms" not in meta or (
                "release_0.3.5_boundary_evidence_at_ms" not in meta
                and connection.execute(
                    "SELECT 1 FROM order_intents WHERE pricing_curve_version IN "
                    "('EXACT_TERM_EXPLORATION_V2','EXACT_TERM_EXPLORATION_V3','EXACT_TERM_EXPLORATION_V4') LIMIT 1"
                ).fetchone() is not None
            ):
                self._initialize_v35_release_boundary(connection, self._now_ms())
            connection.execute("CREATE INDEX IF NOT EXISTS market_bars_bucket_idx ON market_bars(bucket_ms)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS funding_trades_currency_managed_mts_idx "
                "ON funding_trades(currency, managed, mts)"
            )
            for key, value in (
                ("schema_version", "19"), ("schema_family", SCHEMA_FAMILY),
                ("v36_initialization_complete", "1"), ("v35_performance_indexes", "1"),
            ):
                connection.execute("INSERT OR REPLACE INTO schema_meta(key,value) VALUES(?,?)", (key, value))

    @staticmethod
    def _migrate_v18_market_columns(connection):
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(market_trades)")}
        if "raw_signed_amount" not in existing:
            connection.execute("ALTER TABLE market_trades ADD COLUMN raw_signed_amount TEXT")
        if "source_sequence" not in existing:
            connection.execute("ALTER TABLE market_trades ADD COLUMN source_sequence INTEGER NOT NULL DEFAULT 0")
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(book_snapshots)")}
        if "observed_at_ms" not in existing:
            connection.execute("ALTER TABLE book_snapshots ADD COLUMN observed_at_ms INTEGER")
        connection.execute("CREATE INDEX IF NOT EXISTS market_trades_mts_id_idx ON market_trades(mts, trade_id)")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS book_snapshots_visible_idx "
            "ON book_snapshots(COALESCE(observed_at_ms,mts+60000),mts)"
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS market_dirty_minutes (
                minute_ms INTEGER PRIMARY KEY,
                source_sequence INTEGER NOT NULL
            )"""
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS market_dirty_sequence_idx ON market_dirty_minutes(source_sequence)"
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS market_coverage_intervals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ms INTEGER NOT NULL,
                end_ms INTEGER NOT NULL CHECK(end_ms>=start_ms),
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                details_json TEXT,
                recorded_at_ms INTEGER NOT NULL
            )"""
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS market_coverage_window_idx ON market_coverage_intervals(start_ms,end_ms)"
        )
        connection.execute("INSERT OR IGNORE INTO schema_meta(key,value) VALUES('market_source_sequence','0')")

    def _ensure_performance_indexes(self):
        # The existing market_bars primary key starts with interval_name, so
        # retention by bucket_ms alone otherwise scans every retained row.
        # Record completion to keep subsequent Dashboard opens read-only.
        with self.transaction(immediate=True) as connection:
            connection.execute("CREATE INDEX IF NOT EXISTS market_bars_bucket_idx ON market_bars(bucket_ms)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS funding_trades_currency_managed_mts_idx "
                "ON funding_trades(currency, managed, mts)"
            )
            connection.execute("INSERT OR REPLACE INTO schema_meta VALUES('v35_performance_indexes','1')")

    @staticmethod
    def _initialize_v35_release_boundary(connection, now_ms):
        """Recover the V3.5 cutover from the first exact-term pricing session.

        V3.5 groups the exact-term exploration releases into one reporting
        generation.  Existing installations can therefore recover the real
        cutover from durable order and mode events instead of using the later
        GitHub publication time.  Fresh installations keep their first V3.5
        startup as the boundary.
        """

        first_curve = connection.execute(
            """SELECT MIN(created_at_ms) AS first_ms FROM order_intents
               WHERE pricing_curve_version IN (
                   'EXACT_TERM_EXPLORATION_V2', 'EXACT_TERM_EXPLORATION_V3',
                   'EXACT_TERM_EXPLORATION_V4'
               )"""
        ).fetchone()["first_ms"]
        source = "FIRST_V35_START"
        evidence_at = None
        candidate = int(now_ms)
        if first_curve is not None:
            evidence_at = int(first_curve)
            live_event = connection.execute(
                """SELECT MAX(created_at_ms) AS live_ms FROM mode_events
                   WHERE to_mode='LIVE' AND created_at_ms<=?""",
                (evidence_at,),
            ).fetchone()["live_ms"]
            if live_event is not None:
                candidate = int(live_event)
                source = "LIVE_SESSION_BEFORE_EXACT_TERM_EXPLORATION"
            else:
                candidate = evidence_at
                source = "FIRST_EXACT_TERM_EXPLORATION_ORDER"

        current = connection.execute(
            "SELECT value FROM schema_meta WHERE key='release_0.3.5_activated_at_ms'"
        ).fetchone()
        current_ms = None if current is None else int(current["value"])
        if current_ms is None or (first_curve is not None and current_ms > candidate):
            connection.execute(
                """INSERT INTO schema_meta(key, value)
                   VALUES('release_0.3.5_activated_at_ms', ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (str(candidate),),
            )
            connection.execute(
                """INSERT INTO schema_meta(key, value)
                   VALUES('release_0.3.5_boundary_source', ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (source,),
            )
        else:
            connection.execute(
                """INSERT OR IGNORE INTO schema_meta(key, value)
                   VALUES('release_0.3.5_boundary_source', 'FIRST_V35_START')"""
            )
        if evidence_at is not None:
            connection.execute(
                """INSERT INTO schema_meta(key, value)
                   VALUES('release_0.3.5_boundary_evidence_at_ms', ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (str(evidence_at),),
            )

    @staticmethod
    def _migrate_v9_period_selection_columns(connection):
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(period_selection_state)")}
        for name, definition in {
            "challenger_period": "INTEGER",
            "challenger_since_ms": "INTEGER",
        }.items():
            if name not in existing:
                connection.execute(f"ALTER TABLE period_selection_state ADD COLUMN {name} {definition}")

    @staticmethod
    def _migrate_v10_recovery_state(connection):
        connection.execute(
            """CREATE TABLE IF NOT EXISTS recovery_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                active INTEGER NOT NULL DEFAULT 0,
                category TEXT,
                reason TEXT,
                origin_mode TEXT,
                target_mode TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                successful_snapshots INTEGER NOT NULL DEFAULT 0,
                required_snapshots INTEGER NOT NULL DEFAULT 2,
                started_at_ms INTEGER,
                last_probe_at_ms INTEGER,
                next_probe_at_ms INTEGER,
                last_error TEXT,
                heartbeat_at_ms INTEGER,
                manual_required INTEGER NOT NULL DEFAULT 0,
                resume_pending_cycle INTEGER NOT NULL DEFAULT 0
            )"""
        )

    @staticmethod
    def _migrate_v12_single_paused_mode(connection):
        # SAFE is folded into PAUSED.  The durable reason/manual flag still
        # carries the safety lock, so migration changes presentation without
        # weakening write protection.
        connection.execute("UPDATE runtime_state SET mode='PAUSED' WHERE mode='SAFE'")
        connection.execute("UPDATE runtime_state SET previous_mode='PAUSED' WHERE previous_mode='SAFE'")
        connection.execute("UPDATE recovery_state SET origin_mode='PAUSED' WHERE origin_mode='SAFE'")
        connection.execute("UPDATE recovery_state SET target_mode='PAUSED' WHERE target_mode='SAFE'")

    @staticmethod
    def _migrate_v13_unattended_recovery(connection):
        # Only an explicit Dashboard pause stops recovery. Older versions used
        # these flags for non-manual failures; release them into the read-only
        # recovery loop without allowing any speculative write.
        connection.execute("UPDATE runtime_state SET safe_manual=0")
        connection.execute(
            """UPDATE recovery_state
               SET manual_required=0,
                   next_probe_at_ms=COALESCE(next_probe_at_ms, started_at_ms)
               WHERE active=1"""
        )

    @staticmethod
    def _migrate_v14_pricing_curve_columns(connection):
        for table in ("order_intents", "reprice_chains"):
            existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
            if "pricing_curve_version" not in existing:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN pricing_curve_version TEXT NOT NULL DEFAULT 'LEGACY'"
                )

    @staticmethod
    def _migrate_v15_exact_reprice_replacements(connection):
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(reprice_chains)")}
        if "pending_source_offer_id" not in existing:
            connection.execute("ALTER TABLE reprice_chains ADD COLUMN pending_source_offer_id INTEGER")
        # Older pending rows do not contain enough immutable order shape to be
        # safely resubmitted. Preserve them for audit but never let future cash
        # bind to a different term or layer.
        connection.execute(
            """UPDATE reprice_chains
               SET status='ABANDONED_LEGACY', pending_action=NULL,
                   pending_target_rate=NULL, updated_at_ms=updated_at_ms
               WHERE current_offer_id IS NULL AND pending_action IS NOT NULL
                 AND pending_source_offer_id IS NULL"""
        )

    @staticmethod
    def _migrate_v16_fixed_landing_rate(connection):
        for table in ("order_intents", "reprice_chains"):
            existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
            if "fixed_landing_rate" not in existing:
                connection.execute(f"ALTER TABLE {table} ADD COLUMN fixed_landing_rate TEXT")
        connection.execute(
            """UPDATE reprice_chains
               SET fixed_landing_rate=market_anchor_rate
               WHERE fixed_landing_rate IS NULL
                 AND pricing_curve_version='EXACT_TERM_EXPLORATION_V2'
                 AND market_anchor_rate IS NOT NULL"""
        )
        connection.execute(
            """UPDATE reprice_chains
               SET status='ABANDONED_LEGACY', pending_action=NULL,
                   pending_target_rate=NULL, pending_source_offer_id=NULL
               WHERE current_offer_id IS NULL
                 AND pending_action IN ('MARKET_RISE', 'QUICK_PERIOD_CORRECTION')"""
        )

    @staticmethod
    def _migrate_v3_audit_columns(connection):
        required = {
            "order_intents": {
                "display_type": "TEXT",
                "plan_hash": "TEXT",
            },
            "offers": {
                "display_type": "TEXT",
                "plan_hash": "TEXT",
            },
            "reprice_events": {
                "strategy_version": "TEXT",
                "plan_hash": "TEXT",
                "display_type": "TEXT",
            },
            "credits": {
                "layer": "TEXT",
                "display_type": "TEXT",
            },
        }
        for table, columns in required.items():
            existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
            for name, definition in columns.items():
                if name not in existing:
                    connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        connection.execute(
            """UPDATE order_intents SET display_type = CASE
                   WHEN offer_type = 'LIMIT' THEN 'LIMIT'
                   WHEN offer_type = 'FRRDELTAFIX' THEN 'FRR_DELTA_FIXED'
                   WHEN offer_type = 'FRRDELTAVAR' AND CAST(submitted_rate AS REAL) = 0 THEN 'FRR'
                   WHEN offer_type = 'FRRDELTAVAR' THEN 'FRR_DELTA_VARIABLE'
                   ELSE offer_type END
               WHERE display_type IS NULL OR display_type = ''"""
        )

    @staticmethod
    def _migrate_v4_columns(connection):
        required = {
            "order_intents": {
                "write_phase": "TEXT NOT NULL DEFAULT 'NOT_SENT'",
                "resolution": "TEXT",
                "strategy_variant": "TEXT NOT NULL DEFAULT 'baseline'",
                "request_started_at_ms": "INTEGER",
                "resolved_at_ms": "INTEGER",
            },
            "offers": {
                "strategy_variant": "TEXT NOT NULL DEFAULT 'baseline'",
            },
            "credits": {
                "strategy_variant": "TEXT NOT NULL DEFAULT 'baseline'",
            },
        }
        for table, columns in required.items():
            existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
            for name, definition in columns.items():
                if name not in existing:
                    connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        connection.execute(
            """UPDATE offers SET display_type = CASE
                   WHEN offer_type = 'LIMIT' THEN 'LIMIT'
                   WHEN offer_type = 'FRRDELTAFIX' THEN 'FRR_DELTA_FIXED'
                   WHEN offer_type = 'FRRDELTAVAR' AND CAST(rate AS REAL) = 0 THEN 'FRR'
                   WHEN offer_type = 'FRRDELTAVAR' THEN 'FRR_DELTA_VARIABLE'
                   ELSE offer_type END
               WHERE display_type IS NULL OR display_type = ''"""
        )

    @staticmethod
    def _migrate_v5_columns(connection):
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(credits)")}
        if "attribution_state" not in existing:
            connection.execute("ALTER TABLE credits ADD COLUMN attribution_state TEXT NOT NULL DEFAULT 'EXTERNAL'")
        connection.execute(
            """UPDATE credits SET attribution_state = CASE
                   WHEN managed = 1 THEN 'MANAGED'
                   WHEN attribution_state IS NULL OR attribution_state = '' THEN 'EXTERNAL'
                   ELSE attribution_state END"""
        )
        connection.execute(
            """UPDATE credits SET display_type = COALESCE(
                   (SELECT intents.display_type FROM order_intents AS intents
                    WHERE intents.exchange_offer_id = credits.offer_id LIMIT 1),
                   CASE UPPER(COALESCE(rate_type, ''))
                       WHEN 'FIXED' THEN 'LIMIT'
                       WHEN 'LIMIT' THEN 'LIMIT'
                       WHEN 'VAR' THEN 'VARIABLE_UNKNOWN'
                       WHEN 'VARIABLE' THEN 'VARIABLE_UNKNOWN'
                       WHEN 'FRRDELTAVAR' THEN 'VARIABLE_UNKNOWN'
                       ELSE NULL END)
               WHERE display_type IS NULL OR display_type = ''"""
        )

    @staticmethod
    def _migrate_v6_reprice_columns(connection):
        required = {
            "chain_key": "TEXT",
            "stage": "INTEGER",
            "benchmark_rate": "TEXT",
            "floor_rate": "TEXT",
        }
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(reprice_events)")}
        for name, definition in required.items():
            if name not in existing:
                connection.execute(f"ALTER TABLE reprice_events ADD COLUMN {name} {definition}")

    @staticmethod
    def _migrate_v7_reprice_chain_columns(connection):
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(reprice_chains)")}
        if "market_anchor_rate" not in existing:
            connection.execute("ALTER TABLE reprice_chains ADD COLUMN market_anchor_rate TEXT")

    @staticmethod
    def _migrate_v8_recovery_columns(connection):
        existing = {row["name"] for row in connection.execute("PRAGMA table_info(offer_history)")}
        if "amount_original" not in existing:
            connection.execute("ALTER TABLE offer_history ADD COLUMN amount_original TEXT")

    def runtime(self):
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM runtime_state WHERE singleton = 1").fetchone()
        return dict(row)

    @staticmethod
    def _recovery_payload(row):
        value = dict(row)
        return {
            "active": bool(value.get("active")),
            "category": value.get("category"),
            "reason": value.get("reason"),
            "originMode": value.get("origin_mode"),
            "targetMode": value.get("target_mode"),
            "attempts": int(value.get("attempts") or 0),
            "successfulSnapshots": int(value.get("successful_snapshots") or 0),
            "requiredSnapshots": int(value.get("required_snapshots") or RECOVERY_REQUIRED_SNAPSHOTS),
            "startedAt": value.get("started_at_ms"),
            "lastProbeAt": value.get("last_probe_at_ms"),
            "nextProbeAt": value.get("next_probe_at_ms"),
            "lastError": value.get("last_error"),
            "heartbeatAt": value.get("heartbeat_at_ms"),
            "manualRequired": bool(value.get("manual_required")),
        }

    def recovery_status(self):
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
        return self._recovery_payload(row)

    def touch_heartbeat(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE recovery_state SET heartbeat_at_ms=? WHERE singleton=1",
                (now,),
            )
        return now

    @staticmethod
    def _migrate_v11_allocation_state(connection):
        # Tables are created in the normal initialization list. Keeping an
        # explicit migration hook documents the durable Schema 11 boundary.
        connection.execute(
            "CREATE INDEX IF NOT EXISTS demand_confirmation_updated_idx ON demand_confirmation_state(updated_at_ms)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS external_takeover_state_idx ON external_takeover_state(state, updated_at_ms)"
        )

    def begin_recovery(
        self,
        category,
        reason,
        *,
        origin_mode=None,
        target_mode=None,
        manual_required=False,
        now_ms=None,
    ):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            runtime = connection.execute("SELECT * FROM runtime_state WHERE singleton=1").fetchone()
            current = connection.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
            origin = str(origin_mode or runtime["previous_mode"] or runtime["mode"] or "PAUSED").upper()
            target = str(target_mode or ("LIVE" if origin == "LIVE" else origin)).upper()
            if target not in {"LIVE", "PAUSED", "REPLAY"}:
                target = "PAUSED"
            if current["active"]:
                # A recovery episode owns one immutable resume destination.
                # Secondary failures (for example stale market data after a
                # network timeout) may update diagnostics and backoff, but must
                # never downgrade an authorized LIVE recovery to PAUSED.
                if current["origin_mode"] in {"LIVE", "PAUSED", "REPLAY"}:
                    origin = current["origin_mode"]
                if current["target_mode"] in {"LIVE", "PAUSED", "REPLAY"}:
                    target = current["target_mode"]
                manual_required = bool(manual_required or current["manual_required"])
                started = current["started_at_ms"]
                attempts = int(current["attempts"] or 0)
            else:
                started = now
                attempts = 0
            connection.execute(
                """UPDATE recovery_state SET active=1, category=?, reason=?, origin_mode=?,
                   target_mode=?, attempts=?, successful_snapshots=0, required_snapshots=?,
                   started_at_ms=?, last_probe_at_ms=?, next_probe_at_ms=?, last_error=?,
                   manual_required=?, resume_pending_cycle=0 WHERE singleton=1""",
                (
                    str(category),
                    str(reason),
                    origin,
                    target,
                    attempts,
                    RECOVERY_REQUIRED_SNAPSHOTS,
                    started,
                    None,
                    None if manual_required else now + recovery_delay_seconds(attempts) * 1000,
                    str(reason),
                    int(bool(manual_required)),
                ),
            )
        return self.recovery_status()

    def record_recovery_failure(self, error_text, category=None, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
            if not row["active"] or row["manual_required"]:
                return self._recovery_payload(row)
            attempts = int(row["attempts"] or 0) + 1
            delay = recovery_delay_seconds(attempts - 1)
            connection.execute(
                """UPDATE recovery_state SET category=COALESCE(?, category), attempts=?,
                   successful_snapshots=0, last_probe_at_ms=?, next_probe_at_ms=?,
                   last_error=? WHERE singleton=1""",
                (category, attempts, now, now + delay * 1000, str(error_text)),
            )
        return self.recovery_status()

    def recovery_probe_due(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
        return bool(
            row["active"]
            and not row["manual_required"]
            and (row["next_probe_at_ms"] is None or now >= int(row["next_probe_at_ms"]))
        )

    def record_recovery_snapshot(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        resumed = False
        with self.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
            if not row["active"] or row["manual_required"]:
                return {"resumed": False, "recovery": self._recovery_payload(row)}
            last = row["last_probe_at_ms"]
            count = int(row["successful_snapshots"] or 0)
            if last is None or now - int(last) >= RECOVERY_MINIMUM_GAP_MS:
                count += 1
                connection.execute(
                    """UPDATE recovery_state SET successful_snapshots=?, last_probe_at_ms=?,
                       next_probe_at_ms=?, last_error=NULL WHERE singleton=1""",
                    (count, now, now + RECOVERY_MINIMUM_GAP_MS),
                )
            unresolved = connection.execute(
                "SELECT COUNT(*) FROM order_intents WHERE state IN ('PLANNED','SUBMITTING','AMBIGUOUS')"
            ).fetchone()[0]
            if count >= int(row["required_snapshots"] or RECOVERY_REQUIRED_SNAPSHOTS) and not unresolved:
                target = row["target_mode"] if row["target_mode"] in {"LIVE", "PAUSED", "REPLAY"} else "PAUSED"
                current_mode = connection.execute("SELECT mode FROM runtime_state WHERE singleton=1").fetchone()[0]
                connection.execute(
                    """UPDATE runtime_state SET mode=?, previous_mode=NULL, safe_reason=NULL,
                       safe_manual=0, consistent_syncs=0, last_consistent_sync_ms=NULL,
                       updated_at_ms=? WHERE singleton=1""",
                    (target, now),
                )
                connection.execute(
                    "INSERT INTO mode_events(from_mode,to_mode,reason,created_at_ms) VALUES(?,?,?,?)",
                    (current_mode, target, "automatic recovery confirmed", now),
                )
                connection.execute(
                    """UPDATE recovery_state SET active=0, successful_snapshots=0,
                       next_probe_at_ms=NULL, manual_required=0, resume_pending_cycle=1,
                       last_error=NULL WHERE singleton=1"""
                )
                resumed = True
            final = connection.execute("SELECT * FROM recovery_state WHERE singleton=1").fetchone()
        return {"resumed": resumed, "recovery": self._recovery_payload(final)}

    def consume_resume_barrier(self):
        with self.transaction(immediate=True) as connection:
            row = connection.execute("SELECT resume_pending_cycle FROM recovery_state WHERE singleton=1").fetchone()
            pending = bool(row[0])
            if pending:
                connection.execute("UPDATE recovery_state SET resume_pending_cycle=0 WHERE singleton=1")
        return pending

    def clear_recovery(self):
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE recovery_state SET active=0, category=NULL, reason=NULL,
                   origin_mode=NULL, target_mode=NULL, attempts=0, successful_snapshots=0,
                   started_at_ms=NULL, last_probe_at_ms=NULL, next_probe_at_ms=NULL,
                   last_error=NULL, manual_required=0, resume_pending_cycle=0 WHERE singleton=1"""
            )
        return self.recovery_status()

    def latest_mode_event(self):
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM mode_events ORDER BY created_at_ms DESC, id DESC LIMIT 1"
            ).fetchone()
        return None if row is None else dict(row)

    def set_mode(self, mode, reason=""):
        mode = str(mode).upper()
        if mode not in RUNTIME_MODES:
            raise StateStoreError(f"unsupported runtime mode: {mode}")
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            current = connection.execute("SELECT * FROM runtime_state WHERE singleton = 1").fetchone()
            from_mode = current["mode"]
            if current["safe_manual"]:
                raise StateStoreError("protected PAUSED must be resolved through the ambiguous intent workflow")
            if from_mode == "LIVE" and mode == "REPLAY":
                raise StateStoreError("LIVE must transition to PAUSED before REPLAY")
            previous = current["previous_mode"]
            connection.execute(
                """UPDATE runtime_state
                   SET mode = ?, previous_mode = ?, safe_reason = ?, safe_manual = 0,
                       consistent_syncs = 0, last_consistent_sync_ms = NULL, updated_at_ms = ?
                   WHERE singleton = 1""",
                (mode, previous, None, now),
            )
            connection.execute(
                "INSERT INTO mode_events(from_mode, to_mode, reason, created_at_ms) VALUES(?, ?, ?, ?)",
                (from_mode, mode, reason, now),
            )
            if str(reason) in {
                "dashboard_stop",
                "dashboard_pause",
                "worker_build_mismatch_stopped",
                "dashboard_started_without_live_process",
            }:
                connection.execute(
                    """UPDATE recovery_state SET active=0, category=NULL, reason=NULL,
                       origin_mode=NULL, target_mode=NULL, attempts=0, successful_snapshots=0,
                       started_at_ms=NULL, last_probe_at_ms=NULL, next_probe_at_ms=NULL,
                       last_error=NULL, manual_required=0, resume_pending_cycle=0 WHERE singleton=1"""
                )
        return self.runtime()

    def enter_protected_pause(self, reason, manual=False):
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            current = connection.execute("SELECT * FROM runtime_state WHERE singleton = 1").fetchone()
            previous = current["previous_mode"] if current["safe_reason"] else current["mode"]
            sticky_manual = bool(manual or current["safe_manual"])
            safe_reason = current["safe_reason"] if sticky_manual and current["safe_manual"] else str(reason)
            connection.execute(
                """UPDATE runtime_state
                   SET mode = 'PAUSED', previous_mode = ?, safe_reason = ?, safe_manual = ?,
                       consistent_syncs = 0, last_consistent_sync_ms = NULL, updated_at_ms = ?
                   WHERE singleton = 1""",
                (previous, safe_reason, int(sticky_manual), now),
            )
            if not current["safe_reason"]:
                connection.execute(
                    "INSERT INTO mode_events(from_mode, to_mode, reason, created_at_ms) VALUES(?, 'PAUSED', ?, ?)",
                    (current["mode"], str(reason), now),
                )
        category = recovery_category_for_reason(reason)
        if category is not None:
            self.begin_recovery(
                category,
                reason,
                origin_mode=previous,
                target_mode=previous,
                manual_required=manual,
                now_ms=now,
            )
        return self.runtime()

    def record_consistent_sync(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        recovery = self.recovery_status()
        if recovery["active"]:
            result = self.record_recovery_snapshot(now)
            return self.runtime() if result["resumed"] else self.runtime()
        with self.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM runtime_state WHERE singleton = 1").fetchone()
            if (
                row["mode"] != "PAUSED"
                or row["safe_manual"]
                or str(row["safe_reason"] or "") not in AUTO_RECOVERABLE_PAUSE_REASONS
            ):
                return dict(row)
            previous_sync = row["last_consistent_sync_ms"]
            count = row["consistent_syncs"]
            if previous_sync is None or now - previous_sync >= 30_000:
                count += 1
                connection.execute(
                    "UPDATE runtime_state SET consistent_syncs = ?, last_consistent_sync_ms = ?, "
                    "updated_at_ms = ? WHERE singleton = 1",
                    (count, now, now),
                )
            if count >= 2:
                target = row["previous_mode"] if row["previous_mode"] in {"LIVE", "PAUSED", "REPLAY"} else "PAUSED"
                connection.execute(
                    """UPDATE runtime_state SET mode = ?, previous_mode = NULL, safe_reason = NULL,
                       safe_manual = 0, consistent_syncs = 0, last_consistent_sync_ms = NULL,
                       updated_at_ms = ? WHERE singleton = 1""",
                    (target, now),
                )
                connection.execute(
                    "INSERT INTO mode_events(from_mode, to_mode, reason, created_at_ms) "
                    "VALUES('PAUSED', ?, 'reconciled', ?)",
                    (target, now),
                )
        return self.runtime()

    @staticmethod
    def _safe_resume_target(row):
        return row["previous_mode"] if row["previous_mode"] in {"LIVE", "PAUSED", "REPLAY"} else "PAUSED"

    @staticmethod
    def _resume_protected_pause(connection, row, now, reason):
        target = LendingStateStore._safe_resume_target(row)
        connection.execute(
            """UPDATE runtime_state SET mode = 'PAUSED', previous_mode = ?, safe_reason = ?,
               safe_manual = 0, consistent_syncs = 0, last_consistent_sync_ms = NULL,
               updated_at_ms = ? WHERE singleton = 1""",
            (target, "POST_AMBIGUOUS_RECONCILIATION", now),
        )
        connection.execute(
            """UPDATE recovery_state SET active=1, category='AMBIGUOUS_WRITE', reason=?,
               origin_mode=?, target_mode=?, attempts=0, successful_snapshots=0,
               required_snapshots=2, started_at_ms=?, last_probe_at_ms=?,
               next_probe_at_ms=?, last_error=NULL, manual_required=0,
               resume_pending_cycle=0 WHERE singleton=1""",
            (str(reason), target, target, now, now, now + RECOVERY_MINIMUM_GAP_MS),
        )
        connection.execute(
            """INSERT INTO mode_events(from_mode, to_mode, reason, created_at_ms)
               VALUES('PAUSED', 'PAUSED', ?, ?)""",
            (f"{reason}; awaiting clean snapshots", now),
        )
        return "PAUSED"

    def observe_ambiguous_cancel(self, active_offer_ids, now_ms=None):
        """Resolve an uncertain cancel from repeated authoritative Offers snapshots.

        Whether the offer is still present or absent is safe: presence means the
        cancel did not take effect and the strategy may retry it; absence means the
        cancel or a fill removed it. Two complete snapshots at least 30 seconds
        apart protect against a transient or eventually-consistent account view.
        """
        now = int(now_ms if now_ms is not None else self._now_ms())
        active_ids = {int(value) for value in active_offer_ids}
        with self.transaction(immediate=True) as connection:
            row = connection.execute("SELECT * FROM runtime_state WHERE singleton = 1").fetchone()
            reason = str(row["safe_reason"] or "")
            if row["mode"] != "PAUSED" or not reason.startswith("AMBIGUOUS_CANCEL:"):
                return dict(row)
            try:
                offer_id = int(reason.split(":", 1)[1])
            except (TypeError, ValueError):
                return dict(row)
            previous_sync = row["last_consistent_sync_ms"]
            count = int(row["consistent_syncs"])
            if previous_sync is None or now - int(previous_sync) >= 30_000:
                count += 1
                connection.execute(
                    """UPDATE runtime_state SET consistent_syncs=?, last_consistent_sync_ms=?,
                       updated_at_ms=? WHERE singleton=1""",
                    (count, now, now),
                )
            if count >= 2:
                present = offer_id in active_ids
                connection.execute(
                    """INSERT INTO ownership_events(
                           offer_id, credit_id, event_type, details_json, created_at_ms
                       ) VALUES(?, NULL, ?, ?, ?)""",
                    (
                        offer_id,
                        "CANCEL_RECONCILED_PRESENT" if present else "CANCEL_RECONCILED_ABSENT",
                        json.dumps(
                            {"authoritativeSnapshots": count, "offerPresent": present},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        now,
                    ),
                )
                self._resume_protected_pause(connection, row, now, "ambiguous cancel reconciled")
        return self.runtime()

    def save_strategy(self, policy_payload, status="PENDING", *, expected_active_id=None):
        status = str(status).upper()
        if status not in {"DRAFT", "PENDING", "ACTIVE", "ARCHIVED"}:
            raise StateStoreError("invalid strategy status")
        serialized = json.dumps(policy_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        version_id = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            if expected_active_id is not None:
                active = self._strategy_row(connection, "ACTIVE")
                if active is None or active["version_id"] != expected_active_id:
                    raise StaleStrategyVersion("ACTIVE changed while saving draft")
            return self._save_strategy(connection, serialized, version_id, status, now)

    @staticmethod
    def _save_strategy(connection, serialized, version_id, status, now):
        existing = connection.execute(
            "SELECT * FROM strategy_versions WHERE version_id = ?", (version_id,)
        ).fetchone()
        if existing is not None:
            existing_status = str(existing["status"]).upper()
            if existing_status == "ACTIVE" and status in {"DRAFT", "PENDING", "ACTIVE"}:
                return version_id
            if existing_status == "PENDING" and status == "DRAFT":
                raise StaleStrategyVersion("A pending strategy cannot be moved back to DRAFT")
            if existing_status == status:
                return version_id
        if status in {"DRAFT", "PENDING", "ACTIVE"}:
            connection.execute("UPDATE strategy_versions SET status = 'ARCHIVED' WHERE status = ?", (status,))
        connection.execute(
            """INSERT INTO strategy_versions(version_id, status, policy_json, created_at_ms, activated_at_ms)
               VALUES(?, ?, ?, ?, ?)
               ON CONFLICT(version_id) DO UPDATE SET status = excluded.status,
                   policy_json = excluded.policy_json,
                   activated_at_ms = CASE WHEN excluded.status = 'ACTIVE'
                       THEN excluded.activated_at_ms ELSE strategy_versions.activated_at_ms END""",
            (version_id, status, serialized, now, now if status == "ACTIVE" else None),
        )
        return version_id

    @staticmethod
    def _strategy_row(connection, status):
        return connection.execute(
            "SELECT * FROM strategy_versions WHERE status = ? ORDER BY created_at_ms DESC LIMIT 1",
            (status,),
        ).fetchone()

    @staticmethod
    def _strategy_record(row):
        if row is None:
            return None
        result = dict(row)
        result["policy"] = json.loads(result.pop("policy_json"))
        return result

    def apply_confirmed_draft(self, draft_id, active_id, *, queue, validated_policy=None):
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            draft = self._strategy_row(connection, "DRAFT")
            active = self._strategy_row(connection, "ACTIVE")
            if (draft is None or draft["version_id"] != draft_id
                    or active is None or active["version_id"] != active_id):
                raise StaleStrategyVersion("ACTIVE or DRAFT changed during confirmation")
            if self._strategy_row(connection, "PENDING") is not None:
                raise StaleStrategyVersion("Another strategy is already pending")
            v36_details = None
            if validated_policy is not None:
                gate = validation_status(self, validated_policy, connection=connection)
                if not gate["approved"]:
                    raise StaleStrategyVersion("V3.6 validation evidence changed during confirmation")
                v36_details = {"strategyVersion": draft_id, "reportDigest": gate["reportDigest"]}
            connection.execute(
                "UPDATE strategy_versions SET status='PENDING' WHERE version_id=?", (draft_id,),
            )
            if queue:
                return self._strategy_record(self._strategy_row(connection, "PENDING"))
            return self._activate_pending(
                connection, draft_id, now, None, "activated while live process stopped", v36_details,
            )

    def install_validated_strategy(self, policy_payload, certificate, expected_active_id):
        serialized = json.dumps(policy_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        version_id = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            active = self._strategy_row(connection, "ACTIVE")
            active_id = None if active is None else active["version_id"]
            if active_id != expected_active_id:
                raise StaleStrategyVersion("ACTIVE changed during evidence validation")
            pending = self._strategy_row(connection, "PENDING")
            if pending is not None and pending["version_id"] != version_id:
                raise StaleStrategyVersion("Another strategy is already pending")
            persist_validation_certificate(connection, certificate)
            details = {"strategyVersion": version_id, "reportDigest": certificate["reportDigest"]}
            if active_id == version_id:
                self._record_v36_activation(connection, details, now)
                return self._strategy_record(active)
            self._save_strategy(connection, serialized, version_id, "PENDING", now)
            return self._activate_pending(
                connection, version_id, now, None, "V3.6 complete evidence gate passed", details,
            )

    def normalize_active_strategy(self, policy_payload, reason="schema normalization"):
        serialized = json.dumps(policy_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        version_id = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            active = connection.execute(
                "SELECT version_id FROM strategy_versions WHERE status='ACTIVE' ORDER BY activated_at_ms DESC LIMIT 1"
            ).fetchone()
            from_version = None if active is None else active["version_id"]
            if from_version == version_id:
                return version_id
            connection.execute("UPDATE strategy_versions SET status='ARCHIVED' WHERE status='ACTIVE'")
            connection.execute(
                """INSERT INTO strategy_versions(version_id, status, policy_json, created_at_ms, activated_at_ms)
                   VALUES(?, 'ACTIVE', ?, ?, ?)
                   ON CONFLICT(version_id) DO UPDATE SET status='ACTIVE', policy_json=excluded.policy_json,
                       activated_at_ms=excluded.activated_at_ms""",
                (version_id, serialized, now, now),
            )
            if from_version is not None:
                connection.execute(
                    """INSERT OR IGNORE INTO reprice_chains(
                           chain_key, strategy_version, base_slice_key, pool, layer,
                           origin_rate, started_at_ms, current_stage, current_offer_id,
                           last_reprice_at_ms, pending_action, pending_target_rate,
                           pending_source_offer_id, market_anchor_rate, fixed_landing_rate,
                           pricing_curve_version, status, updated_at_ms
                       )
                       SELECT
                           ? || CASE
                               WHEN instr(chain_key, '|') > 0
                                   THEN substr(chain_key, instr(chain_key, '|'))
                               ELSE '|' || chain_key
                           END,
                           ?, base_slice_key, pool, layer, origin_rate, started_at_ms,
                           current_stage, current_offer_id, last_reprice_at_ms,
                           pending_action, pending_target_rate, pending_source_offer_id,
                           market_anchor_rate, fixed_landing_rate,
                           pricing_curve_version,
                           status, ?
                       FROM reprice_chains
                       WHERE strategy_version=? AND status='ACTIVE'""",
                    (version_id, version_id, now, from_version),
                )
            connection.execute(
                """INSERT INTO strategy_events(event_type, from_version, to_version, plan_hash, reason, created_at_ms)
                   VALUES('SCHEMA_NORMALIZATION', ?, ?, NULL, ?, ?)""",
                (from_version, version_id, str(reason), now),
            )
        return version_id

    def repair_normalized_reprice_chains(self, strategy_version, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            event = connection.execute(
                """SELECT from_version FROM strategy_events
                   WHERE event_type='SCHEMA_NORMALIZATION' AND to_version=?
                   ORDER BY created_at_ms DESC, id DESC LIMIT 1""",
                (str(strategy_version),),
            ).fetchone()
            if event is None or not event["from_version"]:
                return 0
            before = connection.total_changes
            connection.execute(
                """INSERT OR IGNORE INTO reprice_chains(
                       chain_key, strategy_version, base_slice_key, pool, layer,
                       origin_rate, started_at_ms, current_stage, current_offer_id,
                       last_reprice_at_ms, pending_action, pending_target_rate,
                       pending_source_offer_id, market_anchor_rate, fixed_landing_rate,
                       pricing_curve_version, status, updated_at_ms
                   )
                   SELECT
                       ? || CASE
                           WHEN instr(chain_key, '|') > 0
                               THEN substr(chain_key, instr(chain_key, '|'))
                           ELSE '|' || chain_key
                       END,
                       ?, base_slice_key, pool, layer, origin_rate, started_at_ms,
                       current_stage, current_offer_id, last_reprice_at_ms,
                       pending_action, pending_target_rate, pending_source_offer_id,
                       market_anchor_rate, fixed_landing_rate,
                       pricing_curve_version,
                       status, ?
                   FROM reprice_chains
                   WHERE strategy_version=? AND status='ACTIVE'""",
                (
                    str(strategy_version),
                    str(strategy_version),
                    now,
                    event["from_version"],
                ),
            )
            return connection.total_changes - before

    def strategy(self, status="ACTIVE"):
        with self.read_connection() as connection:
            return self._strategy_record(self._strategy_row(connection, str(status).upper()))

    def discard_strategy(self, status="DRAFT"):
        status = str(status).upper()
        if status not in {"DRAFT", "PENDING"}:
            raise StateStoreError("only DRAFT or PENDING can be discarded")
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE strategy_versions SET status='ARCHIVED' WHERE status=?",
                (status,),
            )
        return self.strategy(status)

    def activate_pending_strategy(self, expected_pending_id, plan_hash=None,
                                  reason="pending adjustments confirmed", v36_details=None):
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            return self._activate_pending(connection, expected_pending_id, now, plan_hash, reason, v36_details)

    def _activate_pending(self, connection, expected_pending_id, now, plan_hash, reason, v36_details):
        pending = self._strategy_row(connection, "PENDING")
        if pending is None or pending["version_id"] != expected_pending_id:
            raise StaleStrategyVersion("PENDING changed before activation")
        active = self._strategy_row(connection, "ACTIVE")
        connection.execute("UPDATE strategy_versions SET status='ARCHIVED' WHERE status='ACTIVE'")
        connection.execute(
            "UPDATE strategy_versions SET status='ACTIVE', activated_at_ms=? WHERE version_id=?",
            (now, expected_pending_id),
        )
        connection.execute(
            """INSERT INTO strategy_events(event_type,from_version,to_version,plan_hash,reason,created_at_ms)
               VALUES('ACTIVATE',?,?,?,?,?)""",
            (None if active is None else active["version_id"], expected_pending_id, plan_hash, reason, now),
        )
        if v36_details is not None:
            self._record_v36_activation(connection, v36_details, now)
        return self._strategy_record(self._strategy_row(connection, "ACTIVE"))

    def reserved_amount(self, currency="USD", connection=None):
        owns_connection = connection is None
        connection = connection or self._connect()
        try:
            placeholders = ",".join("?" for _ in OPEN_INTENT_STATES)
            row = connection.execute(
                f"SELECT amount FROM order_intents WHERE currency = ? AND state IN ({placeholders})",
                (currency.upper(), *sorted(OPEN_INTENT_STATES)),
            ).fetchall()
            return sum((D(item["amount"]) for item in row), D("0"))
        finally:
            if owns_connection:
                connection.close()

    def submission_attempt_count_since(self, since_ms, currency="USD"):
        """Count conservatively reserved submit attempts in a rolling window."""

        with self.read_connection() as connection:
            row = connection.execute(
                """SELECT COUNT(*) AS attempt_count FROM order_intents
                   WHERE currency=? AND created_at_ms>=?""",
                (str(currency).upper(), int(since_ms)),
            ).fetchone()
        return int(row["attempt_count"] or 0)

    def observe_period_selection(
        self,
        strategy_version,
        pool,
        selected_period,
        scores,
        now_ms=None,
        advantage=D("0.20"),
        hold_ms=600_000,
    ):
        """Persist an active term and require a qualified challenger to hold for ten minutes."""

        now = int(now_ms if now_ms is not None else self._now_ms())
        version = str(strategy_version)
        pool = str(pool)
        period = None if selected_period is None else int(selected_period)
        score_rows = scores or []
        serialized = json.dumps(score_rows, ensure_ascii=False, sort_keys=True, default=str)
        with self.transaction(immediate=True) as connection:
            previous = connection.execute(
                """SELECT * FROM period_selection_state
                   WHERE strategy_version=? AND pool=?""",
                (version, pool),
            ).fetchone()
            transition = period_selection_transition(
                previous, period, score_rows, now, advantage=advantage, hold_ms=hold_ms,
            )
            connection.execute(
                """INSERT INTO period_selection_state(
                       strategy_version, pool, selected_period, selected_since_ms,
                       scores_json, updated_at_ms, challenger_period, challenger_since_ms
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(strategy_version, pool) DO UPDATE SET
                       selected_period=excluded.selected_period,
                       selected_since_ms=excluded.selected_since_ms,
                       scores_json=excluded.scores_json,
                       updated_at_ms=excluded.updated_at_ms,
                       challenger_period=excluded.challenger_period,
                       challenger_since_ms=excluded.challenger_since_ms""",
                (
                    version,
                    pool,
                    transition["selected_period"],
                    transition["selected_since_ms"],
                    serialized,
                    now,
                    transition["challenger_period"],
                    transition["challenger_since_ms"],
                ),
            )
        return {
            "strategyVersion": version,
            "pool": pool,
            "selectedPeriod": transition["selected_period"],
            "selectedSinceMs": transition["selected_since_ms"],
            "leaderPeriod": period,
            "challengerPeriod": transition["challenger_period"],
            "challengerSinceMs": transition["challenger_since_ms"],
            "challengerDurationMs": (
                0 if transition["challenger_since_ms"] is None else max(0, now - transition["challenger_since_ms"])
            ),
            "promoted": transition["promoted"],
            "updatedAtMs": now,
        }

    def observe_demand_confirmation(
        self,
        strategy_version,
        scope,
        signal_key,
        share,
        below_threshold,
        now_ms=None,
        required_cycles=2,
    ):
        """Confirm a low-demand signal only across distinct five-minute buckets."""

        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            previous = connection.execute(
                """SELECT * FROM demand_confirmation_state
                   WHERE strategy_version=? AND scope=? AND signal_key=?""",
                (str(strategy_version), str(scope), str(signal_key)),
            ).fetchone()
            transition = demand_confirmation_transition(
                previous, below_threshold, now, required_cycles=required_cycles,
            )
            connection.execute(
                """INSERT INTO demand_confirmation_state(
                       strategy_version, scope, signal_key, consecutive_cycles,
                       confirmed, last_cycle, last_share, updated_at_ms
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(strategy_version, scope, signal_key) DO UPDATE SET
                       consecutive_cycles=excluded.consecutive_cycles,
                       confirmed=excluded.confirmed,
                       last_cycle=excluded.last_cycle,
                       last_share=excluded.last_share,
                       updated_at_ms=excluded.updated_at_ms""",
                (
                    str(strategy_version),
                    str(scope),
                    str(signal_key),
                    transition["consecutive_cycles"],
                    int(transition["confirmed"]),
                    transition["last_cycle"],
                    None if share is None else _decimal_text(share),
                    now,
                ),
            )
        return {"cycles": transition["consecutive_cycles"], "confirmed": transition["confirmed"],
                "cycle": transition["last_cycle"]}

    def consolidation_status(self):
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM consolidation_state WHERE singleton=1").fetchone()
        return {} if row is None else dict(row)

    def begin_consolidation(self, offer_id, wallet, offer_amount, target_period, strategy_version, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            current = connection.execute("SELECT * FROM consolidation_state WHERE singleton=1").fetchone()
            if current is not None and current["state"] != "IDLE":
                return dict(current)
            connection.execute(
                """INSERT INTO consolidation_state(
                       singleton, state, offer_id, captured_wallet, captured_offer_amount,
                       target_period, strategy_version, started_at_ms, updated_at_ms, last_error
                   ) VALUES(1, 'PLANNED', ?, ?, ?, ?, ?, ?, ?, NULL)
                   ON CONFLICT(singleton) DO UPDATE SET
                       state='PLANNED', offer_id=excluded.offer_id,
                       captured_wallet=excluded.captured_wallet,
                       captured_offer_amount=excluded.captured_offer_amount,
                       target_period=excluded.target_period,
                       strategy_version=excluded.strategy_version,
                       started_at_ms=excluded.started_at_ms,
                       updated_at_ms=excluded.updated_at_ms, last_error=NULL""",
                (
                    int(offer_id),
                    _decimal_text(wallet),
                    _decimal_text(offer_amount),
                    int(target_period),
                    str(strategy_version),
                    now,
                    now,
                ),
            )
            row = connection.execute("SELECT * FROM consolidation_state WHERE singleton=1").fetchone()
        return dict(row)

    def update_consolidation(self, state, error=None, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE consolidation_state SET state=?, last_error=?, updated_at_ms=? WHERE singleton=1",
                (str(state), error, now),
            )
            row = connection.execute("SELECT * FROM consolidation_state WHERE singleton=1").fetchone()
        return dict(row)

    def clear_consolidation(self, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE consolidation_state SET state='IDLE', offer_id=NULL,
                   captured_wallet=NULL, captured_offer_amount=NULL, target_period=NULL,
                   strategy_version=NULL, started_at_ms=NULL, updated_at_ms=?, last_error=NULL
                   WHERE singleton=1""",
                (now,),
            )

    @staticmethod
    def _takeover_digest(offer):
        payload = {
            key: offer.get(key)
            for key in ("id", "amount", "amount_original", "rate", "rate_real", "period", "offer_type", "flags")
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()

    def observe_external_takeover(self, offer, now_ms=None, minimum_gap_ms=30_000):
        now = int(now_ms if now_ms is not None else self._now_ms())
        offer_id = int(offer.get("id") or offer.get("offer_id"))
        digest = self._takeover_digest(offer)
        with self.transaction(immediate=True) as connection:
            previous = connection.execute(
                "SELECT * FROM external_takeover_state WHERE offer_id=?", (offer_id,)
            ).fetchone()
            first_seen = now
            state = "OBSERVED"
            if previous is not None and previous["snapshot_digest"] == digest:
                first_seen = int(previous["first_seen_ms"])
                state = previous["state"]
                if state == "OBSERVED" and now - first_seen >= int(minimum_gap_ms):
                    state = "CONFIRMED"
            connection.execute(
                """INSERT INTO external_takeover_state(
                       offer_id, state, snapshot_digest, first_seen_ms, confirmed_at_ms,
                       updated_at_ms, last_error
                   ) VALUES(?, ?, ?, ?, ?, ?, NULL)
                   ON CONFLICT(offer_id) DO UPDATE SET
                       state=excluded.state, snapshot_digest=excluded.snapshot_digest,
                       first_seen_ms=excluded.first_seen_ms,
                       confirmed_at_ms=excluded.confirmed_at_ms,
                       updated_at_ms=excluded.updated_at_ms, last_error=NULL""",
                (offer_id, state, digest, first_seen, now if state == "CONFIRMED" else None, now),
            )
            row = connection.execute("SELECT * FROM external_takeover_state WHERE offer_id=?", (offer_id,)).fetchone()
        return dict(row)

    def update_external_takeover(self, offer_id, state, error=None, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE external_takeover_state SET state=?, last_error=?, updated_at_ms=?
                   WHERE offer_id=?""",
                (str(state), error, now, int(offer_id)),
            )

    def reset_unconfirmed_external_takeovers(self):
        """Discard snapshot confirmations after a non-authoritative account read."""

        with self.transaction(immediate=True) as connection:
            connection.execute("DELETE FROM external_takeover_state WHERE state IN ('OBSERVED', 'CONFIRMED')")

    def discard_unconfirmed_external_takeover(self, offer_id):
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "DELETE FROM external_takeover_state WHERE offer_id=? AND state IN ('OBSERVED', 'CONFIRMED')",
                (int(offer_id),),
            )

    def external_takeovers(self, states=None):
        query = "SELECT * FROM external_takeover_state"
        params = []
        if states:
            placeholders = ",".join("?" for _ in states)
            query += f" WHERE state IN ({placeholders})"
            params.extend(sorted(states))
        query += " ORDER BY offer_id"
        with self.read_connection() as connection:
            return [dict(row) for row in connection.execute(query, params).fetchall()]

    def release_boundary(self):
        """Return the immutable local activation boundary for the current release."""

        with self.read_connection() as connection:
            rows = {
                row["key"]: row["value"]
                for row in connection.execute(
                    """SELECT key, value FROM schema_meta WHERE key IN (
                           'release_0.3.5_activated_at_ms',
                           'release_0.3.5_boundary_source',
                           'release_0.3.5_boundary_evidence_at_ms',
                           'release_0.3.6_activated_at_ms'
                       )"""
                ).fetchall()
            }
        activated = rows.get("release_0.3.5_activated_at_ms")
        evidence = rows.get("release_0.3.5_boundary_evidence_at_ms")
        return {
            "version": CURRENT_RELEASE,
            "label": CURRENT_RELEASE_LABEL,
            "baselineVersion": "0.3.5.2",
            "baselineLabel": "V3.5",
            "v36ActivatedAtMs": (
                None if rows.get("release_0.3.6_activated_at_ms") is None
                else int(rows["release_0.3.6_activated_at_ms"])
            ),
            "activatedAtMs": None if activated is None else int(activated),
            "boundarySource": rows.get("release_0.3.5_boundary_source"),
            "evidenceAtMs": None if evidence is None else int(evidence),
        }

    def record_v36_activation(self, details=None, now_ms=None):
        """Record the cutover only after a validated live configuration activates."""
        now = int(self._now_ms() if now_ms is None else now_ms)
        with self.transaction(immediate=True) as connection:
            return self._record_v36_activation(connection, details, now)

    @staticmethod
    def _record_v36_activation(connection, details, now):
        existing = connection.execute(
            "SELECT value FROM schema_meta WHERE key='release_0.3.6_activated_at_ms'"
        ).fetchone()
        if existing is not None:
            return int(existing["value"])
        connection.execute(
            "INSERT INTO schema_meta(key,value) VALUES('release_0.3.6_activated_at_ms',?)", (str(now),)
        )
        connection.execute(
            """INSERT INTO strategy_events(event_type,from_version,to_version,reason,created_at_ms)
               VALUES('V36_ACTIVATED','0.3.5.2',?,?,?)""",
            (CURRENT_RELEASE, json.dumps(details or {}, ensure_ascii=False, sort_keys=True, default=str), now),
        )
        return now

    @staticmethod
    def _funding_wait_records(connection, currency, since_ms, until_ms):
        """Resolve waits using explicit offer/chain evidence, never slice similarity.

        All reads run in the caller's snapshot. Histories are loaded in batches;
        the number of queries does not grow with the number of fills.
        """
        trades = connection.execute(
            """SELECT * FROM funding_trades
               WHERE currency=? AND managed=1 AND mts>=? AND mts<? ORDER BY mts, trade_id""",
            (currency, int(since_ms), int(until_ms)),
        ).fetchall()
        if not trades:
            return []
        intents = {row["id"]: dict(row) for row in connection.execute("SELECT * FROM order_intents")}
        offer_intents = defaultdict(list)
        for intent in intents.values():
            if intent["exchange_offer_id"] is not None:
                offer_intents[intent["exchange_offer_id"]].append(intent)
        offer_times = defaultdict(set)
        for table in ("offers", "offer_history"):
            for row in connection.execute(f"SELECT offer_id, currency, period, mts_created FROM {table}"):
                offer_times[row["offer_id"]].add((row["currency"], row["period"], row["mts_created"]))

        chains = {row["chain_key"]: dict(row) for row in connection.execute("SELECT * FROM reprice_chains")}
        members = {key: set() for key in chains}
        invalid_chains = set()
        for key, chain in chains.items():
            for field in ("current_offer_id", "pending_source_offer_id"):
                if chain[field] is not None:
                    members[key].add(chain[field])
            # This is an explicit original-intent ID in the stored chain key,
            # not an inference from a reusable base_slice_key.
            _, marker, root_id = key.rpartition("|intent:")
            if marker:
                root = intents.get(int(root_id)) if root_id.isdigit() else None
                if root is None or root["exchange_offer_id"] is None:
                    invalid_chains.add(key)
                else:
                    members[key].add(root["exchange_offer_id"])

        unresolved_offers = set()
        for event in connection.execute("SELECT offer_id, chain_key FROM reprice_events WHERE chain_key IS NOT NULL"):
            if event["chain_key"] in chains and event["offer_id"] is not None:
                members[event["chain_key"]].add(event["offer_id"])
            elif event["offer_id"] is not None:
                unresolved_offers.add(event["offer_id"])

        merged_offers = {}
        for event in connection.execute(
            """SELECT offer_id, details_json, created_at_ms FROM ownership_events
               WHERE event_type='DUST_CHAIN_BOUND'"""
        ):
            offer_id = event["offer_id"]
            if offer_id is None:
                continue
            merged_offers[offer_id] = min(merged_offers.get(offer_id, event["created_at_ms"]), event["created_at_ms"])
            try:
                details = json.loads(event["details_json"])
            except (TypeError, ValueError):
                details = {}
            if isinstance(details, dict) and details.get("chainKey") in members:
                members[details["chainKey"]].add(offer_id)

        for offer_id, rows in offer_intents.items():
            for row in rows:
                if ":dust-" in row["slice_key"]:
                    merged_offers[offer_id] = min(
                        merged_offers.get(offer_id, row["created_at_ms"]), row["created_at_ms"]
                    )

        # Copies created during strategy migration share exact offer IDs.
        # Join these evidence components so conflicts and merged-fund ancestry
        # survive later replacements, without selecting an arbitrary earliest date.
        parents = {key: key for key in chains}

        def root(key):
            while parents[key] != key:
                parents[key] = parents[parents[key]]
                key = parents[key]
            return key

        offer_chains = defaultdict(set)
        for key, ids in members.items():
            for offer_id in ids:
                offer_chains[offer_id].add(key)
        for keys in offer_chains.values():
            ordered = sorted(keys)
            for key in ordered[1:]:
                parents[root(key)] = root(ordered[0])
        starts = defaultdict(set)
        invalid_roots = set()
        merged_at = {}
        for key, chain in chains.items():
            component = root(key)
            starts[component].add(chain["started_at_ms"])
            if key in invalid_chains:
                invalid_roots.add(component)
            for offer_id in members[key]:
                if offer_id in merged_offers:
                    cutoff = merged_offers[offer_id]
                    merged_at[component] = min(merged_at.get(component, cutoff), cutoff)

        result = []
        for trade in trades:
            offer_id, filled_at = trade["offer_id"], int(trade["mts"])
            known_times = offer_times.get(offer_id, set())
            timestamps = {item[2] for item in known_times}
            linked = offer_intents.get(offer_id, [])
            valid_offer = (
                offer_id not in unresolved_offers
                and len(linked) <= 1
                and all(item["currency"] == currency and item["period"] == trade["period"] for item in linked)
                and len(timestamps) <= 1
                and all(item[0] == currency and item[1] == trade["period"] for item in known_times)
                and all(t is not None and 0 < t <= filled_at for t in timestamps)
            )
            start = None
            keys = offer_chains.get(offer_id)
            if keys:
                component = root(next(iter(keys)))
                candidates = starts[component]
                if (
                    component not in invalid_roots
                    and len(candidates) == 1
                    and filled_at < merged_at.get(component, until_ms)
                ):
                    start = next(iter(candidates))
            else:
                # Fallback only for a proven original exchange offer. Replacements
                # and consolidation intents cannot masquerade as fresh orders.
                if len(linked) == 1 and len(timestamps) == 1 and offer_id not in merged_offers:
                    intent = linked[0]
                    if base_slice_key(intent["slice_key"]) == intent["slice_key"]:
                        start = next(iter(timestamps))
            if (
                not valid_offer
                or start is None
                or start <= 0
                or start > filled_at
                or any(start > t for t in timestamps)
            ):
                start = None
            amount = abs(D(trade["amount"]))
            result.append(
                {
                    **dict(trade),
                    "amount": amount,
                    "wait_seconds": None if start is None or amount <= 0 else D(filled_at - start) / D("1000"),
                }
            )
        return result

    @staticmethod
    def _funding_wait_summary(records):
        valid = [row for row in records if row["wait_seconds"] is not None]
        total_amount = sum((row["amount"] for row in records), D("0"))
        valid_amount = sum((row["amount"] for row in valid), D("0"))
        average = (
            sum((row["amount"] * row["wait_seconds"] for row in valid), D("0")) / valid_amount
            if valid_amount > 0
            else None
        )
        return {
            "averageWaitSeconds": None if average is None else _decimal_text(average),
            "waitValidCount": len(valid),
            "waitMissingCount": len(records) - len(valid),
            "waitCoveragePercent": (
                _decimal_text(valid_amount / total_amount * D("100")) if total_amount > 0 else None
            ),
        }

    def period_activity(self, since_ms, currency="USD", until_ms=None):
        """Return recent robot submissions and managed fills grouped by term."""

        end = 9_223_372_036_854_775_807 if until_ms is None else int(until_ms)
        with self.read_connection() as connection:
            connection.execute("BEGIN")
            submitted = connection.execute(
                """SELECT period, COUNT(*) AS order_count,
                          COALESCE(SUM(CAST(amount AS REAL)), 0) AS amount,
                          COALESCE(
                              SUM(CAST(amount AS REAL) * CAST(effective_rate AS REAL))
                              / NULLIF(SUM(CAST(amount AS REAL)), 0), 0
                          ) AS weighted_rate
                   FROM order_intents
                   WHERE currency=? AND created_at_ms>=? AND created_at_ms<?
                   GROUP BY period ORDER BY period""",
                (str(currency).upper(), int(since_ms), end),
            ).fetchall()
            records = self._funding_wait_records(connection, str(currency).upper(), since_ms, end)
        by_period = defaultdict(list)
        for row in records:
            by_period[int(row["period"])].append(row)
        traded = []
        for period, rows in sorted(by_period.items()):
            amount = sum((row["amount"] for row in rows), D("0"))
            summary = self._funding_wait_summary(rows)
            seconds = summary.pop("averageWaitSeconds")
            traded.append(
                {
                    "period": period,
                    "count": len(rows),
                    "amount": amount,
                    "weightedDailyRate": (
                        sum((row["amount"] * D(row["rate"]) for row in rows), D("0")) / amount if amount else D("0")
                    ),
                    "weightedWaitMinutes": None if seconds is None else D(seconds) / D("60"),
                    **summary,
                }
            )
        return {
            "fromMs": int(since_ms),
            "toMs": None if until_ms is None else end,
            "submitted": [
                {
                    "period": int(row["period"]),
                    "count": int(row["order_count"]),
                    "amount": D(str(row["amount"])),
                    "weightedDailyRate": D(str(row["weighted_rate"])),
                }
                for row in submitted
            ],
            "traded": traded,
        }

    def release_comparison(self, now_ms=None, currency="USD"):
        """Split equal-duration activity windows at the V3.5 activation boundary."""

        now = int(now_ms if now_ms is not None else self._now_ms())
        boundary = self.release_boundary()
        activated = int(boundary["activatedAtMs"] or now)
        elapsed = max(0, now - activated)
        return {
            **boundary,
            "comparisonDurationMs": elapsed,
            "before": self.period_activity(activated - elapsed, currency, activated),
            "after": self.period_activity(activated, currency, now + 1),
        }

    def reserve_intent(self, order, wallet_available):
        fingerprint = order.get("fingerprint") or order_intent_fingerprint(
            order.get("currency", "USD"),
            order["amount"],
            order["submitted_rate"],
            order["period"],
            order["offer_type"],
            order.get("flags", 0),
            order["strategy_version"],
            order["slice_key"],
        )
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT * FROM order_intents WHERE fingerprint = ?", (fingerprint,)
            ).fetchone()
            if existing is not None:
                return False, dict(existing)
            reserved = self.reserved_amount(order.get("currency", "USD"), connection)
            if reserved + D(order["amount"]) > D(wallet_available):
                raise InsufficientReservedBalance(
                    f"reserved {reserved} plus {order['amount']} exceeds wallet {wallet_available}"
                )
            cursor = connection.execute(
                """INSERT INTO order_intents(
                    fingerprint, currency, slice_key, pool, layer, amount, submitted_rate,
                    effective_rate, period, offer_type, display_type, flags, strategy_version, plan_hash, state,
                    write_phase, strategy_variant, pricing_curve_version, fixed_landing_rate,
                    created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PLANNED', 'NOT_SENT', ?, ?, ?, ?, ?)""",
                (
                    fingerprint,
                    order.get("currency", "USD").upper(),
                    str(order["slice_key"]),
                    order["pool"],
                    order["layer"],
                    _decimal_text(order["amount"]),
                    _decimal_text(order["submitted_rate"]),
                    _decimal_text(order["effective_rate"]),
                    int(order["period"]),
                    order["offer_type"],
                    order.get("display_type") or order["offer_type"],
                    int(order.get("flags", 0)),
                    str(order["strategy_version"]),
                    order.get("plan_hash"),
                    str(order.get("strategy_variant") or "baseline"),
                    str(order.get("pricing_curve_version") or EXACT_TERM_EXPLORATION_CURVE),
                    None if order.get("fixed_landing_rate") is None else _decimal_text(order["fixed_landing_rate"]),
                    now,
                    now,
                ),
            )
            row = connection.execute("SELECT * FROM order_intents WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return True, dict(row)

    def _set_intent_state(
        self,
        intent_id,
        state,
        exchange_offer_id=None,
        error_text=None,
        write_phase=None,
        resolution=None,
    ):
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE order_intents SET state = ?, exchange_offer_id = COALESCE(?, exchange_offer_id),
                   error_text = ?, write_phase = COALESCE(?, write_phase),
                   resolution = COALESCE(?, resolution),
                   resolved_at_ms = CASE WHEN ? IS NULL THEN resolved_at_ms ELSE ? END,
                   updated_at_ms = ? WHERE id = ?""",
                (
                    state,
                    exchange_offer_id,
                    error_text,
                    write_phase,
                    resolution,
                    resolution,
                    now,
                    now,
                    int(intent_id),
                ),
            )
            row = connection.execute("SELECT * FROM order_intents WHERE id = ?", (int(intent_id),)).fetchone()
        if row is None:
            raise StateStoreError("unknown order intent")
        return dict(row)

    def mark_submitting(self, intent_id):
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE order_intents SET state='SUBMITTING', write_phase='SENT',
                   request_started_at_ms=?, updated_at_ms=? WHERE id=?""",
                (now, now, int(intent_id)),
            )
        return self.intent(intent_id)

    def confirm_intent(self, intent_id, exchange_offer_id):
        result = self._set_intent_state(
            intent_id,
            "CONFIRMED",
            int(exchange_offer_id),
            write_phase="CONFIRMED",
            resolution="EXCHANGE_CONFIRMED",
        )
        with self.transaction(immediate=True) as connection:
            self._clear_confirmed_submit_protection(connection, int(exchange_offer_id), int(intent_id), self._now_ms())
        return result

    def mark_ambiguous(self, intent_id, error_text):
        self._set_intent_state(
            intent_id,
            "AMBIGUOUS",
            error_text=str(error_text),
            write_phase="UNKNOWN",
            resolution="MANUAL_REQUIRED",
        )
        self.enter_protected_pause(f"AMBIGUOUS_SUBMIT:{intent_id}")
        return self.intent(intent_id)

    def recover_incomplete_writes(self):
        """Close never-sent plans and fail closed on requests that may have reached Bitfinex."""
        now = self._now_ms()
        planned_transition = restart_transition("PLANNED")
        submitting_transition = restart_transition("SUBMITTING")
        with self.transaction(immediate=True) as connection:
            planned = connection.execute(
                """UPDATE order_intents SET state=?, write_phase=?,
                   resolution=?, resolved_at_ms=?, updated_at_ms=?
                   WHERE state='PLANNED'""",
                (
                    planned_transition.state,
                    planned_transition.write_phase,
                    planned_transition.resolution,
                    now,
                    now,
                ),
            ).rowcount
            submitting = connection.execute("""SELECT id FROM order_intents WHERE state='SUBMITTING'""").fetchall()
            if submitting:
                connection.execute(
                    """UPDATE order_intents SET state=?, write_phase=?,
                       resolution=?,
                       error_text='process restarted before exchange confirmation',
                       resolved_at_ms=?, updated_at_ms=? WHERE state='SUBMITTING'""",
                    (
                        submitting_transition.state,
                        submitting_transition.write_phase,
                        submitting_transition.resolution,
                        now,
                        now,
                    ),
                )
        for row in submitting:
            self.enter_protected_pause(f"AMBIGUOUS_SUBMIT:{row['id']}")
        return {"closedBeforeSend": planned, "ambiguousAfterSend": len(submitting)}

    def resolve_ambiguous_intent(self, intent_id, exchange_offer_id=None, close=False):
        """Resolve an uncertain write only after an operator has reconciled it.

        Binding requires a concrete exchange id.  Closing means the operator has
        confirmed that no offer exists. The protected pause remains read-only
        until the normal two-snapshot recovery barrier completes.
        """
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            intent = connection.execute("SELECT * FROM order_intents WHERE id = ?", (int(intent_id),)).fetchone()
            if intent is None or intent["state"] != "AMBIGUOUS":
                raise StateStoreError("intent is not awaiting ambiguous resolution")
            if bool(close) == (exchange_offer_id is not None):
                raise StateStoreError("choose exactly one resolution: bind offer or confirm absent")
            if exchange_offer_id is not None:
                duplicate = connection.execute(
                    "SELECT id FROM order_intents WHERE exchange_offer_id = ? AND id != ?",
                    (int(exchange_offer_id), int(intent_id)),
                ).fetchone()
                if duplicate is not None:
                    raise StateStoreError("exchange offer is already bound to another intent")
                connection.execute(
                    """UPDATE order_intents SET state='CONFIRMED', exchange_offer_id=?,
                       error_text=NULL, write_phase='CONFIRMED', resolution='OPERATOR_BOUND',
                       resolved_at_ms=?, updated_at_ms=? WHERE id=?""",
                    (int(exchange_offer_id), now, now, int(intent_id)),
                )
                self._clear_confirmed_submit_protection(connection, int(exchange_offer_id), int(intent_id), now)
            else:
                connection.execute(
                    """UPDATE order_intents SET state='CLOSED', error_text='operator confirmed absent',
                       write_phase='NOT_CREATED', resolution='OPERATOR_CONFIRMED_ABSENT',
                       resolved_at_ms=?, updated_at_ms=? WHERE id=?""",
                    (now, now, int(intent_id)),
                )
            unresolved = connection.execute(
                "SELECT COUNT(*) AS count FROM order_intents WHERE state='AMBIGUOUS'"
            ).fetchone()["count"]
            runtime = connection.execute("SELECT * FROM runtime_state WHERE singleton=1").fetchone()
            if can_clear_ambiguous_pause(unresolved, runtime["mode"], runtime["safe_manual"]):
                connection.execute(
                    """UPDATE runtime_state SET mode='PAUSED', previous_mode=NULL,
                       safe_reason=NULL, safe_manual=0, consistent_syncs=0,
                       last_consistent_sync_ms=NULL, updated_at_ms=? WHERE singleton=1""",
                    (now,),
                )
                connection.execute(
                    """INSERT INTO mode_events(from_mode, to_mode, reason, created_at_ms)
                       VALUES('PAUSED', 'PAUSED', 'ambiguous intent resolved', ?)""",
                    (now,),
                )
        return {"intent": self.intent(intent_id), "runtime": self.runtime()}

    def close_intent(self, intent_id):
        return self._set_intent_state(intent_id, "CLOSED", resolution="CLOSED")

    def reject_intent(self, intent_id, error_text):
        return self._set_intent_state(
            intent_id,
            "CLOSED",
            error_text=str(error_text),
            write_phase="DEFINITE_REJECT",
            resolution="EXCHANGE_REJECTED",
        )

    def intent(self, intent_id):
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM order_intents WHERE id = ?", (int(intent_id),)).fetchone()
        return None if row is None else dict(row)

    def intents(self, states=None):
        query = "SELECT * FROM order_intents"
        params = ()
        if states:
            normalized = [str(state).upper() for state in states]
            query += " WHERE state IN (" + ",".join("?" for _ in normalized) + ")"
            params = tuple(normalized)
        query += " ORDER BY id"
        with self.read_connection() as connection:
            return [dict(row) for row in connection.execute(query, params).fetchall()]

    def reconcile_ambiguous_candidates(self, confirm_absent=False, now_ms=None):
        """Reconcile uncertain submits and resume the mode that was interrupted.

        A unique offer/trade is bound immediately. Absence is accepted only when
        the caller confirms that both the active Offers and Funding Trades reads
        were authoritative, twice at least 30 seconds apart. Multiple candidates
        remain in protected PAUSED for operator review because choosing one would be guesswork.
        """
        resolved = []
        closed_absent = []
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            intents = connection.execute("SELECT * FROM order_intents WHERE state='AMBIGUOUS' ORDER BY id").fetchall()
            candidate_counts = {}
            for intent in intents:
                request_ms = int(intent["request_started_at_ms"] or intent["updated_at_ms"] or 0)
                start = request_ms - 300_000
                end = request_ms + 600_000
                candidate_ids = set()
                offers = connection.execute(
                    """SELECT offer_id FROM offers
                       WHERE status != 'CLOSED' AND currency=?
                         AND (amount=? OR COALESCE(amount_original, amount)=?) AND period=?
                         AND offer_type=? AND flags=?
                         AND (rate=? OR rate_real=?)
                         AND COALESCE(mts_created, 0) BETWEEN ? AND ?""",
                    (
                        intent["currency"],
                        intent["amount"],
                        intent["amount"],
                        intent["period"],
                        intent["offer_type"],
                        intent["flags"],
                        intent["submitted_rate"],
                        intent["effective_rate"],
                        start,
                        end,
                    ),
                ).fetchall()
                candidate_ids.update(int(row["offer_id"]) for row in offers)
                trades = connection.execute(
                    """SELECT offer_id FROM funding_trades
                       WHERE currency=? AND period=? AND mts BETWEEN ? AND ?
                         AND offer_id IS NOT NULL
                         AND ABS(CAST(rate AS REAL) - CAST(? AS REAL)) <= 0.0000001
                       GROUP BY offer_id
                       HAVING ABS(SUM(CAST(amount AS REAL)) - CAST(? AS REAL)) <= 0.00000001""",
                    (
                        intent["currency"],
                        intent["period"],
                        start,
                        end,
                        intent["effective_rate"],
                        intent["amount"],
                    ),
                ).fetchall()
                candidate_ids.update(int(row["offer_id"]) for row in trades if row["offer_id"] is not None)
                historical_offers = connection.execute(
                    """SELECT offer_id FROM offer_history
                       WHERE currency=? AND (amount=? OR COALESCE(amount_original, amount)=?)
                         AND period=? AND offer_type=? AND flags=?
                         AND (rate=? OR rate_real=?)
                         AND COALESCE(mts_created, 0) BETWEEN ? AND ?""",
                    (
                        intent["currency"],
                        intent["amount"],
                        intent["amount"],
                        intent["period"],
                        intent["offer_type"],
                        intent["flags"],
                        intent["submitted_rate"],
                        intent["effective_rate"],
                        start,
                        end,
                    ),
                ).fetchall()
                candidate_ids.update(int(row["offer_id"]) for row in historical_offers)
                bound = {
                    int(row["exchange_offer_id"])
                    for row in connection.execute(
                        "SELECT exchange_offer_id FROM order_intents WHERE exchange_offer_id IS NOT NULL AND id != ?",
                        (intent["id"],),
                    ).fetchall()
                }
                candidate_ids.difference_update(bound)
                candidate_counts[int(intent["id"])] = len(candidate_ids)
                offer_id = unique_unbound_candidate(candidate_ids, bound)
                if offer_id is None:
                    continue
                connection.execute(
                    """UPDATE order_intents SET state='CONFIRMED', exchange_offer_id=?,
                       error_text=NULL, write_phase='CONFIRMED', resolution='AUTO_UNIQUE_MATCH',
                       resolved_at_ms=?, updated_at_ms=? WHERE id=?""",
                    (offer_id, now, now, intent["id"]),
                )
                connection.execute(
                    """UPDATE offers SET managed=1, pool=COALESCE(pool, ?), layer=COALESCE(layer, ?),
                       plan_hash=COALESCE(plan_hash, ?), strategy_variant=? WHERE offer_id=?""",
                    (
                        intent["pool"],
                        intent["layer"],
                        intent["plan_hash"],
                        intent["strategy_variant"],
                        offer_id,
                    ),
                )
                self._clear_confirmed_submit_protection(connection, offer_id, intent["id"], now)
                resolved.append({"intentId": intent["id"], "offerId": offer_id})
            remaining = connection.execute("SELECT * FROM order_intents WHERE state='AMBIGUOUS' ORDER BY id").fetchall()
            runtime = connection.execute("SELECT * FROM runtime_state WHERE singleton=1").fetchone()
            if (
                remaining
                and confirm_absent
                and all(
                    candidate_counts.get(int(item["id"]), 0) == 0 and str(item["offer_type"]).upper() == "LIMIT"
                    for item in remaining
                )
            ):
                previous_sync = runtime["last_consistent_sync_ms"]
                count = int(runtime["consistent_syncs"])
                if previous_sync is None or now - int(previous_sync) >= 30_000:
                    count += 1
                    connection.execute(
                        """UPDATE runtime_state SET consistent_syncs=?, last_consistent_sync_ms=?,
                           updated_at_ms=? WHERE singleton=1""",
                        (count, now, now),
                    )
                if count >= 2:
                    ids = [int(item["id"]) for item in remaining]
                    placeholders = ",".join("?" for _ in ids)
                    connection.execute(
                        f"""UPDATE order_intents SET state='CLOSED',
                            error_text='authoritative snapshots confirmed absent',
                            write_phase='NOT_CREATED', resolution='AUTO_CONFIRMED_ABSENT',
                            resolved_at_ms=?, updated_at_ms=?
                            WHERE id IN ({placeholders})""",
                        (now, now, *ids),
                    )
                    closed_absent.extend(ids)
                    remaining = []
            elif remaining and any(candidate_counts.get(int(item["id"]), 0) != 0 for item in remaining):
                connection.execute(
                    """UPDATE runtime_state SET consistent_syncs=0, last_consistent_sync_ms=NULL,
                       updated_at_ms=? WHERE singleton=1""",
                    (now,),
                )
            if not remaining and (resolved or closed_absent):
                runtime = connection.execute("SELECT * FROM runtime_state WHERE singleton=1").fetchone()
                if runtime["mode"] == "PAUSED" and str(runtime["safe_reason"] or "").startswith("AMBIGUOUS_SUBMIT:"):
                    self._resume_protected_pause(
                        connection,
                        runtime,
                        now,
                        "ambiguous submit reconciled" if resolved else "ambiguous submit confirmed absent",
                    )
        return resolved

    def record_legacy_migration(self, migration_id, source_path, result):
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT OR IGNORE INTO legacy_migrations(
                       migration_id, source_path, result_json, completed_at_ms
                   ) VALUES(?, ?, ?, ?)""",
                (
                    str(migration_id),
                    os.path.abspath(source_path) if source_path else None,
                    json.dumps(result, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
        return self.legacy_migration(migration_id)

    def legacy_migration(self, migration_id):
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM legacy_migrations WHERE migration_id=?", (str(migration_id),)
            ).fetchone()
        return None if row is None else {**dict(row), "result": json.loads(row["result_json"])}

    def set_rollout(self, candidate_version, candidate_share, status="ACTIVE", reason=""):
        share = int(candidate_share)
        if share not in {0, 10, 25, 50, 100}:
            raise StateStoreError("candidate share must be one of 0, 10, 25, 50, 100")
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO strategy_rollouts(
                       candidate_version, candidate_share, stage_started_at_ms, status, last_reason
                   ) VALUES(?, ?, ?, ?, ?)
                   ON CONFLICT(candidate_version) DO UPDATE SET
                       candidate_share=excluded.candidate_share,
                       stage_started_at_ms=excluded.stage_started_at_ms,
                       status=excluded.status, last_reason=excluded.last_reason""",
                (str(candidate_version), share, now, str(status), str(reason)),
            )
        return self.rollout(candidate_version)

    def rollout(self, candidate_version):
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM strategy_rollouts WHERE candidate_version=?",
                (str(candidate_version),),
            ).fetchone()
        return None if row is None else dict(row)

    def replenishment_slice_key(self, base_key):
        base_key = str(base_key)
        with self.read_connection() as connection:
            rows = connection.execute(
                """SELECT slice_key, state FROM order_intents
                   WHERE slice_key = ? OR slice_key LIKE ? ORDER BY id""",
                (base_key, base_key + ":r%"),
            ).fetchall()
        if not rows:
            return base_key
        latest = rows[-1]
        if latest["state"] != "CLOSED":
            return latest["slice_key"]
        return f"{base_key}:r{len(rows)}"

    @staticmethod
    def _reprice_chain_key(strategy_version, slice_key):
        return f"{strategy_version}|{base_slice_key(slice_key)}"

    @staticmethod
    def _strategy_reprice_stage_count(
        connection,
        strategy_version,
        pool,
        layer=None,
        pricing_curve_version=None,
    ):
        row = connection.execute(
            "SELECT policy_json FROM strategy_versions WHERE version_id=?",
            (str(strategy_version),),
        ).fetchone()
        if row is None:
            return 6
        try:
            policy = json.loads(row["policy_json"])
            field = (
                f"quick_{pool}_reprice_stages_minutes"
                if layer == "quick" and pricing_curve_version in {EXACT_TERM_EXPLORATION_CURVE, QUICK_EXECUTION_CURVE}
                else f"{pool}_reprice_stages_minutes"
            )
            stages = policy.get(field)
            if stages is None and layer == "quick" and pricing_curve_version in {
                EXACT_TERM_EXPLORATION_CURVE, QUICK_EXECUTION_CURVE,
            }:
                # A pre-0.3.5.2 ACTIVE policy can create V4 orders after the
                # application upgrade even though its raw JSON predates the
                # quick-stage fields. Parsed runtime policy supplies the V4
                # ten-stage defaults, so later strategy transitions must map
                # that chain as ten stages too.
                count = 10
            else:
                count = len(stages) if isinstance(stages, (list, tuple)) else 6
        except (TypeError, ValueError, json.JSONDecodeError):
            return 6
        return count if count in {6, 10} else 6

    @staticmethod
    def _map_reprice_stage(current_stage, old_stage_count, new_stage_count):
        stage = max(0, min(int(current_stage or 0), int(old_stage_count)))
        old_market = max(1, int(old_stage_count) // 2)
        new_market = max(1, int(new_stage_count) // 2)
        if stage <= old_market:
            return min(new_market, (stage * new_market + old_market - 1) // old_market)
        old_floor_stage = stage - old_market
        mapped_floor = (old_floor_stage * new_market + old_market - 1) // old_market
        return min(int(new_stage_count), new_market + mapped_floor)

    def ensure_reprice_chain(self, offer, strategy_version, now_ms=None, *, frr_daily_rate=None):
        offer_id = int(offer.get("offer_id") or offer.get("id"))
        now = int(now_ms if now_ms is not None else self._now_ms())
        observed_rate = funding_offer_effective_rate(offer, frr_daily_rate)
        started_at = int(offer.get("mts_created") or now)
        with self.transaction(immediate=True) as connection:
            bound = connection.execute(
                """SELECT * FROM reprice_chains
                   WHERE strategy_version=? AND current_offer_id=? AND status='ACTIVE'
                   ORDER BY updated_at_ms DESC LIMIT 1""",
                (str(strategy_version), offer_id),
            ).fetchone()
            if bound is not None:
                return dict(bound)
            intent = connection.execute(
                "SELECT * FROM order_intents WHERE exchange_offer_id=?",
                (offer_id,),
            ).fetchone()
            if intent is None:
                return None
            base_key = base_slice_key(intent["slice_key"])
            chain_key = f"{self._reprice_chain_key(strategy_version, base_key)}|intent:{int(intent['id'])}"
            current = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (chain_key,),
            ).fetchone()
            if current is None:
                predecessor = connection.execute(
                    """SELECT * FROM reprice_chains
                       WHERE current_offer_id=? AND status='ACTIVE'
                         AND strategy_version<>?
                       ORDER BY updated_at_ms DESC, rowid DESC LIMIT 1""",
                    (offer_id, str(strategy_version)),
                ).fetchone()
                if predecessor is None:
                    if observed_rate is None:
                        return None
                    connection.execute(
                        """INSERT INTO reprice_chains(
                               chain_key, strategy_version, base_slice_key, pool, layer,
                               origin_rate, started_at_ms, current_stage, current_offer_id,
                               fixed_landing_rate, pricing_curve_version, status, updated_at_ms
                           ) VALUES(?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'ACTIVE', ?)""",
                        (
                            chain_key,
                            str(strategy_version),
                            base_key,
                            str(offer.get("pool") or intent["pool"]),
                            str(offer.get("layer") or intent["layer"]),
                            _decimal_text(observed_rate),
                            started_at,
                            offer_id,
                            intent["fixed_landing_rate"],
                            str(intent["pricing_curve_version"] or LEGACY_REPRICE_CURVE),
                            now,
                        ),
                    )
                else:
                    old_stage_count = self._strategy_reprice_stage_count(
                        connection,
                        predecessor["strategy_version"],
                        predecessor["pool"],
                        predecessor["layer"],
                        predecessor["pricing_curve_version"],
                    )
                    new_stage_count = self._strategy_reprice_stage_count(
                        connection,
                        strategy_version,
                        predecessor["pool"],
                        predecessor["layer"],
                        predecessor["pricing_curve_version"],
                    )
                    mapped_stage = self._map_reprice_stage(
                        predecessor["current_stage"],
                        old_stage_count,
                        new_stage_count,
                    )
                    connection.execute(
                        """INSERT INTO reprice_chains(
                               chain_key, strategy_version, base_slice_key, pool, layer,
                               origin_rate, started_at_ms, current_stage, current_offer_id,
                               last_reprice_at_ms, pending_action, pending_target_rate,
                               pending_source_offer_id, market_anchor_rate, fixed_landing_rate,
                               pricing_curve_version, status, updated_at_ms
                           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?)""",
                        (
                            chain_key,
                            str(strategy_version),
                            base_key,
                            predecessor["pool"],
                            predecessor["layer"],
                            predecessor["origin_rate"],
                            predecessor["started_at_ms"],
                            mapped_stage,
                            offer_id,
                            predecessor["last_reprice_at_ms"],
                            predecessor["pending_action"],
                            predecessor["pending_target_rate"],
                            predecessor["pending_source_offer_id"],
                            predecessor["market_anchor_rate"],
                            predecessor["fixed_landing_rate"],
                            predecessor["pricing_curve_version"],
                            now,
                        ),
                    )
            elif int(current["current_offer_id"] or 0) != offer_id:
                connection.execute(
                    """UPDATE reprice_chains
                       SET current_offer_id=?, pending_action=NULL,
                           pending_target_rate=NULL, pending_source_offer_id=NULL,
                           status='ACTIVE', updated_at_ms=?
                       WHERE chain_key=?""",
                    (offer_id, now, chain_key),
                )
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (chain_key,),
            ).fetchone()
        return None if row is None else dict(row)

    def reprice_chain(self, chain_key):
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (str(chain_key),),
            ).fetchone()
        return None if row is None else dict(row)

    def reprice_chain_for_offer(self, offer_id):
        with self.read_connection() as connection:
            row = connection.execute(
                """SELECT * FROM reprice_chains
                   WHERE current_offer_id=? AND status='ACTIVE'
                   ORDER BY updated_at_ms DESC LIMIT 1""",
                (int(offer_id),),
            ).fetchone()
        return None if row is None else dict(row)

    def reprice_chains(self, active_only=False):
        query = "SELECT * FROM reprice_chains"
        if active_only:
            query += " WHERE status='ACTIVE'"
        query += " ORDER BY updated_at_ms, chain_key"
        with self.read_connection() as connection:
            return [dict(row) for row in connection.execute(query).fetchall()]

    def complete_reprice_stage(self, chain_key, stage, now_ms=None, market_anchor_rate=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            if market_anchor_rate is None:
                connection.execute(
                    """UPDATE reprice_chains
                       SET current_stage=MAX(current_stage, ?), updated_at_ms=?
                       WHERE chain_key=?""",
                    (int(stage), now, str(chain_key)),
                )
            else:
                connection.execute(
                    """UPDATE reprice_chains
                       SET current_stage=MAX(current_stage, ?),
                           market_anchor_rate=COALESCE(market_anchor_rate, ?),
                           updated_at_ms=?
                       WHERE chain_key=?""",
                    (int(stage), _decimal_text(market_anchor_rate), now, str(chain_key)),
                )
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (str(chain_key),),
            ).fetchone()
        return None if row is None else dict(row)

    def set_reprice_market_anchor(self, chain_key, rate, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE reprice_chains
                   SET market_anchor_rate=COALESCE(market_anchor_rate, ?), updated_at_ms=?
                   WHERE chain_key=?""",
                (_decimal_text(rate), now, str(chain_key)),
            )
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (str(chain_key),),
            ).fetchone()
        return None if row is None else dict(row)

    def mark_reprice_pending(
        self,
        chain_key,
        action,
        target_rate,
        stage=None,
        now_ms=None,
        market_anchor_rate=None,
        source_offer_id=None,
    ):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            if str(action) == "AGE_STAGE":
                connection.execute(
                    """UPDATE reprice_chains
                       SET current_stage=MAX(current_stage, ?), current_offer_id=NULL,
                           last_reprice_at_ms=?, pending_action=?,
                           pending_target_rate=?,
                           pending_source_offer_id=?,
                           market_anchor_rate=COALESCE(market_anchor_rate, ?),
                           updated_at_ms=?
                       WHERE chain_key=?""",
                    (
                        int(stage or 0),
                        now,
                        str(action),
                        _decimal_text(target_rate),
                        None if source_offer_id is None else int(source_offer_id),
                        None if market_anchor_rate is None else _decimal_text(market_anchor_rate),
                        now,
                        str(chain_key),
                    ),
                )
            else:
                connection.execute(
                    """UPDATE reprice_chains
                       SET current_offer_id=NULL, last_reprice_at_ms=?,
                           pending_action=?, pending_target_rate=?,
                           pending_source_offer_id=?, updated_at_ms=?
                       WHERE chain_key=?""",
                    (
                        now,
                        str(action),
                        _decimal_text(target_rate),
                        None if source_offer_id is None else int(source_offer_id),
                        now,
                        str(chain_key),
                    ),
                )
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (str(chain_key),),
            ).fetchone()
        return None if row is None else dict(row)

    def pending_reprice_for_base(self, base_key, strategy_version):
        chain_key = self._reprice_chain_key(strategy_version, base_key)
        with self.read_connection() as connection:
            row = connection.execute(
                """SELECT * FROM reprice_chains
                   WHERE chain_key=? AND status='ACTIVE'
                     AND pending_action IS NOT NULL AND pending_target_rate IS NOT NULL""",
                (chain_key,),
            ).fetchone()
            if row is None:
                pool, layer = slice_pool_layer(base_key)
                if pool is not None:
                    row = connection.execute(
                        """SELECT * FROM reprice_chains
                           WHERE strategy_version=? AND pool=? AND layer=?
                             AND status='ACTIVE' AND current_offer_id IS NULL
                             AND pending_action IS NOT NULL AND pending_target_rate IS NOT NULL
                           ORDER BY last_reprice_at_ms, updated_at_ms, chain_key LIMIT 1""",
                        (str(strategy_version), pool, layer),
                    ).fetchone()
        return None if row is None else dict(row)

    def set_fixed_landing_rate(self, chain_key, rate, now_ms=None):
        """Persist the first exact-term landing rate without ever revising it."""

        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """UPDATE reprice_chains
                   SET fixed_landing_rate=COALESCE(fixed_landing_rate, ?), updated_at_ms=?
                   WHERE chain_key=?""",
                (_decimal_text(rate), now, str(chain_key)),
            )
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (str(chain_key),),
            ).fetchone()
        return None if row is None else dict(row)

    def pending_reprices(self, strategy_version):
        with self.read_connection() as connection:
            rows = connection.execute(
                """SELECT chains.*, offers.currency AS source_currency,
                          offers.amount AS source_amount, offers.rate AS source_rate,
                          offers.rate_real AS source_rate_real, offers.period AS source_period,
                          offers.offer_type AS source_offer_type,
                          offers.display_type AS source_display_type,
                          offers.flags AS source_flags
                   FROM reprice_chains AS chains
                   JOIN offers ON offers.offer_id=chains.pending_source_offer_id
                   WHERE chains.strategy_version=? AND chains.status='ACTIVE'
                     AND chains.current_offer_id IS NULL
                     AND chains.pending_action IS NOT NULL
                     AND chains.pending_target_rate IS NOT NULL
                     AND chains.pending_source_offer_id IS NOT NULL
                   ORDER BY chains.last_reprice_at_ms, chains.updated_at_ms, chains.chain_key""",
                (str(strategy_version),),
            ).fetchall()
        return [dict(row) for row in rows]

    def bind_reprice_replacement_chain(self, chain_key, offer_id, effective_rate, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                """SELECT * FROM reprice_chains
                   WHERE chain_key=? AND status='ACTIVE' AND pending_action IS NOT NULL""",
                (str(chain_key),),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """UPDATE reprice_chains
                   SET current_offer_id=?, pending_action=NULL,
                       pending_target_rate=NULL, pending_source_offer_id=NULL,
                       status='ACTIVE', updated_at_ms=?
                   WHERE chain_key=?""",
                (int(offer_id), now, str(chain_key)),
            )
            result = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (str(chain_key),),
            ).fetchone()
        return None if result is None else dict(result)

    def bind_consolidation_replacement(self, source_offer_id, replacement_offer_id, now_ms=None):
        """Move one active reprice chain to the first confirmed dust replacement.

        Dust consolidation is an amount-only rewrite.  Unlike an ordinary
        reprice it must not advance or reset any timing, stage, anchor, landing,
        or cooldown fields.  The compare-and-set on ``current_offer_id`` makes
        the operation restart-safe and guarantees that only the first
        successful split replacement inherits the source chain.
        """

        now = int(now_ms if now_ms is not None else self._now_ms())
        source_offer_id = int(source_offer_id)
        replacement_offer_id = int(replacement_offer_id)
        with self.transaction(immediate=True) as connection:
            already_bound = connection.execute(
                """SELECT * FROM reprice_chains
                   WHERE current_offer_id=? AND status='ACTIVE'
                   ORDER BY updated_at_ms DESC LIMIT 1""",
                (replacement_offer_id,),
            ).fetchone()
            if already_bound is not None:
                return None
            source = connection.execute(
                """SELECT * FROM reprice_chains
                   WHERE current_offer_id=? AND status='ACTIVE'
                     AND pending_action IS NULL
                   ORDER BY updated_at_ms DESC LIMIT 1""",
                (source_offer_id,),
            ).fetchone()
            if source is None:
                return None
            connection.execute(
                """UPDATE reprice_chains SET current_offer_id=?, updated_at_ms=?
                   WHERE chain_key=? AND current_offer_id=?
                     AND status='ACTIVE' AND pending_action IS NULL""",
                (replacement_offer_id, now, source["chain_key"], source_offer_id),
            )
            result = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (source["chain_key"],),
            ).fetchone()
        return None if result is None else dict(result)

    def bind_reprice_replacement(
        self,
        base_key,
        strategy_version,
        offer_id,
        effective_rate,
        now_ms=None,
    ):
        now = int(now_ms if now_ms is not None else self._now_ms())
        chain_key = self._reprice_chain_key(strategy_version, base_key)
        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (chain_key,),
            ).fetchone()
            if row is None:
                pool, layer = slice_pool_layer(base_key)
                if pool is not None:
                    row = connection.execute(
                        """SELECT * FROM reprice_chains
                           WHERE strategy_version=? AND pool=? AND layer=?
                             AND status='ACTIVE' AND current_offer_id IS NULL
                             AND pending_action IS NOT NULL
                           ORDER BY last_reprice_at_ms, updated_at_ms, chain_key LIMIT 1""",
                        (str(strategy_version), pool, layer),
                    ).fetchone()
            if row is None:
                return None
            chain_key = row["chain_key"]
            connection.execute(
                """UPDATE reprice_chains
                   SET current_offer_id=?, pending_action=NULL,
                       pending_target_rate=NULL, pending_source_offer_id=NULL,
                       status='ACTIVE', updated_at_ms=?
                   WHERE chain_key=?""",
                (int(offer_id), now, chain_key),
            )
            result = connection.execute(
                "SELECT * FROM reprice_chains WHERE chain_key=?",
                (chain_key,),
            ).fetchone()
        return None if result is None else dict(result)

    def reconcile_offers(self, offers, seen_at_ms=None):
        seen_at = int(seen_at_ms if seen_at_ms is not None else self._now_ms())
        ids = []
        with self.transaction(immediate=True) as connection:
            for offer in offers or []:
                offer_id = int(offer["id"])
                if "amount_original" not in offer:
                    prior = connection.execute(
                        "SELECT amount_original FROM offers WHERE offer_id=?", (offer_id,)
                    ).fetchone()
                    if prior:
                        offer = dict(offer, amount_original=prior["amount_original"])
                self._observe_manual_offer(connection, offer, seen_at)
                ids.append(offer_id)
                intent = connection.execute(
                    "SELECT * FROM order_intents WHERE exchange_offer_id = ?", (offer_id,)
                ).fetchone()
                managed = bool(offer.get("managed", False) or intent is not None)
                pool = offer.get("pool") or (intent["pool"] if intent else None)
                layer = offer.get("layer") or (intent["layer"] if intent else None)
                display_type = offer.get("display_type") or (intent["display_type"] if intent else None)
                if not display_type:
                    raw_type = str(offer.get("offer_type", "LIMIT")).upper()
                    if raw_type == "FRRDELTAVAR":
                        display_type = "FRR" if D(offer.get("rate", 0)) == 0 else "FRR_DELTA_VARIABLE"
                    elif raw_type == "FRRDELTAFIX":
                        display_type = "FRR_DELTA_FIXED"
                    else:
                        display_type = raw_type
                plan_hash = offer.get("plan_hash") or (intent["plan_hash"] if intent else None)
                strategy_variant = offer.get("strategy_variant") or (
                    intent["strategy_variant"] if intent else "baseline"
                )
                connection.execute(
                    """INSERT INTO offers(
                        offer_id, currency, amount, amount_original, rate, rate_real, period,
                        offer_type, display_type, flags, status, managed, pool, layer, plan_hash,
                        strategy_variant, mts_created, mts_updated, last_seen_ms
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(offer_id) DO UPDATE SET amount=excluded.amount, rate=excluded.rate,
                        rate_real=excluded.rate_real, status=excluded.status, flags=excluded.flags,
                        amount_original=excluded.amount_original, period=excluded.period,
                        offer_type=excluded.offer_type,
                        managed=MAX(offers.managed, excluded.managed), pool=COALESCE(excluded.pool, offers.pool),
                        layer=COALESCE(offers.layer, excluded.layer),
                        display_type=COALESCE(excluded.display_type, offers.display_type),
                        plan_hash=COALESCE(offers.plan_hash, excluded.plan_hash), mts_updated=excluded.mts_updated,
                        strategy_variant=CASE WHEN excluded.managed=1 THEN excluded.strategy_variant
                                              ELSE offers.strategy_variant END,
                        last_seen_ms=excluded.last_seen_ms""",
                    (
                        offer_id,
                        str(offer.get("currency", "USD")).upper(),
                        _decimal_text(offer["amount"]),
                        _decimal_text(offer.get("amount_original", offer["amount"])),
                        _decimal_text(offer["rate"]),
                        None if offer.get("rate_real") is None else _decimal_text(offer["rate_real"]),
                        int(offer["period"]),
                        offer.get("offer_type", "LIMIT"),
                        display_type,
                        int(offer.get("flags", 0)),
                        offer.get("status", "ACTIVE"),
                        int(managed),
                        pool,
                        layer,
                        plan_hash,
                        strategy_variant,
                        offer.get("mts_created"),
                        offer.get("mts_updated"),
                        seen_at,
                    ),
                )
            if ids:
                placeholders = ",".join("?" for _ in ids)
                connection.execute(
                    f"UPDATE offers SET status='CLOSED' WHERE status != 'CLOSED' AND offer_id NOT IN ({placeholders})",
                    tuple(ids),
                )
                connection.execute(
                    f"""UPDATE order_intents SET state='CLOSED', updated_at_ms=?
                        WHERE state='CONFIRMED' AND exchange_offer_id NOT IN ({placeholders})""",
                    (seen_at, *ids),
                )
            else:
                connection.execute("UPDATE offers SET status='CLOSED' WHERE status != 'CLOSED'")
                connection.execute(
                    "UPDATE order_intents SET state='CLOSED', updated_at_ms=? WHERE state='CONFIRMED'",
                    (seen_at,),
                )
        with self.transaction(immediate=True) as connection:
            connection.execute("""UPDATE manual_offer_protection SET state='CLOSED',updated_at_ms=?
                WHERE state!='CLOSED' AND offer_id IN (SELECT offer_id FROM offers WHERE status='CLOSED')""",
                               (seen_at,))
        return self.offers(active_only=True)

    def offers(self, active_only=False):
        query = "SELECT * FROM offers" + (" WHERE status != 'CLOSED'" if active_only else "") + " ORDER BY offer_id"
        with self.read_connection() as connection:
            return [dict(row) for row in connection.execute(query).fetchall()]

    def adopt_external_offers(self, offers, strategy_version, now_ms=None, safety_reason=None):
        """Persist explicit preflight-approved ownership without touching the exchange."""
        allowed = {"disabled_type", "hidden_disabled", "period_not_allowed", "below_new_floor", "funding_cap_exceeded"}
        if safety_reason and not set(safety_reason.split("+")).issubset(allowed):
            raise ValueError("unsupported manual protection override")
        adopted = []
        now = self._now_ms() if now_ms is None else int(now_ms)
        with self.transaction(immediate=True) as connection:
            normalized = {int(value.get("id") or value.get("offer_id")): value for value in offers or []}
            for raw_id in sorted(normalized):
                protection = self._observe_manual_offer(connection, normalized[raw_id], now)
                confirmed = connection.execute(
                    "SELECT state FROM external_takeover_state WHERE offer_id=?", (raw_id,)
                ).fetchone()
                if ((protection and protection["protected"] and not safety_reason)
                        or not confirmed or confirmed["state"] != 'CONFIRMED'):
                    continue
                offer = connection.execute(
                    "SELECT * FROM offers WHERE offer_id=? AND status!='CLOSED'", (raw_id,)
                ).fetchone()
                if offer is None:
                    source = normalized[raw_id]
                    connection.execute(
                        """INSERT INTO offers(offer_id, currency, amount, amount_original, rate, rate_real,
                               period, offer_type, display_type, flags, status, managed, pool, layer,
                               plan_hash, strategy_variant, mts_created, mts_updated, last_seen_ms)
                           VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, 'baseline', ?, ?, ?)""",
                        (
                            raw_id,
                            str(source.get("currency", "USD")).upper(),
                            _decimal_text(source["amount"]),
                            _decimal_text(source.get("amount_original") or source["amount"]),
                            _decimal_text(source["rate"]),
                            None if source.get("rate_real") is None else _decimal_text(source["rate_real"]),
                            int(source["period"]),
                            source.get("offer_type", "LIMIT"),
                            source.get("display_type"),
                            int(source.get("flags", 0)),
                            source.get("status", "ACTIVE"),
                            source.get("pool"),
                            source.get("mts_created"),
                            source.get("mts_updated"),
                            now,
                        ),
                    )
                    offer = connection.execute("SELECT * FROM offers WHERE offer_id=?", (raw_id,)).fetchone()
                if offer["managed"]:
                    continue
                pool = offer["pool"] or "short"
                layer = offer["layer"] or "balanced"
                display_type = offer["display_type"] or offer["offer_type"]
                slice_key = f"adopted:{raw_id}"
                fingerprint = order_intent_fingerprint(
                    offer["currency"],
                    offer["amount"],
                    offer["rate_real"] or offer["rate"],
                    offer["period"],
                    offer["offer_type"],
                    offer["flags"],
                    strategy_version,
                    slice_key,
                )
                connection.execute(
                    """INSERT OR IGNORE INTO order_intents(
                           fingerprint, currency, slice_key, pool, layer, amount, submitted_rate,
                           effective_rate, period, offer_type, display_type, flags, strategy_version,
                           plan_hash, state, exchange_offer_id, write_phase, resolution,
                           request_started_at_ms, resolved_at_ms, strategy_variant, pricing_curve_version,
                           created_at_ms, updated_at_ms
                       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'CONFIRMED', ?,
                                'CONFIRMED', 'PREFLIGHT_ADOPTED', ?, ?, 'baseline', 'LEGACY', ?, ?)""",
                    (
                        fingerprint,
                        offer["currency"],
                        slice_key,
                        pool,
                        layer,
                        offer["amount"],
                        offer["rate"],
                        offer["rate_real"] or offer["rate"],
                        offer["period"],
                        offer["offer_type"],
                        display_type,
                        offer["flags"],
                        str(strategy_version),
                        raw_id,
                        now,
                        now,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    """UPDATE offers SET managed=1, pool=?, layer=?, display_type=?,
                              strategy_variant='baseline' WHERE offer_id=?""",
                    (pool, layer, display_type, raw_id),
                )
                connection.execute(
                    """INSERT INTO ownership_events(offer_id, credit_id, event_type, details_json, created_at_ms)
                       VALUES(?, NULL, 'ADOPT_EXTERNAL', ?, ?)""",
                    (raw_id, json.dumps({"strategyVersion": str(strategy_version)}, sort_keys=True), now),
                )
                connection.execute(
                    """INSERT INTO external_takeover_state(
                           offer_id, state, snapshot_digest, first_seen_ms,
                           confirmed_at_ms, updated_at_ms, last_error
                       ) VALUES(?, 'ADOPTED', ?, ?, ?, ?, NULL)
                       ON CONFLICT(offer_id) DO UPDATE SET
                           state='ADOPTED', snapshot_digest=excluded.snapshot_digest,
                           confirmed_at_ms=excluded.confirmed_at_ms,
                           updated_at_ms=excluded.updated_at_ms, last_error=NULL""",
                    (raw_id, self._takeover_digest(normalized[raw_id]), now, now, now),
                )
                adopted.append(raw_id)
        return adopted

    def record_ownership_event(self, event_type, offer_id=None, credit_id=None, details=None):
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO ownership_events(offer_id, credit_id, event_type, details_json, created_at_ms)
                   VALUES(?, ?, ?, ?, ?)""",
                (
                    None if offer_id is None else int(offer_id),
                    None if credit_id is None else int(credit_id),
                    str(event_type),
                    json.dumps(details or {}, sort_keys=True),
                    self._now_ms(),
                ),
            )

    def record_reprice(
        self,
        offer_id,
        reason,
        old_rate=None,
        new_rate=None,
        intent_id=None,
        created_at_ms=None,
        strategy_version=None,
        plan_hash=None,
        display_type=None,
        chain_key=None,
        stage=None,
        benchmark_rate=None,
        floor_rate=None,
    ):
        created = int(created_at_ms if created_at_ms is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT INTO reprice_events(
                    offer_id, intent_id, reason, old_rate, new_rate,
                    strategy_version, plan_hash, display_type, chain_key,
                    stage, benchmark_rate, floor_rate, created_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    None if offer_id is None else int(offer_id),
                    None if intent_id is None else int(intent_id),
                    str(reason),
                    None if old_rate is None else _decimal_text(old_rate),
                    None if new_rate is None else _decimal_text(new_rate),
                    None if strategy_version is None else str(strategy_version),
                    plan_hash,
                    display_type,
                    chain_key,
                    None if stage is None else int(stage),
                    None if benchmark_rate is None else _decimal_text(benchmark_rate),
                    None if floor_rate is None else _decimal_text(floor_rate),
                    created,
                ),
            )

    def reprice_count_since(self, since_ms):
        with self.read_connection() as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM reprice_events WHERE created_at_ms >= ?", (int(since_ms),)
                ).fetchone()["count"]
            )

    def last_reprice_for_family(self, pool, layer):
        with self.read_connection() as connection:
            row = connection.execute(
                """SELECT MAX(events.created_at_ms) AS last_reprice
                   FROM reprice_events AS events
                   JOIN offers ON offers.offer_id = events.offer_id
                   WHERE offers.pool = ? AND offers.layer = ?""",
                (str(pool), str(layer)),
            ).fetchone()
        return None if row is None else row["last_reprice"]

    def reconcile_credits(self, credits, seen_at_ms=None):
        seen_at = int(seen_at_ms if seen_at_ms is not None else self._now_ms())
        ids = []
        with self.transaction(immediate=True) as connection:
            for credit in credits or []:
                credit_id = int(credit["id"])
                ids.append(credit_id)
                offer_id = credit.get("offer_id")
                managed = bool(credit.get("managed", False))
                inferred_pool = credit.get("pool")
                inferred_layer = credit.get("layer")
                inferred_display_type = credit.get("display_type")
                inferred_variant = credit.get("strategy_variant") or "baseline"
                attribution_state = str(credit.get("attribution_state") or "EXTERNAL").upper()
                if offer_id is None and credit.get("mts_opening"):
                    candidates = connection.execute(
                        """SELECT trades.offer_id, trades.amount, intents.pool, intents.layer,
                                  intents.display_type, intents.strategy_variant
                           FROM funding_trades AS trades
                           JOIN order_intents AS intents ON intents.exchange_offer_id = trades.offer_id
                           WHERE trades.period = ? AND ABS(trades.mts - ?) <= 300000
                           ORDER BY ABS(trades.mts - ?)""",
                        (int(credit["period"]), int(credit["mts_opening"]), int(credit["mts_opening"])),
                    ).fetchall()
                    for candidate in candidates:
                        if abs(D(candidate["amount"]) - D(credit["amount"])) <= D("0.00000001"):
                            offer_id = candidate["offer_id"]
                            inferred_pool = candidate["pool"]
                            inferred_layer = candidate["layer"]
                            inferred_display_type = candidate["display_type"]
                            inferred_variant = candidate["strategy_variant"]
                            break
                if offer_id is None and credit.get("mts_opening"):
                    direct = connection.execute(
                        """SELECT intents.exchange_offer_id, intents.pool, intents.layer,
                                  intents.display_type, intents.strategy_variant
                           FROM order_intents AS intents
                           LEFT JOIN offers ON offers.offer_id = intents.exchange_offer_id
                           WHERE intents.state IN ('CONFIRMED', 'CLOSED')
                             AND intents.currency = ? AND intents.period = ? AND intents.amount = ?
                             AND ABS(CAST(intents.effective_rate AS REAL) - ?) <= 0.0000000001
                             AND intents.updated_at_ms BETWEEN ? AND ?
                             AND (offers.offer_id IS NULL OR offers.status = 'CLOSED')""",
                        (
                            str(credit.get("currency", "USD")).upper(),
                            int(credit["period"]),
                            _decimal_text(credit["amount"]),
                            float(D(credit.get("rate_real") or credit["rate"])),
                            int(credit["mts_opening"]) - 300_000,
                            int(credit["mts_opening"]) + 300_000,
                        ),
                    ).fetchall()
                    if len(direct) == 1:
                        candidate = direct[0]
                        offer_id = candidate["exchange_offer_id"]
                        inferred_pool = candidate["pool"]
                        inferred_layer = candidate["layer"]
                        inferred_display_type = candidate["display_type"]
                        inferred_variant = candidate["strategy_variant"]
                    elif direct:
                        attribution_state = "ATTRIBUTION_PENDING"
                if offer_id is not None:
                    intent = connection.execute(
                        "SELECT display_type, strategy_variant FROM order_intents WHERE exchange_offer_id = ?",
                        (int(offer_id),),
                    ).fetchone()
                    managed = managed or intent is not None
                    if intent is not None:
                        inferred_display_type = inferred_display_type or intent["display_type"]
                        inferred_variant = intent["strategy_variant"]
                attribution_state = "MANAGED" if managed else attribution_state
                raw_rate_type = str(credit.get("rate_type") or "").upper()
                if not inferred_display_type:
                    if raw_rate_type in {"FIXED", "LIMIT"}:
                        inferred_display_type = "LIMIT"
                    elif raw_rate_type in {"VAR", "VARIABLE", "FRRDELTAVAR"}:
                        inferred_display_type = "VARIABLE_UNKNOWN"
                connection.execute(
                    """INSERT INTO credits(
                        credit_id, currency, amount, rate, rate_real, period, rate_type, display_type, hidden,
                        status, managed, pool, layer, offer_id, strategy_variant, attribution_state,
                        mts_opening, mts_updated, last_seen_ms
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(credit_id) DO UPDATE SET amount=excluded.amount, rate=excluded.rate,
                        rate_real=excluded.rate_real, status=excluded.status,
                        managed=MAX(credits.managed, excluded.managed), last_seen_ms=excluded.last_seen_ms,
                        pool=COALESCE(credits.pool, excluded.pool),
                        layer=COALESCE(credits.layer, excluded.layer),
                        display_type=COALESCE(credits.display_type, excluded.display_type),
                        offer_id=COALESCE(credits.offer_id, excluded.offer_id),
                        strategy_variant=CASE WHEN excluded.managed=1 THEN excluded.strategy_variant
                                              ELSE credits.strategy_variant END,
                        attribution_state=CASE WHEN excluded.managed=1 THEN 'MANAGED'
                                               WHEN credits.managed=1 THEN 'MANAGED'
                                               ELSE excluded.attribution_state END,
                        mts_updated=excluded.mts_updated""",
                    (
                        credit_id,
                        str(credit.get("currency", "USD")).upper(),
                        _decimal_text(credit["amount"]),
                        _decimal_text(credit["rate"]),
                        None if credit.get("rate_real") is None else _decimal_text(credit["rate_real"]),
                        int(credit["period"]),
                        credit.get("rate_type"),
                        inferred_display_type,
                        int(bool(credit.get("hidden", False))),
                        credit.get("status", "ACTIVE"),
                        int(managed),
                        inferred_pool,
                        inferred_layer,
                        offer_id,
                        inferred_variant,
                        attribution_state,
                        credit.get("mts_opening"),
                        credit.get("mts_updated"),
                        seen_at,
                    ),
                )
            if ids:
                placeholders = ",".join("?" for _ in ids)
                closing = connection.execute(
                    f"SELECT * FROM credits WHERE status != 'CLOSED' AND credit_id NOT IN ({placeholders})",
                    tuple(ids),
                ).fetchall()
                for row in closing:
                    connection.execute(
                        """INSERT OR IGNORE INTO credit_closures(
                            credit_id, currency, amount, rate, period, rate_type, hidden,
                            managed, pool, opened_at_ms, closed_at_ms
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            row["credit_id"],
                            row["currency"],
                            row["amount"],
                            row["rate_real"] or row["rate"],
                            row["period"],
                            row["rate_type"],
                            row["hidden"],
                            row["managed"],
                            row["pool"],
                            row["mts_opening"],
                            seen_at,
                        ),
                    )
                connection.execute(
                    "UPDATE credits SET status='CLOSED' WHERE status != 'CLOSED' "
                    f"AND credit_id NOT IN ({placeholders})",
                    tuple(ids),
                )
            else:
                closing = connection.execute("SELECT * FROM credits WHERE status != 'CLOSED'").fetchall()
                for row in closing:
                    connection.execute(
                        """INSERT OR IGNORE INTO credit_closures(
                            credit_id, currency, amount, rate, period, rate_type, hidden,
                            managed, pool, opened_at_ms, closed_at_ms
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            row["credit_id"],
                            row["currency"],
                            row["amount"],
                            row["rate_real"] or row["rate"],
                            row["period"],
                            row["rate_type"],
                            row["hidden"],
                            row["managed"],
                            row["pool"],
                            row["mts_opening"],
                            seen_at,
                        ),
                    )
                connection.execute("UPDATE credits SET status='CLOSED' WHERE status != 'CLOSED'")

    def credits(self, active_only=False):
        query = "SELECT * FROM credits" + (" WHERE status != 'CLOSED'" if active_only else "") + " ORDER BY credit_id"
        with self.read_connection() as connection:
            return [dict(row) for row in connection.execute(query).fetchall()]

    def import_legacy_managed_offers(self, path):
        if not path or not os.path.exists(path):
            return 0
        try:
            with open(path, "r", encoding="utf-8") as source:
                payload = json.load(source)
        except (OSError, ValueError):
            return 0
        imported = 0
        now = self._now_ms()
        with self.transaction(immediate=True) as connection:
            for raw_id, metadata in (payload.get("offers") or {}).items():
                try:
                    offer_id = int(raw_id)
                except (TypeError, ValueError):
                    continue
                bucket = str(metadata.get("bucket") or "balanced")
                pool = "long" if bucket == "long" else "short"
                layer = "quick" if bucket == "fast" else "balanced" if bucket == "balanced" else "high"
                connection.execute(
                    """INSERT INTO offers(
                        offer_id, currency, amount, rate, period, offer_type, flags, status,
                        managed, pool, layer, mts_created, last_seen_ms
                    ) VALUES(?, ?, '0', '0', 2, ?, 0, 'LEGACY_PENDING_SYNC', 1, ?, ?, ?, ?)
                    ON CONFLICT(offer_id) DO UPDATE SET managed=1, pool=COALESCE(excluded.pool, offers.pool),
                        layer=COALESCE(offers.layer, excluded.layer)""",
                    (
                        offer_id,
                        str(metadata.get("currency", "USD")).upper(),
                        metadata.get("offerType", "LIMIT"),
                        pool,
                        layer,
                        metadata.get("createdAt"),
                        now,
                    ),
                )
                imported += 1
        return imported

    def record_rate_floor_violations(self, violations, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        active = {int(row["credit_id"]): row for row in violations or []}
        with self.transaction(immediate=True) as connection:
            open_rows = {
                int(row["credit_id"]): row
                for row in connection.execute(
                    "SELECT * FROM rate_floor_violations WHERE ended_at_ms IS NULL"
                ).fetchall()
            }
            for credit_id, row in active.items():
                if credit_id not in open_rows:
                    connection.execute(
                        """INSERT INTO rate_floor_violations(
                            credit_id, pool, floor_rate, observed_rate, started_at_ms
                        ) VALUES(?, ?, ?, ?, ?)""",
                        (
                            credit_id,
                            str(row["pool"]),
                            _decimal_text(row["floor_rate"]),
                            _decimal_text(row["observed_rate"]),
                            now,
                        ),
                    )
                else:
                    connection.execute(
                        """UPDATE rate_floor_violations SET observed_rate=?
                           WHERE credit_id=? AND ended_at_ms IS NULL""",
                        (_decimal_text(row["observed_rate"]), credit_id),
                    )
            closed_ids = [credit_id for credit_id in open_rows if credit_id not in active]
            if closed_ids:
                placeholders = ",".join("?" for _ in closed_ids)
                connection.execute(
                    "UPDATE rate_floor_violations SET ended_at_ms=? "
                    "WHERE ended_at_ms IS NULL "
                    f"AND credit_id IN ({placeholders})",
                    (now, *closed_ids),
                )
        return len(active)

    def upsert_offer_history(self, offers):
        with self.transaction(immediate=True) as connection:
            for offer in offers or []:
                managed = (
                    connection.execute(
                        "SELECT 1 FROM order_intents WHERE exchange_offer_id=?", (int(offer["id"]),)
                    ).fetchone()
                    is not None
                )
                connection.execute(
                    """INSERT OR REPLACE INTO offer_history(
                        offer_id, currency, amount, amount_original, rate, rate_real,
                        period, offer_type, flags, status, mts_created, mts_updated, managed
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(offer["id"]),
                        str(offer.get("currency", "USD")).upper(),
                        _decimal_text(offer["amount"]),
                        _decimal_text(offer.get("amount_original", offer["amount"])),
                        _decimal_text(offer["rate"]),
                        None if offer.get("rate_real") is None else _decimal_text(offer["rate_real"]),
                        int(offer["period"]),
                        str(offer.get("offer_type", "LIMIT")),
                        int(offer.get("flags", 0)),
                        str(offer.get("status", "UNKNOWN")),
                        offer.get("mts_created"),
                        offer.get("mts_updated"),
                        int(managed),
                    ),
                )

    def upsert_credit_history(self, credits):
        with self.transaction(immediate=True) as connection:
            for credit in credits or []:
                managed = False
                opening = credit.get("mts_opening")
                if opening:
                    matches = connection.execute(
                        """SELECT 1 FROM funding_trades AS trades
                           JOIN order_intents AS intents ON intents.exchange_offer_id=trades.offer_id
                           WHERE trades.period=? AND ABS(trades.mts-?)<=300000 AND trades.amount=? LIMIT 1""",
                        (int(credit["period"]), int(opening), _decimal_text(credit["amount"])),
                    ).fetchone()
                    managed = matches is not None
                connection.execute(
                    """INSERT OR REPLACE INTO credit_history(
                        credit_id, currency, amount, rate, rate_real, period, rate_type,
                        hidden, status, mts_opening, mts_updated, managed
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(credit["id"]),
                        str(credit.get("currency", "USD")).upper(),
                        _decimal_text(credit["amount"]),
                        _decimal_text(credit["rate"]),
                        None if credit.get("rate_real") is None else _decimal_text(credit["rate_real"]),
                        int(credit["period"]),
                        credit.get("rate_type"),
                        int(bool(credit.get("hidden"))),
                        str(credit.get("status", "UNKNOWN")),
                        opening,
                        credit.get("mts_updated"),
                        int(managed),
                    ),
                )

    def upsert_market_trades(self, trades, *, defer_if_busy=False):
        """Ingest a batch exactly once, atomically marking changed minute bins.

        Positive legacy ``amount`` values do not prove trade direction.  Only
        an explicitly supplied raw signed value (or a still-negative amount)
        can fill that field; a later duplicate may enrich an unknown direction.
        """
        rows_by_id = {}
        for trade in trades or []:
            amount = D(trade["amount"])
            raw = trade.get("raw_signed_amount")
            if raw is None and amount < 0:
                raw = amount
            row = (
                str(trade["id"]), int(trade["mts"]), _decimal_text(abs(amount)),
                _decimal_text(trade["rate"]), int(trade["period"]),
                None if raw is None else _decimal_text(raw),
            )
            previous = rows_by_id.get(row[0])
            if previous is None or (previous[5] is None and row[5] is not None):
                rows_by_id[row[0]] = row
        if not rows_by_id:
            return {"inserted": 0, "updated": 0, "sourceSequence": self.market_source_sequence(), "deferred": False}
        try:
            with self.transaction(immediate=True, timeout_seconds=0.05 if defer_if_busy else None) as connection:
                existing = {}
                ids = list(rows_by_id)
                for offset in range(0, len(ids), 500):
                    chunk = ids[offset:offset + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    existing.update({
                        row["trade_id"]: row for row in connection.execute(
                            "SELECT trade_id,mts,raw_signed_amount FROM market_trades "
                            f"WHERE trade_id IN ({placeholders})",
                            chunk,
                        )
                    })
                changed = [
                    row for key, row in rows_by_id.items()
                    if key not in existing or (existing[key]["raw_signed_amount"] is None and row[5] is not None)
                ]
                sequence = int(connection.execute(
                    "SELECT value FROM schema_meta WHERE key='market_source_sequence'"
                ).fetchone()[0])
                inserted = sum(row[0] not in existing for row in changed)
                if changed:
                    sequence += 1
                    connection.executemany(
                        """INSERT INTO market_trades(
                               trade_id,mts,amount,rate,period,raw_signed_amount,source_sequence
                           ) VALUES(?,?,?,?,?,?,?)
                           ON CONFLICT(trade_id) DO UPDATE SET
                               raw_signed_amount=excluded.raw_signed_amount,
                               source_sequence=excluded.source_sequence
                           WHERE market_trades.raw_signed_amount IS NULL
                             AND excluded.raw_signed_amount IS NOT NULL""",
                        [(*row, sequence) for row in changed],
                    )
                    minutes = {
                        (existing[row[0]]["mts"] if row[0] in existing else row[1]) // 60_000 * 60_000
                        for row in changed
                    }
                    connection.executemany(
                        """INSERT INTO market_dirty_minutes(minute_ms,source_sequence) VALUES(?,?)
                           ON CONFLICT(minute_ms) DO UPDATE SET source_sequence=excluded.source_sequence""",
                        [(minute, sequence) for minute in sorted(minutes)],
                    )
                    connection.execute(
                        "UPDATE schema_meta SET value=? WHERE key='market_source_sequence'", (str(sequence),)
                    )
                return {
                    "inserted": inserted, "updated": len(changed) - inserted,
                    "sourceSequence": sequence, "deferred": False,
                }
        except sqlite3.OperationalError as error:
            if not defer_if_busy or not any(word in str(error).lower() for word in ("busy", "locked")):
                raise
            return {"inserted": 0, "updated": 0, "sourceSequence": None, "deferred": True}

    @staticmethod
    def _market_trade_dict(row):
        return {
            "id": row["trade_id"], "mts": row["mts"], "amount": D(row["amount"]),
            "rate": D(row["rate"]), "period": row["period"],
            "raw_signed_amount": None if row["raw_signed_amount"] is None else D(row["raw_signed_amount"]),
            "sourceSequence": row["source_sequence"],
        }

    def iter_market_trades(self, start_ms=0, end_ms=None, batch_size=2000):
        """Stream individual raw records from one consistent half-open window."""
        if int(batch_size) <= 0:
            raise ValueError("batch_size must be positive")
        query = "SELECT * FROM market_trades WHERE mts>=?"
        params = [int(start_ms)]
        if end_ms is not None:
            query += " AND mts<?"
            params.append(int(end_ms))
        query += " ORDER BY mts,trade_id"
        with self.read_connection() as connection:
            connection.execute("BEGIN")
            cursor = connection.execute(query, params)
            while True:
                rows = cursor.fetchmany(int(batch_size))
                if not rows:
                    break
                for row in rows:
                    yield self._market_trade_dict(row)

    def market_source_sequence(self):
        with self.read_connection() as connection:
            return int(connection.execute(
                "SELECT value FROM schema_meta WHERE key='market_source_sequence'"
            ).fetchone()[0])

    def dirty_market_minutes(self, since_sequence=0, start_ms=0, end_ms=None):
        query = "SELECT minute_ms,source_sequence FROM market_dirty_minutes WHERE source_sequence>? AND minute_ms>=?"
        params = [int(since_sequence), int(start_ms) // 60_000 * 60_000]
        if end_ms is not None:
            query += " AND minute_ms<?"
            params.append(int(end_ms))
        query += " ORDER BY minute_ms"
        with self.read_connection() as connection:
            return [
                {"minuteMs": row["minute_ms"], "minute_ms": row["minute_ms"],
                 "sourceSequence": row["source_sequence"]}
                for row in connection.execute(query, params)
            ]

    def record_market_coverage(self, start_ms, end_ms, status="COMPLETE", source="REST", details=None,
                               *, defer_if_busy=False):
        """Record proven traversal of [start,end), including verified empty spans.

        Merely receiving trades never creates coverage.  Failed/ambiguous REST
        intervals remain visible until a later complete traversal proves them.
        """
        start, end = int(start_ms), int(end_ms)
        if end < start:
            raise ValueError("coverage end must be at or after start")
        state = str(status).upper()
        try:
            with self.transaction(immediate=True, timeout_seconds=0.05 if defer_if_busy else None) as connection:
                cursor = connection.execute(
                    """INSERT INTO market_coverage_intervals(
                           start_ms,end_ms,status,source,details_json,recorded_at_ms
                       ) VALUES(?,?,?,?,?,?)""",
                    (start, end, state, str(source),
                     None if details is None else json.dumps(details, ensure_ascii=False, sort_keys=True, default=str),
                     self._now_ms()),
                )
                connection.execute(
                    "UPDATE schema_meta SET value=CAST(value AS INTEGER)+1 WHERE key='market_source_sequence'"
                )
                return {"id": cursor.lastrowid, "deferred": False}
        except sqlite3.OperationalError as error:
            if not defer_if_busy or not any(word in str(error).lower() for word in ("busy", "locked")):
                raise
            return {"id": None, "deferred": True}

    def market_coverage(self, start_ms, end_ms):
        start, end = int(start_ms), int(end_ms)
        if end < start:
            raise ValueError("coverage end must be at or after start")
        with self.read_connection() as connection:
            rows = connection.execute(
                """SELECT * FROM market_coverage_intervals
                   WHERE start_ms<? AND end_ms>? ORDER BY start_ms,id""", (end, start),
            ).fetchall()
        merged = []
        for row in rows:
            if row["status"] != "COMPLETE":
                continue
            left, right = max(start, row["start_ms"]), min(end, row["end_ms"])
            if right <= left:
                continue
            if merged and left <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], right)
            else:
                merged.append([left, right])

        def interval(left, right, status):
            return {"startMs": left, "endMs": right, "start_ms": left, "end_ms": right, "status": status}

        unresolved = []
        cursor = start
        gaps = []
        for left, right in merged:
            if cursor < left:
                gaps.append((cursor, left))
            cursor = right
        if cursor < end:
            gaps.append((cursor, end))
        failures = [row for row in rows if row["status"] != "COMPLETE"]
        for left, right in gaps:
            relevant = [row for row in failures if row["start_ms"] < right and row["end_ms"] > left]
            boundaries = sorted({left, right, *(
                boundary for row in relevant for boundary in
                (max(left, row["start_ms"]), min(right, row["end_ms"]))
            )})
            for section_start, section_end in zip(boundaries, boundaries[1:]):
                latest = max(
                    (row for row in relevant if row["start_ms"] <= section_start and row["end_ms"] >= section_end),
                    key=lambda row: row["id"], default=None,
                )
                state = "UNVERIFIED" if latest is None else latest["status"]
                if unresolved and unresolved[-1]["endMs"] == section_start and unresolved[-1]["status"] == state:
                    unresolved[-1]["endMs"] = unresolved[-1]["end_ms"] = section_end
                else:
                    unresolved.append(interval(section_start, section_end, state))
        covered = [interval(left, right, "COMPLETE") for left, right in merged]
        return {
            "complete": not unresolved, "coveredDurationMs": sum(right - left for left, right in merged),
            "coveredIntervals": covered, "unresolvedIntervals": unresolved,
            "intervals": covered, "unresolved": unresolved,
        }

    def upsert_funding_stats(self, stats):
        with self.transaction(immediate=True) as connection:
            for row in stats or []:
                mts = int(row.get("mts") or row.get("timestamp") or 0)
                if mts <= 0:
                    continue
                connection.execute(
                    """INSERT INTO funding_stats(mts, frr_daily_rate, utilization, payload_json)
                       VALUES(?, ?, ?, ?)
                       ON CONFLICT(mts) DO UPDATE SET
                           frr_daily_rate=excluded.frr_daily_rate,
                           utilization=excluded.utilization,
                           payload_json=excluded.payload_json""",
                    (
                        mts,
                        None if row.get("frr_daily_rate") is None else _decimal_text(row["frr_daily_rate"]),
                        None if row.get("utilization") is None else _decimal_text(row["utilization"]),
                        json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
                    ),
                )

    def funding_stats(self, start_ms=0, end_ms=None):
        query = "SELECT payload_json FROM funding_stats WHERE mts >= ?"
        params = [int(start_ms)]
        if end_ms is not None:
            query += " AND mts <= ?"
            params.append(int(end_ms))
        query += " ORDER BY mts"
        with self.read_connection() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def record_book_snapshot(self, book, source="REST", now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        bucket = now - (now % 60_000)
        payload = json.dumps(book or [], ensure_ascii=False, sort_keys=True, default=str)
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT OR REPLACE INTO book_snapshots(mts, source, book_json, observed_at_ms)
                   VALUES(?, ?, ?, ?)""",
                (bucket, str(source), payload, now),
            )

    def book_snapshots(self, start_ms=0, end_ms=None):
        query = "SELECT * FROM book_snapshots WHERE mts >= ?"
        params = [int(start_ms)]
        if end_ms is not None:
            query += " AND mts <= ?"
            params.append(int(end_ms))
        query += " ORDER BY mts"
        with self.read_connection() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._book_snapshot_dict(row) for row in rows]

    @staticmethod
    def _book_snapshot_dict(row):
        return {
            **dict(row), "book": json.loads(row["book_json"]),
            "visible_at_ms": row["observed_at_ms"] if row["observed_at_ms"] is not None else row["mts"] + 60_000,
        }

    def iter_book_snapshots(self, start_ms=0, end_ms=None, batch_size=200):
        """Stream books when they became visible, never at an earlier bucket."""
        if int(batch_size) <= 0:
            raise ValueError("batch_size must be positive")
        query = "SELECT * FROM book_snapshots WHERE COALESCE(observed_at_ms,mts+60000)>=?"
        params = [int(start_ms)]
        if end_ms is not None:
            query += " AND COALESCE(observed_at_ms,mts+60000)<?"
            params.append(int(end_ms))
        query += " ORDER BY COALESCE(observed_at_ms,mts+60000),mts"
        with self.read_connection() as connection:
            connection.execute("BEGIN")
            cursor = connection.execute(query, params)
            while True:
                rows = cursor.fetchmany(int(batch_size))
                if not rows:
                    break
                for row in rows:
                    yield self._book_snapshot_dict(row)

    def book_snapshot_coverage(self, start_ms=0, end_ms=None):
        """Summarize research coverage without loading historical book payloads."""
        query = (
            "SELECT COUNT(*) AS count, MIN(mts) AS earliestMs, "
            "MAX(mts) AS latestMs FROM book_snapshots WHERE mts >= ?"
        )
        params = [int(start_ms)]
        if end_ms is not None:
            query += " AND mts <= ?"
            params.append(int(end_ms))
        with self.read_connection() as connection:
            return dict(connection.execute(query, tuple(params)).fetchone())

    def record_market_bars(self, windows, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        bucket = now - (now % 60_000)
        with self.transaction(immediate=True) as connection:
            for interval_name, values in (windows or {}).items():
                connection.execute(
                    """INSERT OR REPLACE INTO market_bars(
                        interval_name, bucket_ms, weighted_median_rate, q25_rate,
                        q75_rate, volume, trade_count
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    (
                        str(interval_name),
                        bucket,
                        _decimal_text(values.get("median", 0)),
                        _decimal_text(values.get("q25", 0)),
                        _decimal_text(values.get("q75", 0)),
                        _decimal_text(values.get("volume", 0)),
                        int(values.get("count", 0)),
                    ),
                )

    def market_trades(self, start_ms=0, end_ms=None):
        query = "SELECT * FROM market_trades WHERE mts >= ?"
        params = [int(start_ms)]
        if end_ms is not None:
            query += " AND mts <= ?"
            params.append(int(end_ms))
        query += " ORDER BY mts"
        with self.read_connection() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        return [self._market_trade_dict(row) for row in rows]

    def prune_market_data(self, retention_days=90, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        threshold = now - int(retention_days) * 86_400_000
        derived_threshold = max(threshold, now - 8 * 86_400_000)
        with self.transaction(immediate=True) as connection:
            trade_count = connection.execute("DELETE FROM market_trades WHERE mts < ?", (threshold,)).rowcount
            bar_count = connection.execute(
                "DELETE FROM market_bars WHERE bucket_ms < ?", (derived_threshold,)
            ).rowcount
            book_count = connection.execute("DELETE FROM book_snapshots WHERE mts < ?", (threshold,)).rowcount
            stats_count = connection.execute("DELETE FROM funding_stats WHERE mts < ?", (threshold,)).rowcount
            # Dirty bins are incremental runtime hints, not research evidence.
            connection.execute(
                "DELETE FROM market_dirty_minutes WHERE minute_ms < ?", (now - 8 * 86_400_000,)
            ).rowcount
            connection.execute(
                "DELETE FROM market_coverage_intervals WHERE end_ms <= ?", (threshold,)
            ).rowcount
            connection.execute(
                "UPDATE market_coverage_intervals SET start_ms=? WHERE start_ms<? AND end_ms>?",
                (threshold, threshold, threshold),
            )
        return {"trades": trade_count, "bars": bar_count, "books": book_count, "stats": stats_count}

    def repayment_calibration(self, start_ms, end_ms):
        """Return training-visible closed-credit evidence without future rows.

        A history closing timestamp is exchange evidence; snapshot disappearance
        is labeled separately because it only bounds the actual repayment time.
        """
        start, end = int(start_ms), int(end_ms)
        with self.read_connection() as connection:
            connection.execute("BEGIN")
            historical = connection.execute(
                """SELECT credit_id,amount,period,mts_opening,mts_updated FROM credit_history
                   WHERE currency='USD' AND UPPER(status) LIKE 'CLOSED%'
                     AND mts_updated>=? AND mts_updated<?
                     AND mts_opening IS NOT NULL AND mts_updated>mts_opening""", (start, end),
            ).fetchall()
            observed = connection.execute(
                """SELECT credit_id,amount,period,opened_at_ms,closed_at_ms FROM credit_closures
                   WHERE currency='USD' AND closed_at_ms>=? AND closed_at_ms<?
                     AND opened_at_ms IS NOT NULL AND closed_at_ms>opened_at_ms""", (start, end),
            ).fetchall()
        results = {
            row["credit_id"]: {
                "credit_id": row["credit_id"], "amount": D(row["amount"]), "period": row["period"],
                "opened_at_ms": row["opened_at_ms"], "closed_at_ms": row["closed_at_ms"],
                "source": "OBSERVED_CLOSE",
            } for row in observed
        }
        for row in historical:
            results[row["credit_id"]] = {
                "credit_id": row["credit_id"], "amount": D(row["amount"]), "period": row["period"],
                "opened_at_ms": row["mts_opening"], "closed_at_ms": row["mts_updated"],
                "source": "HISTORY_CLOSED",
            }
        return sorted(results.values(), key=lambda row: (row["closed_at_ms"], row["credit_id"]))

    def record_account_sample(self, total, wallet, offers, credits, net_interest, mts=None):
        mts = int(mts if mts is not None else self._now_ms())
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """INSERT OR REPLACE INTO account_samples(
                    mts, total_principal, wallet_available, open_offers, active_credits, net_interest_total
                ) VALUES(?, ?, ?, ?, ?, ?)""",
                (mts, *(_decimal_text(value) for value in (total, wallet, offers, credits, net_interest))),
            )

    def upsert_income_ledgers(self, rows, category=28):
        """Store authoritative interest ledger rows, keyed by Bitfinex ledger ID."""
        with self.transaction(immediate=True) as connection:
            for row in rows or []:
                connection.execute(
                    """INSERT INTO ledger_entries(
                        ledger_id, currency, wallet, amount, balance, description, category, mts
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ledger_id) DO UPDATE SET
                        currency=excluded.currency, wallet=excluded.wallet,
                        amount=excluded.amount, balance=excluded.balance,
                        description=excluded.description, category=excluded.category,
                        mts=excluded.mts""",
                    (
                        int(row["id"]),
                        str(row.get("currency") or "USD").upper(),
                        str(row.get("wallet") or "").lower() or None,
                        _decimal_text(row["amount"]),
                        None if row.get("balance") is None else _decimal_text(row["balance"]),
                        str(row.get("description") or ""),
                        int(category),
                        int(row["mts"]),
                    ),
                )

    def income_sync_state(self, currency="USD"):
        currency = str(currency).upper()
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM income_sync_state WHERE currency = ?", (currency,)).fetchone()
        if row is None:
            return {
                "currency": currency,
                "status": "PENDING",
                "next_end_ms": None,
                "earliest_mts": None,
                "last_success_ms": None,
                "completed_at_ms": None,
                "error": None,
                "updated_at_ms": None,
            }
        return dict(row)

    def update_income_sync_state(self, currency="USD", **values):
        allowed = {
            "status",
            "next_end_ms",
            "earliest_mts",
            "last_success_ms",
            "completed_at_ms",
            "error",
        }
        updates = {key: value for key, value in values.items() if key in allowed}
        if not updates:
            return self.income_sync_state(currency)
        currency = str(currency).upper()
        now = self._now_ms()
        assignments = ", ".join(f"{key} = ?" for key in updates)
        with self.transaction(immediate=True) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO income_sync_state(currency, status, updated_at_ms) VALUES(?, 'PENDING', ?)",
                (currency, now),
            )
            connection.execute(
                f"UPDATE income_sync_state SET {assignments}, updated_at_ms = ? WHERE currency = ?",
                (*updates.values(), now, currency),
            )
        return self.income_sync_state(currency)

    def realized_income(self, currency="USD", start_ms=None, end_ms=None):
        clauses = ["currency = ?", "wallet = 'funding'", "category = 28", "CAST(amount AS REAL) > 0"]
        params = [str(currency).upper()]
        if start_ms is not None:
            clauses.append("mts >= ?")
            params.append(int(start_ms))
        if end_ms is not None:
            clauses.append("mts <= ?")
            params.append(int(end_ms))
        with self.read_connection() as connection:
            rows = connection.execute(
                f"SELECT amount FROM ledger_entries WHERE {' AND '.join(clauses)}", tuple(params)
            ).fetchall()
        return sum((D(row["amount"]) for row in rows), D("0"))

    def realized_income_summary(self, currency="USD", now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        local = time.localtime(now / 1000)
        midnight = int(
            time.mktime(
                (
                    local.tm_year,
                    local.tm_mon,
                    local.tm_mday,
                    0,
                    0,
                    0,
                    local.tm_wday,
                    local.tm_yday,
                    local.tm_isdst,
                )
            )
            * 1000
        )
        return {
            "today": _decimal_text(self.realized_income(currency, midnight, now)),
            "thirtyDays": _decimal_text(self.realized_income(currency, now - 30 * 86_400_000, now)),
            "lifetime": _decimal_text(self.realized_income(currency, None, now)),
            "currency": str(currency).upper(),
        }

    def income_history_sync_payload(self, currency="USD"):
        state = self.income_sync_state(currency)
        return {
            "status": state["status"],
            "earliestMts": state["earliest_mts"],
            "lastSuccess": state["last_success_ms"],
            "error": state["error"],
        }

    def statistics(self, window_days=None, now_ms=None):
        now = int(now_ms if now_ms is not None else self._now_ms())
        start = 0 if window_days is None else now - int(window_days) * 86_400_000
        with self.read_connection() as connection:
            connection.execute("BEGIN")
            rows = connection.execute("SELECT * FROM account_samples WHERE mts >= ? ORDER BY mts", (start,)).fetchall()
            reprices = connection.execute(
                "SELECT COUNT(*) AS count FROM reprice_events WHERE created_at_ms >= ?", (start,)
            ).fetchone()["count"]
            waits = self._funding_wait_records(connection, "USD", start, now + 1)
            closures = connection.execute(
                "SELECT * FROM credit_closures WHERE closed_at_ms >= ? ORDER BY closed_at_ms",
                (start,),
            ).fetchall()
        attributed = {}
        early = 0
        for row in closures:
            opened = row["opened_at_ms"] or row["closed_at_ms"]
            elapsed_days = D(max(0, row["closed_at_ms"] - opened)) / D("86400000")
            contractual_days = D(max(1, row["period"]))
            if elapsed_days + D("0.000001") < contractual_days:
                early += 1
            fee = D("0.18") if row["hidden"] else D("0.15")
            earned = D(row["amount"]) * D(row["rate"]) * elapsed_days * (D("1") - fee)
            pool = row["pool"] if row["managed"] and row["pool"] else "EXTERNAL"
            order_type = row["rate_type"] if row["managed"] and row["rate_type"] else "EXTERNAL"
            key = f"{pool}:{order_type}"
            attributed[key] = attributed.get(key, D("0")) + earned
        details = {
            **self._funding_wait_summary(waits),
            "earlyReturnPercent": _decimal_text(D(early) / D(len(closures)) * D("100")) if closures else "0",
            "closedCreditCount": len(closures),
            "returnsByPoolAndType": {key: _decimal_text(value) for key, value in sorted(attributed.items())},
        }
        ledger_interest = self.realized_income("USD", None if window_days is None else start, now)
        if not rows:
            return {
                "windowDays": window_days,
                "sampleFrom": None,
                "sampleTo": None,
                "sampleDays": "0",
                "utilizationPercent": "0",
                "netInterest": _decimal_text(ledger_interest),
                "actualNetAprPercent": "0",
                "idlePrincipalTime": "0",
                "repriceCount": reprices,
                **details,
            }
        utilized_time = D("0")
        principal_time = D("0")
        idle_time = D("0")
        for index, row in enumerate(rows):
            end = rows[index + 1]["mts"] if index + 1 < len(rows) else now
            duration_days = D(max(0, end - row["mts"])) / D("86400000")
            total = D(row["total_principal"])
            utilized = D(row["active_credits"])
            principal_time += total * duration_days
            utilized_time += utilized * duration_days
            idle_time += max(D("0"), total - utilized) * duration_days
        # APR is estimated only for the period where principal samples exist.
        # The displayed realized income can cover a wider ledger interval.
        apr_interest = self.realized_income("USD", max(start, int(rows[0]["mts"])), now)
        sample_from = int(rows[0]["mts"])
        sample_to = int(rows[-1]["mts"])
        sample_days = D(max(0, sample_to - sample_from)) / D("86400000")
        utilization = D("0") if principal_time <= 0 else utilized_time / principal_time
        apr = D("0") if principal_time <= 0 else apr_interest / principal_time * D("365")
        return {
            "windowDays": window_days,
            "sampleFrom": sample_from,
            "sampleTo": sample_to,
            "sampleDays": _decimal_text(sample_days),
            "utilizationPercent": _decimal_text(utilization * D("100")),
            "netInterest": _decimal_text(ledger_interest),
            "actualNetAprPercent": _decimal_text(apr * D("100")),
            "idlePrincipalTime": _decimal_text(idle_time),
            "repriceCount": reprices,
            **details,
        }
