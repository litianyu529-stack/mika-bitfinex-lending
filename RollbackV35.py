"""Offline projection of USD records via a Schema 16 backup into the current V3 schema.

Run only after all lending and dashboard processes have stopped. Sources are
opened read-only; a failed migration removes only its newly created output.
"""

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from Configuration import strategy_v3_config_values
from StateStore import LendingStateStore
from StrategyV3 import StrategyPolicyV3, policy_v3_with_overrides, validate_policy_v3


@contextmanager
def readonly(path):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        yield connection
    finally:
        connection.close()


def migrate(source, legacy_backup, output):
    source, legacy_backup, output = map(lambda p: Path(p).resolve(), (source, legacy_backup, output))
    if output.exists() or output in (source, legacy_backup):
        raise ValueError("output must be a new, separate database")
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    try:
        with readonly(source) as current, readonly(legacy_backup) as original:
            version = original.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            if not version or version[0] != "16":
                raise ValueError("legacy backup must have Schema 16")
            for table in ("order_intents", "offers", "credits", "funding_trades", "ledger_entries"):
                if current.execute(f"SELECT 1 FROM \"{table}\" WHERE currency!='USD' LIMIT 1").fetchone():
                    raise ValueError("source contains non-USD records")
            if current.execute(
                "SELECT 1 FROM order_intents WHERE state IN ('PLANNED','SUBMITTING','AMBIGUOUS') LIMIT 1"
            ).fetchone():
                raise ValueError("unresolved writes must be reconciled before migration")
            if current.execute("SELECT 1 FROM runtime_state WHERE safe_manual=1 LIMIT 1").fetchone():
                raise ValueError("protected pause must be resolved before migration")
            unresolved_chain = current.execute(
                "SELECT 1 FROM reprice_chains c LEFT JOIN offers o ON o.offer_id=c.pending_source_offer_id "
                "WHERE c.pending_action IS NOT NULL AND c.status='ACTIVE' "
                "AND (c.current_offer_id IS NOT NULL OR COALESCE(o.status,'UNKNOWN')!='CLOSED') LIMIT 1"
            ).fetchone()
            if unresolved_chain:
                raise ValueError("unresolved repricing must be reconciled before migration")
            candidates = current.execute(
                "SELECT policy_json FROM strategy_versions WHERE activated_at_ms IS NOT NULL "
                "ORDER BY activated_at_ms DESC"
            )
            payload = next(
                (
                    p
                    for (raw,) in candidates
                    if (p := json.loads(raw)).get("strategy_engine", "legacy_v3") == "legacy_v3"
                ),
                None,
            )
            if payload is None:
                raise ValueError("no previously activated legacy policy")
            fields = vars(StrategyPolicyV3())
            policy = validate_policy_v3(
                policy_v3_with_overrides(StrategyPolicyV3(), {k: v for k, v in payload.items() if k in fields})
            )
            target = sqlite3.connect(output, uri=True)
            try:
                original.backup(target)
                target.execute("ATTACH DATABASE ? AS recent", (source.as_uri() + "?mode=ro",))
                tables = [
                    r[0]
                    for r in target.execute(
                        "SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                    )
                ]
                excluded = {"schema_meta", "runtime_state", "recovery_state", "strategy_versions"}
                with target:
                    for table in tables:
                        if table in excluded:
                            continue
                        old = {r[1] for r in target.execute(f'PRAGMA main.table_info("{table}")')}
                        columns = [r[1] for r in target.execute(f'PRAGMA recent.table_info("{table}")') if r[1] in old]
                        names = ",".join(f'"{c}"' for c in columns)
                        target.execute(f'DELETE FROM main."{table}"')
                        target.execute(f'INSERT INTO main."{table}" ({names}) SELECT {names} FROM recent."{table}"')
                        counts[table] = target.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]
                    target.execute("DELETE FROM strategy_versions")
                    for row in current.execute("SELECT * FROM strategy_versions"):
                        p = json.loads(row[2])
                        if p.get("strategy_engine", "legacy_v3") == "legacy_v3":
                            target.execute(
                                "INSERT INTO strategy_versions VALUES(?,?,?,?,?)",
                                (row[0], "ARCHIVED", row[2], row[3], row[4]),
                            )
                    target.execute(
                        "UPDATE runtime_state SET mode='PAUSED',previous_mode=NULL,safe_reason=NULL,"
                        "safe_manual=0,consistent_syncs=0"
                    )
                    target.execute("DELETE FROM recovery_state")
                    target.execute(
                        "INSERT INTO recovery_state(singleton,active,required_snapshots,manual_required,"
                        "resume_pending_cycle) VALUES(1,0,2,0,0)"
                    )
                    target.execute("DELETE FROM schema_meta WHERE key='v35_initialization_complete'")
                    # A confirmed closed source with no replacement/unknown intent
                    # is historical evidence, not authorization to submit today.
                    target.execute(
                        "UPDATE reprice_chains SET status='CLOSED' WHERE pending_action IS NOT NULL "
                        "AND current_offer_id IS NULL AND pending_source_offer_id IN "
                        "(SELECT offer_id FROM offers WHERE status='CLOSED')"
                    )
                    target.execute(
                        "UPDATE reprice_chains SET status='CLOSED' "
                        "WHERE strategy_version NOT IN (SELECT version_id FROM strategy_versions)"
                    )
                    target.execute(
                        "UPDATE order_intents SET pricing_curve_version='LEGACY',fixed_landing_rate=NULL "
                        "WHERE strategy_version NOT IN (SELECT version_id FROM strategy_versions)"
                    )
                    target.execute("INSERT OR REPLACE INTO schema_meta VALUES('v35_rollback_source',?)", (str(source),))
                if target.execute("PRAGMA foreign_key_check").fetchone():
                    raise ValueError("foreign key check failed")
            finally:
                target.close()
        store = LendingStateStore(output)
        payload = json.loads(json.dumps(vars(policy), default=str))
        version = store.save_strategy(payload, "ACTIVE")
        for offer in store.offers():
            if offer["managed"] and offer["status"] == "ACTIVE":
                chain = store.ensure_reprice_chain(offer, version)
                if chain and chain["started_at_ms"] > int(offer.get("mts_created") or chain["started_at_ms"]):
                    raise ValueError("order waiting clock was reset")
        store.set_mode("PAUSED", "v35_rollback_ready_for_preflight")
        return {
            "counts": counts,
            "strategyVersion": version,
            "strategyConfig": strategy_v3_config_values(policy),
            "output": str(output),
        }
    except Exception:
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(str(output) + suffix)
            except FileNotFoundError:
                pass
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "legacy-backup", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    print(json.dumps(migrate(args.source, args.legacy_backup, args.output), ensure_ascii=False))


if __name__ == "__main__":
    main()
