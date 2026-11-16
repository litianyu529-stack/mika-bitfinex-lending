"""Durable grace period for human funding offers, independent of strategy ownership."""

import hashlib
import json
from decimal import Decimal


MANUAL_PROTECTION_MS = 120 * 60_000


def parameter_digest(offer):
    # Remaining amount and effective FRR change naturally on fills/market ticks.
    values = [
        str(Decimal(str(offer.get("rate") or 0)).normalize()),
        str(Decimal(str(offer.get("amount_original", offer.get("amount", 0)))).normalize()),
        int(offer.get("period") or 0),
        str(offer.get("offer_type") or "LIMIT").upper(),
        int(offer.get("flags") or 0),
    ]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def operation_time(offer, now_ms):
    try:
        created = int(offer.get("mts_created") or 0)
        updated = int(offer.get("mts_updated") or created)
        stamp = max(created, updated)
        if 0 < stamp <= now_ms:
            return stamp, "EXCHANGE"
    except (ValueError, TypeError, OverflowError):
        pass
    return now_ms, "FIRST_OBSERVED"


def protection_payload(record, now_ms):
    if not record:
        return None
    pending = record["state"] == "WAITING"
    until = int(record["protected_until_ms"])
    return {
        "status": ("PROTECTED" if now_ms < until else "READY") if pending else record["state"],
        "protected": pending and now_ms < until,
        "needsTakeover": pending,
        "operatedAtMs": record["operated_at_ms"],
        "protectedUntilMs": until,
        "remainingSeconds": max(0, (until - now_ms + 999) // 1000) if pending else 0,
        "timeSource": record["time_source"],
        "reason": record.get("reason"),
    }


class ManualProtectionStore:
    @staticmethod
    def _migrate_v19_manual_protection(connection):
        connection.execute("""CREATE TABLE IF NOT EXISTS manual_offer_protection (
            offer_id INTEGER PRIMARY KEY,
            parameter_digest TEXT NOT NULL,
            operated_at_ms INTEGER NOT NULL,
            protected_until_ms INTEGER NOT NULL,
            time_source TEXT NOT NULL,
            state TEXT NOT NULL,
            reason TEXT,
            updated_at_ms INTEGER NOT NULL
        )""")

    def _manual_candidate(self, connection, offer, now_ms):
        offer_id = int(offer.get("id") or offer.get("offer_id"))
        row = connection.execute(
            "SELECT * FROM manual_offer_protection WHERE offer_id=?", (offer_id,)
        ).fetchone()
        previous = dict(row) if row else None
        digest = parameter_digest(offer)
        if previous and previous["parameter_digest"] == digest:
            return previous, False
        stored = connection.execute("SELECT * FROM offers WHERE offer_id=?", (offer_id,)).fetchone()
        intent = connection.execute(
            "SELECT * FROM order_intents WHERE exchange_offer_id=? ORDER BY id DESC LIMIT 1", (offer_id,)
        ).fetchone()
        if "amount_original" not in offer:
            original = stored["amount_original"] if stored else intent["amount"] if intent else offer["amount"]
            offer = dict(offer, amount_original=original)
            digest = parameter_digest(offer)
            if previous and previous["parameter_digest"] == digest:
                return previous, False
        managed = bool(offer.get("managed") or (stored and stored["managed"]) or intent)
        reference = dict(stored) if stored else None
        # On first post-upgrade observation, the old worker may already have
        # overwritten the snapshot. Its immutable submit intent still proves a change.
        try:
            created = int(offer.get("mts_created") or 0)
            updated = int(offer.get("mts_updated") or 0)
        except (TypeError, ValueError):
            created = updated = 0
        if (intent and not previous and updated > created > 0
                and (not reference or parameter_digest(reference) == digest)):
            reference = dict(intent, rate=intent["submitted_rate"], amount_original=intent["amount"])
        changed = bool(previous or (reference and parameter_digest(reference) != digest))
        if managed and not changed:
            return None, False
        operated_at, source = operation_time(offer, now_ms)
        # A material change with no newer exchange timestamp cannot shorten an
        # existing grace period (coarse/invalid timestamps or inconsistent reads).
        if previous and operated_at <= previous["operated_at_ms"]:
            operated_at, source = now_ms, "FIRST_OBSERVED"
        return {
            "offer_id": offer_id, "parameter_digest": digest,
            "operated_at_ms": operated_at, "protected_until_ms": operated_at + MANUAL_PROTECTION_MS,
            "time_source": source, "state": "WAITING", "reason": None, "updated_at_ms": now_ms,
        }, True

    def manual_protection_for_offer(self, offer, now_ms=None):
        """Preview against a live offer without mutating state (also used by preflight)."""
        now = self._now_ms() if now_ms is None else int(now_ms)
        with self.read_connection() as connection:
            record, _ = self._manual_candidate(connection, offer, now)
        return protection_payload(record, now)

    def _observe_manual_offer(self, connection, offer, now_ms):
        record, changed = self._manual_candidate(connection, offer, now_ms)
        if not changed:
            return protection_payload(record, now_ms)
        connection.execute("""INSERT INTO manual_offer_protection VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(offer_id) DO UPDATE SET
                parameter_digest=excluded.parameter_digest, operated_at_ms=excluded.operated_at_ms,
                protected_until_ms=excluded.protected_until_ms, time_source=excluded.time_source,
                state='WAITING', reason=NULL, updated_at_ms=excluded.updated_at_ms""", tuple(record.values()))
        offer_id = record["offer_id"]
        # Leave ownership/intents intact for attribution. Only retire scheduling
        # work; an already sent cancel is still reconciled by the existing paths.
        connection.execute("""UPDATE reprice_chains SET status='ABANDONED', pending_action=NULL,
            updated_at_ms=? WHERE (current_offer_id=? OR pending_source_offer_id=?) AND status='ACTIVE'""",
                           (now_ms, offer_id, offer_id))
        connection.execute("""DELETE FROM external_takeover_state WHERE offer_id=?
            AND state NOT IN ('CANCELLING','AMBIGUOUS')""", (offer_id,))
        connection.execute("""INSERT INTO ownership_events(offer_id,event_type,details_json,created_at_ms)
            VALUES(?,'MANUAL_PROTECTION_STARTED',?,?)""",
                           (offer_id, json.dumps(protection_payload(record, now_ms)), now_ms))
        return protection_payload(record, now_ms)

    def observe_manual_offer(self, offer, now_ms=None):
        now = self._now_ms() if now_ms is None else int(now_ms)
        with self.transaction(immediate=True) as connection:
            return self._observe_manual_offer(connection, offer, now)

    def manual_protections(self, now_ms=None):
        now = self._now_ms() if now_ms is None else int(now_ms)
        with self.read_connection() as connection:
            return {int(row["offer_id"]): protection_payload(dict(row), now)
                    for row in connection.execute("SELECT * FROM manual_offer_protection")}

    def finish_manual_protection(self, offer_id, reason, now_ms=None):
        now = self._now_ms() if now_ms is None else int(now_ms)
        with self.transaction(immediate=True) as connection:
            connection.execute("""UPDATE manual_offer_protection SET state='HANDLED',reason=?,updated_at_ms=?
                WHERE offer_id=?""", (reason, now, int(offer_id)))

    @staticmethod
    def _clear_confirmed_submit_protection(connection, offer_id, intent_id, now_ms):
        intent = connection.execute("SELECT * FROM order_intents WHERE id=?", (intent_id,)).fetchone()
        if intent is None:
            return
        digest = parameter_digest(dict(intent, rate=intent["submitted_rate"], amount_original=intent["amount"]))
        result = connection.execute("""UPDATE manual_offer_protection SET state='ROBOT_CONFIRMED',
            reason=NULL,updated_at_ms=? WHERE offer_id=? AND parameter_digest=? AND state='WAITING'""",
                                    (now_ms, offer_id, digest))
        if result.rowcount:
            connection.execute("""DELETE FROM external_takeover_state WHERE offer_id=?
                AND state IN ('OBSERVED','CONFIRMED')""", (offer_id,))

    def queue_manual_retake(self, offer, now_ms):
        """A modified robot offer already has ownership; do not manufacture another intent."""
        offer_id = int(offer.get("id") or offer.get("offer_id"))
        protection = self.manual_protection_for_offer(offer, now_ms)
        if not protection or protection["protected"] or not protection["needsTakeover"]:
            return False
        with self.transaction(immediate=True) as connection:
            result = connection.execute("""UPDATE external_takeover_state SET state='ADOPTED',updated_at_ms=?
                WHERE offer_id=? AND state='CONFIRMED'""", (now_ms, offer_id))
        return bool(result.rowcount)
