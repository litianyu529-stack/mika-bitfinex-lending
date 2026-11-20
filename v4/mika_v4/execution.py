from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal

from .bitfinex import BitfinexClient, submitted_offer_id
from .config import V4Policy
from .domain import AccountSnapshot, AllocationPlan, IntentState, RuntimeMode, WriteOutcome
from .store import V4Store


D = Decimal


class ExecutionBlocked(RuntimeError):
    pass


class SafeExecutor:
    def __init__(self, client: BitfinexClient, store: V4Store, policy: V4Policy) -> None:
        self.client = client
        self.store = store
        self.policy = policy

    @staticmethod
    def _intent_fingerprint(action: str, plan_fingerprint: str, identity: str) -> str:
        return hashlib.sha256(f"{action}:{plan_fingerprint}:{identity}".encode()).hexdigest()

    @staticmethod
    def _shape_fingerprint(rows: list[dict]) -> str:
        payload = json.dumps(
            sorted(rows, key=lambda item: (item["pool"], item["rung"])), sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def sync_account(self, account: AccountSnapshot) -> None:
        open_by_id = {int(offer.offer_id): offer for offer in account.offers}
        for offer_id in self.store.managed_offer_ids() & set(open_by_id):
            offer = open_by_id[offer_id]
            self.store.update_rung_snapshot(offer_id, offer.amount, offer.status, account.as_of_ms)
        self.store.close_missing_rungs(set(open_by_id), account.as_of_ms)

    def reconcile(
        self,
        plan: AllocationPlan,
        account: AccountSnapshot,
        reason: str = "FULL_REPLAN",
        target_pools: set[str] | None = None,
    ) -> str:
        if not account.authoritative:
            raise ExecutionBlocked("account snapshot is not authoritative")
        mode = self.store.mode()
        if mode == RuntimeMode.SAFE:
            return "SAFE"
        if mode != RuntimeMode.LIVE:
            self.store.record_shadow_plan(plan)
            self.store.save_planner_state(plan.state)
            return "SHADOW_RECORDED"

        pending = self.store.get_pending_plan()
        if pending:
            pending_plan, phase, pending_reason, affected = pending
            if phase == "WAIT_CANCELS":
                if self.store.managed_offer_ids(affected) & {offer.offer_id for offer in account.offers}:
                    return "WAITING_FOR_CANCEL_CONFIRMATION"
                self.store.update_pending_phase("READY_TO_SUBMIT")
            plan, reason = pending_plan, pending_reason

        desired_shape = [
            {
                "pool": item.pool,
                "rung": item.rung_index,
                "amount": format(item.amount, "f"),
                "rate": format(item.rate, "f"),
                "period": item.period,
            }
            for item in plan.orders
        ]
        current_shape = self.store.current_offer_shape()
        if current_shape and self._shape_fingerprint(current_shape) == self._shape_fingerprint(desired_shape):
            self.store.clear_pending_plan()
            self.store.save_planner_state(plan.state)
            return "UNCHANGED"

        if not pending:
            pools = target_pools or {"short", "medium", "long"}
            current_by_pool = {pool: [item for item in current_shape if item["pool"] == pool] for pool in pools}
            desired_by_pool = {pool: [item for item in desired_shape if item["pool"] == pool] for pool in pools}
            affected = {
                pool
                for pool in pools
                if self._shape_fingerprint(current_by_pool[pool]) != self._shape_fingerprint(desired_by_pool[pool])
            }
            previous_tier = self.store.planner_state().long_gate.tier
            if (
                "long" in affected
                and current_by_pool["long"]
                and desired_by_pool["long"]
                and previous_tier == plan.long_tier
                and reason != "FLOOR_STALE"
                and account.as_of_ms - self.store.last_rebuild_ms("long") < 3_600_000
            ):
                affected.remove("long")
        if not affected:
            self.store.save_planner_state(plan.state)
            return "NO_ORDERS" if not plan.orders and not current_shape else "UNCHANGED"

        active_ids = self.store.managed_offer_ids(affected) & {offer.offer_id for offer in account.offers}
        if active_ids:
            capped = [
                pool
                for pool in affected
                if pool in {"short", "medium"}
                and self.store.rebuild_count(pool, account.as_of_ms) >= self.policy.max_group_rebuilds_per_hour
            ]
            if capped:
                self.store.record_event("WARNING", "REBUILD_RATE_LIMIT", {"pools": capped})
                return "REBUILD_RATE_LIMIT"
            self.store.set_pending_plan(plan, "WAIT_CANCELS", reason, affected)
            plan_fp = self.store.fingerprint(plan)
            for offer_id in sorted(active_ids):
                fingerprint = self._intent_fingerprint("CANCEL", plan_fp, f"{offer_id}:{account.as_of_ms}")
                if not self.store.create_intent(fingerprint, "CANCEL", offer_id=offer_id):
                    continue
                self.store.set_intent_state(fingerprint, IntentState.SUBMITTING)
                result = self.client.cancel_offer(offer_id)
                if result.outcome == WriteOutcome.UNKNOWN:
                    self.store.set_intent_state(fingerprint, IntentState.AMBIGUOUS, result.error)
                    self.store.enter_safe(
                        f"cancel outcome unknown for offer {offer_id}",
                        category="AMBIGUOUS_WRITE",
                    )
                    return "SAFE_UNKNOWN_CANCEL"
                if result.outcome == WriteOutcome.DEFINITE_REJECT:
                    self.store.set_intent_state(fingerprint, IntentState.REJECTED, result.error)
                    self.store.record_event("ERROR", "CANCEL_REJECTED", {"offer_id": offer_id, "error": result.error})
                    self.store.clear_pending_plan()
                    if result.category == "BALANCE_DRIFT":
                        self.store.enter_safe(str(result.error), category="BALANCE_DRIFT")
                        return "RECOVERY_BALANCE_DRIFT"
                    return "CANCEL_REJECTED"
                self.store.set_intent_state(fingerprint, IntentState.CONFIRMED)
            return "CANCELS_SUBMITTED"

        selected_orders = tuple(order for order in plan.orders if order.pool in affected)
        selected_amount = sum((order.amount for order in selected_orders), D("0"))
        if selected_amount > account.wallet_available:
            self.store.clear_pending_plan()
            self.store.record_event(
                "WARNING",
                "BALANCE_CHANGED",
                {
                    "planned": selected_amount,
                    "available": account.wallet_available,
                },
            )
            return "BALANCE_CHANGED_REPLAN_REQUIRED"

        if not selected_orders:
            self.store.clear_pending_plan()
            self.store.save_planner_state(plan.state)
            return "NO_ORDERS"

        execution_plan = replace(plan, orders=selected_orders, planned_amount=selected_amount)
        self.store.save_plan_rungs(execution_plan, reason)
        plan_fp = self.store.fingerprint(plan)
        for offer in selected_orders:
            fingerprint = self._intent_fingerprint("SUBMIT", plan_fp, offer.key)
            if not self.store.create_intent(
                fingerprint,
                "SUBMIT",
                offer_key=offer.key,
                amount=offer.amount,
                rate=offer.rate,
                period=offer.period,
            ):
                continue
            self.store.set_intent_state(fingerprint, IntentState.SUBMITTING)
            result = self.client.submit_offer(offer.amount, offer.rate, offer.period)
            if result.outcome == WriteOutcome.UNKNOWN:
                self.store.set_intent_state(fingerprint, IntentState.AMBIGUOUS, result.error)
                self.store.enter_safe(
                    f"submit outcome unknown for {offer.key}",
                    category="AMBIGUOUS_WRITE",
                )
                return "SAFE_UNKNOWN_SUBMIT"
            if result.outcome == WriteOutcome.DEFINITE_REJECT:
                self.store.set_intent_state(fingerprint, IntentState.REJECTED, result.error)
                self.store.mark_rung_rejected(offer.key)
                self.store.record_event("ERROR", "SUBMIT_REJECTED", {"key": offer.key, "error": result.error})
                if result.category == "BALANCE_DRIFT":
                    self.store.enter_safe(str(result.error), category="BALANCE_DRIFT")
                    return "RECOVERY_BALANCE_DRIFT"
                continue
            offer_id = submitted_offer_id(result.response)
            if offer_id is None:
                self.store.set_intent_state(fingerprint, IntentState.AMBIGUOUS, "confirmed response had no offer id")
                self.store.enter_safe(
                    f"submit confirmation missing offer id for {offer.key}",
                    category="AMBIGUOUS_WRITE",
                )
                return "SAFE_MISSING_OFFER_ID"
            self.store.update_rung_offer(offer.key, offer_id)
            self.store.set_intent_state(fingerprint, IntentState.CONFIRMED)
        self.store.clear_pending_plan()
        self.store.save_planner_state(plan.state)
        return "SUBMITTED"
