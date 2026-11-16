import json
from io import BytesIO
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import pytest

import MarketDataStream
import RuntimeV3
from bitfinex import Bitfinex, BitfinexAmbiguousWriteError, BitfinexApiError, BitfinexTransientError
from ConnectionDiagnostics import ConnectionDiagnostics
from MarketDataStream import BitfinexMarketDataHub
from RuntimeV3 import LendingRuntimeV3
from StateStore import LendingStateStore
from StrategyV3 import D, StrategyPolicyV3


@pytest.mark.parametrize(("credits", "reason"), [
    ({"error": "ERR_RATE_LIMIT"}, "Invalid account response"),
    ([[42, "fUSD"]], "Invalid account response"),
    ([], "ACCOUNT_RECONCILIATION_MISMATCH"),
])
def test_invalid_rest_snapshot_cannot_close_a_previous_credit(tmp_path, credits, reason):
    now = 2_000_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    store.set_mode("LIVE", "test")
    store.reconcile_credits([{
        "id": 42, "currency": "USD", "amount": D(400), "rate": D(".001"),
        "period": 2, "status": "ACTIVE", "mts_opening": now - 60_000,
    }], now - 60_000)
    client = SimpleNamespace(
        funding_book=lambda *args: [], funding_stats=lambda *args, **kwargs: [],
        funding_trades=lambda *args, **kwargs: [],
        wallets=lambda: [["funding", "USD", 1000, 0, 600]],
        active_funding_offers=lambda *args: [],
        active_funding_credits=lambda *args: credits,
        active_funding_loans=lambda *args: [],
    )
    hub = BitfinexMarketDataHub()
    runtime = LendingRuntimeV3(client, StrategyPolicyV3(), store, hub=hub)
    with pytest.raises(BitfinexApiError, match=reason):
        runtime.sync_rest(now_ms=now)
    assert [row["credit_id"] for row in store.credits(active_only=True)] == [42]
    assert hub.snapshot(now)["restAgeMs"] is None
    with store.read_connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM credit_closures").fetchone()[0] == 0


def test_account_rest_retries_record_only_structured_details(tmp_path, monkeypatch):
    path = tmp_path / "connections.jsonl"
    clock = iter((100, 101, 103))
    diagnostics = ConnectionDiagnostics(path, clock=lambda: next(clock))
    client = Bitfinex("key", "secret", read_retry_delays=(0, 0), retry_sleeper=lambda _: None, diagnostics=diagnostics)

    attempts = 0

    def request(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise BitfinexTransientError("private request details") from URLError(ConnectionRefusedError(111, "secret"))
        return []

    monkeypatch.setattr(client, "_request_json", request)
    assert client.wallets() == []
    lines = path.read_text(encoding="utf-8")
    assert "private request details" not in lines and "secret" not in lines
    first, retry, recovered = map(json.loads, lines.splitlines())
    assert first["channel"] == "rest_account"
    assert first["endpoint"] == "wallets"
    assert first["request_attempt"] == 1 and first["will_retry"]
    assert first["errors"] == ["BitfinexTransientError", "URLError", "ConnectionRefusedError"]
    assert first["errno"] == 111
    assert retry["attempts"] == 2
    assert (recovered["event"], recovered["attempts"], recovered["elapsed_ms"]) == ("recovered", 2, 3000)
    assert recovered["request_attempt"] == 3 and recovered["request_elapsed_ms"] >= 0


def test_other_endpoint_success_does_not_close_an_outage(tmp_path, monkeypatch):
    path = tmp_path / "connections.jsonl"
    clock = iter((100, 103))
    diagnostics = ConnectionDiagnostics(path, clock=lambda: next(clock))
    client = Bitfinex("key", "secret", read_retry_delays=(), diagnostics=diagnostics)
    replies = iter([BitfinexTransientError("offline"), [], []])

    def request(*args, **kwargs):
        reply = next(replies)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(client, "_request_json", request)
    with pytest.raises(BitfinexTransientError) as failure:
        client.active_funding_offers("fUSD")
    assert failure.value.rest_request["endpoint"] == "funding_offers"
    client.wallets()
    client.active_funding_offers("fUSD")
    first, recovered = map(json.loads, path.read_text(encoding="utf-8").splitlines())
    assert first["endpoint"] == recovered["endpoint"] == "funding_offers"
    assert first["will_retry"] is False
    assert recovered["elapsed_ms"] == 3000


def test_write_transport_failure_is_logged_once_without_retry(tmp_path, monkeypatch):
    path = tmp_path / "connections.jsonl"
    diagnostics = ConnectionDiagnostics(path)
    client = Bitfinex("key", "secret", diagnostics=diagnostics)
    calls = []

    def request(*args, **kwargs):
        calls.append(kwargs["ambiguous_on_failure"])
        raise BitfinexAmbiguousWriteError("private order details")

    monkeypatch.setattr(client, "_request_json", request)
    with pytest.raises(BitfinexAmbiguousWriteError):
        client.cancel_funding_offer(456789321)
    content = path.read_text(encoding="utf-8")
    row = json.loads(content)
    assert calls == [True]
    assert (row["channel"], row["endpoint"], row["will_retry"]) == ("rest_write", "cancel_funding_offer", False)
    assert "456789321" not in content and "private order details" not in content


@pytest.mark.parametrize(
    ("endpoint", "trade_feed", "account_reads", "stays_live"),
    [
        ("funding_trades", "fresh", 1, True),
        ("funding_trades", "book_only", 0, False),
        ("funding_trades", "expired_during_snapshot", 1, False),
        ("wallets", "fresh", 1, False),
        ("funding_stats", "fresh", 0, False),
    ],
)
def test_rest_trade_fallback_requires_fresh_trades_and_a_complete_account(
    tmp_path, monkeypatch, endpoint, trade_feed, account_reads, stays_live,
):
    now_ms = 2_000_000_000_000
    clock = [now_ms]
    monkeypatch.setattr(MarketDataStream, "_now_ms", lambda: clock[0])
    path = tmp_path / "connections.jsonl"
    diagnostics = ConnectionDiagnostics(path, clock=lambda: clock[0] / 1000)
    client = Bitfinex("key", "secret", read_retry_delays=(), diagnostics=diagnostics)
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: clock[0] / 1000)
    store.set_mode("LIVE", "test")
    hub = BitfinexMarketDataHub(client.api_key, client.api_secret)
    hub.apply_rest_snapshot(book=[], wallets=[["funding", "USD", 0, 0, 0]], synced_at_ms=now_ms - 1000)
    hub.handle_public_message({"event": "subscribed", "channel": "book", "chanId": 1})
    hub.handle_public_message([1, [[0.001, 2, 1, -200]]])
    hub.handle_public_message({"event": "subscribed", "channel": "trades", "chanId": 2})
    if trade_feed == "book_only":
        hub.handle_public_message([2, "hb"])
    else:
        hub.handle_public_message([2, [[11, now_ms - 1000, 200, 0.001, 2]]])

    failed = [endpoint]
    seen = []
    paths = {
        "wallets": "/v2/auth/r/wallets",
        "funding_trades": "/v2/trades/fUSD/hist",
        "funding_stats": "/v2/funding/stats/fUSD/hist",
    }

    def request(url, **kwargs):
        seen.append(url)
        if failed and paths[failed[0]] in url:
            raise BitfinexTransientError("private diagnostics context")
        if "/wallets" in url:
            return [["funding", "USD", 0, 0, 0]]
        if "/funding/loans" in url and trade_feed == "expired_during_snapshot":
            clock[0] = now_ms + 60_000
        return [[0.001, 2, 1, -200]] if "/book/" in url else []

    monkeypatch.setattr(client, "_request_json_once", request)
    runtime = LendingRuntimeV3(
        client, StrategyPolicyV3(short_floor_apr=D(".05"), medium_floor_apr=D(".06"), long_floor_apr=D(".07")),
        store, hub=hub, clock=lambda: clock[0] / 1000, diagnostics=diagnostics,
    )
    runtime._bootstrapped = True
    runtime._last_history_sync_ms = now_ms
    status = runtime.cycle(now_ms)
    assert sum("/wallets" in url for url in seen) == account_reads
    assert status["operationMode"] == ("LIVE" if stays_live else "PAUSED")
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    incident = next(row for row in records if row["channel"] == "worker")
    assert incident["endpoint"] == endpoint
    assert incident["event"] == ("rest_fallback" if stays_live else "rest_failure")
    if stays_live:
        assert hub.snapshot(now_ms)["trades"][0]["id"] == "11"
    else:
        assert incident["effect"] == "paused"
        assert not status["writeRecovery"]["canSubmit"]
        if trade_feed != "expired_during_snapshot":
            failed.clear()
            for offset, expected in [(30_000, "PAUSED"), (60_000, "LIVE")]:
                clock[0] = now_ms + offset
                status = runtime.cycle(clock[0])
                assert status["operationMode"] == expected


def test_websocket_auth_reports_recovery_only_after_authentication(tmp_path):
    path = tmp_path / "connections.jsonl"
    diagnostics = ConnectionDiagnostics(path)
    hub = BitfinexMarketDataHub(diagnostics=diagnostics)
    hub.handle_auth_message({"event": "auth", "status": "ERROR", "code": 10100, "msg": "private response"})
    hub.handle_auth_message([0, "hb"])
    hub.handle_auth_message({"event": "auth", "status": "OK"})
    text = path.read_text(encoding="utf-8")
    assert "private response" not in text
    failure, recovered = map(json.loads, text.splitlines())
    assert (failure["channel"], failure["protocol_code"]) == ("ws_auth", 10100)
    assert (recovered["channel"], recovered["event"]) == ("ws_auth", "recovered")


def test_offer_push_advances_probe_but_full_account_controls_resume(tmp_path, monkeypatch):
    now = 2_000_000_000_000
    clock = [now]
    monkeypatch.setattr(MarketDataStream, "_now_ms", lambda: clock[0])
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: clock[0] / 1000)
    store.set_mode("LIVE", "test")
    path = tmp_path / "connections.jsonl"
    diagnostics = ConnectionDiagnostics(path, clock=lambda: clock[0] / 1000)
    hub = BitfinexMarketDataHub("key", "secret", diagnostics=diagnostics)
    hub.handle_auth_message({"event": "auth", "status": "OK"})
    hub.apply_rest_snapshot(book=[], wallets=[["funding", "USD", 100, 0, 0]], synced_at_ms=now - 1000)
    offer = [7, "fUSD", now - 1000, now, 100, 100, "LIMIT", None, None, 0, "ACTIVE", None, None, None, .001, 2]
    client = Bitfinex("key", "secret", read_retry_delays=(), diagnostics=diagnostics)
    offline = [True]
    loans_calls = []

    def request(url, **kwargs):
        if "/funding/loans" in url:
            loans_calls.append(clock[0])
            clock[0] += 2000
            if offline[0]:
                raise BitfinexTransientError("network offline")
        if "/wallets" in url:
            return [["funding", "USD", 100, 0, 0]]
        if "/funding/offers" in url:
            return [offer]
        return []

    def advance(seconds):
        clock[0] += int(seconds * 1000)

    def wait_for_offer(seconds):
        if hub.offer_update_event.is_set():
            return True
        advance(seconds)
        return False

    monkeypatch.setattr(client, "_request_json_once", request)
    monkeypatch.setattr(hub.offer_update_event, "wait", wait_for_offer)
    monkeypatch.setattr(RuntimeV3.time, "sleep", advance)
    runtime = LendingRuntimeV3(
        client, StrategyPolicyV3(short_floor_apr=D(".05"), medium_floor_apr=D(".06"), long_floor_apr=D(".07")),
        store, hub=hub, clock=lambda: clock[0] / 1000, diagnostics=diagnostics,
    )
    runtime._bootstrapped = True

    initial = runtime.cycle()
    assert initial["operationMode"] == "PAUSED"
    assert initial["recovery"]["nextProbeAt"] == now + 7000
    assert initial["recovery"]["lastFailedEndpoint"] == "funding_loans"

    advance(1)
    hub.handle_auth_message([0, "fou", offer])
    runtime.wait_for_next_cycle(60)
    assert clock[0] == now + 3000 == store.recovery_status()["nextProbeAt"]
    failed = runtime.cycle()
    assert failed["recovery"]["successfulSnapshots"] == 0
    assert failed["openOffers"][0]["id"] == 7
    assert failed["operationMode"] == "PAUSED"

    advance(1)
    hub.handle_auth_message([0, "fou", offer])
    assert not hub.offer_update_event.is_set()
    offer[3] = clock[0]
    hub.handle_auth_message([0, "fou", offer])
    assert hub.offer_update_event.is_set()
    offline[0] = False
    runtime.wait_for_next_cycle(60)
    assert clock[0] == now + 15_000
    first = runtime.cycle()
    assert first["recovery"]["successfulSnapshots"] == 1
    assert first["recovery"]["nextProbeAt"] == now + 32_000
    assert not store.expedite_network_probe(first["recovery"]["startedAt"], now + 18_000, now + 18_000)

    runtime.wait_for_next_cycle(60)
    assert clock[0] == now + 32_000
    resumed = runtime.cycle()
    assert resumed["operationMode"] == "LIVE"
    assert resumed["strategyV3"]["recoveryResumeBarrier"]
    assert resumed["strategyV3"]["submitted"] == []
    runtime.wait_for_next_cycle(60, resumed=True)
    assert clock[0] == now + 35_000
    assert not runtime.cycle()["strategyV3"].get("recoveryResumeBarrier")
    assert loans_calls == [now, now + 3000, now + 15_000, now + 32_000]
    probes = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    probes = [entry for entry in probes if entry["event"] == "recovery_probe"]
    assert [(entry["wait_ms"], entry["probe_elapsed_ms"]) for entry in probes] == [
        (1000, 2000), (10_000, 2000), (15_000, 2000),
    ]


@pytest.mark.parametrize(("header", "limit_seconds"), [("90", 90), (None, 60)])
def test_rate_limit_stops_read_retries_and_blocks_offer_wake(tmp_path, monkeypatch, header, limit_seconds):
    now = 2_000_000_000_000
    store = LendingStateStore(tmp_path / "state.sqlite3", clock=lambda: now / 1000)
    store.set_mode("LIVE", "test")
    episode = store.begin_recovery("NETWORK_TRANSPORT", "offline", pause_live=True)
    client = Bitfinex("key", "secret", retry_sleeper=lambda _: pytest.fail("429 must not retry immediately"))

    def limited(req):
        headers = {"Retry-After": header} if header else None
        raise HTTPError(req.full_url, 429, "Too Many Requests", headers, BytesIO(b"{}"))

    monkeypatch.setattr(client, "_open_request", limited)
    with pytest.raises(BitfinexTransientError) as failure:
        client.wallets()
    exc = failure.value
    assert exc.status_code == 429
    assert exc.rest_request["request_attempt"] == 1
    status = store.record_recovery_failure(exc, exc.category)
    assert status["nextProbeAt"] == now + limit_seconds * 1000
    assert not store.expedite_network_probe(episode["startedAt"], now + 1000, now + 1000)
