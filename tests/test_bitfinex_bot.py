import configparser
import hashlib
import hmac
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from unittest import mock

from bitfinex import APP_VERSION, Bitfinex, BitfinexApiError
from FileUtils import atomic_write_text
import lendingbot


class CaptureLog:
    def __init__(self):
        self.offers = []
        self.cancels = []
        self.lines = []
        self.status = {}
        self.meta = {}

    def offer(self, amt, cur, rate, days, msg):
        self.offers.append((amt, cur, rate, days, msg))

    def cancelOrders(self, cur, msg):
        self.cancels.append((cur, msg))

    def log(self, msg):
        self.lines.append(str(msg))

    def updateStatusValue(self, coin, key, value):
        self.status.setdefault(coin, {})[key] = str(value)

    def updateMetaValue(self, key, value):
        self.meta[key] = value

    def refreshStatus(self, message):
        self.meta["lastStatus"] = message


class CancelCaptureClient:
    def __init__(self):
        self.canceled = []

    def cancel_funding_offer(self, offer_id):
        self.canceled.append(offer_id)
        return [0, "fon-cancel", None, None, None, None, "SUCCESS", "canceled"]


class OfferCaptureClient:
    def __init__(self):
        self.submitted = []

    def submit_funding_offer(self, *args):
        self.submitted.append(args)
        return [0, "fon-submit", None, None, None, None, "SUCCESS", "submitted"]


class LedgerClient:
    def __init__(self, rows_by_currency):
        self.rows_by_currency = rows_by_currency
        self.calls = []

    def has_credentials(self):
        return True

    def ledgers(self, currency=None, **kwargs):
        self.calls.append((currency, kwargs))
        return self.rows_by_currency.get(currency, [])


class FakePreflightClient:
    permissions = [
        ["wallets", 1, 0],
        ["funding", 1, 1],
        ["withdraw", 0, 0],
        ["ui_withdraw", 0, 0],
    ]
    wallet_rows = [
        ["funding", "USD", "500", None, "500"],
        ["exchange", "USD", "80", None, "75"],
    ]

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    def has_credentials(self):
        return bool(self.key and self.secret)

    def key_permissions(self):
        return self.permissions

    def wallets(self):
        return self.wallet_rows

    def funding_book(self, symbol, length=250):
        return [["0.0004", 2, 1, "1000"]]


class FakeControlledProcess:
    def __init__(self, pid=4321):
        self.pid = pid
        self.return_code = None

    def poll(self):
        return self.return_code

    def terminate(self):
        self.return_code = 0

    def kill(self):
        self.return_code = -9

    def wait(self, timeout=None):
        return self.return_code


def write_test_config(path, extra_bot=None):
    bot = {
        "mindailyrate": "0.04",
        "maxdailyrate": "2",
        "spreadlend": "3",
        "minloansize": "150",
        "maxtolent": "400",
        "statedbfile": os.path.join(os.path.dirname(path), "state.sqlite3"),
    }
    bot.update(extra_bot or {})
    config = configparser.ConfigParser()
    config.read_dict(
        {
            "BITFINEX": {"apikey": "key", "secret": "secret", "currencies": "USD"},
            "BOT": bot,
            "STRATEGY_V3": {
                "short_floor_apr": "6",
                "medium_floor_apr": "8",
                "long_floor_apr": "10",
                "enable_limit": "true",
                "enable_frr": "false",
                "enable_frr_delta_fixed": "false",
                "enable_frr_delta_variable": "false",
            },
        }
    )
    with open(path, "w", encoding="utf-8") as file:
        config.write(file)


def build_strategy_settings(extra_bot=None):
    bot = {
        "mindailyrate": "0.04",
        "maxdailyrate": "2",
        "spreadlend": "3",
        "minloansize": "150",
        "smartstrategy": "true",
    }
    bot.update(extra_bot or {})
    config = configparser.ConfigParser()
    config.read_dict(
        {
            "BITFINEX": {"apikey": "key", "secret": "secret", "currencies": "USD"},
            "BOT": bot,
        }
    )
    settings = lendingbot.build_settings(lendingbot.parse_args([]), config)
    lendingbot.validate_settings(settings)
    return settings


class BitfinexBotTests(unittest.TestCase):
    def test_release_version(self):
        self.assertEqual(APP_VERSION, "0.3.6.1-rc.1")

    def test_auth_headers_signature(self):
        client = Bitfinex("key", "secret")
        path = "v2/auth/r/wallets"
        nonce = "12345"
        headers = client.auth_headers_for_test(path, nonce, {})
        payload = "/api/" + path + nonce + "{}"
        expected = hmac.new(b"secret", payload.encode("utf-8"), hashlib.sha384).hexdigest()
        self.assertEqual(headers["bfx-apikey"], "key")
        self.assertEqual(headers["bfx-nonce"], nonce)
        self.assertEqual(headers["bfx-signature"], expected)

    def test_nonce_is_monotonic(self):
        client = Bitfinex("key", "secret")
        first = int(client._nonce())
        client._last_nonce = max(first + 10, int(time.time() * 1000000) + 1000000)
        previous = client._last_nonce
        self.assertEqual(int(client._nonce()), previous + 1)

    def test_authenticated_requests_serialize_monotonic_nonces(self):
        client = Bitfinex("key", "secret")
        observed = []
        active = 0
        maximum_active = 0
        request_lock = threading.Lock()

        def fake_request(*args, **kwargs):
            nonlocal active, maximum_active
            with request_lock:
                active += 1
                maximum_active = max(maximum_active, active)
                observed.append(int(kwargs["headers"]["bfx-nonce"]))
            time.sleep(0.005)
            with request_lock:
                active -= 1
            return []

        with mock.patch.object(client, "_request_json", side_effect=fake_request):
            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(lambda _: client.wallets(), range(24)))
        self.assertEqual(maximum_active, 1)
        self.assertEqual(observed, sorted(observed))
        self.assertEqual(len(observed), len(set(observed)))

    def test_key_permissions_uses_official_endpoint(self):
        client = Bitfinex("key", "secret")
        with mock.patch.object(client, "_auth_post", return_value=[]) as auth_post:
            client.key_permissions()
        auth_post.assert_called_once_with("v2/auth/r/permissions")

    def test_write_notification_rejects_http_200_business_error(self):
        client = Bitfinex("key", "secret")
        response = [123, "fon-req", None, None, None, 10020, "ERROR", "not enough balance"]
        with mock.patch.object(client, "_auth_post", return_value=response):
            with self.assertRaisesRegex(BitfinexApiError, "not enough balance"):
                client.submit_funding_offer("fUSD", "150", "0.0004", 2)

    def test_write_notification_accepts_explicit_success(self):
        client = Bitfinex("key", "secret")
        response = [123, "fon-req", None, None, [], None, "SUCCESS", "submitted"]
        with mock.patch.object(client, "_auth_post", return_value=response):
            self.assertIs(client.cancel_funding_offer(123), response)

    def test_dryrun_argument_is_rejected(self):
        with self.assertRaises(SystemExit):
            lendingbot.parse_args(["--dryrun"])

    def test_bot_requires_explicit_live_flag(self):
        with mock.patch.object(lendingbot, "read_config") as read_config:
            self.assertEqual(lendingbot.main([]), 1)
        read_config.assert_not_called()

    def test_dashboard_is_available_without_live_flag(self):
        args = lendingbot.parse_args(["--dashboard"])
        self.assertTrue(args.dashboard)
        self.assertFalse(args.live)

    def test_frontend_change_updates_dashboard_build_without_invalidating_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            for relative in lendingbot.DASHBOARD_BUILD_FILES:
                path = os.path.join(directory, relative)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as file:
                    file.write(relative)
            worker_before = lendingbot.worker_build_id(directory)
            dashboard_before = lendingbot.dashboard_build_id(directory)
            css_path = os.path.join(directory, "www", "lendingbot.css")
            with open(css_path, "a", encoding="utf-8") as file:
                file.write("\n/* visual-only change */")
            self.assertEqual(lendingbot.worker_build_id(directory), worker_before)
            self.assertNotEqual(lendingbot.dashboard_build_id(directory), dashboard_before)

    def test_internal_worker_status_detects_build_loaded_at_process_start(self):
        with tempfile.TemporaryDirectory() as directory:
            context = lendingbot.AppContext.for_project(directory)
            process = mock.Mock(pid=os.getpid())
            process.poll.return_value = None
            context.process_state.process = process
            lock = lendingbot.LiveProcessLock(context.live_lock_path)
            self.assertTrue(
                lock.acquire(
                    context.config_path,
                    {"role": "live_worker", "service": "mika-lending-worker-v3", "buildId": "loaded-build"},
                )
            )
            try:
                with mock.patch.object(lendingbot, "worker_build_id", return_value="current-build"):
                    status = lendingbot.controlled_bot_status(context.config_path, context)
            finally:
                lock.release()
            self.assertEqual(status["workerBuildId"], "loaded-build")
            self.assertTrue(status["buildMismatch"])

    def test_dashboard_main_never_constructs_trading_client(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path, {"statedbfile": os.path.join(directory, "state.sqlite3")})
            with (
                mock.patch.object(lendingbot, "start_web_server") as start_web_server,
                mock.patch.object(lendingbot, "stop_controlled_bot"),
                mock.patch.object(lendingbot, "stop_web_server"),
                mock.patch.object(lendingbot, "reconcile_orphaned_live_runtime"),
                mock.patch.object(lendingbot.LiveProcessLock, "acquire", return_value=True),
                mock.patch.object(lendingbot.LiveProcessLock, "release"),
                mock.patch.object(
                    lendingbot, "Bitfinex", side_effect=AssertionError("dashboard must not create API client")
                ),
            ):
                self.assertEqual(lendingbot.main(["--dashboard", "--config", path]), 0)
        start_web_server.assert_called_once()

    def test_dashboard_main_releases_lock_and_fails_when_web_server_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path, {"statedbfile": os.path.join(directory, "state.sqlite3")})
            with (
                mock.patch.object(lendingbot, "start_web_server", side_effect=OSError("bind failed")),
                mock.patch.object(lendingbot, "stop_web_server"),
                mock.patch.object(lendingbot, "reconcile_orphaned_live_runtime"),
                mock.patch.object(lendingbot.LiveProcessLock, "acquire", return_value=True),
                mock.patch.object(lendingbot.LiveProcessLock, "release") as release,
            ):
                self.assertEqual(lendingbot.main(["--dashboard", "--config", path]), 1)
        release.assert_called_once_with()

    def test_web_server_failure_closes_socket_and_clears_process_state(self):
        with tempfile.TemporaryDirectory() as directory:
            context = lendingbot.AppContext.for_project(directory)
            server = mock.Mock()
            server.serve_forever.side_effect = OSError("serve failed")
            with (
                mock.patch.object(lendingbot, "websocket_dependency_available", return_value=False),
                mock.patch.object(lendingbot, "ThreadingHTTPServer", return_value=server),
                self.assertRaisesRegex(OSError, "serve failed"),
            ):
                lendingbot.start_web_server(
                    CaptureLog(),
                    context.config_path,
                    context.status_path,
                    context,
                    raise_errors=True,
                )
            server.server_close.assert_called_once_with()
            self.assertIsNone(context.process_state.dashboard_server)

    def test_preflight_permission_parser(self):
        permissions = lendingbot.parse_key_permissions([["wallets", "1", "0"], ["funding", 1, 1]])
        self.assertTrue(permissions["wallets"]["read"])
        self.assertFalse(permissions["wallets"]["write"])
        self.assertTrue(permissions["funding"]["write"])

    def test_v3_preview_and_preflight_share_version_plan_hash_and_order_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            config_payload = lendingbot.config_api_payload(path)
            preview = lendingbot.strategy_v3_preview(
                path,
                {"strategyV3": config_payload["strategyV3"]},
                FakePreflightClient,
                now_ms=2_000_000_000_000,
            )
            preflight = lendingbot.evaluate_live_preflight(path, FakePreflightClient)
        self.assertEqual(preview["proposedVersion"], preflight["summary"]["activeStrategyVersion"])
        self.assertEqual(preview["plan"]["plan_hash"], preflight["summary"]["planHash"])
        self.assertEqual(preview["orderSizing"], preflight["summary"]["orderSizing"])
        self.assertEqual(preview["orderSizing"]["remainderPolicy"], "EVENLY_DISTRIBUTE")
        preview_orders = [
            (row["display_type"], row["amount"], row["period"], row["submitted_rate"], row["flags"])
            for row in preview["plan"]["plan"]
        ]
        preflight_orders = [
            (row["display_type"], row["amount"], row["period"], row["submitted_rate"], row["flags"])
            for row in preflight["summary"]["strategyPlan"]
        ]
        self.assertEqual(preview_orders, preflight_orders)

    def test_v3_draft_does_not_write_config_and_blocks_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            config_before = open(path, "rb").read()
            payload = lendingbot.config_api_payload(path)["strategyV3"]
            payload["max_lend_percent"] = "90"
            preview = lendingbot.strategy_v3_preview(path, {"strategyV3": payload}, FakePreflightClient)
            saved = lendingbot.save_strategy_v3_draft(
                path,
                {
                    "strategyV3": payload,
                    "previewToken": preview["previewToken"],
                },
            )
            result = lendingbot.evaluate_live_preflight(path, FakePreflightClient)
            self.assertEqual(open(path, "rb").read(), config_before)
        self.assertEqual(saved["status"], "DRAFT")
        state_check = next(row for row in result["checks"] if row["id"] == "strategy_state")
        self.assertEqual(state_check["status"], "fail")

    def test_v3_saving_loaded_active_policy_is_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            loaded = lendingbot.config_api_payload(path)["strategyV3"]
            preview = lendingbot.strategy_v3_preview(path, {"strategyV3": loaded}, FakePreflightClient)
            saved = lendingbot.save_strategy_v3_draft(
                path,
                {
                    "strategyV3": loaded,
                    "previewToken": preview["previewToken"],
                },
            )
            store, _ = lendingbot.v3_store_for_config(path)
            draft = store.strategy("DRAFT")
        self.assertEqual(saved["status"], "UNCHANGED")
        self.assertIsNone(draft)

    def test_config_payload_exposes_only_v3_strategy(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            payload = lendingbot.config_api_payload(path)
        self.assertNotIn("strategy", payload)
        self.assertEqual(payload["strategyV3"]["version"], 3)

    def test_v3_apply_is_bound_to_exact_saved_draft(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            policy = lendingbot.config_api_payload(path)["strategyV3"]
            policy["max_lend_percent"] = "90"
            preview = lendingbot.strategy_v3_preview(path, {"strategyV3": policy}, FakePreflightClient)
            saved = lendingbot.save_strategy_v3_draft(
                path,
                {
                    "strategyV3": policy,
                    "previewToken": preview["previewToken"],
                },
            )
            with self.assertRaisesRegex(lendingbot.ApiRequestError, "草稿"):
                lendingbot.apply_strategy_v3_draft(
                    path,
                    {
                        "draftVersionId": "another-tab-draft",
                        "applyToken": saved["applyToken"],
                    },
                    FakePreflightClient,
                )

    def test_fixed_credit_is_limit_and_unknown_variable_is_not_false_disabled(self):
        policy = lendingbot.StrategyPolicyV3(
            short_floor_apr=Decimal("6"),
            medium_floor_apr=Decimal("8"),
            long_floor_apr=Decimal("10"),
            enable_limit=True,
            enable_frr=False,
            enable_frr_delta_fixed=False,
            enable_frr_delta_variable=False,
        )
        fixed = {"rate_type": "FIXED", "period": 30, "rate": "0.001", "rate_real": "0.001"}
        variable = {"rate_type": "VARIABLE", "period": 30, "rate": "0.001", "rate_real": "0.001"}
        self.assertEqual(lendingbot.v3_credit_display_type(fixed), "LIMIT")
        self.assertNotIn("disabled_type", lendingbot.v3_credit_violations(fixed, policy))
        self.assertEqual(lendingbot.v3_credit_display_type(variable), "VARIABLE_UNKNOWN")
        self.assertNotIn("disabled_type", lendingbot.v3_credit_violations(variable, policy))

    def test_dashboard_static_snapshot_embeds_build_and_is_immutable(self):
        with tempfile.TemporaryDirectory() as directory:
            html = os.path.join(directory, "lendingbot.html")
            with open(html, "wb") as file:
                file.write(b"<meta content='__MIKA_DASHBOARD_BUILD_ID__'><title>v__MIKA_APPLICATION_VERSION__</title>")
            snapshot = lendingbot.load_dashboard_static_snapshot(directory, "build-one")
            with open(html, "wb") as file:
                file.write(b"changed-on-disk")
            self.assertIn(b"build-one", snapshot["lendingbot.html"])
            self.assertIn(lendingbot.APP_VERSION.encode("ascii"), snapshot["lendingbot.html"])
            self.assertNotIn(b"__MIKA_APPLICATION_VERSION__", snapshot["lendingbot.html"])
            self.assertNotIn(b"changed-on-disk", snapshot["lendingbot.html"])

    def test_active_schema_normalization_backs_up_and_audits(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            database = os.path.join(directory, "state.sqlite3")
            write_test_config(path, {"statedbfile": database})
            config, _ = lendingbot.read_config(path)
            settings = lendingbot.build_settings(lendingbot.parse_args(["--config", path]), config)
            raw = lendingbot.json_decimal(settings.strategy_v3.__dict__)
            raw.pop("max_lend_percent", None)
            raw.pop("max_lend_amount", None)
            raw["enable_frr"] = True
            raw["short_reprice_stages_minutes"] = [10, 30, 60]
            raw["medium_reprice_stages_minutes"] = [20, 60, 120]
            raw["long_reprice_stages_minutes"] = [60, 180, 360]
            store = lendingbot.LendingStateStore(database)
            old_version = store.save_strategy(raw, status="ACTIVE")
            result = lendingbot.normalize_current_active_strategy(path)
            active = store.strategy("ACTIVE")
            with store.read_connection() as connection:
                events = [dict(row) for row in connection.execute("SELECT * FROM strategy_events")]
            backups_exist = os.path.exists(result["backup"]["config"]) and os.path.exists(result["backup"]["database"])
        self.assertTrue(result["changed"])
        self.assertEqual(result["fromVersion"], old_version)
        self.assertTrue(active["policy"]["enable_frr"])
        self.assertEqual(active["policy"]["max_lend_percent"], "100")
        self.assertEqual(active["policy"]["short_reprice_stages_minutes"], [10, 30, 60, 90, 120, 180])
        self.assertEqual(active["policy"]["medium_reprice_stages_minutes"], [20, 60, 120, 180, 240, 360])
        self.assertEqual(active["policy"]["long_reprice_stages_minutes"], [60, 180, 360, 480, 720, 1440])
        self.assertTrue(backups_exist)
        self.assertEqual([event["event_type"] for event in events], ["SCHEMA_NORMALIZATION"])

    def test_preflight_summary_and_warning(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path, {"maxtolent": "0", "maxpercenttolent": "0"})
            result = lendingbot.evaluate_live_preflight(path, FakePreflightClient)
        self.assertTrue(all(check["status"] == "pass" for check in result["checks"]))
        self.assertEqual(result["summary"]["strategyVersion"], 3)
        self.assertEqual(result["summary"]["account"]["wallet"], "500")
        self.assertEqual(result["summary"]["fundingLimit"]["effectiveCap"], "500.00000000")
        self.assertNotIn("strategyProfile", result["summary"])
        self.assertNotIn("UNLIMITED_EXPOSURE", [warning["code"] for warning in result["warnings"]])

    def test_withdraw_permission_blocks_preflight(self):
        class UnsafeClient(FakePreflightClient):
            permissions = [["wallets", 1, 0], ["funding", 1, 1], ["withdraw", 0, 1], ["ui_withdraw", 0, 0]]

        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            result = lendingbot.evaluate_live_preflight(path, UnsafeClient)
        check = next(item for item in result["checks"] if item["id"] == "withdraw_disabled")
        self.assertEqual(check["status"], "fail")

    def test_wallet_write_required_only_for_auto_transfer(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path, {"transferablecurrencies": "USD"})
            result = lendingbot.evaluate_live_preflight(path, FakePreflightClient)
        check = next(item for item in result["checks"] if item["id"] == "wallets_write")
        self.assertEqual(check["status"], "fail")

    def test_preflight_token_is_single_use(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            context = lendingbot.AppContext.for_project(directory, config_path=path)
            response = lendingbot.create_controlled_bot_preflight(path, FakePreflightClient, now=1000, context=context)
            lendingbot.consume_controlled_bot_preflight(path, response["preflightId"], now=1001, context=context)
            with self.assertRaisesRegex(lendingbot.ConfigError, "已使用"):
                lendingbot.consume_controlled_bot_preflight(path, response["preflightId"], now=1001, context=context)

    def test_preflight_token_expires(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            context = lendingbot.AppContext.for_project(directory, config_path=path)
            response = lendingbot.create_controlled_bot_preflight(path, FakePreflightClient, now=1000, context=context)
            with self.assertRaisesRegex(lendingbot.ConfigError, "过期"):
                lendingbot.consume_controlled_bot_preflight(path, response["preflightId"], now=1301, context=context)

    def test_preflight_token_rejects_config_change(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            context = lendingbot.AppContext.for_project(directory, config_path=path)
            response = lendingbot.create_controlled_bot_preflight(path, FakePreflightClient, now=1000, context=context)
            with open(path, "a", encoding="utf-8") as file:
                file.write("\n# changed\n")
            with self.assertRaisesRegex(lendingbot.ConfigError, "发生变化"):
                lendingbot.consume_controlled_bot_preflight(path, response["preflightId"], now=1001, context=context)

    def test_preflight_token_rejects_account_change_within_five_minutes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            original = FakePreflightClient.wallet_rows
            try:
                context = lendingbot.AppContext.for_project(directory, config_path=path)
                response = lendingbot.create_controlled_bot_preflight(
                    path, FakePreflightClient, now=1000, context=context
                )
                FakePreflightClient.wallet_rows = [["funding", "USD", "500", None, "449"]]
                with self.assertRaisesRegex(lendingbot.ConfigError, "账户快照"):
                    lendingbot.consume_controlled_bot_preflight(
                        path, response["preflightId"], now=1001, context=context
                    )
            finally:
                FakePreflightClient.wallet_rows = original

    def test_preflight_does_not_adopt_protected_external_offer(self):
        now_ms = 2_000_000_000_000

        class ExternalOfferClient(FakePreflightClient):
            wallet_rows = [["funding", "USD", "675", None, "500"]]
            offer_rows = [
                [
                    9001,
                    "fUSD",
                    now_ms - 600_000,
                    now_ms - 600_000,
                    "175",
                    "175",
                    "LIMIT",
                    None,
                    None,
                    0,
                    "ACTIVE",
                    None,
                    None,
                    None,
                    "0.0004",
                    2,
                ]
            ]

            def active_funding_offers(self, symbol):
                return self.offer_rows

            def active_funding_credits(self, symbol):
                return []

        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "test.cfg")
            write_test_config(path)
            config = configparser.ConfigParser()
            config.read(path, encoding="utf-8")
            config.set("STRATEGY_V3", "adopt_external_offers", "true")
            with open(path, "w", encoding="utf-8") as target:
                config.write(target)
            context = lendingbot.AppContext.for_project(directory, config_path=path, client_factory=ExternalOfferClient)
            response = lendingbot.create_controlled_bot_preflight(path, now=2000, context=context)
            assert response["summary"]["externalAdoptionCandidates"] == []
            consumed = lendingbot.consume_controlled_bot_preflight(
                path, response["preflightId"], now=2001, context=context
            )
            store, _ = lendingbot.v3_store_for_config(path)
            assert consumed["adoptedOfferIds"] == []
            assert store.offers(active_only=True) == []

    def test_legacy_status_never_exposes_balance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "botlog.json")
            with open(path, "w", encoding="utf-8") as file:
                json.dump({"raw_data": {"USD": {"totalCoins": "999999"}}, "operationMode": "dry"}, file)
            status = lendingbot.read_status_payload(path)
        self.assertTrue(status["legacyIgnored"])
        self.assertEqual(status["raw_data"], {})
        self.assertIn("旧版状态已忽略", status["last_status"])

    def test_atomic_write_keeps_original_when_replace_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "status.json")
            with open(path, "w", encoding="utf-8") as file:
                file.write("original")
            with mock.patch("FileUtils.os.replace", side_effect=OSError("replace failed")):
                with self.assertRaises(OSError):
                    atomic_write_text(path, "replacement")
            with open(path, "r", encoding="utf-8") as file:
                self.assertEqual(file.read(), "original")

    def test_atomic_write_retries_transient_permission_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "status.json")
            with open(path, "w", encoding="utf-8") as file:
                file.write("original")
            real_replace = os.replace
            calls = 0

            def transient_replace(source, target):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise PermissionError(5, "destination is briefly in use", target)
                return real_replace(source, target)

            with mock.patch("FileUtils.os.replace", side_effect=transient_replace):
                with mock.patch("FileUtils.time.sleep"):
                    atomic_write_text(path, "replacement")

            self.assertEqual(calls, 2)
            with open(path, "r", encoding="utf-8") as file:
                self.assertEqual(file.read(), "replacement")

    def test_dashboard_concurrent_start_creates_one_process(self):
        popen_calls = []
        call_lock = threading.Lock()

        def fake_popen(*args, **kwargs):
            time.sleep(0.05)
            with call_lock:
                popen_calls.append((args, kwargs))
            return FakeControlledProcess()

        with tempfile.TemporaryDirectory() as directory:
            config_path = os.path.join(directory, "test.cfg")
            status_path = os.path.join(directory, "botlog.json")
            write_test_config(config_path)
            context = lendingbot.AppContext.for_project(
                directory,
                config_path=config_path,
                status_path=status_path,
                client_factory=FakePreflightClient,
            )
            response = lendingbot.create_controlled_bot_preflight(config_path, context=context)
            store, _ = lendingbot.v3_store_for_config(config_path)
            store.begin_recovery(
                "WORKER_HEARTBEAT_TIMEOUT",
                "old worker",
                origin_mode="PAUSED",
                target_mode="PAUSED",
            )

            def attempt_start(_):
                try:
                    return lendingbot.start_controlled_bot(
                        config_path,
                        status_path,
                        response["preflightId"],
                        context=context,
                    )
                except lendingbot.ConfigError:
                    return None

            with mock.patch.object(lendingbot.subprocess, "Popen", side_effect=fake_popen):
                with ThreadPoolExecutor(max_workers=8) as executor:
                    results = list(executor.map(attempt_start, range(8)))
            self.assertEqual(len(popen_calls), 1)
            self.assertEqual(sum(result is not None for result in results), 1)
            self.assertIn("--live", popen_calls[0][0][0])
            self.assertTrue(store.recovery_status()["active"])
            lendingbot.stop_controlled_bot(config_path, context=context)
            lendingbot.cleanup_controlled_bot_handle(context)

    def test_live_process_lock_is_discoverable_across_dashboard_state(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = os.path.join(directory, "test.cfg")
            write_test_config(config_path)
            context = lendingbot.AppContext.for_project(directory, config_path=config_path)
            lock_path = context.live_lock_path
            first = lendingbot.LiveProcessLock(lock_path)
            second = lendingbot.LiveProcessLock(lock_path)
            self.assertTrue(first.acquire(config_path))
            try:
                inspection = lendingbot.LiveProcessLock.inspect(lock_path)
                self.assertTrue(inspection["locked"])
                self.assertEqual(inspection["metadata"]["pid"], os.getpid())
                self.assertFalse(second.acquire(config_path))
                status = lendingbot.controlled_bot_status(config_path, context)
                self.assertTrue(status["running"])
                self.assertTrue(status["managedExternally"])
                self.assertEqual(status["pid"], os.getpid())
            finally:
                first.release()
            self.assertFalse(lendingbot.LiveProcessLock.inspect(lock_path)["locked"])

    def test_unexpected_error_pauses_and_is_persisted_immediately(self):
        with tempfile.TemporaryDirectory() as directory:
            status_path = os.path.join(directory, "botlog.json")
            store = lendingbot.LendingStateStore(os.path.join(directory, "state.sqlite3"))
            store.set_mode("LIVE", "test")
            logger = lendingbot.Logger(status_path, 20)
            lendingbot.publish_safe_status(logger, store, RuntimeError("network down"))
            with open(status_path, "r", encoding="utf-8") as file:
                status = json.load(file)
            self.assertEqual(status["schemaVersion"], 3)
            self.assertEqual(status["operationMode"], "PAUSED")
            self.assertEqual(status["runtime"]["mode"], "PAUSED")
            self.assertIn("RuntimeError", status["last_status"])
            self.assertTrue(status["last_update"])

    def test_stopped_pending_strategy_can_be_discarded_but_running_pending_cannot(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = os.path.join(directory, "test.cfg")
            context = lendingbot.AppContext.for_project(directory, config_path=config_path)
            store = lendingbot.LendingStateStore(os.path.join(directory, "state.sqlite3"))
            active = store.save_strategy({"name": "active"}, "ACTIVE")
            draft = store.save_strategy({"name": "replacement"}, "DRAFT")
            store.apply_confirmed_draft(draft, active, queue=True)
            with (
                mock.patch.object(lendingbot, "v3_store_for_config", return_value=(store, mock.Mock())),
                mock.patch.object(lendingbot, "controlled_bot_running", return_value=False),
            ):
                result = lendingbot.discard_strategy_v3_draft(config_path, context)
            self.assertEqual(result["discarded"], ["PENDING"])
            self.assertIsNone(store.strategy("PENDING"))

            store.save_strategy({"name": "replacement-2"}, "PENDING")
            store.set_mode("LIVE", "test")
            with (
                mock.patch.object(lendingbot, "v3_store_for_config", return_value=(store, mock.Mock())),
                mock.patch.object(lendingbot, "controlled_bot_running", return_value=True),
                self.assertRaisesRegex(lendingbot.ApiRequestError, "先停止机器人"),
            ):
                lendingbot.discard_strategy_v3_draft(config_path, context)
            self.assertIsNotNone(store.strategy("PENDING"))

    def test_dashboard_reconciles_orphaned_live_runtime_and_overlays_status(self):
        with tempfile.TemporaryDirectory() as directory:
            database = os.path.join(directory, "state.sqlite3")
            config_path = os.path.join(directory, "test.cfg")
            status_path = os.path.join(directory, "botlog.json")
            write_test_config(config_path, {"statedbfile": database})
            context = lendingbot.AppContext.for_project(directory, config_path=config_path, status_path=status_path)
            store = lendingbot.LendingStateStore(database)
            store.set_mode("LIVE", "test")
            with open(status_path, "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "schemaVersion": 3,
                        "operationMode": "LIVE",
                        "last_update": "2026-07-21 15:00:00",
                        "account": {"total": "123"},
                        "log": ["last worker line"],
                    },
                    file,
                )
            with mock.patch.object(lendingbot, "controlled_bot_running", return_value=False):
                runtime = lendingbot.reconcile_orphaned_live_runtime(config_path, context)
            self.assertEqual(runtime["mode"], "PAUSED")
            payload = lendingbot.dashboard_status_payload(status_path, config_path, context)
            self.assertEqual(payload["operationMode"], "PAUSED")
            self.assertNotIn("account", payload)
            self.assertEqual(payload["last_update"], "")
            self.assertFalse(payload["snapshotAvailable"])
            self.assertEqual(payload["log"], ["last worker line"])
            self.assertEqual(payload["process"]["stopReason"], "dashboard_started_without_live_process")
            self.assertEqual(payload["lastStopReason"], "dashboard_started_without_live_process")
            self.assertIn("控制台启动时未发现实盘进程", payload["last_status"])

    def test_dashboard_poll_uses_worker_statistics_without_history_queries(self):
        published = {
            "schemaVersion": 3,
            "operationMode": "LIVE",
            "last_update": "2026-10-12 15:00:00",
            "account": {"total": "123", "walletAvailableKnown": True},
            "statistics": {"30d": {"netInterest": "9.87"}},
            "realizedIncome": {"total": "12.34"},
            "incomeHistorySync": {"currency": "USD"},
        }
        store = mock.Mock()
        store.runtime.return_value = {"mode": "PAUSED", "safe_reason": "manual_pause"}
        store.recovery_status.return_value = {"active": False}
        with (
            mock.patch.object(lendingbot, "read_status_payload", return_value=published.copy()),
            mock.patch.object(lendingbot, "v3_store_for_config", return_value=(store, mock.Mock())),
            mock.patch.object(lendingbot, "controlled_bot_status", return_value={"running": True}),
            mock.patch.object(lendingbot, "stats_v3_payload", side_effect=AssertionError("history scan in UI poll")),
        ):
            payload = lendingbot.dashboard_status_payload("botlog.json", "test.cfg")
        self.assertEqual(payload["applicationVersion"], lendingbot.APP_VERSION)
        self.assertEqual(payload["statistics"], published["statistics"])
        self.assertEqual(payload["realizedIncome"], published["realizedIncome"])
        self.assertEqual(payload["incomeHistorySync"], published["incomeHistorySync"])
        self.assertEqual(payload["last_update"], published["last_update"])
        self.assertEqual(payload["operationMode"], "PAUSED")
        self.assertIn("manual_pause", payload["last_status"])
        self.assertTrue(payload["snapshotAvailable"])
        store.statistics.assert_not_called()

    def test_dashboard_poll_without_worker_history_does_not_fabricate_statistics(self):
        store = mock.Mock()
        store.runtime.return_value = {"mode": "LIVE"}
        store.recovery_status.return_value = {"active": False}
        with (
            mock.patch.object(lendingbot, "read_status_payload", return_value=lendingbot.empty_status_payload()),
            mock.patch.object(lendingbot, "v3_store_for_config", return_value=(store, mock.Mock())),
            mock.patch.object(lendingbot, "controlled_bot_status", return_value={"running": True}),
            mock.patch.object(lendingbot, "stats_v3_payload", side_effect=AssertionError("history scan in UI poll")),
        ):
            payload = lendingbot.dashboard_status_payload("botlog.json", "test.cfg")
        self.assertFalse(payload["snapshotAvailable"])
        self.assertEqual(payload["last_update"], "")
        self.assertNotIn("statistics", payload)
        store.statistics.assert_not_called()

    def test_dashboard_status_failure_hides_stale_account_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            status_path = os.path.join(directory, "botlog.json")
            with open(status_path, "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "schemaVersion": 3,
                        "operationMode": "LIVE",
                        "account": {"total": "9999"},
                        "openOffers": [{"id": 123}],
                        "log": ["preserved diagnostic"],
                    },
                    file,
                )
            with mock.patch.object(lendingbot, "v3_store_for_config", side_effect=OSError("database unavailable")):
                payload = lendingbot.dashboard_status_payload(status_path, os.path.join(directory, "test.cfg"))
            self.assertEqual(payload["operationMode"], "UNKNOWN")
            self.assertTrue(payload["statusUnavailable"])
            self.assertFalse(payload["snapshotAvailable"])
            self.assertNotIn("account", payload)
            self.assertEqual(payload["openOffers"], [])
            self.assertEqual(payload["log"], ["preserved diagnostic"])

    def test_expired_strategy_tokens_are_pruned_and_cache_is_bounded(self):
        with lendingbot.strategy_token_lock:
            lendingbot.strategy_preview_tokens.clear()
            lendingbot.strategy_apply_tokens.clear()
            lendingbot.strategy_preview_tokens["expired"] = {"expiresAt": 10}
            for index in range(lendingbot.STRATEGY_TOKEN_CACHE_LIMIT + 5):
                lendingbot.strategy_apply_tokens[str(index)] = {"expiresAt": 1_000 + index}
        removed = lendingbot._prune_strategy_tokens(100)
        self.assertEqual(removed, 6)
        self.assertNotIn("expired", lendingbot.strategy_preview_tokens)
        self.assertEqual(len(lendingbot.strategy_apply_tokens), lendingbot.STRATEGY_TOKEN_CACHE_LIMIT)
        with lendingbot.strategy_token_lock:
            lendingbot.strategy_preview_tokens.clear()
            lendingbot.strategy_apply_tokens.clear()


if __name__ == "__main__":
    unittest.main()
