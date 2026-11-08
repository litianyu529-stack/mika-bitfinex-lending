import hashlib
import hmac
import http.client
import json
import socket
import threading
import time
from decimal import Decimal
from urllib import error, parse, request

from DomainTypes import WriteOutcome, WriteResult


APP_VERSION = "0.3.6.1-rc.1"


class BitfinexApiError(Exception):
    def __init__(
        self,
        message,
        *,
        retryable=False,
        category="BITFINEX_API",
        status_code=None,
        manual_required=False,
    ):
        super().__init__(str(message))
        self.retryable = bool(retryable)
        self.category = str(category)
        self.status_code = status_code
        self.manual_required = bool(manual_required)


class BitfinexAmbiguousWriteError(BitfinexApiError):
    """The request may have reached Bitfinex but no authoritative result was received."""

    def __init__(self, message):
        super().__init__(message, category="AMBIGUOUS_WRITE", manual_required=True)


class BitfinexTransientError(BitfinexApiError):
    def __init__(self, message, *, category="NETWORK_TRANSPORT", status_code=None):
        super().__init__(message, retryable=True, category=category, status_code=status_code)


FUNDING_OFFER_TYPES = {"LIMIT", "FRR", "FRRDELTAFIX", "FRRDELTAVAR"}
HIDDEN_OFFER_FLAG = 64
READ_RETRY_DELAYS = (0.25, 1.0)


def _write_rejection_category(message):
    value = str(message).lower()
    if "balance" in value and any(token in value for token in ("not enough", "insufficient", "available")):
        return "BALANCE_DRIFT"
    return "WRITE_REJECTED"


class Bitfinex:
    AUTH_BASE_URL = "https://api.bitfinex.com"
    PUBLIC_BASE_URL = "https://api-pub.bitfinex.com"

    def __init__(
        self,
        api_key="",
        api_secret="",
        timeout=30,
        read_retry_delays=READ_RETRY_DELAYS,
        retry_sleeper=time.sleep,
    ):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.timeout = timeout
        self.read_retry_delays = tuple(float(delay) for delay in read_retry_delays)
        self.retry_sleeper = retry_sleeper
        self._last_nonce = 0
        # Bitfinex nonces are scoped to an API key.  Keep nonce creation and the
        # corresponding authenticated request in one critical section so a
        # background history reader cannot overtake a live trading request.
        self._auth_lock = threading.RLock()

    @staticmethod
    def is_placeholder_credential(value):
        if not value:
            return True
        lowered = value.strip().lower()
        return lowered in {"yourapikey", "yoursecret", "yourbitfinexapikey", "yourbitfinexsecret"}

    def has_credentials(self):
        return not (self.is_placeholder_credential(self.api_key) or self.is_placeholder_credential(self.api_secret))

    def _nonce(self):
        nonce = int(time.time() * 1000000)
        if nonce <= self._last_nonce:
            nonce = self._last_nonce + 1
        self._last_nonce = nonce
        return str(nonce)

    @staticmethod
    def _body(payload):
        return json.dumps(payload or {}, separators=(",", ":"))

    def _headers(self, path, nonce, body):
        signature_payload = f"/api/{path}{nonce}{body}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            signature_payload.encode("utf-8"),
            hashlib.sha384,
        ).hexdigest()
        return {
            "bfx-nonce": nonce,
            "bfx-apikey": self.api_key,
            "bfx-signature": signature,
            "content-type": "application/json",
        }

    def auth_headers_for_test(self, path, nonce, payload=None):
        body = self._body(payload)
        return self._headers(path, nonce, body)

    def _request_json(self, url, method="GET", data=None, headers=None, ambiguous_on_failure=False):
        """Return one JSON response, retrying only requests known to be read-only.

        Authenticated Bitfinex reads use POST, so HTTP method alone cannot
        distinguish them from writes. ``ambiguous_on_failure`` is the safety
        boundary: writes still fail immediately as UNKNOWN, while idempotent
        reads may retry transient transport, HTTP, decoding, or JSON failures.
        """

        authenticated = "bfx-nonce" in {str(key).lower() for key in (headers or {})}
        # Authenticated retries must generate a new nonce and signature, so
        # ``_auth_post`` owns that retry loop instead of reusing these headers.
        delays = () if ambiguous_on_failure or authenticated else self.read_retry_delays
        for attempt in range(len(delays) + 1):
            try:
                return self._request_json_once(
                    url,
                    method=method,
                    data=data,
                    headers=headers,
                    ambiguous_on_failure=ambiguous_on_failure,
                )
            except BitfinexTransientError:
                if attempt >= len(delays):
                    raise
                self.retry_sleeper(delays[attempt])

    def _request_json_once(self, url, method="GET", data=None, headers=None, ambiguous_on_failure=False):
        data_bytes = None
        if data is not None:
            data_bytes = data.encode("utf-8")
        request_headers = {
            "User-Agent": f"MikaLendingBot/{APP_VERSION} Python",
            "Accept": "application/json",
        }
        request_headers.update(headers or {})
        req = request.Request(url, data=data_bytes, headers=request_headers, method=method)
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                content = json.loads(raw)
            except ValueError:
                content = raw
            message = f"HTTP {exc.code} {exc.reason}: {content}"
            retryable = exc.code in {408, 425, 429} or 500 <= exc.code <= 599
            if ambiguous_on_failure and retryable:
                raise BitfinexAmbiguousWriteError(message) from exc
            if retryable:
                raise BitfinexTransientError(message, category="BITFINEX_HTTP_TRANSIENT", status_code=exc.code) from exc
            category = "AUTH_PERMISSION" if exc.code in {401, 403} else "BITFINEX_HTTP"
            raise BitfinexApiError(
                message,
                category=category,
                status_code=exc.code,
                manual_required=exc.code in {401, 403},
            ) from exc
        except (
            error.URLError,
            TimeoutError,
            socket.timeout,
            ConnectionError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
            OSError,
            UnicodeError,
        ) as exc:
            if ambiguous_on_failure:
                raise BitfinexAmbiguousWriteError("Bitfinex write result is unknown after a transport failure") from exc
            raise BitfinexTransientError(str(exc)) from exc

        try:
            payload = json.loads(raw)
        except ValueError as exc:
            if ambiguous_on_failure:
                raise BitfinexAmbiguousWriteError(
                    "Bitfinex write result is unknown because the response was not valid JSON"
                ) from exc
            raise BitfinexTransientError(
                f"Invalid JSON response: {raw[:200]}", category="INVALID_READ_RESPONSE"
            ) from exc
        if isinstance(payload, list) and payload and payload[0] == "error":
            raise BitfinexApiError(str(payload))
        return payload

    def _auth_post(self, path, payload=None, ambiguous_on_failure=False):
        if not self.has_credentials():
            raise BitfinexApiError("Bitfinex API key/secret are not configured")
        with self._auth_lock:
            body = self._body(payload)
            delays = () if ambiguous_on_failure else self.read_retry_delays
            for attempt in range(len(delays) + 1):
                nonce = self._nonce()
                headers = self._headers(path, nonce, body)
                try:
                    return self._request_json(
                        f"{self.AUTH_BASE_URL}/{path}",
                        method="POST",
                        data=body,
                        headers=headers,
                        ambiguous_on_failure=ambiguous_on_failure,
                    )
                except BitfinexTransientError:
                    if attempt >= len(delays):
                        raise
                    self.retry_sleeper(delays[attempt])

    def _auth_write(self, path, payload=None):
        response = self._auth_post(path, payload, ambiguous_on_failure=True)
        if not isinstance(response, list) or len(response) < 8:
            raise BitfinexAmbiguousWriteError(
                f"Bitfinex write result is unknown because the notification was incomplete: {response}"
            )
        status = str(response[6] or "").upper()
        if status != "SUCCESS":
            message = response[7] or response
            rendered = f"Bitfinex write failed ({status or 'UNKNOWN'}): {message}"
            category = _write_rejection_category(rendered)
            raise BitfinexApiError(rendered, category=category, retryable=category == "BALANCE_DRIFT")
        return response

    def _auth_write_result(self, path, payload=None):
        try:
            return WriteResult(WriteOutcome.CONFIRMED, response=self._auth_write(path, payload))
        except BitfinexAmbiguousWriteError as exc:
            return WriteResult(WriteOutcome.UNKNOWN, error=str(exc), category=exc.category)
        except BitfinexApiError as exc:
            return WriteResult(
                WriteOutcome.DEFINITE_REJECT,
                error=str(exc),
                category=exc.category,
                retryable=exc.retryable,
            )

    def _public_get(self, path, query=None):
        url = f"{self.PUBLIC_BASE_URL}/{path}"
        if query:
            url += "?" + parse.urlencode(query)
        return self._request_json(url, method="GET")

    def wallets(self):
        return self._auth_post("v2/auth/r/wallets")

    def key_permissions(self):
        return self._auth_post("v2/auth/r/permissions")

    def active_funding_offers(self, symbol=None):
        path = "v2/auth/r/funding/offers"
        if symbol:
            path += f"/{symbol}"
        return self._auth_post(path)

    def active_funding_loans(self, symbol=None):
        path = "v2/auth/r/funding/loans"
        if symbol:
            path += f"/{symbol}"
        return self._auth_post(path)

    def active_funding_credits(self, symbol=None):
        path = "v2/auth/r/funding/credits"
        if symbol:
            path += f"/{symbol}"
        return self._auth_post(path)

    def submit_funding_offer(
        self,
        symbol,
        amount,
        rate,
        period,
        offer_type="LIMIT",
        flags=0,
        hidden=False,
    ):
        normalized_type = str(offer_type or "LIMIT").upper()
        if normalized_type not in FUNDING_OFFER_TYPES:
            raise BitfinexApiError(f"Unsupported funding offer type: {offer_type}")
        if normalized_type == "FRR":
            normalized_type = "FRRDELTAVAR"
            rate = "0"
        numeric_rate = Decimal(str(rate))
        if normalized_type in {"FRRDELTAFIX", "FRRDELTAVAR"} and numeric_rate < 0:
            raise BitfinexApiError(f"{normalized_type} rate offset cannot be negative")
        numeric_period = int(period)
        if numeric_period < 2 or numeric_period > 120:
            raise BitfinexApiError("Funding offer period must be 2-120 days")
        numeric_flags = int(flags or 0)
        if hidden:
            numeric_flags |= HIDDEN_OFFER_FLAG
        if numeric_flags & ~HIDDEN_OFFER_FLAG:
            raise BitfinexApiError("Unsupported funding offer flags")
        return self._auth_write(
            "v2/auth/w/funding/offer/submit",
            {
                "type": normalized_type,
                "symbol": symbol,
                "amount": str(amount),
                "rate": str(rate),
                "period": numeric_period,
                "flags": numeric_flags,
            },
        )

    def submit_funding_offer_result(self, *args, **kwargs):
        try:
            response = self.submit_funding_offer(*args, **kwargs)
            return WriteResult(WriteOutcome.CONFIRMED, response=response)
        except BitfinexAmbiguousWriteError as exc:
            return WriteResult(WriteOutcome.UNKNOWN, error=str(exc))
        except BitfinexApiError as exc:
            return WriteResult(WriteOutcome.DEFINITE_REJECT, error=str(exc))

    def cancel_all_funding_offers(self, currency):
        return self._auth_write(
            "v2/auth/w/funding/offer/cancel/all",
            {"currency": currency},
        )

    def cancel_funding_offer(self, offer_id):
        return self._auth_write(
            "v2/auth/w/funding/offer/cancel",
            {"id": int(offer_id)},
        )

    def cancel_funding_offer_result(self, offer_id):
        return self._auth_write_result(
            "v2/auth/w/funding/offer/cancel",
            {"id": int(offer_id)},
        )

    def ledgers(self, currency=None, start=None, end=None, limit=None, wallet=None, category=None):
        path = "v2/auth/r/ledgers"
        if currency:
            path += f"/{currency.strip().upper()}"
        path += "/hist"
        payload = {}
        for key, value in (
            ("start", start),
            ("end", end),
            ("limit", limit),
            ("wallet", wallet),
            ("category", category),
        ):
            if value is not None:
                payload[key] = value
        return self._auth_post(path, payload)

    @staticmethod
    def _history_payload(start=None, end=None, limit=None, sort=None):
        payload = {}
        for key, value in (("start", start), ("end", end), ("limit", limit), ("sort", sort)):
            if value is not None:
                payload[key] = int(value)
        return payload

    def funding_offers_history(self, symbol=None, start=None, end=None, limit=None):
        path = "v2/auth/r/funding/offers"
        if symbol:
            path += f"/{symbol}"
        path += "/hist"
        return self._auth_post(path, self._history_payload(start, end, limit))

    def funding_credits_history(self, symbol=None, start=None, end=None, limit=None):
        path = "v2/auth/r/funding/credits"
        if symbol:
            path += f"/{symbol}"
        path += "/hist"
        return self._auth_post(path, self._history_payload(start, end, limit))

    def funding_loans_history(self, symbol=None, start=None, end=None, limit=None):
        path = "v2/auth/r/funding/loans"
        if symbol:
            path += f"/{symbol}"
        path += "/hist"
        return self._auth_post(path, self._history_payload(start, end, limit))

    def funding_trades_history(self, symbol=None, start=None, end=None, limit=None, sort=None):
        path = "v2/auth/r/funding/trades"
        if symbol:
            path += f"/{symbol}"
        path += "/hist"
        return self._auth_post(path, self._history_payload(start, end, limit, sort))

    def funding_info(self, symbol):
        return self._auth_post(f"v2/auth/r/info/funding/{symbol}")

    def transfer_between_wallets(self, from_wallet, to_wallet, currency, amount):
        return self._auth_write(
            "v2/auth/w/transfer",
            {
                "from": from_wallet,
                "to": to_wallet,
                "currency": currency,
                "currency_to": currency,
                "amount": str(amount),
            },
        )

    def transfer_between_wallets_result(self, from_wallet, to_wallet, currency, amount):
        return self._auth_write_result(
            "v2/auth/w/transfer",
            {
                "from": from_wallet,
                "to": to_wallet,
                "currency": currency,
                "currency_to": currency,
                "amount": str(amount),
            },
        )

    def funding_book(self, symbol, length=250):
        return self._public_get(f"v2/book/{symbol}/P0", {"len": int(length)})

    def funding_trades(self, symbol, start=None, end=None, limit=10000, sort=-1):
        query = {"limit": min(10000, max(1, int(limit))), "sort": int(sort)}
        if start is not None:
            query["start"] = int(start)
        if end is not None:
            query["end"] = int(end)
        return self._public_get(f"v2/trades/{symbol}/hist", query)

    def funding_stats(self, symbol, start=None, end=None, limit=250):
        query = {"limit": min(250, max(1, int(limit)))}
        if start is not None:
            query["start"] = int(start)
        if end is not None:
            query["end"] = int(end)
        return self._public_get(f"v2/funding/stats/{symbol}/hist", query)

    def ticker(self, symbol):
        return self._public_get(f"v2/ticker/{symbol}")


def currency_to_symbol(currency):
    currency = currency.strip().upper()
    if currency.startswith("F"):
        return currency
    return f"f{currency}"


def symbol_to_currency(symbol):
    symbol = symbol.strip().upper()
    if symbol.startswith("F"):
        return symbol[1:]
    return symbol


def decimal_from_api(value):
    return Decimal(str(value))
