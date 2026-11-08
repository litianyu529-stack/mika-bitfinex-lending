from decimal import Decimal


D = Decimal
FRR_OFFSET_OFFER_TYPES = {"FRRDELTAFIX", "FRRDELTAVAR"}


def funding_offer_effective_rate(offer, frr_daily_rate=None):
    """Resolve an open offer's gross daily rate, or None without a usable FRR."""
    rate_real = offer.get("rate_real")
    if rate_real is not None:
        return D(str(rate_real))
    rate = D(str(offer.get("rate") or 0))
    if str(offer.get("offer_type") or "LIMIT").upper() not in FRR_OFFSET_OFFER_TYPES:
        return rate
    frr = D(str(frr_daily_rate or 0))
    return frr + rate if frr.is_finite() and frr > 0 else None


def _currency(value):
    symbol = str(value or "USD").upper()
    return symbol[1:] if symbol.startswith("F") else symbol


def parse_book(rows):
    result = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 4:
            continue
        try:
            result.append(
                {
                    "rate": D(str(row[0])),
                    "period": int(row[1]),
                    "count": int(row[2]),
                    "amount": D(str(row[3])),
                }
            )
        except (TypeError, ValueError, ArithmeticError):
            continue
    return result


def parse_wallet_rows(rows, currency="USD"):
    result = []
    wanted = str(currency).upper()
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        try:
            row_currency = _currency(row[1])
            if wanted and row_currency != wanted:
                continue
            balance = D(str(row[2]))
            result.append(
                {
                    "wallet_type": str(row[0]).lower(),
                    "currency": row_currency,
                    "balance": balance,
                    # Bitfinex explicitly uses null when BALANCE_AVAILABLE has
                    # not been calculated yet. Treating that as the full wallet
                    # balance can authorize writes against reserved funds.
                    "available": None if len(row) < 5 or row[4] is None else D(str(row[4])),
                    "unsettled_interest": D(str(row[3] or 0)) if len(row) > 3 else D("0"),
                }
            )
        except (TypeError, ValueError, ArithmeticError):
            continue
    return result


def parse_offer_rows(rows, currency="USD"):
    result = []
    wanted = str(currency).upper()
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 16:
            continue
        try:
            row_currency = _currency(row[1])
            if wanted and row_currency != wanted:
                continue
            result.append(
                {
                    "id": int(row[0]),
                    "currency": row_currency,
                    "mts_created": int(row[2] or 0),
                    "mts_updated": int(row[3] or 0),
                    "amount": abs(D(str(row[4]))),
                    "amount_original": abs(D(str(row[5]))),
                    "offer_type": str(row[6]),
                    "flags": int(row[9] or 0),
                    "status": str(row[10]),
                    "rate": D(str(row[14])),
                    "period": int(row[15]),
                    "hidden": bool(row[17]) if len(row) > 17 else bool(int(row[9] or 0) & 64),
                    "rate_real": D(str(row[20])) if len(row) > 20 and row[20] is not None else None,
                }
            )
        except (TypeError, ValueError, ArithmeticError):
            continue
    return result


def _parse_active_funding_rows(rows, currency="USD", funding_state="credit"):
    result = []
    wanted = str(currency).upper()
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 13:
            continue
        try:
            row_currency = _currency(row[1])
            if wanted and row_currency != wanted:
                continue
            result.append(
                {
                    "id": int(row[0]),
                    "currency": row_currency,
                    "side": int(row[2] or 0),
                    "mts_created": int(row[3] or 0),
                    "mts_updated": int(row[4] or 0),
                    "amount": abs(D(str(row[5]))),
                    "status": str(row[7]),
                    "rate_type": str(row[8]) if len(row) > 8 and row[8] is not None else None,
                    "rate": D(str(row[11])),
                    "period": int(row[12]),
                    "mts_opening": int(row[13] or 0) if len(row) > 13 else 0,
                    "hidden": bool(row[16]) if len(row) > 16 else False,
                    "rate_real": D(str(row[19])) if len(row) > 19 and row[19] is not None else None,
                    "funding_state": str(funding_state),
                }
            )
        except (TypeError, ValueError, ArithmeticError):
            continue
    return result


def parse_credit_rows(rows, currency="USD"):
    return _parse_active_funding_rows(rows, currency=currency, funding_state="credit")


def parse_loan_rows(rows, currency="USD"):
    return _parse_active_funding_rows(rows, currency=currency, funding_state="loan")


def parse_funding_trade_history(rows, currency="USD"):
    result = []
    wanted = str(currency).upper()
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 7:
            continue
        try:
            row_currency = _currency(row[1])
            if wanted and row_currency != wanted:
                continue
            result.append(
                {
                    "id": int(row[0]),
                    "currency": row_currency,
                    "mts": int(row[2]),
                    "offer_id": int(row[3]),
                    "amount": abs(D(str(row[4]))),
                    "rate": D(str(row[5])),
                    "period": int(row[6]),
                    "maker": row[7] if len(row) > 7 else None,
                }
            )
        except (TypeError, ValueError, ArithmeticError):
            continue
    return result


def parse_funding_trades(rows):
    trades = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        try:
            rate = D(str(row[3]))
            raw_signed_amount = D(str(row[2]))
            amount = abs(raw_signed_amount)
            period = int(row[4])
            mts = int(row[1])
        except (ValueError, TypeError, ArithmeticError):
            continue
        if not rate.is_finite() or not amount.is_finite():
            continue
        if rate <= 0 or amount <= 0 or period < 2 or period > 120 or mts < 0:
            continue
        trades.append({"id": str(row[0]), "mts": mts, "amount": amount, "rate": rate, "period": period,
                       "raw_signed_amount": raw_signed_amount})
    return sorted(trades, key=lambda item: item["mts"])


def parse_funding_stats(rows):
    stats = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 9:
            continue
        try:
            mts = int(row[0])
            frr_daily = D(str(row[3])) * D("365")
            average_period = D(str(row[4]))
            provided = abs(D(str(row[7])))
            used = abs(D(str(row[8])))
        except (ValueError, TypeError, ArithmeticError):
            continue
        utilization = D("0") if provided <= 0 else min(D("1"), used / provided)
        stats.append(
            {
                "mts": mts,
                "frr_daily_rate": frr_daily,
                "average_period": average_period,
                "provided": provided,
                "used": used,
                "utilization": utilization,
            }
        )
    return sorted(stats, key=lambda item: item["mts"])


def extract_submitted_offer_id(response):
    if not isinstance(response, list) or len(response) < 5:
        return None
    offer = response[4]
    if isinstance(offer, list) and offer:
        if isinstance(offer[0], list):
            offer = offer[0]
        if offer and not isinstance(offer[0], (list, dict)):
            return offer[0]
    if isinstance(offer, dict):
        return offer.get("id") or offer.get("ID")
    return None
