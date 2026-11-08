"""Configuration parsing, validation, persistence, and strategy-version storage."""

import configparser
import datetime
import hashlib
import json
import os
import shutil
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

from bitfinex import Bitfinex
from FileUtils import atomic_write_text
from StateStore import LendingStateStore
from StrategyV3 import (
    StrategyPolicyV3,
    json_decimal,
    policy_v3_to_json,
    policy_v3_with_overrides,
    validate_policy_v3,
)


DEFAULT_CONFIG = "default.cfg"
DEFAULT_CONFIG_EXAMPLE = "default.cfg.example"
DEFAULT_DASHBOARD_JSON = os.path.join("www", "botlog.json")
DEFAULT_V3_STATE_DB = os.path.join(".state", "lendingbot-v3.sqlite3")


class ConfigError(Exception):
    pass


@dataclass
class Settings:
    api_key: str
    api_secret: str
    currencies: list[str]
    sleep_active: float
    sleep_inactive: float
    transferable_currencies: list[str]
    transfer_from_wallets: list[str]
    output_currency: str
    json_file: str
    json_log_size: int
    web_server: bool
    once: bool
    strategy_v3: StrategyPolicyV3
    state_db_file: str


def _settings_args():
    return SimpleNamespace(
        apikey=None,
        apisecret=None,
        sleeptimeactive=None,
        sleeptimeinactive=None,
        jsonfile=None,
        jsonlogsize=None,
        startwebserver=False,
        no_server=False,
        once=False,
    )


def ensure_config_file(config_location):
    if os.path.exists(config_location):
        return False
    if config_location == DEFAULT_CONFIG and os.path.exists(DEFAULT_CONFIG_EXAMPLE):
        shutil.copy(DEFAULT_CONFIG_EXAMPLE, config_location)
        return True
    return False


def read_config(config_location):
    created = ensure_config_file(config_location)
    config = configparser.ConfigParser()
    config.read(config_location)
    return config, created


def get_option(config, section, option, fallback=None):
    if config.has_option(section, option):
        return config.get(section, option)
    return fallback


def get_decimal(config, section, option, fallback):
    return Decimal(str(get_option(config, section, option, fallback)))


def get_decimal_percent(config, section, option, fallback):
    return Decimal(str(get_option(config, section, option, fallback))) / Decimal("100")


def get_boolean(config, section, option, fallback):
    if config.has_option(section, option):
        return config.getboolean(section, option)
    return fallback


def split_csv(raw):
    if not raw:
        return []
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


V3_PERCENT_FIELDS = {
    "short_floor_apr",
    "medium_floor_apr",
    "long_floor_apr",
    "normal_fee_rate",
    "hidden_fee_rate",
    "minimum_rate_change",
    "outlier_min_volume_share",
}
V3_BOOL_FIELDS = {
    "enable_limit",
    "enable_frr",
    "enable_frr_delta_fixed",
    "enable_frr_delta_variable",
    "enable_hidden",
    "adopt_external_offers",
}
V3_INT_FIELDS = {
    "minimum_offer_minutes",
    "reprice_cooldown_minutes",
    "max_reprices_per_hour",
    "ws_fallback_seconds",
    "rest_stale_seconds",
    "market_retention_days",
}
V3_LIST_FIELDS = {
    "short_periods",
    "medium_periods",
    "long_periods",
    "short_reprice_stages_minutes",
    "medium_reprice_stages_minutes",
    "long_reprice_stages_minutes",
    "quick_short_reprice_stages_minutes",
    "quick_medium_reprice_stages_minutes",
    "quick_long_reprice_stages_minutes",
}
V3_PERIOD_FIELDS = {"short_periods", "medium_periods", "long_periods"}
V3_STRING_FIELDS = {"quick_pricing_mode"}
V3_FIXED_FIELDS = {"max_pool_shift", "adopt_external_offers"}
V3_CONFIG_FIELDS = tuple(
    name
    for name in StrategyPolicyV3.__dataclass_fields__
    if name not in {"version", "currency", *V3_FIXED_FIELDS}
)


def strategy_v3_from_config(config):
    section = "STRATEGY_V3"
    values = {}
    if config.has_section(section):
        for field_name in V3_CONFIG_FIELDS:
            raw = get_option(config, section, field_name, None)
            if raw is None:
                continue
            if (
                field_name
                in {
                    "short_floor_apr",
                    "medium_floor_apr",
                    "long_floor_apr",
                    "hidden_max_share",
                    "max_lend_amount",
                }
                and str(raw).strip() == ""
            ):
                values[field_name] = None
            elif field_name in V3_PERCENT_FIELDS:
                values[field_name] = Decimal(str(raw)) / Decimal("100")
            elif field_name in V3_BOOL_FIELDS:
                values[field_name] = str(raw).strip().lower() in {"1", "true", "yes", "on"}
            elif field_name in V3_STRING_FIELDS:
                values[field_name] = str(raw).strip()
            elif field_name in V3_INT_FIELDS:
                values[field_name] = int(raw)
            elif field_name in V3_PERIOD_FIELDS:
                values[field_name] = raw
            elif field_name in V3_LIST_FIELDS:
                values[field_name] = tuple(int(item.strip()) for item in str(raw).split(",") if item.strip())
            else:
                values[field_name] = Decimal(str(raw))
    try:
        return validate_policy_v3(policy_v3_with_overrides(StrategyPolicyV3(), values))
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc


def strategy_v3_config_values(policy):
    values = {}
    for field_name in V3_CONFIG_FIELDS:
        value = getattr(policy, field_name)
        if value is None:
            values[field_name] = ""
        elif field_name in V3_PERCENT_FIELDS:
            values[field_name] = decimal_percent_to_config(value)
        elif isinstance(value, bool):
            values[field_name] = str(value).lower()
        elif field_name in V3_PERIOD_FIELDS:
            values[field_name] = ",".join(str(item) for item in value)
        elif isinstance(value, tuple):
            values[field_name] = ",".join(str(item) for item in value)
        elif isinstance(value, Decimal):
            values[field_name] = decimal_to_config(value)
        else:
            values[field_name] = str(value)
    return values


def strategy_v3_api_values(policy):
    payload = policy_v3_to_json(policy)
    payload.pop("max_pool_shift", None)
    for field_name in V3_PERCENT_FIELDS:
        value = getattr(policy, field_name)
        payload[field_name] = None if value is None else decimal_percent_to_config(value)
    payload["hidden_max_share"] = (
        None if policy.hidden_max_share is None else decimal_to_config(policy.hidden_max_share)
    )
    payload["gross_daily_floors_percent"] = {
        pool: None
        if policy.floor_apr(pool) is None
        else decimal_percent_to_config(Decimal(payload["gross_daily_floors"][pool]))
        for pool in ("short", "medium", "long")
    }
    payload["fixedSafety"] = {
        "minimumOrderUsd": "150",
        "dustReinvestMinimumUsd": "1",
        "demandWeightPercent": "70",
        "fillProbabilityWeightPercent": "30",
        "lowDemandThresholdPercent": "5",
        "lowDemandConfirmationCycles": 2,
        "allocationCurve": "100/0,90/10,75/25,60/40",
        "automaticExternalTakeover": True,
        "submissionLimitPer60Seconds": 60,
    }
    return payload


def strategy_v3_from_api_payload(payload, base=None):
    values = {}
    payload = payload or {}
    for field_name in V3_CONFIG_FIELDS:
        if field_name not in payload:
            continue
        value = payload[field_name]
        if field_name in {
            "short_floor_apr",
            "medium_floor_apr",
            "long_floor_apr",
            "hidden_max_share",
            "max_lend_amount",
        } and value in (None, ""):
            values[field_name] = None
        elif field_name in V3_PERCENT_FIELDS:
            values[field_name] = Decimal(str(value)) / Decimal("100")
        elif field_name in V3_BOOL_FIELDS:
            values[field_name] = (
                value if isinstance(value, bool) else str(value).strip().lower() in {"1", "true", "yes", "on"}
            )
        elif field_name in V3_INT_FIELDS:
            values[field_name] = int(value)
        elif field_name in V3_STRING_FIELDS:
            values[field_name] = str(value).strip()
        elif field_name in V3_PERIOD_FIELDS:
            values[field_name] = value
        elif field_name in V3_LIST_FIELDS:
            raw = value if isinstance(value, (list, tuple)) else str(value).split(",")
            values[field_name] = tuple(int(item) for item in raw if str(item).strip())
        else:
            values[field_name] = Decimal(str(value))
    try:
        return validate_policy_v3(policy_v3_with_overrides(base or StrategyPolicyV3(), values))
    except (ValueError, ArithmeticError) as exc:
        raise ConfigError(str(exc)) from exc


def strategy_v3_from_record(record, require_live=False):
    if record is None:
        return None
    try:
        return validate_policy_v3(
            policy_v3_with_overrides(StrategyPolicyV3(), record["policy"]),
            require_live_floors=require_live,
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc


def strategy_v3_semantically_equal(left, right):
    return json_decimal(left.__dict__) == json_decimal(right.__dict__)


def ensure_active_strategy_v3(store, settings):
    active = store.strategy("ACTIVE")
    if active is None:
        store.save_strategy(json_decimal(settings.strategy_v3.__dict__), status="ACTIVE")
        active = store.strategy("ACTIVE")
    return active, strategy_v3_from_record(active)


def mirror_active_strategy_v3(config_path, policy):
    update_config_file_preserving_comments(
        config_path,
        {"STRATEGY_V3": strategy_v3_config_values(policy)},
    )


def backup_strategy_state(config_path, state_db_file):
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(state_db_file)), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    suffix = datetime.datetime.now().astimezone().strftime("%Y%m%d-%H%M%S-%f")
    config_backup = os.path.join(backup_dir, f"default-{suffix}.cfg")
    database_backup = os.path.join(backup_dir, f"lendingbot-v3-{suffix}.sqlite3")
    shutil.copy2(config_path, config_backup)
    source = sqlite3.connect(state_db_file)
    target = sqlite3.connect(database_backup)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()
    return {"config": config_backup, "database": database_backup}


def _legacy_quick_defaults_are_compatible(record, canonical):
    """Accept only an intact V3.5 policy extended by the two inert defaults.

    Keeping its original content hash avoids creating a second strategy and
    rebinding existing chains merely because the application gained fields.
    """
    raw = record.get("policy")
    defaults = {"quick_pricing_mode": "EXPLORATION_V4", "quick_start_premium_apr_points": "0.10"}
    if not isinstance(raw, dict) or set(canonical) - set(raw) != set(defaults):
        return False
    if any(canonical.get(key) != value for key, value in defaults.items()):
        return False
    if set(raw) != set(canonical) - set(defaults):
        return False
    original = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    expected = json.dumps({key: value for key, value in canonical.items() if key not in defaults},
                          ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (original == expected
            and record.get("version_id") == hashlib.sha256(original.encode("utf-8")).hexdigest()[:16])


def normalize_current_active_strategy(config_path):
    config, _ = read_config(config_path)
    settings = build_settings(_settings_args(), config)
    pre_migration_active = None
    if os.path.exists(settings.state_db_file):
        try:
            connection = sqlite3.connect(settings.state_db_file)
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT * FROM strategy_versions WHERE status='ACTIVE' ORDER BY activated_at_ms DESC LIMIT 1"
            ).fetchone()
            if row is not None:
                pre_migration_active = dict(row)
                pre_migration_active["policy"] = json.loads(pre_migration_active.pop("policy_json"))
        except (sqlite3.Error, ValueError):
            pre_migration_active = None
        finally:
            try:
                connection.close()
            except (NameError, sqlite3.Error):
                pass
    backup = None
    if pre_migration_active is not None:
        pre_policy = strategy_v3_from_record(pre_migration_active)
        pre_serialized = json.dumps(
            json_decimal(pre_policy.__dict__), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        pre_version = hashlib.sha256(pre_serialized.encode("utf-8")).hexdigest()[:16]
        if pre_migration_active["version_id"] != pre_version and not _legacy_quick_defaults_are_compatible(
            pre_migration_active, json_decimal(pre_policy.__dict__),
        ):
            backup = backup_strategy_state(config_path, settings.state_db_file)
    store = LendingStateStore(settings.state_db_file)
    active, policy = ensure_active_strategy_v3(store, settings)
    canonical = json_decimal(policy.__dict__)
    serialized = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    canonical_version = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
    if active["version_id"] == canonical_version or _legacy_quick_defaults_are_compatible(active, canonical):
        store.repair_normalized_reprice_chains(active["version_id"])
        return {"changed": False, "versionId": active["version_id"], "backup": None}
    if backup is None:
        backup = backup_strategy_state(config_path, settings.state_db_file)
    version_id = store.normalize_active_strategy(
        canonical,
        reason=f"normalized incomplete ACTIVE {active['version_id']} to full V3 schema",
    )
    mirror_active_strategy_v3(config_path, policy)
    return {"changed": True, "fromVersion": active["version_id"], "versionId": version_id, "backup": backup}


def build_settings(args, config, config_created=False):
    api_key = args.apikey or os.environ.get("BITFINEX_API_KEY") or get_option(config, "BITFINEX", "apikey", "")
    api_secret = args.apisecret or os.environ.get("BITFINEX_API_SECRET") or get_option(config, "BITFINEX", "secret", "")
    currencies = split_csv(get_option(config, "BITFINEX", "currencies", "USD"))

    return Settings(
        api_key=api_key,
        api_secret=api_secret,
        currencies=currencies,
        sleep_active=float(args.sleeptimeactive or get_option(config, "BOT", "sleeptimeactive", "60")),
        sleep_inactive=float(args.sleeptimeinactive or get_option(config, "BOT", "sleeptimeinactive", "300")),
        transferable_currencies=split_csv(get_option(config, "BOT", "transferablecurrencies", "")),
        transfer_from_wallets=split_csv(get_option(config, "BOT", "transferfromwallets", "exchange,margin")),
        output_currency="USD",
        json_file=args.jsonfile or get_option(config, "BOT", "jsonfile", ""),
        json_log_size=int(args.jsonlogsize or get_option(config, "BOT", "jsonlogsize", "-1")),
        web_server=(args.startwebserver or config.getboolean("BOT", "startwebserver", fallback=False))
        and not args.no_server,
        once=args.once,
        strategy_v3=strategy_v3_from_config(config),
        state_db_file=get_option(config, "BOT", "statedbfile", DEFAULT_V3_STATE_DB),
    )


def validate_settings(settings):
    if settings.sleep_active < 1 or settings.sleep_active > 3600:
        raise ConfigError("sleeptimeactive must be 1-3600")
    if settings.sleep_inactive < 1 or settings.sleep_inactive > 3600:
        raise ConfigError("sleeptimeinactive must be 1-3600")
    if settings.currencies != ["USD"]:
        raise ConfigError("strategy v3 supports exactly currencies = USD")
    if set(settings.transferable_currencies) - {"USD"}:
        raise ConfigError("transferablecurrencies can only contain USD")
    try:
        validate_policy_v3(settings.strategy_v3)
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc


def decimal_percent_to_config(value):
    text = format(Decimal(value) * Decimal("100"), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def decimal_to_config(value):
    text = format(Decimal(value), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def status_decimal(value):
    return format(Decimal(value).quantize(Decimal("0.00000001")), "f")


def config_api_payload(config_path):
    config, _ = read_config(config_path)
    settings = build_settings(_settings_args(), config)
    validate_settings(settings)
    client = Bitfinex(settings.api_key, settings.api_secret)
    store = LendingStateStore(settings.state_db_file)
    active, active_policy = ensure_active_strategy_v3(store, settings)
    return {
        "configPath": os.path.abspath(config_path),
        "credentialsConfigured": client.has_credentials(),
        "bitfinex": {
            "currencies": ",".join(settings.currencies),
        },
        "bot": {
            "sleeptimeactive": str(
                int(settings.sleep_active) if settings.sleep_active.is_integer() else settings.sleep_active
            ),
            "sleeptimeinactive": str(
                int(settings.sleep_inactive) if settings.sleep_inactive.is_integer() else settings.sleep_inactive
            ),
            "transferablecurrencies": ",".join(settings.transferable_currencies),
            "transferfromwallets": ",".join(settings.transfer_from_wallets).lower(),
            "outputcurrency": settings.output_currency,
            "jsonfile": settings.json_file or DEFAULT_DASHBOARD_JSON.replace("\\", "/"),
            "jsonlogsize": str(settings.json_log_size if settings.json_log_size != -1 else 200),
            "startwebserver": str(settings.web_server).lower(),
        },
        "strategyV3": strategy_v3_api_values(active_policy),
        "strategyV3Draft": None
        if store.strategy("DRAFT") is None
        else strategy_v3_api_values(strategy_v3_from_record(store.strategy("DRAFT"))),
        "strategyV3Pending": None
        if store.strategy("PENDING") is None
        else strategy_v3_api_values(strategy_v3_from_record(store.strategy("PENDING"))),
        "strategyV3State": {
            "active": active,
            "draft": store.strategy("DRAFT"),
            "pending": store.strategy("PENDING"),
        },
        "runtimeV3": {
            "defaultMode": "PAUSED",
            "stateDatabase": os.path.abspath(settings.state_db_file),
            "supportedCurrencies": ["USD"],
        },
    }


def update_config_file_preserving_comments(config_path, updates):
    if not os.path.exists(config_path):
        ensure_config_file(config_path)
    with open(config_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    pending = {section: dict(values) for section, values in updates.items() if values}
    output = []
    current_section = None

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            if current_section in pending and pending[current_section]:
                for key, value in pending[current_section].items():
                    output.append(f"{key} = {value}\n")
                pending[current_section].clear()
            current_section = stripped[1:-1].upper()
            output.append(line)
            continue

        handled = False
        if current_section in pending and "=" in line and not stripped.startswith("#"):
            key_part = line.split("=", 1)[0].strip().lower()
            section_updates = pending.get(current_section, {})
            if key_part in section_updates:
                output.append(f"{key_part} = {section_updates.pop(key_part)}\n")
                handled = True
        if not handled:
            output.append(line)

    if current_section in pending and pending[current_section]:
        for key, value in pending[current_section].items():
            output.append(f"{key} = {value}\n")
        pending[current_section].clear()

    for section, values in pending.items():
        if values:
            output.append(f"\n[{section}]\n")
            for key, value in values.items():
                output.append(f"{key} = {value}\n")

    atomic_write_text(config_path, "".join(output))


def strategy_v3_version_id(policy):
    serialized = json.dumps(json_decimal(policy.__dict__), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
