from __future__ import annotations

import configparser
import os
import tempfile
from io import StringIO
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from pathlib import Path


D = Decimal


class ConfigError(ValueError):
    pass


def _csv_ints(value: str, fallback: tuple[int, ...]) -> tuple[int, ...]:
    if not str(value or "").strip():
        return fallback
    try:
        values = tuple(sorted({int(item.strip()) for item in str(value).replace("/", ",").split(",") if item.strip()}))
    except ValueError as exc:
        raise ConfigError("期限必须是逗号分隔的整数") from exc
    return values


def _optional_decimal(value: str | None) -> D | None:
    return None if value is None or not str(value).strip() else D(str(value).strip())


@dataclass(frozen=True)
class V4Policy:
    currency: str = "USD"
    short_floor_apr_percent: D = D("6.89")
    medium_floor_apr_percent: D = D("8")
    long_floor_apr_percent: D = D("10")
    short_weight: D = D("60")
    medium_weight: D = D("20")
    short_periods: tuple[int, ...] = (2, 4, 7)
    medium_periods: tuple[int, ...] = (14, 30)
    long_period: int = 120
    short_max_rungs: int = 5
    medium_max_rungs: int = 4
    grid_min_step_percent: D = D("0.001")
    grid_max_step_percent: D = D("0.005")
    grid_iqr_fraction: D = D("0.5")
    partial_fill_trigger_percent: D = D("50")
    idle_merge_trigger: D = D("5")
    short_floor_stale_minutes: int = 60
    medium_floor_stale_minutes: int = 180
    long_floor_stale_minutes: int = 720
    max_group_rebuilds_per_hour: int = 6
    max_authenticated_requests_per_minute: int = 45
    normal_fee_percent: D = D("15")
    max_lend_amount: D | None = None
    max_lend_percent: D = D("100")
    adopt_external_offers: bool = False
    fast_sync_seconds: int = 60
    full_replan_seconds: int = 300
    market_stale_seconds: int = 90
    dashboard_port: int = 8001
    shadow_days: int = 7

    @property
    def fee_fraction(self) -> D:
        return self.normal_fee_percent / D("100")

    def floor_apr_percent(self, pool: str) -> D:
        return getattr(self, f"{pool}_floor_apr_percent")

    def periods(self, pool: str) -> tuple[int, ...]:
        if pool == "long":
            return (self.long_period,)
        return getattr(self, f"{pool}_periods")

    def max_rungs(self, pool: str) -> int:
        return getattr(self, f"{pool}_max_rungs")


@dataclass(frozen=True)
class V4Settings:
    api_key: str
    api_secret: str
    policy: V4Policy
    state_db: Path
    status_file: Path
    config_file: Path
    repository_root: Path


def validate_policy(policy: V4Policy) -> V4Policy:
    if policy.currency.upper() != "USD":
        raise ConfigError("V4 仅支持 USD")
    if policy.short_weight <= 0 or policy.medium_weight <= 0:
        raise ConfigError("短期和中期权重必须为正数")
    for pool in ("short", "medium", "long"):
        if not D("0") < policy.floor_apr_percent(pool) <= D("1000"):
            raise ConfigError(f"{pool} 最低净年化必须在 0-1000% 之间")
    if not policy.short_periods or any(day < 2 or day > 7 for day in policy.short_periods):
        raise ConfigError("短期期限必须在 2-7 天")
    if not policy.medium_periods or any(day < 8 or day > 30 for day in policy.medium_periods):
        raise ConfigError("中期期限必须在 8-30 天")
    if policy.long_period != 120:
        raise ConfigError("V4 长期固定为 120 天")
    if not 1 <= policy.short_max_rungs <= 5 or not 1 <= policy.medium_max_rungs <= 4:
        raise ConfigError("短期最多 5 档，中期最多 4 档")
    if not D("0") < policy.grid_min_step_percent <= policy.grid_max_step_percent <= D("0.1"):
        raise ConfigError("网格间距范围无效")
    if not D("1") <= policy.partial_fill_trigger_percent <= D("100"):
        raise ConfigError("部分成交触发比例必须在 1-100%")
    if policy.idle_merge_trigger != D("5"):
        raise ConfigError("V4 小额余额合并阈值固定为 5 USD")
    if any(
        value <= 0
        for value in (
            policy.short_floor_stale_minutes,
            policy.medium_floor_stale_minutes,
            policy.long_floor_stale_minutes,
        )
    ):
        raise ConfigError("底线等待时间必须为正数")
    if policy.max_group_rebuilds_per_hour != 6:
        raise ConfigError("短中期每组每小时重建上限固定为 6 次")
    if not 1 <= policy.max_authenticated_requests_per_minute <= 45:
        raise ConfigError("认证请求内部上限必须在 1-45 次/分钟")
    if policy.max_authenticated_requests_per_minute != 45:
        raise ConfigError("V4 认证请求限流固定为 45 次/分钟")
    if not D("0") <= policy.normal_fee_percent < D("100"):
        raise ConfigError("费用比例无效")
    if policy.max_lend_amount is not None and policy.max_lend_amount < 0:
        raise ConfigError("最大放贷额不能为负")
    if not D("0") <= policy.max_lend_percent <= D("100"):
        raise ConfigError("最大放贷比例必须在 0-100%")
    if policy.fast_sync_seconds != 60 or policy.full_replan_seconds != 300:
        raise ConfigError("V4 调度固定为 60 秒快速同步和 300 秒完整重算")
    if policy.market_stale_seconds != 90:
        raise ConfigError("V4 行情过期阈值固定为 90 秒")
    if policy.dashboard_port != 8001:
        raise ConfigError("V4 Dashboard 端口固定为 8001")
    if policy.grid_iqr_fraction != D("0.5"):
        raise ConfigError("V4 IQR 网格系数固定为 0.5")
    if policy.shadow_days < 7:
        raise ConfigError("V4 LIVE 前至少需要 7 天 SHADOW")
    return policy


def _get_bool(section: configparser.SectionProxy, name: str, fallback: bool) -> bool:
    if name not in section:
        return fallback
    return section.getboolean(name)


def _find_repository_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start.parent


def load_settings(path: str | os.PathLike[str]) -> V4Settings:
    config_path = Path(path).resolve()
    parser = configparser.ConfigParser()
    if not parser.read(config_path, encoding="utf-8"):
        raise ConfigError(f"找不到 V4 配置：{config_path}")
    bitfinex = parser["BITFINEX"] if parser.has_section("BITFINEX") else {}
    strategy = parser["STRATEGY_V4"] if parser.has_section("STRATEGY_V4") else {}
    bot = parser["BOT_V4"] if parser.has_section("BOT_V4") else {}
    base = V4Policy()
    policy = replace(
        base,
        short_floor_apr_percent=D(strategy.get("short_floor_apr", base.short_floor_apr_percent)),
        medium_floor_apr_percent=D(strategy.get("medium_floor_apr", base.medium_floor_apr_percent)),
        long_floor_apr_percent=D(strategy.get("long_floor_apr", base.long_floor_apr_percent)),
        short_weight=D(strategy.get("short_weight", base.short_weight)),
        medium_weight=D(strategy.get("medium_weight", base.medium_weight)),
        short_periods=_csv_ints(strategy.get("short_periods", ""), base.short_periods),
        medium_periods=_csv_ints(strategy.get("medium_periods", ""), base.medium_periods),
        short_max_rungs=int(strategy.get("short_max_rungs", base.short_max_rungs)),
        medium_max_rungs=int(strategy.get("medium_max_rungs", base.medium_max_rungs)),
        grid_min_step_percent=D(strategy.get("grid_min_step_percent", base.grid_min_step_percent)),
        grid_max_step_percent=D(strategy.get("grid_max_step_percent", base.grid_max_step_percent)),
        partial_fill_trigger_percent=D(strategy.get("partial_fill_trigger_percent", base.partial_fill_trigger_percent)),
        idle_merge_trigger=D(strategy.get("idle_merge_trigger", base.idle_merge_trigger)),
        short_floor_stale_minutes=int(strategy.get("short_floor_stale_minutes", base.short_floor_stale_minutes)),
        medium_floor_stale_minutes=int(strategy.get("medium_floor_stale_minutes", base.medium_floor_stale_minutes)),
        long_floor_stale_minutes=int(strategy.get("long_floor_stale_minutes", base.long_floor_stale_minutes)),
        max_group_rebuilds_per_hour=int(strategy.get("max_group_rebuilds_per_hour", base.max_group_rebuilds_per_hour)),
        normal_fee_percent=D(strategy.get("normal_fee_percent", base.normal_fee_percent)),
        max_lend_amount=_optional_decimal(strategy.get("max_lend_amount")),
        max_lend_percent=D(strategy.get("max_lend_percent", base.max_lend_percent)),
        adopt_external_offers=_get_bool(strategy, "adopt_external_offers", base.adopt_external_offers),
        fast_sync_seconds=int(bot.get("fast_sync_seconds", base.fast_sync_seconds)),
        full_replan_seconds=int(bot.get("full_replan_seconds", base.full_replan_seconds)),
        market_stale_seconds=int(bot.get("market_stale_seconds", base.market_stale_seconds)),
        dashboard_port=int(bot.get("dashboard_port", base.dashboard_port)),
        shadow_days=int(bot.get("shadow_days", base.shadow_days)),
    )
    validate_policy(policy)
    root = config_path.parent
    repository_root = _find_repository_root(root)
    return V4Settings(
        api_key=os.getenv("BITFINEX_API_KEY", str(bitfinex.get("apikey", ""))).strip(),
        api_secret=os.getenv("BITFINEX_API_SECRET", str(bitfinex.get("secret", ""))).strip(),
        policy=policy,
        state_db=(root / str(bot.get("state_db", ".state/lendingbot-v4.sqlite3"))).resolve(),
        status_file=(root / str(bot.get("status_file", "www/status.json"))).resolve(),
        config_file=config_path,
        repository_root=repository_root,
    )


def policy_payload(policy: V4Policy) -> dict[str, object]:
    payload = asdict(policy)
    for key, value in tuple(payload.items()):
        if isinstance(value, D):
            payload[key] = format(value, "f")
        elif isinstance(value, tuple):
            payload[key] = list(value)
    return payload


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def migrate_v3_config(source: Path, target: Path) -> Path:
    old = configparser.ConfigParser()
    if not old.read(source, encoding="utf-8"):
        raise ConfigError(f"找不到 V3 配置：{source}")
    v3 = old["STRATEGY_V3"] if old.has_section("STRATEGY_V3") else {}
    result = configparser.ConfigParser()
    result["BITFINEX"] = {"apikey": "", "secret": ""}
    result["BOT_V4"] = {
        "fast_sync_seconds": "60",
        "full_replan_seconds": "300",
        "market_stale_seconds": "90",
        "dashboard_port": "8001",
        "state_db": ".state/lendingbot-v4.sqlite3",
        "status_file": "www/status.json",
        "shadow_days": "7",
    }
    result["STRATEGY_V4"] = {
        "short_floor_apr": v3.get("short_floor_apr", "6.89"),
        "medium_floor_apr": v3.get("medium_floor_apr", "8"),
        "long_floor_apr": v3.get("long_floor_apr", "10"),
        "short_weight": v3.get("short_share", "60"),
        "medium_weight": v3.get("medium_share", "20"),
        "short_periods": v3.get("short_periods", "2,4,7"),
        "medium_periods": v3.get("medium_periods", "14,30"),
        "short_max_rungs": "5",
        "medium_max_rungs": "4",
        "grid_min_step_percent": "0.001",
        "grid_max_step_percent": "0.005",
        "partial_fill_trigger_percent": "50",
        "idle_merge_trigger": "5",
        "short_floor_stale_minutes": "60",
        "medium_floor_stale_minutes": "180",
        "long_floor_stale_minutes": "720",
        "max_group_rebuilds_per_hour": "6",
        "normal_fee_percent": v3.get("normal_fee_rate", "15"),
        "max_lend_amount": v3.get("max_lend_amount", ""),
        "max_lend_percent": v3.get("max_lend_percent", "100"),
        "adopt_external_offers": v3.get("adopt_external_offers", "false"),
    }
    buffer = StringIO()
    buffer.write("# V4 独立配置。API 凭据优先使用 BITFINEX_API_KEY / BITFINEX_API_SECRET。\n")
    buffer.write("# V3 的 FRR、Delta、Hidden 设置不会迁移到 V4。\n\n")
    result.write(buffer)
    atomic_write(target, buffer.getvalue())
    return target


EDITABLE_POLICY_FIELDS = {
    "short_floor_apr_percent": "short_floor_apr",
    "medium_floor_apr_percent": "medium_floor_apr",
    "long_floor_apr_percent": "long_floor_apr",
    "short_weight": "short_weight",
    "medium_weight": "medium_weight",
    "short_periods": "short_periods",
    "medium_periods": "medium_periods",
    "short_max_rungs": "short_max_rungs",
    "medium_max_rungs": "medium_max_rungs",
    "grid_min_step_percent": "grid_min_step_percent",
    "grid_max_step_percent": "grid_max_step_percent",
    "partial_fill_trigger_percent": "partial_fill_trigger_percent",
    "short_floor_stale_minutes": "short_floor_stale_minutes",
    "medium_floor_stale_minutes": "medium_floor_stale_minutes",
    "long_floor_stale_minutes": "long_floor_stale_minutes",
    "max_lend_amount": "max_lend_amount",
    "max_lend_percent": "max_lend_percent",
    "adopt_external_offers": "adopt_external_offers",
}


def update_editable_policy(settings: V4Settings, changes: dict[str, object]) -> V4Policy:
    unexpected = sorted(set(changes) - set(EDITABLE_POLICY_FIELDS))
    if unexpected:
        raise ConfigError(f"这些设置不可从 Dashboard 修改：{', '.join(unexpected)}")
    current_policy = load_settings(settings.config_file).policy
    values: dict[str, object] = {}
    for name, raw in changes.items():
        current = getattr(current_policy, name)
        if isinstance(current, tuple):
            values[name] = _csv_ints(
                ",".join(str(item) for item in raw) if isinstance(raw, list) else str(raw), current
            )
        elif isinstance(current, bool):
            values[name] = raw if isinstance(raw, bool) else str(raw).strip().lower() in {"1", "true", "yes", "on"}
        elif isinstance(current, int):
            values[name] = int(raw)
        elif isinstance(current, D):
            values[name] = D(str(raw))
        elif current is None:
            values[name] = _optional_decimal(None if raw is None else str(raw))
        else:
            values[name] = raw
    candidate = validate_policy(replace(current_policy, **values))
    parser = configparser.ConfigParser()
    parser.read(settings.config_file, encoding="utf-8")
    if not parser.has_section("STRATEGY_V4"):
        parser.add_section("STRATEGY_V4")
    for field_name, value in values.items():
        option = EDITABLE_POLICY_FIELDS[field_name]
        if isinstance(value, tuple):
            rendered = ",".join(str(item) for item in value)
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        elif value is None:
            rendered = ""
        else:
            rendered = str(value)
        parser.set("STRATEGY_V4", option, rendered)
    buffer = StringIO()
    parser.write(buffer)
    atomic_write(settings.config_file, buffer.getvalue())
    return candidate
