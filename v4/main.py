from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from mika_v4.config import load_settings, migrate_v3_config
from mika_v4.dashboard import DashboardServer
from mika_v4.domain import RuntimeMode
from mika_v4.migration import import_v3_history
from mika_v4.history import HistoricalCollector
from mika_v4.runtime import LendingRuntime
from mika_v4.store import V4Store
from mika_v4.supervisor import V4Supervisor
from mika_v4.validation import load_real_evidence, shadow_audit, validate_90_days


ROOT = Path(__file__).resolve().parent


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Mika Lending Bot V4")
    result.add_argument("--config", default=str(ROOT / "default.cfg"))
    sub = result.add_subparsers(dest="command")
    worker = sub.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("--session", required=True)
    sub.add_parser("serve", help="启动 Worker 与 127.0.0.1:8001 Dashboard")
    sub.add_parser("once", help="执行一次账户同步与完整策略重算")
    mode = sub.add_parser("mode", help="切换 PAUSED/SHADOW/LIVE")
    mode.add_argument("value", choices=["PAUSED", "SHADOW", "LIVE"])
    mode.add_argument("--confirm", default="")
    migrate_config = sub.add_parser("migrate-config", help="从 V3 配置生成独立 V4 配置")
    migrate_config.add_argument("source")
    migrate_config.add_argument("--target", default=str(ROOT / "default.cfg"))
    migrate_db = sub.add_parser("import-v3", help="只读导入 V3 历史与统计数据")
    migrate_db.add_argument("source")
    backfill = sub.add_parser("backfill", help="回填 Funding Trades/Stats；不生成盘口")
    backfill.add_argument("--days", type=int, default=90)
    sub.add_parser("capture-book", help="保存一份真实盘口快照")
    sub.add_parser("validate", help="用 60/15/15 天真实证据运行 V4/V3 配对回放")
    sub.add_parser("shadow-report", help="检查七天 SHADOW 安全条件")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "migrate-config":
        print(migrate_v3_config(Path(args.source), Path(args.target)))
        return 0
    settings = load_settings(args.config)
    store = V4Store(settings.state_db)
    if args.command == "import-v3":
        print(json.dumps(import_v3_history(Path(args.source), store), ensure_ascii=False, indent=2))
        return 0
    runtime = LendingRuntime(settings, store=store)
    if args.command == "worker":
        store.record_event("INFO", "WORKER_SESSION", {"session": args.session})
        runtime.run()
        return 0
    if args.command == "backfill":
        collector = HistoricalCollector(runtime.client, store)
        print(json.dumps(collector.backfill(args.days), ensure_ascii=False, indent=2))
        return 0
    if args.command == "capture-book":
        print(HistoricalCollector(runtime.client, store).capture_real_book())
        return 0
    if args.command == "validate":
        trades, books = load_real_evidence(store)
        report = validate_90_days(settings.policy, trades, books)
        rendered = report.as_json()
        store.record_validation_report(report.passed, rendered, max(mts for mts, _ in books))
        print(rendered)
        return 0 if report.passed else 2
    if args.command == "shadow-report":
        print(
            json.dumps(shadow_audit(store, settings.policy.shadow_days, settings.policy), ensure_ascii=False, indent=2)
        )
        return 0
    if args.command == "once":
        runtime.worker_lock.acquire()
        store.close_unsent_planned_intents()
        if store.mode() == RuntimeMode.LIVE:
            runtime.live_lock.acquire()
        try:
            runtime.bootstrap_market()
            print(json.dumps(runtime.cycle(force_full=True).mode.value, ensure_ascii=False))
        finally:
            runtime.live_lock.release()
            runtime.worker_lock.release()
        return 0
    if args.command == "mode":
        requested = RuntimeMode(args.value)
        if requested == RuntimeMode.LIVE:
            runtime.enable_live(args.confirm)
        else:
            runtime.disable_live(requested)
        print(requested.value)
        return 0
    supervisor = V4Supervisor(settings)
    server = DashboardServer(supervisor)
    supervisor.start()
    print(f"Mika V4 Dashboard: http://127.0.0.1:{settings.policy.dashboard_port} ({store.mode().value})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        supervisor.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
