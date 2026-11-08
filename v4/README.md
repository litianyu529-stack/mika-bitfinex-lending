# Mika Lending Bot V4（v0.4.0）

V4 是仓库内完全自包含的 fUSD 放贷机器人。它不导入根目录 V3 的源码、配置或运行状态；默认以 `SHADOW` 启动，在 `127.0.0.1:8001` 展示计划但不向交易所写入。

核心特性：

- 公共 WebSocket 持续更新行情，60 秒同步账户，5 分钟完整重算。
- 基于最佳借款价、5 分钟 VWAP 和 1 小时中位数的稳健市场锚点。
- 短期最多 5 档、中期最多 4 档的联动网格，每档至少 150 USD。
- 长期 120 天单使用净 APR 门槛、三档资金比例和趋势确认。
- 所有交易写入持久化为 `PLANNED → SUBMITTING → CONFIRMED/CLOSED/AMBIGUOUS`。
- 结果未知立即进入 SAFE；两次至少相隔 30 秒的权威快照才能恢复。
- 只逐笔撤销 V4 托管 Offer ID；不会调用“取消全部订单”。
- V3/V4 共享仓库根目录 `.state/lendingbot-live.lock`，禁止同账户双 LIVE。

## 快速开始

```powershell
cd v4
Copy-Item default.cfg.example default.cfg
$env:BITFINEX_API_KEY = "你的只读/交易 API Key"
$env:BITFINEX_API_SECRET = "你的 API Secret"
python -m pip install -r requirements.txt
python main.py serve
```

也可以双击 `start-v4.cmd`。首次启动脚本只会复制示例配置，不会启用 LIVE。

Dashboard：<http://127.0.0.1:8001>

常用命令：

```powershell
# 从 V3 配置迁移允许的字段
python main.py migrate-config ..\default.cfg --target .\default.cfg

# 只读导入历史行情和统计；不导入活跃订单归属
python main.py import-v3 ..\.state\lendingbot-v3.sqlite3

# 回填 90 天成交与 Funding Stats（不会伪造历史盘口）
python main.py backfill --days 90

# 定期保存真实分钟盘口；生产部署应由任务调度器每分钟调用
python main.py capture-book

# 60/15/15 天顺序验证与七天影子审计
python main.py validate
python main.py shadow-report

# 完整质量检查
powershell -ExecutionPolicy Bypass -File .\verify-v4.ps1
```

LIVE 必须由 Dashboard 输入精确确认词 `ENABLE V4 LIVE`，或执行：

```powershell
python main.py mode LIVE --confirm "ENABLE V4 LIVE"
```

这只开放 V4 的 LIVE 状态，不代表七天影子门槛已经达标。首次实盘前仍须人工检查 `shadow-report`、验证报告、API 权限、账户归属和 V3 状态。

## 文档

- [架构与数据流](docs/architecture.md)
- [联动网格示例](docs/linked-grid.md)
- [长期门控与 APR 换算](docs/long-gate.md)
- [配置和历史迁移](docs/migration.md)
- [SAFE 恢复](docs/safe-recovery.md)
- [无人值守自动自愈](docs/unattended-recovery.md)
- [影子运行、验证和回退](docs/shadow-and-rollback.md)
- [第三方策略研究附录](docs/research.md)

软件不承诺收益。Funding 存在利率、流动性、交易所、API 和操作风险；任何 LIVE 切换均由账户所有者人工决定。
