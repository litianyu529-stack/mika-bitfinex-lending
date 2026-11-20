# 配置与历史迁移

## 配置迁移

在 `v4/` 目录执行：

```powershell
python main.py migrate-config ..\default.cfg --target .\default.cfg
```

迁移字段包括三类净 APR 底线、候选期限、短中权重、最大金额/比例、普通费用和外部单接管偏好。V3 的 `FRR`、`Delta`、`Hidden`、分层字段不会在 V4 启用。

API 凭据不从 V3 文件复制。推荐只设置进程环境变量：

```powershell
$env:BITFINEX_API_KEY = "..."
$env:BITFINEX_API_SECRET = "..."
```

`v4/default.cfg` 已被 Git 忽略，不应提交真实凭据。

## 历史数据迁移

```powershell
python main.py import-v3 ..\.state\lendingbot-v3.sqlite3
```

导入器以 SQLite `mode=ro` 打开 V3 数据库，并只复制：

- 市场成交；
- Funding Stats 原始统计；
- 真实盘口快照；
- 匿名化账户收益/利用率样本。

不会复制 V3 活跃订单、Credit 归属、写入意图、模式或 SAFE 恢复状态。源文件按路径、大小和修改时间生成指纹，重复执行不会重复导入。

以下任一条件会拒绝迁移：

- V3 `runtime_state.mode = SAFE`；
- V3 存在 `PLANNED`、`SUBMITTING` 或 `AMBIGUOUS` 写入意图；
- 源数据库不存在或不可只读打开。

迁移不代表实盘归属切换。只有 V3 已停止 Worker、未决写入清零、账户快照一致并完成人工审查后，才能考虑让 V4 接管具体外部单。

## 外部订单接管

默认 `adopt_external_offers = false`。如确需接管：

1. 停止交易 Worker并切换为 `PAUSED`；
2. 确认最新权威账户快照；
3. 在配置中显式开放接管并重启；
4. 对每个 Offer ID 分别输入 `ADOPT <ID>`；
5. V4 只接受 USD、可见、固定利率 LIMIT 和受支持期限。

未接管的人工单或旧机器人订单永久只读，V4 不会撤销。
