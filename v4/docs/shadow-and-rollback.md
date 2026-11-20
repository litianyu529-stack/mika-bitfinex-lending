# 七天 SHADOW、验证与回退

## 并行运行

- V3 Dashboard：`127.0.0.1:8000`；V3 可保持 LIVE。
- V4 Dashboard：`127.0.0.1:8001`；V4 保持 SHADOW。
- SHADOW 会读取真实账户和行情，持久化假设计划，但不会调用 submit/cancel。
- 两个版本的数据库和状态文件独立，LIVE 锁位于共同仓库根目录 `.state/lendingbot-live.lock`。

## 离线证据

先回填 90 天成交和 Funding Stats，再通过真实运行每分钟保存盘口：

```powershell
python main.py backfill --days 90
python main.py capture-book
```

Bitfinex 无法补回任意历史时刻的完整盘口，因此验证器在真实分钟盘口不足 90 天时会明确报错，不会用合成盘口伪装证据。

`python main.py validate` 使用无未来信息的顺序窗口：

- 前 60 天训练观察；
- 接着 15 天验证；
- 最后 15 天测试。

保守回放包括盘口排队折扣、部分成交、每次计划变化的撤挂写入、闲置与费用。V3 基线是 V4 验证模块内固定版本的单层可成交模型，不导入 V3 运行代码。通过条件：

- 验证集和测试集扣除 15% 费用后的本金时间净利息均改善至少 3%；
- 配对 Bootstrap 95% 下界大于 0；
- 利用率下降不超过 2 个百分点；
- V4 无 150 USD、收益底线或金额安全违规。

## 七天影子门槛

```powershell
python main.py shadow-report
```

最少检查：

- 覆盖时长达到 7 天；
- 没有重复计划指纹；
- SHADOW 时间窗内没有交易写入意图；
- 任一短/中资金池每小时重建不超过 6 次；
- 账户资金守恒、收益底线和 3% 提升由验证报告及人工报告共同确认。

代码不会自动把草稿 PR 转为 Ready，也不会自动切换 LIVE。

## 切换与回退

切换到 V4 前：停止 V3 LIVE Worker，确认 V3 非 SAFE、没有未决写入且共享锁已释放；然后在 V4 Dashboard 重新同步账户并输入精确 LIVE 确认词。

回退到 V3：

1. 将 V4 切换为 PAUSED；
2. 等待所有 V4 已发出的写入获得权威确认；
3. 在 Bitfinex 核对 V4 未成交 Offers 和已成交 Credits；
4. 逐笔决定撤销或保留 V4 Offers；Credits 不提前关闭；
5. 确认共享 LIVE 锁释放；
6. 按 V3 的只读预检和人工确认流程重新启动。

数据库保留用于审计，不要覆盖或复制成另一版本的活跃状态库。
