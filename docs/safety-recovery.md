# 安全恢复手册

## UNKNOWN 写入

出现超时、连接中断、响应截断、非法 JSON、通知不完整或提交成功但缺少 Offer ID 时：

1. Worker 停止继续提交，运行状态进入自动恢复中的 `PAUSED`。
2. 不要再次点击启动，也不要手工复制同参数提交。
3. 在 Bitfinex Offers、Funding Trades 和历史记录中核对金额、期限、类型、flags 与请求时间。
4. 如果系统找到唯一记录，会自动绑定，但模式只回到 `PAUSED`。
5. 无唯一记录时，在 Dashboard 的 ambiguous 处理入口填写唯一 Offer ID，或在已确认不存在后选择 `confirmAbsent`。
6. 连续两次权威快照确认后，自动恢复此前的 LIVE；只有人工暂停才不自动启动。

## 崩溃恢复

- `PLANNED / NOT_SENT`：确认未发出，启动时关闭。
- `SUBMITTING / SENT`：可能已到达交易所，转为 `AMBIGUOUS / UNKNOWN`。
- `CONFIRMED`：按绑定 Offer ID 对账。
- 撤单为 UNKNOWN：进入保护性 PAUSED，用两次至少间隔 30 秒的完整 Offers 快照确认该单仍存在或已消失；随后自动恢复此前模式。仍存在时策略可重新撤单，已消失时继续后续计划。
- 钱包转账为 UNKNOWN：进入保护性 PAUSED。完整钱包快照会显示源钱包剩余余额；两次完整同步后恢复，已完成的全额转账不会因重试而重复转出。
- 挂单提交为 UNKNOWN：每轮同步其请求时间附近的 Offers、Funding Trades 和历史 Offers。唯一匹配时自动绑定；两次权威历史与账户快照均确认不存在时自动关闭；多个候选时持续自动对账，但在无法唯一判断前保持只读 PAUSED。

## WebSocket 重连

控制台分别显示网络重连、账户对账、交易写入对账和手动挂单保护。处于保护期的挂单会显示预计可接管的时间。

实盘 Worker 将公开与账户 WebSocket、REST 失败和恢复写入状态库目录下的 `connection-events.jsonl`。记录包含 UTC 时间、错误类型和代码、尝试次数及恢复耗时。文件每 1 MB 轮转，保留最近三个归档。请求内容和凭据不进入该文件。

REST 记录用固定的 `endpoint` 字段区分钱包、挂单、放贷资产、公开盘口和成交等接口。每条失败记录提供本次请求的尝试序号、累计耗时和是否继续重试。各接口的故障时段独立计算。排查策略暂停时，可在 `channel=worker` 的 `rest_failure` 记录中用 `effect=paused` 查到触发暂停的接口。

正常周期中，公开成交历史遇到可重试的错误时，可在当前 WebSocket 成交连接仍有效、消息新鲜度小于 60 秒的情况下继续完成 REST 账户同步。日志记录 `rest_fallback`。盘口、FRR、钱包、挂单、Credits 和 Loans 仍须通过现有检查。故障恢复沿用两次权威快照。

REST 钱包、挂单、Credits 和 Loans 的响应通过结构、记录唯一性和 USD 资金平衡校验后，才更新持久化账户、关闭记录及 Hub 快照。格式无效或对账失败会进入现有只读恢复；不可用的恢复快照不计入资金采样。用于确认未知提交是否存在的历史响应也要通过结构校验。

纯网络传输故障按 5、10、20、40、60、120 秒重试。期间收到新的 USD 挂单变更可提前启动只读检查，每 60 秒最多触发一次。首次完整对账通过后，从完成时起等 15 秒再查询第二次。两次通过且无未决写入后恢复 LIVE，恢复当轮只读。遇到 HTTP 429 至少等待 60 秒，并遵守服务端给出的更长 `Retry-After`。日志中的 `recovery_probe` 分别记录检查前的等待和本轮检查耗时。

短连接与同轮内连接复用可通过 `python scripts/compare_private_rest.py --pairs 10` 对照。工具需要 urllib3，并在 Worker 停止后独占实盘锁，避免同一个 API 密钥的私有请求并发。它只查询账户，每轮之间至少间隔 12 秒，交替先后顺序，记录完整对账失败数、成功轮次 P95 及实际复用次数。

`scripts/probe_bitfinex_proxy.py` 可通过指定的本地代理做 24 小时只读探测，每两分钟采样公开 REST 及两个 WebSocket 主机。账户主机仅测试连通性，无需 API 密钥。使用日志中的 UTC 时间与 Worker 事件对齐。

每次连接建立都是新代次。新 Book snapshot 替换旧盘口；Wallet、Offers、Credits 也必须在同一认证连接代次收到完整快照。未齐全时 WebSocket 不可用于实盘，只有足够新鲜的完整 REST 快照可以暂时降级；超过时限进入 PAUSED 并自动重读。

## 数据库迁移失败

schema v5 迁移前会在数据库同级 `backups/` 创建在线备份。DDL 与版本号更新在同一事务内；失败后保持旧版本。处理步骤：

1. 保持 Worker 停止。
2. 保存错误日志和失败数据库副本。
3. 验证最新 `schema-vN-*.sqlite3` 可打开且 `PRAGMA integrity_check` 为 `ok`。
4. 修复后在复制出的数据库上运行测试，再替换生产状态库。

## 候选策略回退

候选异常时把新增资金占比设为 0，并只撤销该 `strategy_variant` 的未成交托管挂单。已经成交的 Credits 不强行处理，等待自然归还。每个 `10% → 25% → 50% → 100%` 阶段至少观察 7 天；存在保护性 PAUSED、UNKNOWN 或归属错误时不得提升。
