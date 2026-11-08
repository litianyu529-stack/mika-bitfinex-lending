# 第三方放贷机器人研究附录

研究日期：2026-08-08。这里只记录公开页面可观察功能和可验证的设计启发；第三方收益数字、客户案例和营销表达不作为回测证据，也不复制其专有实现。

## Willy

[Willy 官方页面](https://findingwilly.com/)公开强调自动化放贷、寻找较高利率机会和复投。V4 吸收的是两个通用思路：

- 市场处于可验证的高息上行阶段时，才增加长周期敞口；
- 新增利息余额尽快并入可成交的短中期订单。

V4 没有采用不可审计的“最高收益”承诺，也没有把第三方服务接入账户。

## LendFinex

[LendFinex Strategy 页面](https://lendfinex.com/en/strategy)公开描述不同策略、资金拆分和市场条件适配。V4 借鉴为：

- 多档阶梯而非单一利率押注；
- 钱包小额余额触发再分配；
- 长周期资金使用趋势门控；
- 策略参数与执行安全参数分离。

V4 的具体档数、150 USD 下限、50% 联动条件、两周期防抖和收益底线均由本仓库规则独立定义。

## FuNi

[FuNi 方法说明](https://funi-bot.com/en/about)公开介绍数据驱动和历史观察。V4 采用可复现的统计方法：

- 24 小时 Q25/Q75 与 IQR 异常过滤；
- 滚动 5 分钟、1 小时、6 小时确认；
- 严格按时间顺序的 60/15/15 天验证；
- 配对 Bootstrap 和独立影子期门槛。

V4 不使用第三方不可复核的模型输出或收益宣传。

## 官方约束

- Bitfinex Funding Offer 最低金额说明：[What is the minimum offer for Funding?](https://support.bitfinex.com/hc/en-us/articles/213918949-What-is-the-minimum-offer-for-Funding)
- Bitfinex 逐笔撤单接口：[Cancel Funding Offer](https://docs.bitfinex.com/reference/rest-auth-cancel-funding-offer)

实现内部把认证请求限制为 45 次/分钟，为官方接口限制留出余量；只使用具体 Offer ID 撤单。

## 结论

可借鉴的是资金分散、余额复投、趋势门控和滚动验证这类可解释机制。任何“保证收益”“始终最高利率”或没有真实盘口支持的回测结果，都不能成为 V4 自动切换实盘的依据。
