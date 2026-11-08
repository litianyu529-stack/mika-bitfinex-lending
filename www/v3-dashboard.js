(async () => {
    "use strict";

    if (!(await (window.mikaBuildReady || Promise.resolve(false)))) return;

    const state = {
        policy: null, runtime: null, preview: null, previewPolicy: null, dirty: false,
        draftVersionId: null, applyToken: null,
    };
    const byId = (id) => document.getElementById(id);
    const dashboardCsrf = document.querySelector('meta[name="mika-dashboard-csrf"]')?.content || "";
    const surface = byId("v3StrategySurface");
    if (!surface) return;

    const groups = [
        {
            title: "目标期限资金池",
            description: "配置比例是短、中、长的正常目标；全市场需求和成交概率决定池内期限与低需求池的最低150美元保留额。已成交 Credits 不参与新挂单配比。",
            fields: [
                ["short_share", "短期比例", "number", "%", { min: 0, max: 100, step: 1 }],
                ["medium_share", "中期比例", "number", "%", { min: 0, max: 100, step: 1 }],
                ["long_share", "长期比例", "number", "%", { min: 0, max: 100, step: 1 }],
                ["short_floor_apr", "短期最低净年化", "number", "%", { min: 0, step: 0.01, placeholder: "LIVE 必填", floor: "short" }],
                ["medium_floor_apr", "中期最低净年化", "number", "%", { min: 0, step: 0.01, placeholder: "LIVE 必填", floor: "medium" }],
                ["long_floor_apr", "长期最低净年化", "number", "%", { min: 0, step: 0.01, placeholder: "LIVE 必填", floor: "long" }],
                ["short_periods", "短期候选天数", "text", "天", { placeholder: "2,4,7" }],
                ["medium_periods", "中期候选天数", "text", "天", { placeholder: "14,30" }],
                ["long_periods", "长期候选天数", "text", "天", { placeholder: "120" }],
            ],
        },
        {
            title: "切片与目标成交层",
            description: "订单以 150 USD 为最低基数尽可能多地拆分，尾数平均分摊到所有订单；每 60 秒最多新建 60 单，后续循环自动补齐。成交层比例约束机器人可控的未成交挂单。",
            fields: [
                ["max_lend_amount", "最大放贷金额", "number", "USD", { min: 0, step: 0.01, placeholder: "不限制" }],
                ["max_lend_percent", "最大放贷比例", "number", "%", { min: 0, max: 100, step: 1 }],
                ["quick_share", "快速成交层", "number", "%", { min: 0, max: 100, step: 1 }],
                ["balanced_share", "平衡层", "number", "%", { min: 0, max: 100, step: 1 }],
                ["high_share", "高收益层", "number", "%", { min: 0, max: 100, step: 1 }],
            ],
        },
        {
            title: "订单类型与费用",
            description: "自动识别并安全接管外部 fUSD 挂单；接管前需两次权威快照确认，之后只按具体 Offer ID 撤销并重新规划。",
            fields: [
                ["enable_limit", "LIMIT", "checkbox", "", {}],
                ["enable_frr", "FRR", "checkbox", "", {}],
                ["enable_frr_delta_fixed", "FRR Delta Fixed", "checkbox", "", {}],
                ["enable_frr_delta_variable", "FRR Delta Variable", "checkbox", "", {}],
                ["enable_hidden", "Hidden", "checkbox", "", {}],
                ["variable_max_share", "Variable最高占比", "number", "%", { min: 0, max: 100, step: 1 }],
                ["hidden_max_share", "Hidden最高占比", "number", "%", { min: 0, max: 100, step: 1 }],
                ["normal_fee_rate", "普通手续费", "number", "%", { min: 0, max: 99, step: 0.1 }],
                ["hidden_fee_rate", "Hidden手续费", "number", "%", { min: 0, max: 99, step: 0.1 }],
            ],
        },
        {
            title: "市场、撤挂与数据",
            details: true,
            fields: [
                ["minimum_offer_minutes", "最短挂单时间", "number", "分钟", { min: 1 }],
                ["quick_pricing_mode", "快速层报价模式", "select", "通过验收后启用", {}],
                ["quick_start_premium_apr_points", "快速层净年化溢价", "number", "百分点", { min: 0, max: 1, step: 0.01 }],
                ["balanced_start_premium_percent", "平衡层／旧快速层起始溢价", "number", "%（相对底线）", { min: 0, max: 100, step: 0.1 }],
                ["high_start_premium_percent", "高收益层起始溢价", "number", "%（相对底线）", { min: 0, max: 100, step: 0.1 }],
                ["balanced_landing_stage", "快速/平衡层落点阶段", "number", "阶段", { min: 1, max: 10, step: 1 }],
                ["high_landing_stage", "高收益层落点阶段", "number", "阶段", { min: 1, max: 10, step: 1 }],
                ["quick_short_reprice_stages_minutes", "快速层短期降价阶段", "text", "分钟", { placeholder: "5 / 10 / 15 / 20 / 30 / 40 / 50 / 60 / 75 / 90" }],
                ["quick_medium_reprice_stages_minutes", "快速层中期降价阶段", "text", "分钟", { placeholder: "5 / 10 / 20 / 30 / 60 / 75 / 90 / 120 / 150 / 180" }],
                ["quick_long_reprice_stages_minutes", "快速层长期降价阶段", "text", "分钟", { placeholder: "15 / 30 / 60 / 90 / 180 / 240 / 360 / 480 / 600 / 720" }],
                ["short_reprice_stages_minutes", "平衡/高收益层短期降价阶段", "text", "分钟", { placeholder: "5 / 10 / 20 / 30 / 60 / 75 / 90 / 120 / 150 / 180" }],
                ["medium_reprice_stages_minutes", "平衡/高收益层中期降价阶段", "text", "分钟", { placeholder: "10 / 20 / 40 / 60 / 120 / 150 / 180 / 240 / 300 / 360" }],
                ["long_reprice_stages_minutes", "平衡/高收益层长期降价阶段", "text", "分钟", { placeholder: "30 / 60 / 120 / 180 / 360 / 480 / 720 / 960 / 1200 / 1440" }],
                ["reprice_cooldown_minutes", "重定价冷却", "number", "分钟", { min: 1 }],
                ["max_reprices_per_hour", "每小时重定价上限", "number", "次", { min: 0, max: 90 }],
                ["minimum_rate_change", "显著利率变化", "number", "%/日", { min: 0, step: 0.0001 }],
                ["iqr_change_fraction", "IQR阈值系数", "number", "", { min: 0, max: 2, step: 0.01 }],
                ["spike_volume_ratio", "尖峰成交量倍数", "number", "倍", { min: 1, step: 0.1 }],
                ["outlier_min_volume_share", "异常值最小量占比", "number", "%", { min: 0, max: 100, step: 0.1 }],
                ["ws_fallback_seconds", "实时断线降级时限", "number", "秒", { min: 30 }],
                ["rest_stale_seconds", "REST数据过期", "number", "秒", { min: 5 }],
                ["market_retention_days", "市场数据保留", "number", "天", { min: 1 }],
            ],
        },
    ];

    const allFields = groups.flatMap((group) => group.fields.map((field) => field[0]));
    const periodFields = new Map([
        ["short_periods", [2, 7]],
        ["medium_periods", [7, 30]],
        ["long_periods", [30, 120]],
    ]);
    const listFields = new Set([
        "short_periods", "medium_periods", "long_periods",
        "short_reprice_stages_minutes", "medium_reprice_stages_minutes", "long_reprice_stages_minutes",
        "quick_short_reprice_stages_minutes", "quick_medium_reprice_stages_minutes", "quick_long_reprice_stages_minutes",
    ]);

    function createField([name, label, type, unit, options]) {
        const wrapper = document.createElement("label");
        wrapper.className = type === "checkbox" ? "v3-check" : "v3-field";
        const input = document.createElement(type === "select" ? "select" : "input");
        input.name = name;
        if (type === "select") {
            for (const [value, label] of [["EXPLORATION_V4", "V3.5 原机制"], ["EXECUTABLE_PREMIUM_V1", "V3.6 可成交参考价＋溢价"]]) {
                const option = document.createElement("option");
                option.value = value;
                option.textContent = label;
                input.append(option);
            }
        } else input.type = type;
        for (const [key, value] of Object.entries(options || {})) {
            if (["floor"].includes(key)) continue;
            if (typeof value === "boolean") {
                if (value) input.setAttribute(key, "");
            } else input.setAttribute(key, value);
        }
        if (type === "checkbox") {
            const text = document.createElement("span");
            text.textContent = label;
            if (name.endsWith("_reprice_stages_minutes")) {
                text.textContent = `${label}（订单创建后的累计分钟）`;
            }
            wrapper.append(input, text);
        } else {
            const text = document.createElement("span");
            text.textContent = label;
            if (name.endsWith("_reprice_stages_minutes")) {
                text.textContent = `${label}（订单创建后的累计分钟）`;
            }
            const control = document.createElement("div");
            control.append(input);
            if (unit) {
                const suffix = document.createElement("em");
                suffix.textContent = unit;
                control.append(suffix);
            }
            wrapper.append(text, control);
            if (options.floor) {
                const hint = document.createElement("small");
                hint.dataset.dailyFloor = options.floor;
                hint.textContent = "下单日利率 --";
                wrapper.append(hint);
            }
        }
        return wrapper;
    }

    function renderShell() {
        surface.innerHTML = `
            <div class="v3-mode-bar">
                <div><span>运行模式</span><strong id="v3Mode">PAUSED</strong></div>
                <div><span>ACTIVE</span><strong id="v3ActiveVersion">--</strong></div>
                <div><span>DRAFT</span><strong id="v3DraftVersion">--</strong></div>
                <div><span>PENDING</span><strong id="v3PendingVersion">--</strong></div>
                <div><span>账户快照</span><strong id="v3AccountSource">--</strong></div>
                <div><span>市场行情</span><strong id="v3MarketSource">--</strong></div>
                <div><span>市场状态</span><strong id="v3Regime">--</strong></div>
                <div><span>行情机制</span><strong id="v3HistoryMode">--</strong></div>
                <div><span>候选验收</span><strong id="v3Validation">--</strong></div>
                <div class="v3-mode-actions">
                    <button id="v3PauseButton" class="button secondary" type="button">暂停</button>
                    <button id="v3ReplayButton" class="button secondary" type="button">回放</button>
                    <button id="v3LiveButton" class="button danger" type="button">实盘预检</button>
                </div>
            </div>
            <div class="v3-workspace">
                <form id="v3StrategyForm" class="v3-editor"></form>
                <aside class="v3-monitor" aria-live="polite">
                    <section><h2>实时市场</h2><dl class="v3-metrics">
                        <div><dt>FRR日利率</dt><dd id="v3Frr">--</dd></div>
                        <div><dt>最高Bid</dt><dd id="v3BestBid">--</dd></div>
                        <div><dt>最低Offer</dt><dd id="v3BestOffer">--</dd></div>
                        <div><dt>资金使用率</dt><dd id="v3Utilization">--</dd></div>
                        <div><dt>组合本金</dt><dd id="v3Principal">--</dd></div>
                        <div><dt>计划订单</dt><dd id="v3PlanCount">--</dd></div>
                    </dl></section>
                    <section><h2>计划分布</h2><div id="v3PlanList" class="v3-plan-list"><p>等待预览</p></div></section>
                    <section><h2>实际统计</h2><div id="v3Stats" class="v3-stats"></div></section>
                    <section><h2>期限自主选择</h2><div id="v3PeriodSelection" class="v3-period-selection"><p>等待市场评分</p></div></section>
                    <section><h2>近24小时期限分布</h2><div id="v3PeriodActivity" class="v3-period-activity"><p>等待运行数据</p></div></section>
                </aside>
            </div>`;
        const form = byId("v3StrategyForm");
        const fixedSafety = document.createElement("p");
        fixedSafety.className = "v3-fixed-safety";
        fixedSafety.textContent = "固定规则：需求 70%＋成交概率 30% · 低需求 5% 连续两周期确认 · 池内分配 100/0、90/10、75/25、60/40 · 最低订单 150 USD · 余额满 1 USD 自动合并复投。";
        form.append(fixedSafety);
        for (const group of groups) {
            const section = document.createElement(group.details ? "details" : "section");
            section.className = "v3-section";
            const heading = document.createElement(group.details ? "summary" : "div");
            heading.className = "v3-section-heading";
            const title = document.createElement(group.details ? "span" : "h2");
            title.textContent = group.title;
            heading.append(title);
            if (group.description) {
                const description = document.createElement("p");
                description.textContent = group.description;
                heading.append(description);
            }
            const grid = document.createElement("div");
            grid.className = "v3-grid";
            group.fields.forEach((field) => grid.append(createField(field)));
            section.append(heading, grid);
            form.append(section);
        }
        const actions = document.createElement("div");
        actions.className = "v3-form-actions";
        actions.innerHTML = `<p id="v3FormMessage" role="status">等待载入 v3 配置</p><div>
            <button id="v3PreviewButton" class="button secondary" type="button">重新计算</button>
            <button id="v3DiscardButton" class="button secondary" type="button">放弃草稿</button>
            <button class="button primary" type="submit">保存草稿</button>
            <button id="v3ApplyButton" class="button danger" type="button">应用策略</button>
        </div>`;
        form.append(actions);
    }

    async function requestJson(path, options = {}) {
        const response = await fetch(path, { cache: "no-store", ...options });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || data.ok === false) throw new Error(data.error || `请求失败 (${response.status})`);
        return data;
    }

    function postJson(path, payload) {
        return requestJson(path, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Mika-CSRF": dashboardCsrf },
            body: JSON.stringify(payload || {}),
        });
    }

    function input(name) {
        return byId("v3StrategyForm").elements[name];
    }

    function fillPolicy(policy) {
        state.policy = typeof structuredClone === "function"
            ? structuredClone(policy || {})
            : JSON.parse(JSON.stringify(policy || {}));
        for (const name of allFields) {
            const element = input(name);
            let value = policy?.[name];
            if (name === "quick_pricing_mode" && value == null) value = "EXPLORATION_V4";
            if (name === "quick_start_premium_apr_points" && value == null) value = "0.10";
            if (listFields.has(name) && Array.isArray(value)) {
                value = periodFields.has(name) ? value.join(",") : value.join(" / ");
            }
            if (element.type === "checkbox") element.checked = Boolean(value);
            else element.value = value ?? "";
        }
        input("hidden_max_share").disabled = !input("enable_hidden").checked;
        input("quick_pricing_mode").disabled = !("quick_pricing_mode" in (policy || {}));
        input("quick_start_premium_apr_points").disabled = input("quick_pricing_mode").disabled;
        state.dirty = false;
        updateDerived();
    }

    function collectPolicy() {
        const policy = {};
        for (const name of allFields) {
            const element = input(name);
            if (element.type === "checkbox") policy[name] = element.checked;
            else if (listFields.has(name)) {
                policy[name] = element.value.split(/[,/、\s]+/).map((value) => value.trim()).filter(Boolean).map(Number);
            } else policy[name] = element.value === "" ? null : element.value;
        }
        return policy;
    }

    function numberValue(name) {
        const value = Number(input(name)?.value);
        return Number.isFinite(value) ? value : 0;
    }

    function validateForm() {
        const poolTotal = numberValue("short_share") + numberValue("medium_share") + numberValue("long_share");
        const layerTotal = numberValue("quick_share") + numberValue("balanced_share") + numberValue("high_share");
        if (poolTotal !== 100) throw new Error(`期限资金池比例当前为 ${poolTotal}%，必须等于100%`);
        if (layerTotal !== 100) throw new Error(`成交层比例当前为 ${layerTotal}%，必须等于100%`);
        for (const name of ["balanced_start_premium_percent", "high_start_premium_percent"]) {
            const value = numberValue(name);
            if (value < 0 || value > 100) throw new Error("起始溢价必须在 0–100% 之间");
        }
        const quickPremium = numberValue("quick_start_premium_apr_points");
        if (!Number.isFinite(quickPremium) || quickPremium < 0 || quickPremium > 1) {
            throw new Error("快速层溢价须在 0–1 个净年化百分点之间");
        }
        for (const name of ["balanced_landing_stage", "high_landing_stage"]) {
            const value = numberValue(name);
            if (!Number.isInteger(value) || value < 1 || value > 10) throw new Error("落点阶段必须是 1–10 的整数");
        }
        for (const [name, [minimum, maximum]] of periodFields) {
            const periods = input(name).value.split(/[,/、\s]+/).filter(Boolean).map(Number);
            const unique = new Set(periods);
            if (
                periods.length === 0
                || unique.size !== periods.length
                || periods.some((value) => !Number.isInteger(value) || value < minimum || value > maximum)
            ) {
                throw new Error(`${name.startsWith("short") ? "短期" : name.startsWith("medium") ? "中期" : "长期"}天数必须是 ${minimum}–${maximum} 范围内、不重复的整数，用逗号分隔`);
            }
        }
        const stageFields = [
            ["quick_short_reprice_stages_minutes", "快速层短期"],
            ["quick_medium_reprice_stages_minutes", "快速层中期"],
            ["quick_long_reprice_stages_minutes", "快速层长期"],
            ["short_reprice_stages_minutes", "平衡/高收益层短期"],
            ["medium_reprice_stages_minutes", "平衡/高收益层中期"],
            ["long_reprice_stages_minutes", "平衡/高收益层长期"],
        ];
        for (const [name, label] of stageFields) {
            const stages = input(name).value.split(/[,/、\s]+/).filter(Boolean).map(Number);
            if (
                stages.length !== 10
                || stages.some((value) => !Number.isInteger(value) || value < 1 || value > 10080)
                || stages.some((value, index) => index > 0 && stages[index - 1] >= value)
            ) {
                throw new Error(`${label}降价阶段必须是十个递增的 1–10080 分钟整数`);
            }
        }
        if (input("enable_hidden").checked && numberValue("hidden_max_share") <= 0) throw new Error("启用Hidden时必须设置最高占比");
        if (![input("enable_limit"), input("enable_frr"), input("enable_frr_delta_fixed"), input("enable_frr_delta_variable")].some((item) => item.checked)) {
            throw new Error("至少启用一种Funding订单类型");
        }
    }

    function updateDerived() {
        const fee = numberValue("normal_fee_rate") / 100;
        for (const pool of ["short", "medium", "long"]) {
            const apr = numberValue(`${pool}_floor_apr`);
            const hint = surface.querySelector(`[data-daily-floor="${pool}"]`);
            hint.textContent = apr > 0 && fee < 1 ? `下单毛日利率 ≥ ${(apr / 365 / (1 - fee)).toFixed(6)}%` : "下单日利率 --";
        }
        input("hidden_max_share").disabled = !input("enable_hidden").checked;
        const poolTotal = numberValue("short_share") + numberValue("medium_share") + numberValue("long_share");
        const layerTotal = numberValue("quick_share") + numberValue("balanced_share") + numberValue("high_share");
        byId("v3FormMessage").textContent = `资金池 ${poolTotal}% · 成交层 ${layerTotal}%${state.dirty ? " · 未保存" : ""}`;
    }

    function percentDaily(value) {
        const number = Number(value);
        return Number.isFinite(number) ? `${(number * 100).toFixed(5)}%` : "--";
    }

    function scorePercent(value) {
        const number = Number(value);
        return Number.isFinite(number) ? `${(number * 100).toFixed(1)}%` : "--";
    }

    function renderPeriodSelection(selection, activity) {
        const container = byId("v3PeriodSelection");
        if (!container) return;
        container.replaceChildren();
        const byPool = selection?.byPool || {};
        for (const pool of ["short", "medium", "long"]) {
            const data = byPool[pool];
            if (!data) continue;
            const block = document.createElement("div");
            block.className = "v3-period-pool";
            const title = document.createElement("strong");
            const duration = Number(data.selectedDurationMs || 0);
            title.textContent = `${pool} · 当前 ${data.selectedPeriod ?? "闲置"} 天 · ${data.selectionReason || "--"}${duration ? ` · 持续 ${Math.floor(duration / 60000)} 分钟` : ""}`;
            const challengerMinutes = Math.floor(Number(data.challengerDurationMs || 0) / 60000);
            const challenger = data.challengerPeriod == null
                ? "无挑战期限"
                : `挑战 ${data.challengerPeriod} 天已持续 ${challengerMinutes}/10 分钟`;
            const allocation = data.poolAllocation || {};
            const minimum = allocation.minimumApplied ? " · 最低150 USD" : "";
            title.textContent = `${pool}池 · 第一 ${data.selectedPeriod ?? "闲置"}天 · 第二 ${data.runnerUpPeriod ?? "无"}天 · 配置 ${allocation.configuredShare ?? "--"}% · 全市场需求 ${scorePercent(allocation.absoluteDemandShare)} · 目标 ${Number(allocation.targetAmount || 0).toFixed(2)} / 当前 ${Number(allocation.currentManagedOffers || 0).toFixed(2)} USD${minimum} · ${challenger}`;
            block.append(title);
            for (const row of data.scores || []) {
                const item = document.createElement("div");
                item.className = "v3-period-score";
                const windows = row.windows || {};
                const demandState = row.lowDemandConfirmed
                    ? "低于5%，分配为0"
                    : row.lowDemandCycles
                        ? `低需求确认 ${row.lowDemandCycles}/2`
                        : "参与分配";
                item.textContent = `${row.period}天｜池内需求 ${scorePercent(row.relativeDemandShare)}｜全市场需求 ${scorePercent(row.absoluteDemandShare)}｜成交概率 ${scorePercent(row.fillScore)}｜综合评分 ${scorePercent(row.totalScore)}｜${demandState}｜1h ${windows["1h"]?.tradeCount || 0}笔/${Number(windows["1h"]?.tradeVolume || 0).toFixed(2)}｜24h ${windows["24h"]?.tradeCount || 0}笔/${Number(windows["24h"]?.tradeVolume || 0).toFixed(2)}｜7d ${windows["7d"]?.tradeCount || 0}笔/${Number(windows["7d"]?.tradeVolume || 0).toFixed(2)}｜最佳借款价 ${percentDaily(row.bestBorrowRate)}｜${row.marketEligible ? "市场支持底线" : row.eligibilityReason || "最低单等待"}`;
                block.append(item);
            }
            container.append(block);
        }
        if (!container.children.length) container.textContent = "当前没有期限评分";

        const activityContainer = byId("v3PeriodActivity");
        if (!activityContainer) return;
        activityContainer.replaceChildren();
        for (const [label, rows] of [["提交", activity?.submitted], ["成交", activity?.traded]]) {
            const line = document.createElement("p");
            line.textContent = `${label}：${(rows || []).map((row) => `${row.period}天 ${row.count}笔/${Number(row.amount || 0).toFixed(2)} USD`).join("；") || "暂无"}`;
            activityContainer.append(line);
        }
        for (const row of activity?.traded || []) {
            const line = document.createElement("p");
            line.textContent = `${row.period}天 · ${formatFundingWait(row, "weightedWaitMinutes", 1)}`;
            activityContainer.append(line);
        }
    }

    function renderPreview(data) {
        state.preview = data;
        const signals = data.signals || {};
        const plan = data.plan || {};
        byId("v3Regime").textContent = signals.regime || "--";
        byId("v3Frr").textContent = percentDaily(signals.frr_daily_rate);
        byId("v3BestBid").textContent = percentDaily(signals.best_bid);
        byId("v3BestOffer").textContent = percentDaily(signals.best_offer);
        byId("v3Utilization").textContent = signals.utilization == null ? "--" : `${(Number(signals.utilization) * 100).toFixed(1)}%`;
        byId("v3Principal").textContent = `${Number(data.principal || plan.planned_amount || 0).toLocaleString("zh-CN")} USD`;
        byId("v3PlanCount").textContent = `${(plan.plan || []).length} / ${plan.target_slice_count || 0}`;
        renderPeriodSelection(data.periodSelection || signals.periodSelection, data.periodActivity);
        const basis = data.accountSnapshot || {};
        byId("v3AccountSource").textContent = basis.stale
            ? `历史快照 · ${basis.timestamp ? new Date(basis.timestamp).toLocaleString("zh-CN", { hour12: false }) : "无数据"}`
            : "实时账户";
        const list = byId("v3PlanList");
        list.replaceChildren();
        const orders = plan.plan || [];
        if (!orders.length) {
            const empty = document.createElement("p");
            const reasons = {
                NO_AVAILABLE_BALANCE: "当前没有可用 Funding 余额",
                BELOW_MINIMUM: "可用余额低于最低单笔金额",
                OFFER_RATIOS_SATISFIED: "当前挂单比例已在允许范围内",
                MARKET_BELOW_FLOOR: "当前市场利率低于收益下限",
                FUNDING_CAP_REACHED: "已达到最大放贷资金上限",
            };
            empty.textContent = reasons[plan.emptyReason || plan.empty_reason] || (data.warnings || []).join("；") || "当前没有新挂单计划";
            list.append(empty);
            return;
        }
        const summary = new Map();
        for (const order of orders) {
            const key = `${order.pool}|${order.layer}|${order.display_type}${order.hidden ? " Hidden" : ""}|${order.period}`;
            const row = summary.get(key) || { count: 0, amount: 0, rate: 0 };
            row.count += 1;
            row.amount += Number(order.amount);
            row.rate += Number(order.effective_rate);
            summary.set(key, row);
        }
        for (const [key, row] of summary) {
            const [pool, layer, type, period] = key.split("|");
            const item = document.createElement("div");
            item.className = "v3-plan-summary";
            const heading = document.createElement("strong");
            const detail = document.createElement("span");
            const value = document.createElement("b");
            heading.textContent = `${pool} · ${layer}`;
            detail.textContent = `${type} · ${period}天 · ${row.count}笔`;
            value.textContent = `${row.amount.toFixed(2)} USD · ${((row.rate / row.count) * 100).toFixed(5)}%`;
            item.append(heading, detail, value);
            list.append(item);
        }
    }

    function renderRuntime(data, status) {
        const runtime = data.runtime || {};
        state.runtime = runtime;
        byId("v3Mode").textContent = data.displayMode || runtime.mode || "PAUSED";
        for (const [id, version] of [
            ["v3ActiveVersion", data.activeStrategy?.version_id],
            ["v3DraftVersion", data.draftStrategy?.version_id],
            ["v3PendingVersion", data.pendingStrategy?.version_id],
        ]) {
            const element = byId(id);
            element.textContent = version || "--";
            element.title = version || "";
        }
        const market = status?.market || {};
        const marketData = status?.marketData || data.marketSnapshot || {};
        renderPeriodSelection(
            status?.strategyV3?.periodSelection || status?.market?.periodSelection,
            status?.strategyV3?.periodActivity,
        );
        byId("v3MarketSource").textContent = `${marketData.source || "--"}${marketData.publicAgeMs != null ? ` · ${marketData.publicAgeMs}ms` : ""}`;
        const history = status?.marketHistory || {};
        const fallbackLabels = {
            V35_CONFIGURED: "配置使用原机制", VALIDATION_GATE_NOT_PASSED: "候选未通过验收",
            WARMING: "七天统计恢复中", BACKFILLING: "七天覆盖核验中", STALE: "统计已过期",
            DEGRADED: "行情核验失败", HISTORY_UNAVAILABLE: "行情统计不可用",
        };
        byId("v3HistoryMode").textContent = history.inputMode === "COMPLETE_WINDOW"
            ? "完整七天／分期限" : `V3.5 · ${fallbackLabels[history.fallbackReason] || history.fallbackReason || "等待新状态"}`;
        byId("v3Validation").textContent = status?.v36Validation?.approved ? "通过" : "未通过／保持原机制";
        if (status?.last_update) byId("v3AccountSource").textContent = `实时账户 · ${status.last_update}`;
        if (market.regime) byId("v3Regime").textContent = market.regime;
        if (market.frr_daily_rate != null) byId("v3Frr").textContent = percentDaily(market.frr_daily_rate);
        if (market.best_bid != null) byId("v3BestBid").textContent = percentDaily(market.best_bid);
        if (market.best_offer != null) byId("v3BestOffer").textContent = percentDaily(market.best_offer);
        if (market.utilization != null) byId("v3Utilization").textContent = `${(Number(market.utilization) * 100).toFixed(1)}%`;
        if (status?.account?.total != null) byId("v3Principal").textContent = `${Number(status.account.total).toLocaleString("zh-CN")} USD`;
        const dust = status?.strategyV3?.dustConsolidation || {};
        const takeoverRows = status?.strategyV3?.externalTakeover?.offers || [];
        const activeTakeovers = takeoverRows.filter((row) => !["CLOSED", "ERROR"].includes(row.state));
        if (runtime.mode === "PAUSED" && runtime.safe_reason) {
            byId("v3FormMessage").textContent = `PAUSED：${runtime.safe_reason}`;
        } else if (dust.state && dust.state !== "IDLE") {
            const labels = {
                PLANNED: "小额余额等待合并",
                CANCELLING: "正在撤销最小短期单",
                READY: `准备按 ${dust.target_period || "当前赢家"} 天重新挂出`,
                SUBMITTING: `正在按 ${dust.target_period || "当前赢家"} 天重新挂出`,
                AMBIGUOUS: "小额复投写入待安全对账",
            };
            byId("v3FormMessage").textContent = labels[dust.state] || `小额复投：${dust.state}`;
        } else if ((status?.openOffers || []).some((offer) => offer.manualProtection?.protected)) {
            const count = status.openOffers.filter((offer) => offer.manualProtection?.protected).length;
            byId("v3FormMessage").textContent = `手动挂单保护中：${count} 笔 · 两小时后接管剩余挂单 · 安全规则可提前处理`;
        } else if (activeTakeovers.length) {
            byId("v3FormMessage").textContent = `外部挂单接管中：${activeTakeovers.length} 笔`;
        }
        byId("v3ApplyButton").disabled = !data.draftStrategy;
        const canDiscardPending = Boolean(
            data.pendingStrategy
            && !data.process?.running
            && runtime.mode !== "LIVE"
        );
        const discardButton = byId("v3DiscardButton");
        discardButton.disabled = !data.draftStrategy && !canDiscardPending;
        discardButton.textContent = data.draftStrategy
            ? "放弃草稿"
            : canDiscardPending
                ? "撤销待应用策略"
                : "放弃草稿";
    }

    function renderStats(statistics) {
        const container = byId("v3Stats");
        container.replaceChildren();
        for (const key of ["7d", "30d", "90d", "all"]) {
            const row = statistics?.[key];
            if (!row) continue;
            const item = document.createElement("div");
            item.className = "v3-stat-summary";
            const heading = document.createElement("strong");
            const utilization = document.createElement("span");
            const interest = document.createElement("span");
            const apr = document.createElement("b");
            const coverage = document.createElement("small");
            heading.textContent = key === "all" ? "全部" : key;
            utilization.textContent = `利用率 ${Number(row.utilizationPercent).toFixed(1)}%`;
            interest.textContent = `净利息 ${Number(row.netInterest).toFixed(4)} USD`;
            apr.textContent = `净APR ${Number(row.actualNetAprPercent).toFixed(2)}%`;
            coverage.textContent = `本金采样覆盖 ${Number(row.sampleDays || 0).toFixed(2)} 天`;
            item.append(heading, utilization, interest, apr, coverage);
            container.append(item);
        }
    }

    async function loadAll() {
        let config;
        try {
            config = await requestJson("/api/config");
            if (!state.dirty) fillPolicy(config.strategyV3Draft || config.strategyV3Pending || config.strategyV3);
        } catch (error) {
            byId("v3FormMessage").textContent = `配置载入失败：${error.message}`;
            return false;
        }

        const [runtimeResult, statusResult, statsResult] = await Promise.allSettled([
                requestJson("/api/runtime/v3"),
                requestJson("/api/status"),
                requestJson("/api/stats/v3"),
        ]);
        const runtime = runtimeResult.status === "fulfilled" ? runtimeResult.value : {};
        const status = statusResult.status === "fulfilled" ? statusResult.value : {};
        const stats = statsResult.status === "fulfilled" ? statsResult.value : {};
        renderRuntime(runtime, status);
        renderStats(stats.statistics);
        const failures = [runtimeResult, statusResult, statsResult]
            .filter((result) => result.status === "rejected")
            .map((result) => result.reason?.message || "未知错误");
        if (failures.length) {
            byId("v3FormMessage").textContent = `配置已载入；部分运行数据失败：${failures.join("；")}`;
        }
        return true;
    }

    async function preview() {
        try {
            validateForm();
            byId("v3FormMessage").textContent = "正在读取公共市场并生成计划…";
            const data = await postJson("/api/strategy/v3/preview", { strategyV3: collectPolicy() });
            renderPreview(data);
            state.previewPolicy = JSON.stringify(collectPolicy());
            byId("v3FormMessage").textContent = (data.warnings || []).join("；") || "预览完成";
        } catch (error) {
            byId("v3FormMessage").textContent = error.message;
        }
    }

    async function saveDraft(event) {
        event.preventDefault();
        try {
            validateForm();
            const strategyV3 = collectPolicy();
            byId("v3FormMessage").textContent = "正在生成影响预览…";
            const impact = await postJson("/api/strategy/v3/preview", { strategyV3 });
            renderPreview(impact);
            state.previewPolicy = JSON.stringify(strategyV3);
            const result = await postJson("/api/strategy/v3/draft", {
                strategyV3,
                previewToken: impact.previewToken,
            });
            state.dirty = false;
            state.draftVersionId = result.draftVersionId || null;
            state.applyToken = result.applyToken || null;
            byId("v3FormMessage").textContent = result.status === "DRAFT" ? "草稿已保存；预览后点击应用策略" : "策略已保存并生效";
            await loadAll();
        } catch (error) {
            byId("v3FormMessage").textContent = error.message;
        }
    }

    async function applyDraft() {
        try {
            if (!state.preview || state.previewPolicy !== JSON.stringify(collectPolicy())) {
                throw new Error("配置已变化，请先重新计算影响预览");
            }
            const plan = state.preview.plan || {};
            const accepted = window.confirm(
                `确认应用当前 V3 策略？\n` +
                `预计新计划 ${(plan.plan || []).length} 笔，金额 ${Number(plan.planned_amount || 0).toFixed(2)} USD。\n` +
                `将先撤销 ${(state.preview.incompatibleOffers || []).filter((row) => row.managed).length} 笔不兼容机器人挂单。\n` +
                `${(state.preview.nonChangeableCredits || []).length} 笔已成交贷款无法改变。`
            );
            if (!accepted) return;
            if (!state.draftVersionId || !state.applyToken) {
                throw new Error("应用确认已失效，请重新计算并保存草稿");
            }
            const result = await postJson("/api/strategy/v3/apply", {
                draftVersionId: state.draftVersionId,
                applyToken: state.applyToken,
            });
            state.applyToken = null;
            state.draftVersionId = null;
            byId("v3FormMessage").textContent = result.status === "PENDING" ? "策略已进入待应用状态，机器人将按撤挂节奏完成切换" : "策略已生效";
            await loadAll();
        } catch (error) {
            byId("v3FormMessage").textContent = error.message;
        }
    }

    async function setMode(mode) {
        try {
            const result = await postJson("/api/runtime/v3/mode", { mode });
            if (result.replay) renderPreview({ signals: result.replay.signals || {}, plan: { plan: result.replay.orders || [] }, replay: result.replay });
            await loadAll();
        } catch (error) {
            byId("v3FormMessage").textContent = error.message;
        }
    }

    async function discardDraft() {
        try {
            const result = await postJson("/api/strategy/v3/discard", {});
            state.preview = null;
            state.previewPolicy = null;
            state.applyToken = null;
            state.draftVersionId = null;
            const discarded = result.discarded || [];
            byId("v3FormMessage").textContent = discarded.includes("PENDING")
                ? "待应用策略已撤销，恢复 ACTIVE 策略"
                : discarded.includes("DRAFT")
                    ? "草稿已放弃，恢复 ACTIVE 策略"
                    : "没有可放弃的策略";
            await loadAll();
        } catch (error) {
            byId("v3FormMessage").textContent = error.message;
        }
    }

    renderShell();
    byId("v3StrategyForm").addEventListener("input", () => { state.dirty = true; updateDerived(); });
    byId("v3StrategyForm").addEventListener("change", () => { state.dirty = true; updateDerived(); });
    byId("v3StrategyForm").addEventListener("submit", saveDraft);
    byId("v3PreviewButton").addEventListener("click", preview);
    byId("v3ApplyButton").addEventListener("click", applyDraft);
    byId("v3DiscardButton").addEventListener("click", discardDraft);
    byId("v3PauseButton").addEventListener("click", () => setMode("PAUSED"));
    byId("v3ReplayButton").addEventListener("click", () => setMode("REPLAY"));
    byId("v3LiveButton").addEventListener("click", () => byId("preflightButton")?.click());
    loadAll().then((loaded) => { if (loaded) preview(); });
    window.setInterval(loadAll, 15000);
})();
