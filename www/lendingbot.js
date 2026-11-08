"use strict";

const expectedDashboardBuild = document.querySelector('meta[name="mika-dashboard-build"]')?.content || "";
window.mikaBuildCompatible = false;
window.mikaBuildReady = (async () => {
    try {
        const response = await fetch("/api/health", { cache: "no-store" });
        const health = await response.json();
        const compatible = response.ok
            && health.service === "mika-lending-dashboard-v3"
            && health.buildId === expectedDashboardBuild
            && expectedDashboardBuild
            && !expectedDashboardBuild.includes("__MIKA_");
        if (!compatible) throw new Error("页面与 Dashboard 后端版本不一致");
        window.mikaBuildCompatible = true;
        window.mikaDashboardHealth = health;
        return true;
    } catch (error) {
        const blocker = document.createElement("div");
        blocker.className = "build-blocker";
        blocker.setAttribute("role", "alert");
        const section = document.createElement("section");
        const marker = document.createElement("p");
        const heading = document.createElement("h1");
        const detail = document.createElement("div");
        const diagnostic = document.createElement("small");
        marker.textContent = "VERSION MISMATCH";
        heading.textContent = "控制台版本不一致，所有操作已阻断";
        detail.textContent = "请关闭此页面并重新双击桌面的“Bitfinex 自动放贷机器人”图标。程序不会在版本不一致时保存策略、预检或启动实盘。";
        diagnostic.textContent = String(error.message || error);
        section.append(marker, heading, detail, diagnostic);
        blocker.append(section);
        document.body.append(blocker);
        document.querySelectorAll("button, input, select, textarea").forEach((element) => { element.disabled = true; });
        return false;
    }
})();

const $ = (id) => document.getElementById(id);
const dashboardCsrf = document.querySelector('meta[name="mika-dashboard-csrf"]')?.content || "";
const state = {
    config: null,
    status: null,
    control: { running: false, pid: null, startedAt: null, returnCode: null, stopReason: null },
    preflight: null,
    activeDialog: null,
    previousFocus: null,
};

function formatFundingWait(summary, field = "averageWaitSeconds", unitsPerMinute = 60) {
    const raw = summary?.[field];
    const minutes = raw == null || raw === "" ? NaN : Number(raw) / unitsPerMinute;
    const value = Number.isFinite(minutes) && minutes >= 0 ? `${minutes.toFixed(1)} 分钟` : "—";
    const coverageRaw = summary?.waitCoveragePercent;
    const coverage = coverageRaw == null || coverageRaw === "" ? NaN : Number(coverageRaw);
    const coverageText = Number.isFinite(coverage) ? `${coverage.toFixed(1)}%` : "—";
    const valid = summary?.waitValidCount ?? "—";
    const missing = summary?.waitMissingCount ?? "—";
    return `首次挂单至成交平均等待：${value} · 按成交金额加权 · 金额覆盖率 ${coverageText} · 有效 ${valid} 笔 / 缺失 ${missing} 笔`;
}

function safeNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : 0;
}

function formatAmount(value, digits = 2) {
    const number = safeNumber(value);
    return number.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function formatPercent(value, digits = 4) {
    if (value === null || value === undefined || value === "") return "--";
    return `${formatAmount(value, digits)}%`;
}

function apiError(response, data) {
    if (!response.ok || data?.ok === false) throw new Error(data?.error || `请求失败（${response.status}）`);
    return data;
}

async function getJson(url) {
    const response = await fetch(url, { cache: "no-store" });
    return apiError(response, await response.json());
}

async function postJson(url, payload = {}) {
    const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-Mika-CSRF": dashboardCsrf },
        body: JSON.stringify(payload),
    });
    return apiError(response, await response.json());
}

function setConnection(connected) {
    const chip = $("connectionChip");
    chip.classList.toggle("offline", !connected);
    chip.querySelector("span").textContent = connected ? "本地已连接" : "连接中断";
}

let toastTimer;
function showToast(message) {
    const toast = $("toast");
    toast.textContent = message;
    toast.classList.add("visible");
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => toast.classList.remove("visible"), 3200);
}

function activeRoute() {
    const route = (location.hash || "#overview").slice(1);
    return ["overview", "strategy", "logs"].includes(route) ? route : "overview";
}

function renderRoute() {
    const route = activeRoute();
    for (const name of ["overview", "strategy", "logs"]) {
        const tab = $(`${name}Tab`);
        const panel = $(`${name}Panel`);
        const selected = name === route;
        tab.setAttribute("aria-selected", String(selected));
        tab.tabIndex = selected ? 0 : -1;
        panel.hidden = !selected;
    }
    if (route === "overview") window.requestAnimationFrame(drawDistribution);
}

function navigateTabs(event) {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    const tabs = [...document.querySelectorAll('.tabs [role="tab"]')];
    const current = tabs.indexOf(document.activeElement);
    if (current < 0) return;
    event.preventDefault();
    let target = current;
    if (event.key === 'Home') target = 0;
    else if (event.key === 'End') target = tabs.length - 1;
    else if (event.key === 'ArrowLeft') target = (current - 1 + tabs.length) % tabs.length;
    else target = (current + 1) % tabs.length;
    tabs[target].focus();
    tabs[target].click();
}

function statusMode(status = state.status || {}) {
    return String(status.operationMode || status.runtime?.mode || "PAUSED").toUpperCase();
}

function statusAge(status = state.status || {}) {
    if (!status.last_update) return Infinity;
    const parsed = Date.parse(String(status.last_update).replace(" ", "T"));
    return Number.isFinite(parsed) ? Math.max(0, Date.now() - parsed) : Infinity;
}

function statusIsStale(status = state.status || {}) {
    return statusAge(status) > 180000;
}

function normalizedOffer(offer) {
    const rate = "dailyRatePercent" in offer
        ? offer.dailyRatePercent
        : (offer.rate == null ? null : safeNumber(offer.rate) * 100);
    return {
        ...offer,
        dailyRatePercent: rate,
        offerType: offer.offerType || offer.offer_type || offer.rate_type || "LIMIT",
        managedByBot: offer.managedByBot ?? Boolean(offer.managed),
        bucket: offer.bucket || [offer.pool, offer.layer].filter(Boolean).join(" · "),
        created: offer.created || offer.mts_created,
        elapsedMinutes: offerElapsedMinutes(offer),
    };
}

const creditGroupDefinitions = [
    { key: "short", label: "短期", range: "2–7 天" },
    { key: "medium", label: "中期", range: "7–30 天" },
    { key: "long", label: "长期", range: "30–120 天" },
];

function creditDisplayPool(credit) {
    if (["short", "medium", "long"].includes(credit.displayPool)) return credit.displayPool;
    if (["short", "medium", "long"].includes(credit.pool)) return credit.pool;
    const period = safeNumber(credit.period);
    if (period <= 7) return "short";
    return period <= 30 ? "medium" : "long";
}

function normalizedCredit(credit) {
    const effectiveRate = credit.effectiveRate ?? credit.rate_real ?? credit.rate;
    const dailyRatePercent = credit.dailyRatePercent ?? (
        effectiveRate == null ? null : safeNumber(effectiveRate) * 100
    );
    const opening = credit.mts_opening || credit.mts_created || null;
    const elapsedDays = credit.elapsedDays ?? (
        opening ? Math.max(0, Date.now() - safeNumber(opening)) / 86400000 : null
    );
    const contractEndAtMs = credit.contractEndAtMs ?? (
        opening ? safeNumber(opening) + safeNumber(credit.period) * 86400000 : null
    );
    return {
        ...credit,
        displayPool: creditDisplayPool(credit),
        dailyRatePercent,
        elapsedDays,
        contractEndAtMs,
        opening,
        managedByBot: credit.managedByBot ?? Boolean(credit.managed),
        displayType: credit.display_type || credit.rate_type || "FIXED",
        fundingState: credit.funding_state || "credit",
    };
}

function fundingStateLabel(credit) {
    return credit.fundingState === "loan" ? "Funding Loan" : "Funding Credit";
}

function formatDays(value, digits = 1) {
    if (value === null || value === undefined || value === "") return "--";
    return `${formatAmount(value, digits)} 天`;
}

function formatDateTime(value) {
    if (!value) return "--";
    const date = new Date(safeNumber(value));
    if (Number.isNaN(date.getTime())) return "--";
    return date.toLocaleString("zh-CN", {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
    });
}

function creditAttribution(credit) {
    if (!credit.managedByBot) return "外部订单";
    const pool = credit.pool || credit.displayPool;
    const poolLabel = { short: "短期", medium: "中期", long: "长期" }[pool] || pool;
    const layerLabel = {
        quick: "快速成交",
        balanced: "均衡",
        high: "高收益",
    }[credit.layer] || credit.layer;
    return `机器人 · ${poolLabel}${layerLabel ? ` · ${layerLabel}` : ""}`;
}

function distributionValues() {
    const account = state.status?.account || {};
    return [
        { label: "已放贷", value: safeNumber(account.credits), color: "#069a91" },
        { label: "活跃挂单", value: safeNumber(account.offers), color: "#83cdbf" },
        { label: "可用余额", value: safeNumber(account.wallet), color: "#669aa4" },
    ];
}

function drawDistribution() {
    const canvas = $("distributionChart");
    if (!canvas || canvas.offsetParent === null) return;
    const size = canvas.clientWidth || 232;
    const ratio = Math.max(1, window.devicePixelRatio || 1);
    canvas.width = Math.round(size * ratio);
    canvas.height = Math.round(size * ratio);
    const context = canvas.getContext("2d");
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, size, size);
    const values = distributionValues();
    const total = values.reduce((sum, item) => sum + item.value, 0);
    const radius = size * .39;
    const lineWidth = size * .115;
    context.lineWidth = lineWidth;
    context.lineCap = "butt";
    if (total <= 0) {
        context.beginPath();
        context.strokeStyle = "#dce6e4";
        context.arc(size / 2, size / 2, radius, 0, Math.PI * 2);
        context.stroke();
        return;
    }
    let start = -Math.PI / 2;
    for (const item of values) {
        if (item.value <= 0) continue;
        const end = start + (item.value / total) * Math.PI * 2;
        context.beginPath();
        context.strokeStyle = item.color;
        context.arc(size / 2, size / 2, radius, start, end);
        context.stroke();
        start = end;
    }
}

function renderLegend(values, total) {
    const container = $("distributionLegend");
    container.replaceChildren();
    for (const item of values) {
        const row = document.createElement("div");
        row.className = "legend-row";
        const swatch = document.createElement("i");
        swatch.className = "legend-swatch";
        swatch.style.setProperty("--legend-color", item.color);
        const label = document.createElement("span");
        label.textContent = item.label;
        const amount = document.createElement("strong");
        const percent = total > 0 ? ` · ${formatAmount(item.value / total * 100, 1)}%` : "";
        amount.textContent = `${formatAmount(item.value)}${percent}`;
        row.append(swatch, label, amount);
        container.append(row);
    }
}

function durationSortValue(value) {
    if (value == null || value === "") return Infinity;
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : Infinity;
}

function offerElapsedMinutes(offer) {
    const cumulative = durationSortValue(offer.repriceState?.elapsedMinutes);
    if (Number.isFinite(cumulative)) return cumulative;
    const created = Number(offer.created || offer.mts_created);
    return Number.isFinite(created) && created > 0 && created <= Date.now()
        ? (Date.now() - created) / 60000 : null;
}

function ageLabel(elapsedMinutes) {
    if (!Number.isFinite(durationSortValue(elapsedMinutes))) return "--";
    const minutes = Math.floor(Number(elapsedMinutes));
    if (minutes < 60) return `${minutes} 分钟`;
    const hours = Math.floor(minutes / 60);
    return hours < 24
        ? `${hours} 小时 ${minutes % 60} 分钟`
        : `${Math.floor(hours / 24)} 天 ${hours % 24} 小时 ${minutes % 60} 分钟`;
}

function repriceLabel(offer) {
    const age = ageLabel(offer.elapsedMinutes);
    const protection = offer.manualProtection;
    if (protection?.protected) {
        const until = new Date(protection.protectedUntilMs).toLocaleString("zh-CN");
        const minutes = Math.max(0, Math.ceil((protection.protectedUntilMs - Date.now()) / 60000));
        const source = protection.timeSource === "FIRST_OBSERVED" ? " · 操作时间异常，按首次发现计时" : "";
        return `${age} · 手动保护中 · 剩余 ${minutes} 分钟 · 预计 ${until} 后接管${source} · 安全规则可提前处理`;
    }
    if (protection?.needsTakeover) return `${age} · 手动保护已到期，等待安全核验后接管`;
    if (protection?.reason && protection.reason !== "external_takeover") {
        return `${age} · 安全规则提前处理：${protection.reason}`;
    }
    const state = offer.repriceState;
    if (!state) return age;
    const floorState = state.floorState;
    const landingState = state.landingState;
    const explorationCurve = ["EXACT_TERM_EXPLORATION_V2", "EXACT_TERM_EXPLORATION_V3", "EXACT_TERM_EXPLORATION_V4"].includes(state.curveVersion);
    const totalStages = Math.max(1, safeNumber(state.totalStages) || 6);
    const pricing = state.landingPolicy === "FIXED_AT_CREATION"
        ? `固定落点 ${formatPercent(safeNumber(state.landingRate) * 100, 5)} · 当前市场 ${formatPercent(safeNumber(state.currentMarketRate) * 100, 5)} · 底线 ${formatPercent(safeNumber(state.floorRate) * 100, 5)}`
        : "";
    const describe = (message) => pricing ? `${message} · ${pricing}` : message;
    if (state.repriceBlockedReason === "BELOW_REPOST_MINIMUM") {
        return describe(`${age} · 剩余金额低于 150 USD，无法安全撤单重挂`);
    }
    if (floorState === "REPRICE_PENDING") return describe(`${age} · 正在重挂到策略底线`);
    if (floorState === "REPRICE_REQUIRED") return describe(`${age} · 已到第 ${totalStages} 阶段 · 等待重挂到底线`);
    if (floorState === "SATISFIED_WITHIN_TOLERANCE") return describe(`${age} · 已达到策略底线（容差内）`);
    const stage = safeNumber(state.stage);
    const stageType = state.stageType === "FLOOR" ? "底线" : "市场";
    const nextStageType = state.nextStageType === "FLOOR" ? "底线" : "市场";
    if (explorationCurve && landingState === "SATISFIED_WITHIN_TOLERANCE" && state.nextStageAtMs) {
        const remaining = Math.max(0, safeNumber(state.nextStageAtMs) - Date.now());
        const minutes = Math.ceil(remaining / 60000);
        return describe(`${age} · 已接近固定期限市场落点（容差内） · ${minutes} 分钟后继续向底线调价`);
    }
    if (stage <= 0 && state.nextStageAtMs) {
        const remaining = Math.max(0, safeNumber(state.nextStageAtMs) - Date.now());
        const minutes = Math.ceil(remaining / 60000);
        return describe(`${age} · 等待${nextStageType}第 1 阶段 · ${minutes} 分钟后检查`);
    }
    if (!state.nextStageAtMs) return describe(`${age} · ${stageType}第 ${stage} 阶段已检查`);
    const remaining = Math.max(0, safeNumber(state.nextStageAtMs) - Date.now());
    const minutes = Math.ceil(remaining / 60000);
    return describe(`${age} · ${stageType}第 ${stage} 阶段 · ${minutes} 分钟后检查 · 下一阶段：${nextStageType}`);
}

function appendCell(row, text, className = "") {
    const cell = document.createElement("td");
    if (className) {
        const span = document.createElement("span");
        span.className = className;
        span.textContent = text;
        cell.append(span);
    } else {
        cell.textContent = text;
    }
    row.append(cell);
}

function renderOffers() {
    const offers = Array.isArray(state.status?.openOffers)
        ? state.status.openOffers.map(normalizedOffer)
            .sort((left, right) => durationSortValue(left.elapsedMinutes) - durationSortValue(right.elapsedMinutes))
        : [];
    const snapshotAvailable = state.status?.snapshotAvailable !== false;
    const table = $("offersTable");
    const cards = $("offersCards");
    const empty = $("offersEmpty");
    table.replaceChildren();
    cards.replaceChildren();
    empty.hidden = offers.length > 0;
    empty.textContent = snapshotAvailable ? "暂无活跃挂单" : "挂单状态未知；启动实盘并完成同步后更新";
    $("offersMeta").textContent = !snapshotAvailable
        ? "挂单状态未知"
        : (offers.length ? `${offers.length} 笔真实挂单` : "暂无挂单");
    for (const offer of offers) {
        const row = document.createElement("tr");
        appendCell(row, offer.currency || "--", "currency-pill");
        appendCell(row, formatAmount(offer.amount));
        appendCell(row, formatPercent(offer.dailyRatePercent));
        appendCell(row, `${offer.period || "--"} 天`);
        appendCell(row, offer.offerType || "LIMIT");
        appendCell(row, offer.managedByBot ? `机器人 · ${offer.bucket || "--"}` : "外部挂单");
        appendCell(row, repriceLabel(offer));
        appendCell(row, offer.status || "ACTIVE", "status-pill");
        table.append(row);

        const card = document.createElement("article");
        card.className = "offer-card";
        const header = document.createElement("header");
        const currency = document.createElement("span");
        currency.className = "currency-pill";
        currency.textContent = offer.currency || "--";
        const status = document.createElement("span");
        status.className = "status-pill";
        status.textContent = offer.status || "ACTIVE";
        header.append(currency, status);
        const list = document.createElement("dl");
        for (const [label, value] of [["金额", formatAmount(offer.amount)], ["日利率", formatPercent(offer.dailyRatePercent)], ["周期", `${offer.period || "--"} 天`], ["类型", offer.offerType || "LIMIT"], ["归属", offer.managedByBot ? `机器人 · ${offer.bucket || "--"}` : "外部挂单"], ["累计挂单时间", repriceLabel(offer)]]) {
            const block = document.createElement("div");
            const term = document.createElement("dt");
            const detail = document.createElement("dd");
            term.textContent = label;
            detail.textContent = value;
            block.append(term, detail);
            list.append(block);
        }
        card.append(header, list);
        cards.append(card);
    }
}

function appendCreditGroupStat(container, label, value) {
    const block = document.createElement("div");
    block.className = "credit-group-stat";
    const term = document.createElement("span");
    const detail = document.createElement("strong");
    term.textContent = label;
    detail.textContent = value;
    block.append(term, detail);
    container.append(block);
}

function appendCreditCardFact(list, label, value) {
    const block = document.createElement("div");
    const term = document.createElement("dt");
    const detail = document.createElement("dd");
    term.textContent = label;
    detail.textContent = value;
    block.append(term, detail);
    list.append(block);
}

function renderCreditGroup(definition, summary, credits) {
    const section = document.createElement("section");
    section.className = "credit-group";
    section.setAttribute("aria-label", `${definition.label}贷出订单`);

    const heading = document.createElement("header");
    heading.className = "credit-group-heading";
    const title = document.createElement("div");
    title.className = "credit-group-title";
    const titleText = document.createElement("strong");
    const titleMeta = document.createElement("small");
    titleText.textContent = `${definition.label}贷出`;
    titleMeta.textContent = `${definition.range} · ${summary.orderCount || 0} 笔 · ${formatAmount(summary.principal)} USD`;
    title.append(titleText, titleMeta);
    heading.append(title);
    appendCreditGroupStat(heading, "加权平均日利率", formatPercent(summary.averageDailyRatePercent, 5));
    appendCreditGroupStat(heading, "加权平均期限", formatDays(summary.averageContractDays));
    appendCreditGroupStat(heading, "平均已贷时长", formatDays(summary.averageElapsedDays));
    appendCreditGroupStat(heading, "预计净年化", formatPercent(summary.estimatedNetAprPercent, 2));
    section.append(heading);

    if (!credits.length) {
        const empty = document.createElement("p");
        empty.className = "credit-group-empty";
        empty.textContent = `暂无${definition.label}计息订单`;
        section.append(empty);
        return section;
    }

    const tableWrap = document.createElement("div");
    tableWrap.className = "credit-table-wrap";
    const table = document.createElement("table");
    table.className = "credit-table";
    const head = document.createElement("thead");
    const headerRow = document.createElement("tr");
    for (const label of ["金额", "日利率", "净年化", "期限", "已贷时长", "开始时间", "最晚到期", "类型", "资金状态", "归属", "状态"]) {
        const cell = document.createElement("th");
        cell.textContent = label;
        headerRow.append(cell);
    }
    head.append(headerRow);
    const body = document.createElement("tbody");
    for (const credit of credits) {
        const row = document.createElement("tr");
        appendCell(row, `${formatAmount(credit.amount)} ${credit.currency || "USD"}`);
        appendCell(row, formatPercent(credit.dailyRatePercent, 5));
        appendCell(row, formatPercent(credit.netAprPercent, 2));
        appendCell(row, `${credit.period || "--"} 天`);
        appendCell(row, formatDays(credit.elapsedDays));
        appendCell(row, formatDateTime(credit.opening));
        appendCell(row, formatDateTime(credit.contractEndAtMs));
        appendCell(row, credit.displayType);
        appendCell(row, fundingStateLabel(credit));
        appendCell(row, creditAttribution(credit), "credit-attribution");
        appendCell(row, credit.status || "ACTIVE", "status-pill");
        body.append(row);
    }
    table.append(head, body);
    tableWrap.append(table);

    const cards = document.createElement("div");
    cards.className = "credit-order-cards";
    for (const credit of credits) {
        const card = document.createElement("article");
        card.className = "credit-order-card";
        const cardHeader = document.createElement("header");
        const amount = document.createElement("strong");
        const status = document.createElement("span");
        amount.textContent = `${formatAmount(credit.amount)} ${credit.currency || "USD"}`;
        status.className = "status-pill";
        status.textContent = credit.status || "ACTIVE";
        cardHeader.append(amount, status);
        const facts = document.createElement("dl");
        appendCreditCardFact(facts, "日利率", formatPercent(credit.dailyRatePercent, 5));
        appendCreditCardFact(facts, "净年化", formatPercent(credit.netAprPercent, 2));
        appendCreditCardFact(facts, "期限", `${credit.period || "--"} 天`);
        appendCreditCardFact(facts, "已贷时长", formatDays(credit.elapsedDays));
        appendCreditCardFact(facts, "开始时间", formatDateTime(credit.opening));
        appendCreditCardFact(facts, "最晚到期", formatDateTime(credit.contractEndAtMs));
        appendCreditCardFact(facts, "类型", credit.displayType);
        appendCreditCardFact(facts, "资金状态", fundingStateLabel(credit));
        appendCreditCardFact(facts, "归属", creditAttribution(credit));
        card.append(cardHeader, facts);
        cards.append(card);
    }
    section.append(tableWrap, cards);
    return section;
}

function renderCredits(valid, stale, total) {
    const status = state.status || {};
    const snapshotAvailable = status.snapshotAvailable !== false;
    const credits = valid && snapshotAvailable && Array.isArray(status.credits)
        ? status.credits.map(normalizedCredit)
        : [];
    const dashboardSummary = valid && snapshotAvailable ? status.activeCreditSummary || {} : {};
    const overall = dashboardSummary.overall || {};
    const groups = dashboardSummary.groups || {};
    const principal = safeNumber(overall.principal);

    $("creditPrincipal").textContent = valid && snapshotAvailable ? formatAmount(principal) : "--";
    $("creditOrderCount").textContent = valid && snapshotAvailable ? `${overall.orderCount || 0} 笔正在计息` : "--";
    $("creditUtilization").textContent = valid && snapshotAvailable
        ? formatPercent(overall.utilizationPercent, 1)
        : "--";
    $("creditIdleAmount").textContent = valid && snapshotAvailable
        ? `未计息资金 ${formatAmount(Math.max(0, total - principal))} USD`
        : "未计息资金 --";
    $("creditAverageRate").textContent = valid && snapshotAvailable
        ? formatPercent(overall.averageDailyRatePercent, 5)
        : "--";
    $("creditNetApr").textContent = valid && snapshotAvailable
        ? `净年化估算 ${formatPercent(overall.estimatedNetAprPercent, 2)}`
        : "净年化估算 --";
    $("creditAveragePeriod").textContent = valid && snapshotAvailable
        ? formatDays(overall.averageContractDays)
        : "--";
    $("creditAverageElapsed").textContent = valid && snapshotAvailable
        ? `平均已贷 ${formatDays(overall.averageElapsedDays)}`
        : "平均已贷 --";
    $("creditDailyIncome").textContent = valid && snapshotAvailable
        ? `${formatAmount(overall.estimatedNetIncomePerDay, 4)} USD`
        : "--";
    $("currentLendingRate").textContent = valid && snapshotAvailable
        ? formatPercent(overall.averageDailyRatePercent, 5)
        : "--";
    $("creditsMeta").textContent = !snapshotAvailable
        ? "贷出状态未知"
        : (stale ? "数据已过期" : `${credits.length} 笔正在计息`);

    const container = $("creditGroups");
    container.replaceChildren();
    for (const definition of creditGroupDefinitions) {
        const rows = credits
            .filter((credit) => credit.displayPool === definition.key)
            .sort((left, right) => durationSortValue(left.elapsedDays) - durationSortValue(right.elapsedDays));
        container.append(renderCreditGroup(definition, groups[definition.key] || {}, rows));
    }
    const empty = $("creditsEmpty");
    empty.hidden = credits.length > 0;
    empty.textContent = snapshotAvailable
        ? "暂无正在计息的订单"
        : "贷出状态未知；启动实盘并完成同步后更新";
}

function renderStatus() {
    const status = state.status || {};
    const valid = status.schemaVersion === 3 && !status.legacyIgnored && Boolean(status.last_update);
    const stale = valid && statusIsStale(status);
    const mode = statusMode(status);
    const snapshotAvailable = status.snapshotAvailable !== false;
    const accountValid = valid && snapshotAvailable;
    const account = accountValid ? status.account || {} : {};
    const total = safeNumber(account.total);
    const lent = safeNumber(account.credits);
    const offers = safeNumber(account.offers);
    const available = safeNumber(account.wallet);
    const statistics = valid ? status.statistics || {} : {};
    const realized = valid ? status.realizedIncome || {} : {};
    const incomeSync = valid ? status.incomeHistorySync || {} : {};
    const currency = status.strategyV3?.currency || status.outputCurrency?.currency || "USD";

    $("totalCoins").textContent = accountValid ? formatAmount(total) : "--";
    $("totalLent").textContent = accountValid ? formatAmount(lent) : "--";
    $("totalOffers").textContent = accountValid ? formatAmount(offers) : "--";
    $("totalAvailable").textContent = accountValid ? formatAmount(available) : "--";
    $("totalCoins").title = accountValid
        ? `Funding 钱包余额 ${formatAmount(account.walletBalance)} USD；组成项对账 ${account.reconciliationStatus || "--"}`
        : "";
    $("totalLent").title = accountValid
        ? `Funding Credits ${formatAmount(account.creditPrincipal)} USD + Funding Loans ${formatAmount(account.loanPrincipal)} USD`
        : "";
    $("earningsToday").textContent = realized.today != null
        ? formatAmount(realized.today)
        : (statistics["1d"] ? formatAmount(statistics["1d"].netInterest) : "--");
    $("earningsThirtyDays").textContent = realized.thirtyDays != null
        ? formatAmount(realized.thirtyDays)
        : (statistics["30d"] ? formatAmount(statistics["30d"].netInterest) : "--");
    $("earningsLifetime").textContent = realized.lifetime != null ? formatAmount(realized.lifetime) : "--";
    $("earningsApy").textContent = statistics["30d"] ? formatPercent(statistics["30d"].actualNetAprPercent, 2) : "--";
    $("fundingWaitSummary").textContent = `近30日 · ${formatFundingWait(statistics["30d"])}`;
    const earliestIncomeDate = incomeSync.earliestMts
        ? new Date(Number(incomeSync.earliestMts)).toLocaleDateString("zh-CN")
        : "尚无记录";
    const incomeState = $("incomeHistoryState");
    if (incomeSync.status === "COMPLETE") {
        incomeState.textContent = "USD · 全部历史真实入账";
        incomeState.title = earliestIncomeDate === "尚无记录" ? "未发现利息入账" : `已覆盖至 ${earliestIncomeDate}`;
    } else if (incomeSync.status === "ERROR") {
        incomeState.textContent = "USD · 同步警告 · 已保留结果";
        incomeState.title = incomeSync.error || "历史收益将在后台自动重试";
    } else {
        incomeState.textContent = `USD · 同步中 · 已覆盖至 ${earliestIncomeDate}`;
        incomeState.title = "正在后台向更早历史回填，不影响实盘交易";
    }
    incomeState.classList.toggle("income-warning", incomeSync.status === "ERROR");
    $("totalCurrency").textContent = currency;
    $("offerCount").textContent = status.snapshotAvailable === false
        ? "--"
        : `${Array.isArray(status.openOffers) ? status.openOffers.length : 0} 笔`;
    $("chartTotal").textContent = accountValid ? formatAmount(total) : "--";
    const safeReason = status.runtime?.safe_reason;
    const recovery = status.recovery || {};
    const writeRecovery = status.writeRecovery || {};
    const writeBlockers = Array.isArray(writeRecovery.blockers) ? writeRecovery.blockers : [];
    const blockingWriteItems = writeBlockers.filter((item) => item.blocking !== false);
    const writeBlocked = writeRecovery.canSubmit === false;
    const recoverySyncs = Math.max(0, Number(recovery.successfulSnapshots || 0));
    const recoveryRequired = Math.max(2, Number(recovery.requiredSnapshots || 2));
    const nextRetrySeconds = recovery.nextProbeAt
        ? Math.max(0, Math.ceil((Number(recovery.nextProbeAt) - Date.now()) / 1000))
        : null;
    const recoveryDetail = recovery.active
        ? (recovery.manualRequired
            ? " · 需要人工处理"
            : ` · 权威快照 ${recoverySyncs}/${recoveryRequired}${nextRetrySeconds === null ? "" : ` · ${nextRetrySeconds}秒后重试`}`)
        : "";
    $("statusHeadline").textContent = stale
        ? "状态已过期，请检查实盘进程或网络。"
        : (recovery.active
            ? `${recovery.manualRequired ? "需要人工处理" : "自动修复中"}：${recovery.reason || safeReason || "正在重新读取权威数据"}${recoveryDetail}`
            : (writeBlocked
                ? `自动恢复中：${blockingWriteItems.length
                    ? blockingWriteItems.map((item) => `${item.kind}:${item.state}`).join("，")
                    : "等待写入对账"}`
            : (safeReason
                ? `PAUSED：${safeReason}`
                : (status.last_status || "实盘状态已同步。"))));
    $("headerSync").textContent = status.last_update || "--";
    $("railSync").textContent = status.last_update || "--";

    const version = document.querySelector('meta[name="mika-application-version"]')?.content;
    const versionLabel = version ? `v${version}` : "版本未知";
    const badge = $("statusSchemaBadge");
    badge.textContent = `${versionLabel} · ${!valid ? "等待实盘状态" : stale ? "状态已过期" : mode}`;
    badge.classList.toggle("invalid", !valid || stale || Boolean(safeReason) || recovery.active || writeBlocked);
    $("schemaState").textContent = versionLabel;
    const releaseComparison = status.releaseComparison || {};
    $("releaseBoundary").textContent = releaseComparison.activatedAtMs
        ? formatDateTime(releaseComparison.activatedAtMs)
        : "等待首次启动";
    $("releaseBoundary").title = releaseComparison.activatedAtMs
        ? `${releaseComparison.boundarySource === "LIVE_SESSION_BEFORE_EXACT_TERM_EXPLORATION"
            ? "按首笔新定价订单所在的 LIVE 会话确定"
            : "按首次 V3.5 启动确定"}；订单与成交统计以此时间分为更新前后，并使用等长时间窗口对比。`
        : "首次使用 V3.5 状态库时自动写入，不会随重启改变。";

    const market = valid && status.market?.anchor_rate != null ? safeNumber(status.market.anchor_rate) * 100 : null;
    const plan = Array.isArray(status.strategyV3?.plan) ? status.strategyV3.plan : [];
    const plannedAmount = plan.reduce((sum, row) => sum + safeNumber(row.amount), 0);
    const strategy = plannedAmount > 0
        ? plan.reduce((sum, row) => sum + safeNumber(row.effective_rate) * safeNumber(row.amount), 0) / plannedAmount * 100
        : null;
    $("marketRate").textContent = market === null ? "--" : formatPercent(market);
    $("strategyRate").textContent = strategy === null ? "--" : formatPercent(strategy);
    $("rateSpread").textContent = market === null || strategy === null ? "--" : formatPercent(Math.max(0, strategy - market));

    const values = distributionValues();
    renderLegend(values, total);
    renderCredits(valid, stale, total);
    renderOffers();
    renderLogs(status.log || []);
    drawDistribution();
}

function renderLogs(lines) {
    const filter = ($("logFilter").value || "").trim().toLowerCase();
    const visible = (Array.isArray(lines) ? lines : []).filter((line) => String(line).toLowerCase().includes(filter));
    const stream = $("logStream");
    stream.replaceChildren();
    if (!visible.length) {
        const empty = document.createElement("p");
        empty.className = "log-empty";
        empty.textContent = filter ? "没有匹配的日志" : "暂无运行日志";
        stream.append(empty);
        return;
    }
    for (const line of visible) {
        const item = document.createElement("div");
        item.className = "log-line";
        item.textContent = String(line);
        stream.append(item);
    }
}

function renderControl() {
    const control = state.control || {};
    const running = Boolean(control.running);
    const rail = document.querySelector(".control-rail");
    rail.classList.toggle("running", running);
    const mode = statusMode();
    const stale = state.status?.last_update ? statusIsStale() : false;
    const recovery = state.status?.recovery || {};
    const writeRecovery = state.status?.writeRecovery || {};
    const writeBlocked = writeRecovery.canSubmit === false;
    $("controlTitle").textContent = running
        ? (recovery.active
            ? (recovery.manualRequired ? "等待人工处理" : "自动修复中")
            : (writeBlocked ? "自动恢复中" : (mode === "PAUSED" ? "PAUSED" : (stale ? "进程运行 · 状态过期" : "运行中"))))
        : "已停止";
    $("controlDetail").textContent = running
        ? (recovery.active
            ? `${recovery.manualRequired ? "自动写入保持关闭。" : "正在只读同步，恢复后将在下一正常周期继续放贷。"} 已尝试 ${Number(recovery.attempts || 0)} 次。`
            : `实盘进程 PID ${control.pid || "--"}，启动于 ${control.startedAt || "--"}。${control.managedExternally ? " 已从单实例锁恢复控制。" : ""}`)
        : "普通启动不会下单。启动实盘前必须完成只读安全预检。";
    $("primaryControlButton").textContent = running ? "停止机器人" : "启动实盘";
    $("credentialState").textContent = state.config?.credentialsConfigured ? "已配置" : "未配置";
    $("permissionState").textContent = running
        ? "启动前已通过"
        : (state.preflight ? (state.preflight.canStart ? "预检通过" : "存在阻断项") : "待预检");
    $("preflightButton").disabled = running;
}

function renderConfig() {
    renderControl();
}

async function loadConfig() {
    state.config = await getJson("/api/config");
    renderConfig();
}

async function loadStatus() {
    state.status = await getJson("/api/status");
    renderStatus();
}

async function loadControl() {
    state.control = await getJson("/api/control/status");
    renderControl();
}

async function refreshAll(showMessage = false) {
    try {
        await Promise.all([loadConfig(), loadStatus(), loadControl()]);
        setConnection(true);
        $("railError").textContent = "";
        if (showMessage) showToast("控制台已刷新");
    } catch (error) {
        setConnection(false);
        $("railError").textContent = error.message;
        if (showMessage) showToast(error.message);
    }
}

function focusableElements(dialog) {
    return [...dialog.querySelectorAll("button:not([disabled]), input:not([disabled]), [href], [tabindex]:not([tabindex='-1'])")].filter((element) => !element.hidden);
}

function openDialog(backdropId) {
    const backdrop = $(backdropId);
    state.previousFocus = document.activeElement;
    state.activeDialog = backdrop;
    backdrop.hidden = false;
    document.body.style.overflow = "hidden";
    window.requestAnimationFrame(() => focusableElements(backdrop)[0]?.focus());
}

function closeDialog(backdrop = state.activeDialog) {
    if (!backdrop) return;
    backdrop.hidden = true;
    state.activeDialog = null;
    document.body.style.overflow = "";
    state.previousFocus?.focus();
}

function trapDialogKey(event) {
    if (!state.activeDialog) return;
    if (event.key === "Escape") {
        event.preventDefault();
        closeDialog();
        return;
    }
    if (event.key !== "Tab") return;
    const items = focusableElements(state.activeDialog);
    if (!items.length) return;
    const first = items[0];
    const last = items[items.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
}

function allocationPercentages(allocation) {
    const entries = Object.entries(allocation?.current || {}).map(([name, value]) => [name, safeNumber(value)]);
    const total = entries.reduce((sum, [, value]) => sum + value, 0);
    if (total <= 0) return "当前无机器人挂单";
    return entries.map(([name, value]) => `${name} ${formatAmount(value / total * 100, 1)}%`).join(" / ");
}

function renderPreflight(data) {
    $("preflightLoading").hidden = true;
    $("preflightContent").hidden = false;
    const checks = $("preflightChecks");
    checks.replaceChildren();
    for (const check of data.checks || []) {
        const item = document.createElement("li");
        item.className = check.status === "pass" ? "passed" : "failed";
        const marker = document.createElement("b");
        marker.textContent = check.status === "pass" ? "通过" : "阻断";
        const copy = document.createElement("div");
        const label = document.createElement("span");
        const detail = document.createElement("small");
        label.textContent = check.label;
        detail.textContent = check.detail;
        copy.append(label, detail);
        item.append(marker, copy);
        checks.append(item);
    }
    const warnings = $("preflightWarnings");
    warnings.replaceChildren();
    for (const warning of data.warnings || []) {
        const item = document.createElement("li");
        item.textContent = warning.message;
        warnings.append(item);
    }
    $("warningSection").hidden = !warnings.childElementCount;

    const summary = data.summary || {};
    if (summary.strategyVersion !== 3) {
        throw new Error("后端返回了非 V3 预检，已阻断启动");
    }
    const items = [
        ["币种", "USD"],
        ["策略来源", `SQLite ACTIVE · ${summary.activeStrategyVersion || "--"}`],
        ["真实账户本金", `${formatAmount(summary.account?.total)} USD`],
        ["真实可用余额", `${formatAmount(summary.account?.wallet)} USD`],
        ["允许订单类型", (summary.enabledOrderTypes || []).join(" / ") || "--"],
        ["目标期限资金池", Object.entries(summary.fundingPools || {}).map(([name, row]) => `${name} ${row.share}%`).join(" / ") || "--"],
        ["当前机器人挂单期限", allocationPercentages(summary.offerPoolAllocation)],
        ["目标成交层", Object.entries(summary.executionLayers || {}).map(([name, value]) => `${name} ${value}%`).join(" / ") || "--"],
        ["当前机器人挂单成交层", allocationPercentages(summary.offerLayerAllocation)],
        ["资金上限", `${formatAmount(summary.fundingLimit?.effectiveCap)} USD · ${summary.fundingLimit?.maxPercent ?? "--"}%`],
        ["切片", `${summary.actualSlices ?? 0} / ${summary.targetSlices ?? 0} 笔`],
        ["计划哈希", summary.planHash ? summary.planHash.slice(0, 16) : "--"],
        ["启动后先撤销", `${(summary.pendingCancellations || []).length} 笔不兼容机器人挂单`],
        ["确认后接管外部挂单", `${(summary.externalAdoptionCandidates || []).length} 笔`],
        ["比例再平衡预计撤销", `${(summary.ratioRebalanceCancellations || []).length} 笔`],
        ["无法撤销的贷款", `${(summary.nonChangeableCredits || []).length} 笔`],
        ["账户快照", summary.accountSnapshot?.stale ? "历史快照（阻止启动）" : "实时账户"],
    ];
    const grouped = (summary.strategyPlan || []).map((row) => `${row.display_type} ${row.period}天 ${formatAmount(row.amount)} USD`);
    items.push(["实际挂单计划", grouped.join(" · ") || "当前无新挂单计划"]);
    const adoptionRows = (summary.externalAdoptionCandidates || []).map(
        (row) => `#${row.id} · ${formatAmount(row.amount)} USD · ${row.period}天 · ${row.display_type || row.offer_type || "--"}`
    );
    if (adoptionRows.length) items.push(["待接管外部挂单明细", adoptionRows.join("；")]);
    const ratioRows = (summary.ratioRebalanceCancellations || []).map(
        (row) => `#${row.offer_id || row.id} · ${formatAmount(row.amount)} USD · ${row.period}天`
    );
    if (ratioRows.length) items.push(["比例再平衡撤单明细", ratioRows.join("；")]);
    if (data.preflightId) {
        const expires = new Date(data.expiresAt);
        items.push(["本地启动确认有效至", Number.isNaN(expires.getTime()) ? data.expiresAt : `${expires.toLocaleTimeString("zh-CN", { hour12: false })}（最长 5 分钟，不是 Bitfinex API 令牌）`]);
    }
    const container = $("preflightSummary");
    container.replaceChildren();
    for (const [label, value] of items) {
        const block = document.createElement("div");
        block.className = "summary-item";
        const name = document.createElement("span");
        const detail = document.createElement("strong");
        name.textContent = label;
        detail.textContent = value;
        block.append(name, detail);
        container.append(block);
    }
    $("confirmStartButton").disabled = !data.canStart;
    $("goStrategyButton").hidden = data.canStart;
    $("goStrategyButton").textContent = "前往策略设置";
    $("goStrategyButton").dataset.action = "strategy";
    $("permissionState").textContent = data.canStart ? "预检通过" : "存在阻断项";
}

async function runPreflight() {
    if (state.control?.running) return;
    state.preflight = null;
    $("preflightError").hidden = true;
    $("preflightError").textContent = "";
    $("preflightLoading").hidden = false;
    $("preflightContent").hidden = true;
    $("confirmStartButton").disabled = true;
    $("goStrategyButton").hidden = true;
    openDialog("preflightDialog");
    try {
        const data = await postJson("/api/control/preflight", {});
        state.preflight = data;
        renderPreflight(data);
    } catch (error) {
        $("preflightLoading").hidden = true;
        $("preflightError").hidden = false;
        $("preflightError").textContent = error.message;
    }
}

async function confirmStart() {
    const button = $("confirmStartButton");
    const errorBox = $("preflightError");
    button.disabled = true;
    errorBox.hidden = true;
    try {
        const data = await postJson("/api/control/start", { preflightId: state.preflight?.preflightId || "" });
        state.control = data.bot;
        closeDialog($("preflightDialog"));
        renderControl();
        await loadStatus();
        showToast("实盘机器人已启动");
    } catch (error) {
        errorBox.hidden = false;
        errorBox.textContent = `${error.message}。请修正后重新运行预检。`;
        $("goStrategyButton").hidden = false;
        $("goStrategyButton").textContent = "重新运行预检";
        $("goStrategyButton").dataset.action = "retry";
    }
}

async function confirmStop() {
    const button = $("confirmStopButton");
    const errorBox = $("stopError");
    button.disabled = true;
    errorBox.hidden = true;
    try {
        const data = await postJson("/api/control/stop", {});
        state.control = data.bot;
        closeDialog($("stopDialog"));
        renderControl();
        showToast("机器人进程已停止；账户已有挂单未撤销");
    } catch (error) {
        errorBox.hidden = false;
        errorBox.textContent = error.message;
    } finally {
        button.disabled = false;
    }
}

function primaryControl() {
    if (state.control?.running) {
        $("stopError").hidden = true;
        openDialog("stopDialog");
    } else {
        runPreflight();
    }
}

function bindEvents() {
    window.addEventListener("hashchange", renderRoute);
    window.addEventListener("resize", drawDistribution);
    document.addEventListener("keydown", trapDialogKey);
    document.querySelector(".tabs").addEventListener("keydown", navigateTabs);
    $("refreshButton").addEventListener("click", () => refreshAll(true));
    $("logFilter").addEventListener("input", () => renderLogs(state.status?.log || []));
    $("primaryControlButton").addEventListener("click", primaryControl);
    $("preflightButton").addEventListener("click", runPreflight);
    $("confirmStartButton").addEventListener("click", confirmStart);
    $("confirmStopButton").addEventListener("click", confirmStop);
    $("goStrategyButton").addEventListener("click", () => {
        const action = $("goStrategyButton").dataset.action;
        closeDialog();
        if (action === "retry") runPreflight();
        else location.hash = "#strategy";
    });
    document.querySelectorAll("[data-close-dialog]").forEach((button) => button.addEventListener("click", () => closeDialog(button.closest(".dialog-backdrop"))));
    document.querySelectorAll(".dialog-backdrop").forEach((backdrop) => backdrop.addEventListener("mousedown", (event) => { if (event.target === backdrop) closeDialog(backdrop); }));
}

window.mikaBuildReady.then((compatible) => {
    if (!compatible) return;
    bindEvents();
    renderRoute();
    refreshAll();
    window.setInterval(() => loadStatus().then(() => setConnection(true)).catch(() => setConnection(false)), 30000);
    window.setInterval(() => loadControl().catch(() => setConnection(false)), 5000);
});
