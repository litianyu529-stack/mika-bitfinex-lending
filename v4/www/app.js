const $ = (id) => document.getElementById(id);
const pct = (rate) => rate == null ? "—" : `${(Number(rate) * 100).toFixed(5)}%`;
const money = (value) => value == null ? "—" : Number(value).toFixed(2);
const date = (value) => value ? new Date(Number(value)).toLocaleString("zh-CN") : "—";
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
})[char]);

async function request(path, options = {}) {
  const response = await fetch(path, {headers: {"Content-Type": "application/json"}, ...options});
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}

function renderStatus(data) {
  const runtime = data.runtime || {};
  const market = data.market || {};
  const account = data.account || {};
  const plan = data.plan || {};
  const recovery = data.recovery || {};
  const mode = runtime.mode || "UNKNOWN";
  $("mode").textContent = mode;
  $("mode").className = `badge ${mode.toLowerCase()}`;
  $("action").textContent = data.last_action || "—";
  $("safePanel").classList.toggle("hidden", mode !== "SAFE" && !recovery.active);
  $("safeReason").textContent = recovery.manualRequired
    ? `需要人工处理：${recovery.reason || runtime.safe_reason || "未知故障"}`
    : recovery.active
      ? `自动修复中：${recovery.reason || runtime.safe_reason || "正在重新读取权威数据"}`
      : runtime.safe_reason || "";
  $("recoveryProgress").textContent = recovery.active && !recovery.manualRequired
    ? `权威快照 ${recovery.successfulSnapshots || 0}/${recovery.requiredSnapshots || 2}；恢复目标 ${recovery.targetMode || "PAUSED"}`
    : "";
  const retrySeconds = recovery.nextProbeAt
    ? Math.max(0, Math.ceil((Number(recovery.nextProbeAt) - Date.now()) / 1000))
    : null;
  $("recoveryRetry").textContent = recovery.active && !recovery.manualRequired
    ? `第 ${recovery.attempts || 0} 次重试；${retrySeconds == null ? "等待下一次探测" : `${retrySeconds} 秒后重试`}`
    : "";
  $("wallet").textContent = money(account.wallet_available);
  $("anchor").textContent = pct(market.robust_anchor);
  $("step").textContent = pct(market.grid_step);
  $("planned").textContent = money(plan.planned_amount);
  $("longApr").textContent = `${data.policy?.long_floor_apr_percent || "—"}%`;
  $("longDaily").textContent = pct(data.floor_daily_rates?.long);
  $("fresh").textContent = market.fresh ? `${market.valid_components}/3 新鲜` : "数据不足";
  $("fresh").className = `badge ${market.fresh ? "" : "neutral"}`;
  const signals = [
    ["最佳借款价", pct(market.best_borrower_rate)], ["5 分钟 VWAP", pct(market.vwap_5m)],
    ["5 分钟中位数", pct(market.median_5m)], ["1 小时中位数", pct(market.median_1h)],
    ["6 小时中位数", pct(market.median_6h)], ["24 小时 Q25 / Q75", `${pct(market.q25_24h)} / ${pct(market.q75_24h)}`],
  ];
  $("signals").innerHTML = signals.map(([key, value]) => `<dt>${esc(key)}</dt><dd>${esc(value)}</dd>`).join("");
  $("pending").textContent = data.pending_plan ? `${data.pending_plan.phase} · ${data.pending_plan.reason}` : "无待确认重建";
  const rungs = data.active_rungs || [];
  $("rungs").innerHTML = rungs.length ? rungs.map((row) => `<tr><td>${esc(row.pool)}</td><td>${Number(row.rung_index) + 1}</td><td>${esc(row.offer_id || "—")}</td><td>${Number(row.period)} 天</td><td>${money(row.amount_original)}</td><td>${money(row.amount_remaining)}</td><td>${pct(row.rate)}</td><td>${esc(row.status)}</td></tr>`).join("") : '<tr><td colspan="8">暂无 V4 托管订单</td></tr>';
  const managedIds = new Set(rungs.map((row) => Number(row.offer_id)).filter(Boolean));
  const external = (account.offers || []).filter((item) => !managedIds.has(Number(item.offer_id)));
  $("externalOffers").textContent = external.length
    ? external.map((item) => `#${item.offer_id} · ${money(item.amount)} USD · ${pct(item.rate)} · ${item.period} 天 · ${item.offer_type}`).join("\n")
    : "最新快照中没有外部订单。";
  const events = data.events || [];
  $("events").innerHTML = events.length ? events.map((item) => `<li><span>${date(item.mts)}</span><strong>${esc(item.kind)}</strong><span>${esc(item.payload)}</span></li>`).join("") : "<li>暂无事件</li>";
}

async function refresh() {
  try { renderStatus(await request("/api/status")); }
  catch (error) { $("action").textContent = error.message; }
}

async function loadConfig() {
  const config = await request("/api/config");
  for (const [key, value] of Object.entries(config)) {
    const input = document.querySelector(`[name="${key}"]`);
    if (!input) continue;
    if (input.type === "checkbox") input.checked = Boolean(value);
    else input.value = Array.isArray(value) ? value.join(",") : (value ?? "");
  }
}

document.querySelectorAll("[data-mode]").forEach((button) => button.addEventListener("click", async () => {
  const mode = button.dataset.mode;
  const confirmation = mode === "LIVE" ? prompt("输入 ENABLE V4 LIVE 以确认启用实盘：") || "" : "";
  try {
    const result = await request("/api/mode", {method: "POST", body: JSON.stringify({mode, confirmation})});
    $("modeMessage").textContent = `已切换到 ${result.mode}`;
    await refresh();
  } catch (error) { $("modeMessage").textContent = error.message; }
}));

$("configForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = {};
  for (const input of event.currentTarget.elements) {
    if (!input.name) continue;
    data[input.name] = input.type === "checkbox" ? input.checked : input.value;
  }
  try {
    await request("/api/config", {method: "POST", body: JSON.stringify(data)});
    $("configMessage").textContent = "配置已安全写入；重启 V4 后生效。";
  } catch (error) { $("configMessage").textContent = error.message; }
});

$("adoptForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const offerIds = $("adoptIds").value.split(",").map((item) => Number(item.trim())).filter(Number.isInteger);
  if (!offerIds.length) { $("adoptMessage").textContent = "请输入至少一个 Offer ID。"; return; }
  const confirmations = {};
  for (const offerId of offerIds) {
    const value = prompt(`输入 ADOPT ${offerId} 以确认接管该订单：`) || "";
    confirmations[String(offerId)] = value;
  }
  try {
    const result = await request("/api/adopt", {method: "POST", body: JSON.stringify({offer_ids: offerIds, confirmations})});
    $("adoptMessage").textContent = `已接管 ${result.adopted} 张订单。`;
    await refresh();
  } catch (error) { $("adoptMessage").textContent = error.message; }
});

loadConfig().catch((error) => { $("configMessage").textContent = error.message; });
refresh();
setInterval(refresh, 5000);
