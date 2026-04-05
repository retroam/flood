const form = document.querySelector("#query-form");
const questionInput = document.querySelector("#question");
const sqlOutput = document.querySelector("#sql-output");
const resultOutput = document.querySelector("#result-output");
const metricsOutput = document.querySelector("#metrics-output");
const submitButton = document.querySelector("#submit-button");
const statusOutput = document.querySelector("#status");
const copySqlButton = document.querySelector("#copy-sql");
const rowBadge = document.querySelector("#row-badge");
const resultDetails = document.querySelector("#result-details");
const metricsDetails = document.querySelector("#metrics-details");
const queryModeBadge = document.querySelector("#query-mode-badge");

function escapeHtml(value) {
  return `${value}`
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function modeLabel(mode) {
  if (mode === "cfg") return "CFG";
  if (mode === "no_cfg") return "No CFG";
  return `${mode}`;
}

function selectedQueryMode() {
  const selected = document.querySelector('input[name="query-mode"]:checked');
  return selected?.value === "no_cfg" ? "no_cfg" : "cfg";
}

function selectedQueryUsesCfg() {
  return selectedQueryMode() === "cfg";
}

function renderQueryModeBadge(mode) {
  if (!queryModeBadge) return;
  queryModeBadge.textContent = mode === "cfg" ? "CFG enabled" : "CFG disabled";
}

/* ── Tabs ── */
let mermaidRendered = false;
document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach((c) => c.classList.remove("active"));
    tab.classList.add("active");
    document.querySelector(`#tab-${tab.dataset.tab}`).classList.add("active");

    if (tab.dataset.tab === "evals" && !evalsLoaded) {
      loadEvals();
    }
    if (tab.dataset.tab === "architecture" && !mermaidRendered && window.__mermaid) {
      mermaidRendered = true;
      window.__mermaid.run();
    }
  });
});

/* ── Example chips: click to fill AND auto-submit ── */
document.querySelectorAll(".example-chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    questionInput.value = chip.dataset.example ?? "";
    form.requestSubmit();
  });
});

/* ── Ctrl/Cmd+Enter shortcut ── */
questionInput.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    form.requestSubmit();
  }
});

/* ── Copy SQL button ── */
copySqlButton.addEventListener("click", async () => {
  const sql = sqlOutput.textContent;
  try {
    await navigator.clipboard.writeText(sql);
    copySqlButton.textContent = "Copied!";
    setTimeout(() => { copySqlButton.textContent = "Copy"; }, 1500);
  } catch {
    copySqlButton.textContent = "Failed";
    setTimeout(() => { copySqlButton.textContent = "Copy"; }, 1500);
  }
});

function formatValue(value) {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "number") return Number.isInteger(value) ? `${value}` : value.toFixed(4);
  return `${value}`;
}

function renderTable(result) {
  if (!result || !result.columns || result.columns.length === 0) {
    rowBadge.style.display = "none";
    return "<p>No rows returned.</p>";
  }

  rowBadge.textContent = `${result.row_count} row${result.row_count !== 1 ? "s" : ""}`;
  rowBadge.style.display = "inline-block";

  const headers = result.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const rows = result.rows
    .map((row) => {
      const cells = row.map((value) => `<td>${escapeHtml(formatValue(value))}</td>`).join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");

  return `
    <table class="result-table">
      <thead><tr>${headers}</tr></thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderMetrics(payload) {
  const usage = payload.usage ?? {};
  metricsOutput.innerHTML = `
    <div><dt>Mode</dt><dd>${escapeHtml(modeLabel(payload.generation_mode ?? "cfg"))}</dd></div>
    <div><dt>Latency</dt><dd>${payload.latency_seconds?.toFixed(2) ?? "n/a"}s</dd></div>
    <div><dt>Estimated cost</dt><dd>${usage.estimated_cost_usd !== undefined ? `$${usage.estimated_cost_usd.toFixed(6)}` : "n/a"}</dd></div>
    <div><dt>Total tokens</dt><dd>${usage.total_tokens ?? "n/a"}</dd></div>
  `;
}

async function runQuery(event) {
  event.preventDefault();

  const question = questionInput.value.trim();
  if (!question) {
    statusOutput.textContent = "Enter a question first.";
    return;
  }

  const queryMode = selectedQueryMode();
  renderQueryModeBadge(queryMode);
  submitButton.disabled = true;
  statusOutput.textContent = `Generating SQL and querying ClickHouse with ${modeLabel(queryMode)}...`;
  sqlOutput.textContent = "Working...";
  sqlOutput.classList.add("placeholder-text");
  resultOutput.innerHTML = "";
  resultOutput.classList.remove("placeholder-text");
  rowBadge.style.display = "none";
  copySqlButton.style.display = "none";

  try {
    const response = await fetch("/api/query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, use_cfg: selectedQueryUsesCfg() }),
    });

    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail ?? "Unknown server error.");
    }

    sqlOutput.textContent = payload.sql;
    sqlOutput.classList.remove("placeholder-text");
    copySqlButton.style.display = "inline-block";
    renderMetrics(payload);
    metricsDetails.open = true;

    if (payload.unsupported) {
      resultOutput.innerHTML = `<div class="error">${escapeHtml(payload.error)}</div>`;
      statusOutput.textContent = `Query rejected safely with ${modeLabel(payload.generation_mode ?? queryMode)}.`;
      rowBadge.textContent = "unsupported";
      rowBadge.style.display = "inline-block";
      return;
    }

    resultOutput.innerHTML = renderTable(payload.result);
    statusOutput.textContent = `Returned ${payload.result?.row_count ?? 0} rows with ${modeLabel(payload.generation_mode ?? queryMode)}.`;
  } catch (error) {
    sqlOutput.textContent = "No SQL generated.";
    sqlOutput.classList.add("placeholder-text");
    resultOutput.innerHTML = `<div class="error">${escapeHtml(error.message)}</div>`;
    statusOutput.textContent = "Request failed.";
  } finally {
    submitButton.disabled = false;
  }
}

form.addEventListener("submit", runQuery);
renderQueryModeBadge(selectedQueryMode());
document.querySelectorAll('input[name="query-mode"]').forEach((input) => {
  input.addEventListener("change", () => {
    renderQueryModeBadge(selectedQueryMode());
  });
});

/* ══════════════════════════════════════════════════════
   Evals Tab
   ══════════════════════════════════════════════════════ */

let evalsLoaded = false;
const runEvalsButton = document.querySelector("#run-evals-button");
const evalRunStatus = document.querySelector("#eval-run-status");
const evalModelLabel = document.querySelector("#eval-model-label");
const evalSampleCountInput = document.querySelector("#eval-sample-count");

const COLORS = {
  bars: ["#0b2b39", "#9b45e9", "#e66f4f", "#f1ecee"],
  barsBg: ["rgba(11,43,57,0.12)", "rgba(155,69,233,0.12)", "rgba(230,111,79,0.12)", "rgba(241,236,238,0.12)"],
  winner: "#0b2b39",
  grid: "#f0f0f0",
  label: "#6b7280",
  value: "#1a1a1a",
};

/* ── Palette for feature charts ── */
const RD = {
  teal:     "#0b2b39",
  blue:     "#9b45e9",
  cyan:     "#e66f4f",
  pink:     "#f1ecee",
  darkTeal: "#0b2b39",
  lightBg:  "#f1ecee",
  grid:     "#e0e0e0",
  gridFine: "#f0f0f0",
  label:    "#616061",
  text:     "#1d1c1d",
  models:   ["#0b2b39", "#9b45e9", "#e66f4f"],   // per-model
  modelsBg: ["rgba(11,43,57,0.15)", "rgba(155,69,233,0.15)", "rgba(230,111,79,0.15)"],
};

function shortModel(name) {
  return name.replace("openai/", "");
}

function comparisonLabel(item) {
  return modeLabel(item.generation_mode);
}

function comparisonBubbleLabel(item) {
  return item.generation_mode === "cfg" ? "CFG" : "No CFG";
}

function findModeRun(data, generationMode) {
  return (data.summary ?? []).find((item) => item.generation_mode === generationMode) ?? null;
}

function formatModeList(modes) {
  return (modes ?? []).map(modeLabel).join(" and ");
}

function formatSigned(value, digits = 1) {
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${Math.abs(value).toFixed(digits)}`;
}

function setEvalRunStatus(message, isError = false) {
  if (!evalRunStatus) return;
  evalRunStatus.textContent = message;
  evalRunStatus.classList.toggle("eval-run-status--error", isError);
}

function setRunEvalsBusy(isBusy) {
  if (!runEvalsButton) return;
  runEvalsButton.disabled = isBusy;
  runEvalsButton.textContent = isBusy ? "Running..." : "Run Evals";
}

function renderEvalModelLabel(activeModel) {
  if (!evalModelLabel || !activeModel) return;
  evalModelLabel.textContent = `Current model: ${activeModel}`;
}

function syncEvalSampleCountInput(data) {
  if (!evalSampleCountInput) return;
  const max = Number(data.max_sample_count ?? evalSampleCountInput.max ?? 25);
  const loaded = data.selected_sample_count ?? data.summary?.[0]?.sample_count;
  const fallback = Number(data.default_sample_count ?? 3);
  const selected = loaded ?? fallback;
  evalSampleCountInput.max = `${max}`;
  evalSampleCountInput.value = `${Math.min(Math.max(selected, 1), max)}`;
}

function selectedEvalSampleCount() {
  if (!evalSampleCountInput) return 3;
  const max = Number(evalSampleCountInput.max || 25);
  const parsed = Number.parseInt(evalSampleCountInput.value, 10);
  if (!Number.isFinite(parsed)) return 3;
  return Math.min(Math.max(parsed, 1), max);
}

function renderEvalData(data) {
  renderEvalModelLabel(data.active_model);
  syncEvalSampleCountInput(data);

  const loading = document.getElementById("evals-loading");
  const content = document.getElementById("evals-content");
  const hasResults = Boolean(data.summary && data.summary.length);

  if (!hasResults) {
    loading.textContent = `No eval results yet for ${shortModel(data.active_model ?? "the current model")}. Click Run Evals to benchmark CFG vs No CFG.`;
    loading.style.display = "block";
    content.style.display = "none";
    return;
  }

  loading.style.display = "none";
  content.style.display = "block";

  const labels = data.summary.map((item) => comparisonLabel(item));

  renderRationale(data);
  drawAccuracyHallucinationChart("chart-acc-hall", data);
  drawAccuracyCostChart("chart-acc-cost", data);
  drawBarChart("chart-accuracy", labels, data.summary.map((item) => item.accuracy), { format: "pct", highlightMax: true });
  drawBarChart("chart-hallucination", labels, data.summary.map((item) => item.hallucination), { format: "pct", highlightMax: true });
  drawBarChart("chart-latency", labels, data.summary.map((item) => item.latency_mean), { format: "seconds", highlightMin: true });
  drawBarChart("chart-cost", labels, data.summary.map((item) => item.cost_mean), { format: "cost", highlightMin: true });

  renderSummaryTable(data);
  renderSamplesTable(data);
}

/* ── Horizontal bar chart drawn on canvas ── */
function drawBarChart(canvasId, labels, values, { format = "pct", highlightMax = true, highlightMin = false } = {}) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const container = canvas.parentElement;
  const dpr = window.devicePixelRatio || 1;
  const w = container.clientWidth;
  const barH = 36;
  const gap = 10;
  const longestLabel = labels.reduce((max, label) => Math.max(max, label.length), 0);
  const labelW = Math.max(110, Math.min(190, longestLabel * 7));
  const valueW = 80;
  const chartLeft = labelW;
  const chartRight = w - valueW;
  const chartW = chartRight - chartLeft;
  const h = labels.length * (barH + gap) + gap;

  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + "px";
  canvas.style.height = h + "px";
  container.style.height = h + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  const maxVal = Math.max(...values, 0.001);
  const positiveValues = values.filter((value) => value > 0);
  const bestIdx = highlightMax
    ? values.indexOf(Math.max(...values))
    : highlightMin
      ? (positiveValues.length ? values.indexOf(Math.min(...positiveValues)) : 0)
      : -1;

  labels.forEach((label, i) => {
    const y = gap + i * (barH + gap);
    const barW = Math.max((values[i] / maxVal) * chartW, 2);
    const isBest = i === bestIdx;
    const colorIdx = i % COLORS.bars.length;

    // Bar background
    ctx.fillStyle = COLORS.barsBg[colorIdx];
    ctx.beginPath();
    ctx.roundRect(chartLeft, y, chartW, barH, 6);
    ctx.fill();

    // Bar fill
    ctx.fillStyle = COLORS.bars[colorIdx];
    ctx.globalAlpha = isBest ? 1 : 0.7;
    ctx.beginPath();
    ctx.roundRect(chartLeft, y, barW, barH, 6);
    ctx.fill();
    ctx.globalAlpha = 1;

    if (isBest) {
      ctx.strokeStyle = COLORS.value;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.roundRect(chartLeft, y, barW, barH, 6);
      ctx.stroke();
    }

    // Label
    ctx.fillStyle = isBest ? COLORS.value : COLORS.label;
    ctx.font = `${isBest ? "600" : "400"} 13px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    ctx.fillText(label, chartLeft - 12, y + barH / 2);

    // Value
    let displayVal;
    if (format === "pct") displayVal = values[i].toFixed(1) + "%";
    else if (format === "seconds") displayVal = values[i].toFixed(2) + "s";
    else if (format === "cost") displayVal = "$" + values[i].toFixed(4);
    else displayVal = values[i].toString();

    ctx.fillStyle = COLORS.value;
    ctx.font = `600 13px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`;
    ctx.textAlign = "left";
    ctx.fillText(displayVal, chartRight + 10, y + barH / 2);
  });
}

/* ── Grouped bar: Accuracy & Hallucination side-by-side ── */
function drawAccuracyHallucinationChart(canvasId, data) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const container = canvas.parentElement;
  const dpr = window.devicePixelRatio || 1;
  const w = container.clientWidth;
  const h = container.clientHeight || 280;

  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + "px";
  canvas.style.height = h + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  const labels = data.summary.map((item) => comparisonLabel(item));
  const accVals = data.summary.map((item) => item.accuracy);
  const sqlVals = data.summary.map((item) => item.sql_equivalence || 0);
  const halVals = data.summary.map((item) => item.hallucination);
  const n = labels.length;

  const font = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
  const padLeft = 56, padRight = 24, padTop = 44, padBottom = 48;
  const plotW = w - padLeft - padRight;
  const plotH = h - padTop - padBottom;
  const groupW = plotW / n;
  const barW = Math.min(groupW * 0.22, 32);
  const barGap = 4;
  const totalBarGroupW = barW * 3 + barGap * 2;

  // Y axis gridlines
  ctx.strokeStyle = RD.gridFine;
  ctx.lineWidth = 1;
  for (let tick = 0; tick <= 100; tick += 20) {
    const y = padTop + plotH - (tick / 100) * plotH;
    ctx.beginPath();
    ctx.moveTo(padLeft, y);
    ctx.lineTo(w - padRight, y);
    ctx.stroke();
    ctx.fillStyle = RD.label;
    ctx.font = `400 11px ${font}`;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    ctx.fillText(tick + "%", padLeft - 8, y);
  }

  // Legend
  const legendY = 16;
  ctx.font = `500 11px ${font}`;
  let lx = padLeft;
  const legendItems = [
    { label: "Result Acc.", color: RD.teal },
    { label: "SQL Equiv.", color: "#f59e0b" },
    { label: "Halluc. Control", color: RD.blue },
  ];
  legendItems.forEach(({ label, color }) => {
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.roundRect(lx, legendY - 6, 12, 12, 3); ctx.fill();
    ctx.fillStyle = RD.text;
    ctx.textAlign = "left";
    ctx.fillText(label, lx + 16, legendY + 1);
    lx += ctx.measureText(label).width + 30;
  });

  const barColors = [
    { fill: RD.teal, gradient: "#061c26" },
    { fill: "#f59e0b", gradient: "#78350f" },
    { fill: RD.blue, gradient: "#7a2fbf" },
  ];

  // Bars
  labels.forEach((label, i) => {
    const cx = padLeft + groupW * i + groupW / 2;
    const startX = cx - totalBarGroupW / 2;

    [accVals[i], sqlVals[i], halVals[i]].forEach((val, bi) => {
      const bx = startX + bi * (barW + barGap);
      const bh = (val / 100) * plotH;
      const by = padTop + plotH - bh;

      ctx.fillStyle = RD.modelsBg[bi % RD.modelsBg.length];
      ctx.beginPath(); ctx.roundRect(bx, padTop, barW, plotH, [6, 6, 0, 0]); ctx.fill();

      const grad = ctx.createLinearGradient(0, by, 0, padTop + plotH);
      grad.addColorStop(0, barColors[bi].fill);
      grad.addColorStop(1, barColors[bi].gradient);
      ctx.fillStyle = grad;
      ctx.beginPath(); ctx.roundRect(bx, by, barW, bh, [6, 6, 0, 0]); ctx.fill();

      ctx.font = `600 10px ${font}`;
      ctx.textAlign = "center";
      ctx.fillStyle = RD.darkTeal;
      ctx.fillText(val.toFixed(1) + "%", bx + barW / 2, by - 5);
    });

    ctx.font = `500 12px ${font}`;
    ctx.fillStyle = RD.text;
    ctx.textAlign = "center";
    ctx.fillText(label, cx, padTop + plotH + 20);
  });
}

/* ── Scatter: Accuracy vs. Cost ── */
function drawAccuracyCostChart(canvasId, data) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const container = canvas.parentElement;
  const dpr = window.devicePixelRatio || 1;
  const w = container.clientWidth;
  const h = container.clientHeight || 340;

  canvas.width = w * dpr;
  canvas.height = h * dpr;
  canvas.style.width = w + "px";
  canvas.style.height = h + "px";

  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  const font = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
  const bubbleR = 22;
  const padLeft = 66, padRight = 40, padTop = bubbleR + 30, padBottom = 52;
  const plotW = w - padLeft - padRight;
  const plotH = h - padTop - padBottom;

  const costs = data.summary.map((item) => item.cost_mean);
  const accs = data.summary.map((item) => item.accuracy);
  const maxCost = Math.max(...costs, 0.0001) * 1.3;
  const minAcc = Math.max(0, Math.min(...accs) - 10);
  const maxAcc = Math.max(...accs) + 5;
  const accRange = Math.max(maxAcc - minAcc, 1);

  function xPos(cost) { return padLeft + (cost / maxCost) * plotW; }
  function yPos(acc) { return padTop + plotH - ((acc - minAcc) / accRange) * plotH; }

  // "Better" region (top-left) — subtle gradient
  const betterGrad = ctx.createLinearGradient(padLeft, padTop, padLeft + plotW * 0.4, padTop + plotH * 0.4);
  betterGrad.addColorStop(0, "rgba(46,182,125,0.08)");
  betterGrad.addColorStop(1, "rgba(46,182,125,0)");
  ctx.fillStyle = betterGrad;
  ctx.fillRect(padLeft, padTop, plotW, plotH);

  // Grid
  ctx.strokeStyle = RD.gridFine;
  ctx.lineWidth = 1;
  const costTicks = 5;
  for (let i = 0; i <= costTicks; i++) {
    const val = (maxCost / costTicks) * i;
    const x = xPos(val);
    ctx.beginPath(); ctx.moveTo(x, padTop); ctx.lineTo(x, padTop + plotH); ctx.stroke();
    ctx.fillStyle = RD.label;
    ctx.font = `400 10px ${font}`;
    ctx.textAlign = "center";
    ctx.fillText("$" + val.toFixed(4), x, padTop + plotH + 16);
  }
  const accStep = 10;
  for (let tick = Math.ceil(minAcc / accStep) * accStep; tick <= maxAcc; tick += accStep) {
    const y = yPos(tick);
    ctx.beginPath(); ctx.moveTo(padLeft, y); ctx.lineTo(padLeft + plotW, y); ctx.stroke();
    ctx.fillStyle = RD.label;
    ctx.font = `400 10px ${font}`;
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    ctx.fillText(tick + "%", padLeft - 8, y);
  }

  // Axis labels
  ctx.fillStyle = RD.text;
  ctx.font = `500 12px ${font}`;
  ctx.textAlign = "center";
  ctx.fillText("Cost per Query (USD)", padLeft + plotW / 2, h - 6);
  ctx.save();
  ctx.translate(16, padTop + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText("Accuracy (%)", 0, 0);
  ctx.restore();

  // "Better" arrow hint
  ctx.save();
  ctx.globalAlpha = 0.25;
  ctx.fillStyle = RD.teal;
  ctx.font = `500 11px ${font}`;
  ctx.textAlign = "left";
  ctx.fillText("\u2196 better", padLeft + 6, padTop + 16);
  ctx.restore();

  // Plot points — compute positions first, then resolve callout collisions
  const points = data.summary.map((m, i) => ({
    m, i,
    x: xPos(m.cost_mean),
    y: yPos(m.accuracy),
    color: RD.models[i % RD.models.length],
  }));

  // Default callout above; if two points are close, push the second one below
  points.forEach((p) => { p.calloutY = p.y - bubbleR - 10; p.calloutAbove = true; });
  for (let a = 0; a < points.length; a++) {
    for (let b = a + 1; b < points.length; b++) {
      const dx = points[a].x - points[b].x;
      const dy = points[a].y - points[b].y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < bubbleR * 3) {
        // Place the lower-accuracy point's callout below its bubble
        const lower = points[a].m.accuracy <= points[b].m.accuracy ? a : b;
        points[lower].calloutY = points[lower].y + bubbleR + 16;
        points[lower].calloutAbove = false;
      }
    }
  }

  points.forEach((p) => {
    const { m, x, y, color, calloutY } = p;

    // Glow
    const glow = ctx.createRadialGradient(x, y, 0, x, y, bubbleR * 2);
    glow.addColorStop(0, color + "30");
    glow.addColorStop(1, color + "00");
    ctx.fillStyle = glow;
    ctx.beginPath(); ctx.arc(x, y, bubbleR * 2, 0, Math.PI * 2); ctx.fill();

    // Bubble
    const grad = ctx.createRadialGradient(x - 4, y - 4, 2, x, y, bubbleR);
    grad.addColorStop(0, color + "ee");
    grad.addColorStop(1, color);
    ctx.fillStyle = grad;
    ctx.beginPath(); ctx.arc(x, y, bubbleR, 0, Math.PI * 2); ctx.fill();

    // Border
    ctx.strokeStyle = "#ffffff";
    ctx.lineWidth = 2.5;
    ctx.beginPath(); ctx.arc(x, y, bubbleR, 0, Math.PI * 2); ctx.stroke();

    // Label
    const label = comparisonBubbleLabel(m);
    ctx.font = `600 ${label.length > 4 ? "10" : "11"}px ${font}`;
    ctx.fillStyle = "#ffffff";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(label, x, y);

    // Tooltip-style callout
    ctx.font = `500 10px ${font}`;
    ctx.fillStyle = RD.text;
    ctx.textAlign = "center";
    ctx.fillText(`${m.accuracy}% / $${m.cost_mean.toFixed(4)}`, x, calloutY);
  });

  // Plot border
  ctx.strokeStyle = RD.grid;
  ctx.lineWidth = 1;
  ctx.strokeRect(padLeft, padTop, plotW, plotH);
}

function renderRationale(data) {
  const el = document.getElementById("eval-rationale");
  if (!el || !data.summary.length) return;

  const cfg = findModeRun(data, "cfg");
  const noCfg = findModeRun(data, "no_cfg");
  const sampleCount = cfg?.sample_count ?? noCfg?.sample_count ?? data.summary[0]?.sample_count ?? 0;
  const activeModel = shortModel(data.active_model ?? data.summary[0]?.model ?? "current model");

  if (!cfg || !noCfg) {
    const availableModes = formatModeList(data.available_modes);
    el.innerHTML = `
      <p>We are comparing <span class="model-winner">${escapeHtml(activeModel)}</span> with and without CFG on the same benchmark cases.</p>
      <ul>
        <li><strong>Available results:</strong> ${escapeHtml(availableModes || "none yet")}.</li>
        <li><strong>Coverage:</strong> ${sampleCount} benchmark cases are ready for the modes that have completed.</li>
        <li><strong>Next step:</strong> run the missing mode to get a full CFG vs No CFG comparison.</li>
      </ul>
    `;
    return;
  }

  const accuracyDelta = cfg.accuracy - noCfg.accuracy;
  const sqlEquivDelta = (cfg.sql_equivalence || 0) - (noCfg.sql_equivalence || 0);
  const hallucinationDelta = cfg.hallucination - noCfg.hallucination;
  const latencyDelta = cfg.latency_mean - noCfg.latency_mean;
  const costDelta = cfg.cost_mean - noCfg.cost_mean;

  function describePointsDelta(delta, improved, reduced, metric, suffix) {
    const abs = Math.abs(delta).toFixed(1);
    if (abs === "0.0") return `CFG showed no change in ${metric} (${suffix}).`;
    const verb = delta > 0 ? improved : reduced;
    return `CFG ${verb} ${metric} by ${abs} points (${suffix}).`;
  }

  function describeLatencyDelta(delta, cfgVal, noCfgVal) {
    const abs = Math.abs(delta).toFixed(2);
    const suffix = `${cfgVal}s vs ${noCfgVal}s`;
    if (abs === "0.00") return `CFG showed no meaningful latency change (${suffix}).`;
    const verb = delta > 0 ? "added" : "saved";
    return `CFG ${verb} ${abs}s of mean latency (${suffix}).`;
  }

  function describeCostDelta(delta, cfgVal, noCfgVal) {
    const abs = Math.abs(delta).toFixed(4);
    const suffix = `$${cfgVal.toFixed(4)} vs $${noCfgVal.toFixed(4)}`;
    if (abs === "0.0000") return `CFG showed no meaningful cost difference (${suffix}).`;
    const verb = delta > 0 ? "added" : "saved";
    return `CFG ${verb} $${abs} per query on average (${suffix}).`;
  }

  el.innerHTML = `
    <p>We evaluated <span class="model-winner">${escapeHtml(activeModel)}</span> across the same ${sampleCount} benchmark cases with and without CFG.</p>
    <ul>
      <li><strong>Result accuracy:</strong> ${describePointsDelta(accuracyDelta, "improved", "reduced", "result-set accuracy", `${cfg.accuracy}% vs ${noCfg.accuracy}%`)} Compares query outputs on shared columns.</li>
      <li><strong>SQL equivalence:</strong> ${describePointsDelta(sqlEquivDelta, "improved", "reduced", "SQL-structure match", `${cfg.sql_equivalence || 0}% vs ${noCfg.sql_equivalence || 0}%`)} Compares WHERE/GROUP BY/ORDER BY/LIMIT, ignoring column selection.</li>
      <li><strong>Hallucination control:</strong> ${describePointsDelta(hallucinationDelta, "improved", "reduced", "schema-safety pass rate", `${cfg.hallucination}% vs ${noCfg.hallucination}%`)}</li>
      <li><strong>Latency:</strong> ${describeLatencyDelta(latencyDelta, cfg.latency_mean, noCfg.latency_mean)}</li>
      <li><strong>Cost:</strong> ${describeCostDelta(costDelta, cfg.cost_mean, noCfg.cost_mean)}</li>
    </ul>
  `;
}

function renderSummaryTable(data) {
  const el = document.getElementById("eval-summary-table");
  if (!el || !data.summary.length) return;

  const headers = ["Mode", "Result Acc.", "SQL Equiv.", "Hallucination", "Latency Budget", "Cost Budget", "Mean Latency", "P95 Latency", "Mean Cost"];
  const hrow = headers.map(h => `<th>${h}</th>`).join("");
  const rows = data.summary.map(m => `<tr>
    <td><strong>${escapeHtml(comparisonLabel(m))}</strong></td>
    <td>${m.accuracy}%</td>
    <td>${m.sql_equivalence || 0}%</td>
    <td>${m.hallucination}%</td>
    <td>${m.latency_budget}%</td>
    <td>${m.cost_budget}%</td>
    <td>${m.latency_mean}s</td>
    <td>${m.latency_p95}s</td>
    <td>$${m.cost_mean.toFixed(4)}</td>
  </tr>`).join("");

  el.innerHTML = `<table class="result-table"><thead><tr>${hrow}</tr></thead><tbody>${rows}</tbody></table>`;
}

function renderSamplesTable(data) {
  const el = document.getElementById("eval-samples-table");
  if (!el || !data.samples.length) return;

  const headers = ["Mode", "ID", "Category", "Question", "Result", "SQL", "Halluc.", "Latency", "Cost"];
  const hrow = headers.map(h => `<th>${h}</th>`).join("");
  const rows = data.samples.map(s => {
    const acc = s.accuracy ? "C" : "I";
    const sql = s.sql_equivalence ? "C" : "I";
    const hal = s.hallucination ? "C" : "I";
    const accStyle = s.accuracy ? "color:var(--accent)" : "color:var(--danger)";
    const sqlStyle = s.sql_equivalence ? "color:var(--accent)" : "color:var(--danger)";
    const halStyle = s.hallucination ? "color:var(--accent)" : "color:var(--danger)";
    return `<tr>
      <td>${escapeHtml(comparisonLabel(s))}</td>
      <td><code>${escapeHtml(s.id)}</code></td>
      <td>${escapeHtml(s.category)}</td>
      <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escapeHtml(s.question)}">${escapeHtml(s.question)}</td>
      <td style="${accStyle};font-weight:600">${acc}</td>
      <td style="${sqlStyle};font-weight:600">${sql}</td>
      <td style="${halStyle};font-weight:600">${hal}</td>
      <td>${s.latency}s</td>
      <td>$${s.cost.toFixed(4)}</td>
    </tr>`;
  }).join("");

  el.innerHTML = `<table class="result-table"><thead><tr>${hrow}</tr></thead><tbody>${rows}</tbody></table>`;
}

async function loadEvals() {
  try {
    const res = await fetch("/api/evals");
    if (!res.ok) {
      throw new Error("Failed to load eval results.");
    }
    const data = await res.json();
    renderEvalData(data);
    if (data.run_errors?.length) {
      const details = data.run_errors.map((item) => `${modeLabel(item.generation_mode)}: ${item.error}`).join(" | ");
      setEvalRunStatus(`Loaded results with errors: ${details}`, true);
    } else if (data.complete) {
      setEvalRunStatus(`Loaded latest ${data.selected_sample_count ?? data.summary[0]?.sample_count ?? "saved"}-case CFG vs No CFG results for ${shortModel(data.active_model)}. Click Run Evals to refresh them.`);
    } else if (data.available_modes?.length) {
      setEvalRunStatus(`Loaded partial results for ${shortModel(data.active_model)}: ${formatModeList(data.available_modes)}. Run the missing mode for a full comparison.`);
    } else {
      setEvalRunStatus(`No saved eval results yet for ${shortModel(data.active_model)}. Click Run Evals to generate CFG and No CFG runs.`);
    }
    evalsLoaded = true;
  } catch (err) {
    document.getElementById("evals-loading").textContent = "Failed to load eval results.";
    setEvalRunStatus("Failed to load eval results.", true);
  }
}

async function runEvals() {
  const sampleCount = selectedEvalSampleCount();
  setRunEvalsBusy(true);
  setEvalRunStatus(`Running fresh ${sampleCount}-case benchmark evals for CFG and No CFG. This can take a few minutes.`);
  try {
    const res = await fetch("/api/evals/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sample_count: sampleCount }),
    });
    if (!res.ok) {
      throw new Error("Failed to run evals.");
    }
    const data = await res.json();
    renderEvalData(data);
    evalsLoaded = true;

    if (data.run_errors?.length) {
      const details = data.run_errors.map((item) => `${modeLabel(item.generation_mode)}: ${item.error}`).join(" | ");
      setEvalRunStatus(`Eval run finished with issues. ${details}`, true);
      return;
    }

    if (data.complete) {
      setEvalRunStatus(`Fresh ${sampleCount}-case CFG vs No CFG evals completed for ${shortModel(data.active_model)}.`);
    } else {
      setEvalRunStatus(`Eval run finished, but only ${formatModeList(data.available_modes)} results are available.`, true);
    }
  } catch (err) {
    const message = err?.message || "Failed to run evals.";
    setEvalRunStatus(message, true);
  } finally {
    setRunEvalsBusy(false);
  }
}

runEvalsButton?.addEventListener("click", runEvals);
