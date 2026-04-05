from __future__ import annotations

from pathlib import Path

import nbformat as nbf


NOTEBOOK_PATH = Path("notebooks/cfg_sql_eval.ipynb")


def main() -> None:
    notebook = nbf.v4.new_notebook()
    notebook.cells = [
        nbf.v4.new_markdown_cell(
            """# CFG SQL Evaluation

This notebook can either run the `inspect-ai` benchmark again or load existing successful runs from `logs/inspect`.

It compares the same benchmark cases in two generation modes:

- CFG
- No CFG

It then summarizes the four report dimensions used in the Quake SQL benchmark:

- Accuracy
- Hallucination control
- Latency
- Cost per answer

The comparison figures below are styled for side-by-side run inspection, using the same run artifacts that power the app evaluation workflow."""
        ),
        nbf.v4.new_code_cell(
            """from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import display

from quake_sql.evals import (
    GENERATION_MODE_CFG,
    GENERATION_MODE_NO_CFG,
    category_pass_rate_dataframe,
    comparison_samples_dataframe,
    comparison_summary_dataframe,
    generation_mode_label,
    latest_successful_log_paths,
    load_eval_logs,
    run_eval_suite,
)

pd.set_option("display.max_colwidth", 160)
pd.set_option("display.max_rows", 200)"""
        ),
        nbf.v4.new_code_cell(
            """LOG_DIR = Path("logs/inspect")
MODEL_COST_CONFIG = "evals/model_costs.json"
TARGET_MODELS = [
    "openai/gpt-5.4",
    "openai/gpt-5.4-mini",
    "openai/gpt-5.4-nano",
]
TARGET_GENERATION_MODES = [GENERATION_MODE_CFG, GENERATION_MODE_NO_CFG]

# Leave empty to auto-pick the latest successful full-length run per model and generation mode.
PINNED_LOG_PATHS: list[str] = []

# Flip this on if you want the notebook to execute new evals instead of loading existing logs.
RUN_FRESH = False
FRESH_LIMIT = None

MODEL_LABELS = {
    "openai/gpt-5.4": "GPT-5.4",
    "openai/gpt-5.4-mini": "GPT-5.4 Mini",
    "openai/gpt-5.4-nano": "GPT-5.4 Nano",
}
MODEL_COLORS = {
    "openai/gpt-5.4": "#4E79A7",
    "openai/gpt-5.4-mini": "#59A14F",
    "openai/gpt-5.4-nano": "#F28E2B",
}
METRIC_LABELS = {
    "accuracy_pass": "Accuracy",
    "hallucination_pass": "Hallucination",
    "latency_budget_pass": "Latency Budget",
    "cost_budget_pass": "Cost Budget",
}
METRIC_COLORS = {
    "accuracy_pass": "#4E79A7",
    "hallucination_pass": "#76B7B2",
    "latency_budget_pass": "#E15759",
    "cost_budget_pass": "#EDC948",
}
TARGET_RUNS = [
    (model, generation_mode)
    for model in TARGET_MODELS
    for generation_mode in TARGET_GENERATION_MODES
]
RUN_ORDER = {run: idx for idx, run in enumerate(TARGET_RUNS)}


def build_run_label(model: str, generation_mode: str) -> str:
    return f"{MODEL_LABELS.get(model, model)} / {generation_mode_label(generation_mode)}"


def add_run_labels(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["label"] = result.apply(
        lambda row: build_run_label(row["model"], row["generation_mode"]),
        axis=1,
    )
    return result


def order_runs(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["run_order"] = result.apply(
        lambda row: RUN_ORDER.get((row["model"], row["generation_mode"]), len(RUN_ORDER)),
        axis=1,
    )
    sort_columns = ["run_order"] + [column for column in ("category", "metric", "id") if column in result.columns]
    return result.sort_values(sort_columns, na_position="last").drop(columns="run_order").reset_index(drop=True)

mpl.rcParams.update(
    {
        "figure.figsize": (10, 6),
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlesize": 14,
        "axes.labelsize": 11,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.frameon": False,
        "figure.dpi": 130,
    }
)"""
        ),
        nbf.v4.new_code_cell(
            """if RUN_FRESH:
    eval_logs = [
        run_eval_suite(
            log_dir=str(LOG_DIR),
            model=model,
            limit=FRESH_LIMIT,
            model_cost_config=MODEL_COST_CONFIG,
            use_cfg=generation_mode == GENERATION_MODE_CFG,
        )
        for model in TARGET_MODELS
        for generation_mode in TARGET_GENERATION_MODES
    ]
    selected_log_paths = [Path(log.location) for log in eval_logs]
else:
    selected_log_paths = (
        [Path(path) for path in PINNED_LOG_PATHS]
        if PINNED_LOG_PATHS
        else latest_successful_log_paths(
            log_dir=str(LOG_DIR),
            models=TARGET_MODELS,
            generation_modes=TARGET_GENERATION_MODES,
        )
    )
    eval_logs = load_eval_logs(selected_log_paths)

if not selected_log_paths:
    raise RuntimeError("No eval logs found. Either set PINNED_LOG_PATHS or run the benchmark first.")

selected_runs = order_runs(add_run_labels(comparison_summary_dataframe(selected_log_paths)))
selected_runs[["model", "generation_mode", "label", "sample_count", "status", "log_path"]]"""
        ),
        nbf.v4.new_code_cell(
            """comparison_summary = order_runs(add_run_labels(comparison_summary_dataframe(selected_log_paths)))
comparison_samples = order_runs(add_run_labels(comparison_samples_dataframe(selected_log_paths)))
category_summary = order_runs(add_run_labels(category_pass_rate_dataframe(selected_log_paths)))

display(comparison_summary)"""
        ),
        nbf.v4.new_markdown_cell(
            """## Aggregate Comparison"""
        ),
        nbf.v4.new_code_cell(
            """metric_order = [
    "accuracy_pass",
    "hallucination_pass",
    "latency_budget_pass",
    "cost_budget_pass",
]

summary_plot = comparison_summary.copy()
x = np.arange(len(summary_plot))
width = 0.18

fig, ax = plt.subplots(figsize=(10, 4.8))
for idx, metric_name in enumerate(metric_order):
    values = 100 * summary_plot[metric_name].to_numpy()
    offset = (idx - 1.5) * width
    bars = ax.bar(
        x + offset,
        values,
        width=width,
        color=METRIC_COLORS[metric_name],
        edgecolor="white",
        linewidth=0.6,
        label=METRIC_LABELS[metric_name],
    )
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            value + 1.5,
            f"{value:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

ax.set_xticks(x)
ax.set_xticklabels(summary_plot["label"], rotation=20, ha="right")
ax.set_ylabel("Pass Rate (%)")
ax.set_ylim(0, 110)
ax.set_title("Benchmark Pass Rates by Model and CFG Mode")
ax.grid(axis="y", alpha=0.2)
ax.legend(ncols=2, loc="upper center", bbox_to_anchor=(0.5, 1.18))
plt.show()"""
        ),
        nbf.v4.new_markdown_cell(
            """## Efficiency Tradeoffs"""
        ),
        nbf.v4.new_code_cell(
            """summary_plot = comparison_summary.copy()
labels = summary_plot["label"].tolist()
colors = [MODEL_COLORS.get(model, "#999999") for model in summary_plot["model"]]
x = np.arange(len(summary_plot))

fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), constrained_layout=True)

lat_mean = summary_plot["latency_mean_seconds"].to_numpy()
lat_p95 = summary_plot["latency_p95_seconds"].to_numpy()
axes[0].bar(x, lat_mean, color=colors, edgecolor="white", linewidth=0.6)
axes[0].scatter(x, lat_p95, color="#222222", s=45, zorder=3, label="P95")
for idx, (mean_v, p95_v) in enumerate(zip(lat_mean, lat_p95)):
    axes[0].text(idx, mean_v + 0.6, f"{mean_v:.1f}s", ha="center", va="bottom", fontsize=9)
    axes[0].text(idx, p95_v + 0.8, f"{p95_v:.1f}s", ha="center", va="bottom", fontsize=8, color="#222222")
axes[0].set_xticks(x)
axes[0].set_xticklabels(labels, rotation=20, ha="right")
axes[0].set_ylabel("Seconds")
axes[0].set_title("Latency by Model and CFG Mode")
axes[0].grid(axis="y", alpha=0.2)
axes[0].legend(loc="upper left")

cost_mean = summary_plot["cost_mean_usd"].to_numpy()
cost_p95 = summary_plot["cost_p95_usd"].to_numpy()
axes[1].bar(x, cost_mean, color=colors, edgecolor="white", linewidth=0.6)
axes[1].scatter(x, cost_p95, color="#222222", s=45, zorder=3, label="P95")
for idx, (mean_v, p95_v) in enumerate(zip(cost_mean, cost_p95)):
    axes[1].text(idx, mean_v + 0.00003, f"${mean_v:.4f}", ha="center", va="bottom", fontsize=9)
    axes[1].text(idx, p95_v + 0.00004, f"${p95_v:.4f}", ha="center", va="bottom", fontsize=8, color="#222222")
axes[1].set_xticks(x)
axes[1].set_xticklabels(labels, rotation=20, ha="right")
axes[1].set_ylabel("USD")
axes[1].set_title("Cost by Model and CFG Mode")
axes[1].grid(axis="y", alpha=0.2)
axes[1].legend(loc="upper left")

plt.show()"""
        ),
        nbf.v4.new_markdown_cell(
            """## Category Heatmaps"""
        ),
        nbf.v4.new_code_cell(
            """category_order = sorted(comparison_samples["category"].dropna().unique())
metric_order = [
    "accuracy_pass",
    "hallucination_pass",
    "latency_budget_pass",
    "cost_budget_pass",
]

fig, axes = plt.subplots(2, 2, figsize=(14, 8), constrained_layout=True)
axes = axes.flatten()

for ax, metric_name in zip(axes, metric_order):
    heat = (
        category_summary[category_summary["metric"] == metric_name]
        .pivot(index="category", columns="label", values="pass_rate")
        .reindex(index=category_order, columns=comparison_summary["label"])
        .fillna(0.0)
    )
    image = ax.imshow(heat.to_numpy() * 100, cmap="YlGnBu", vmin=0, vmax=100, aspect="auto")
    ax.set_title(METRIC_LABELS[metric_name])
    ax.set_xticks(np.arange(len(comparison_summary)))
    ax.set_xticklabels(comparison_summary["label"], rotation=25, ha="right")
    ax.set_yticks(np.arange(len(category_order)))
    ax.set_yticklabels(category_order)
    for row_idx in range(len(category_order)):
        for col_idx in range(len(comparison_summary)):
            value = heat.iloc[row_idx, col_idx] * 100
            ax.text(
                col_idx,
                row_idx,
                f"{value:.0f}",
                ha="center",
                va="center",
                color="white" if value >= 60 else "#222222",
                fontsize=8,
            )

fig.colorbar(image, ax=axes, shrink=0.78, label="Pass Rate (%)")
plt.show()"""
        ),
        nbf.v4.new_markdown_cell(
            """## Failure Slice"""
        ),
        nbf.v4.new_code_cell(
            """failure_slice = comparison_samples[
    (comparison_samples["accuracy_pass"] != "C")
    | (comparison_samples["hallucination_pass"] != "C")
].copy()

failure_slice = failure_slice[
    [
        "label",
        "id",
        "category",
        "question",
        "accuracy_pass",
        "hallucination_pass",
        "latency_seconds",
        "estimated_cost_usd",
        "error",
    ]
].sort_values(["label", "category", "id"])

failure_slice"""
        ),
    ]
    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    NOTEBOOK_PATH.write_text(nbf.writes(notebook), encoding="utf-8")
    print(f"Wrote {NOTEBOOK_PATH}")


if __name__ == "__main__":
    main()
