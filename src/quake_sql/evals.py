from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from inspect_ai import Task, eval as inspect_eval, task
from inspect_ai.dataset import MemoryDataset, Sample
from inspect_ai.log import EvalLog, read_eval_log
from inspect_ai.model import ModelOutput, ModelUsage
from inspect_ai.scorer import (
    CORRECT,
    INCORRECT,
    SampleScore,
    Score,
    Target,
    accuracy,
    mean,
    metric,
    scorer,
)
from inspect_ai.solver import Generate, Solver, TaskState, solver

from quake_sql.clickhouse import execute_query, get_client
from quake_sql.config import get_settings
from quake_sql.openai_sql import OpenAISqlGenerator


LATENCY_THRESHOLD_SECONDS = 6.0
COST_THRESHOLD_USD = 0.01
GENERATION_MODE_CFG = "cfg"
GENERATION_MODE_NO_CFG = "no_cfg"


def generation_mode_name(use_cfg: bool) -> str:
    return GENERATION_MODE_CFG if use_cfg else GENERATION_MODE_NO_CFG


def generation_mode_label(generation_mode: str) -> str:
    return "CFG" if generation_mode == GENERATION_MODE_CFG else "No CFG"


def run_label(model: str, generation_mode: str) -> str:
    return f"{model} / {generation_mode_label(generation_mode)}"


def _task_name(use_cfg: bool) -> str:
    return "quake_sql_benchmark" if use_cfg else "quake_sql_benchmark_no_cfg"


def _generation_mode_from_payload(payload: dict[str, Any] | None) -> str:
    if not payload:
        return GENERATION_MODE_CFG
    generation_mode = str(payload.get("generation_mode") or "").strip()
    if generation_mode in {GENERATION_MODE_CFG, GENERATION_MODE_NO_CFG}:
        return generation_mode
    return GENERATION_MODE_CFG


def generation_mode_for_eval_log(eval_log: EvalLog) -> str:
    for sample in eval_log.samples or []:
        payload = sample.output.metadata or {}
        return _generation_mode_from_payload(payload)
    return GENERATION_MODE_CFG


def _project_root() -> Path:
    env = os.environ.get("PROJECT_ROOT")
    if env:
        return Path(env)
    # When installed as a package (e.g. via pip install), __file__ lives in
    # site-packages and parents[2] resolves to the Python lib directory, not
    # the project root.  Detect this and fall back to the current working
    # directory which is set to the project root via WORKDIR in the Dockerfile.
    candidate = Path(__file__).resolve().parents[2]
    if "site-packages" in str(candidate) or not (candidate / "logs").exists():
        return Path.cwd()
    return candidate


def _cases_path() -> Path:
    return _project_root() / "evals" / "cases.json"


def _model_costs_path() -> Path:
    return _project_root() / "evals" / "model_costs.json"


def load_cases() -> list[dict[str, Any]]:
    return json.loads(_cases_path().read_text(encoding="utf-8"))


def benchmark_sample_count() -> int:
    return len(load_cases())


def build_dataset() -> MemoryDataset:
    samples = [
        Sample(
            id=case["id"],
            input=case["question"],
            target=case["expected_sql"],
            metadata={
                "category": case["category"],
                "expected_sql": case["expected_sql"],
                "supported": case["expected_sql"] != "UNSUPPORTED",
            },
        )
        for case in load_cases()
    ]
    return MemoryDataset(samples, name="quake_sql_cases")


@lru_cache(maxsize=1)
def _generator() -> OpenAISqlGenerator:
    return OpenAISqlGenerator(get_settings())


@lru_cache(maxsize=1)
def _clickhouse_client():
    return get_client(get_settings())


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 6)
    return value


def _serialize_query(sql: str) -> dict[str, Any]:
    result = execute_query(_clickhouse_client(), sql)
    return {
        "columns": list(result.columns),
        "column_count": len(result.columns),
        "row_count": result.row_count,
        "rows": [[_normalize_scalar(value) for value in row] for row in result.rows],
    }


def _get_cached_query(state: TaskState, store_key: str, sql: str) -> dict[str, Any]:
    cached = state.store.get(store_key)
    if cached is not None:
        return cached
    payload = _serialize_query(sql)
    state.store.set(store_key, payload)
    return payload


def _sample_payload(state: TaskState) -> dict[str, Any]:
    return state.output.metadata or {}


def _selected_model(state: TaskState) -> str | None:
    model = str(state.model)
    return None if model == "none/none" else model


def _is_schema_hallucination_error(error: str | None) -> bool:
    if not error:
        return False
    lowered = error.lower()
    return (
        "unknown columns" in lowered
        or "only the `earthquakes` table is allowed" in lowered
        or "generated statement is not a select query" in lowered
        or "generated sql could not be parsed" in lowered
    )


def _usage_from_payload(payload: dict[str, Any]) -> ModelUsage | None:
    usage = payload.get("usage")
    if usage is None:
        return None
    return ModelUsage(
        input_tokens=max(usage["input_tokens"] - usage["cached_input_tokens"], 0),
        input_tokens_cache_read=usage["cached_input_tokens"],
        output_tokens=usage["output_tokens"],
        reasoning_tokens=usage["reasoning_tokens"],
        total_tokens=usage["total_tokens"],
        total_cost=usage["estimated_cost_usd"],
    )


@solver
def generate_sql(use_cfg: bool = True) -> Solver:
    generation_mode = generation_mode_name(use_cfg)

    async def solve(state: TaskState, generate: Generate) -> TaskState:
        try:
            result = _generator().generate(
                state.input_text,
                model=_selected_model(state),
                use_cfg=use_cfg,
            )
            payload = {
                "model": result.model,
                "generation_mode": generation_mode,
                "generated_sql": result.sql,
                "unsupported": result.unsupported,
                "latency_seconds": result.latency_seconds,
                "usage": asdict(result.usage) if result.usage else None,
                "error": None,
            }
            state.output = ModelOutput.from_content(
                model=result.model,
                content=result.sql,
            )
            state.output.time = result.latency_seconds
            state.output.metadata = payload
            state.output.usage = _usage_from_payload(payload)
        except Exception as exc:
            payload = {
                "model": _selected_model(state) or f"openai/{get_settings().openai_model}",
                "generation_mode": generation_mode,
                "generated_sql": "UNSUPPORTED",
                "unsupported": True,
                "latency_seconds": 0.0,
                "usage": None,
                "error": str(exc),
            }
            state.output = ModelOutput.from_content(
                model=payload["model"],
                content="UNSUPPORTED",
                error=str(exc),
            )
            state.output.metadata = payload
        state.completed = True
        return state

    return solve


@metric
def p95() -> Any:
    def compute(scores: list[SampleScore]) -> float:
        values = sorted(score.score.as_float() for score in scores)
        if not values:
            return 0.0
        index = int(round(0.95 * (len(values) - 1)))
        return float(values[index])

    return compute


def _project_results(
    expected_result: dict[str, Any],
    generated_result: dict[str, Any],
) -> tuple[list[list[Any]], list[list[Any]]]:
    """Project generated result rows onto expected columns for comparison.

    If the generated query returned extra columns (e.g. SELECT * vs specific
    columns), we compare only the overlapping columns so that broader column
    selection is not penalised.
    """
    exp_cols: list[str] = expected_result.get("columns", [])
    gen_cols: list[str] = generated_result.get("columns", [])

    if exp_cols == gen_cols:
        return expected_result["rows"], generated_result["rows"]

    # Map generated column names to their indices
    gen_col_index = {name: idx for idx, name in enumerate(gen_cols)}

    # Find which expected columns exist in the generated result
    shared_indices_exp: list[int] = []
    shared_indices_gen: list[int] = []
    for exp_idx, col in enumerate(exp_cols):
        if col in gen_col_index:
            shared_indices_exp.append(exp_idx)
            shared_indices_gen.append(gen_col_index[col])

    if not shared_indices_exp:
        # No overlapping columns — fall back to full comparison (will fail)
        return expected_result["rows"], generated_result["rows"]

    proj_expected = [[row[i] for i in shared_indices_exp] for row in expected_result["rows"]]
    proj_generated = [[row[i] for i in shared_indices_gen] for row in generated_result["rows"]]
    return proj_expected, proj_generated


@scorer(metrics=[accuracy(), mean()], name="accuracy_pass")
def accuracy_pass():
    """Result-set accuracy: compares query outputs, tolerant of extra columns."""

    async def score(state: TaskState, target: Target) -> Score:
        payload = _sample_payload(state)
        generated_sql = payload.get("generated_sql", "")
        expected_sql = state.metadata["expected_sql"]
        explanation = generated_sql

        if payload.get("error"):
            return Score(value=INCORRECT, answer=generated_sql, explanation=payload["error"])

        if expected_sql == "UNSUPPORTED":
            correct = generated_sql == "UNSUPPORTED"
            return Score(
                value=CORRECT if correct else INCORRECT,
                answer=generated_sql,
                explanation=explanation,
            )

        if generated_sql == "UNSUPPORTED":
            return Score(value=INCORRECT, answer=generated_sql, explanation="Model rejected a supported question.")

        expected_result = _get_cached_query(state, "expected_result", expected_sql)
        generated_result = _get_cached_query(state, "generated_result", generated_sql)

        # Row counts must match
        if expected_result["row_count"] != generated_result["row_count"]:
            return Score(
                value=INCORRECT,
                answer=generated_sql,
                explanation=(
                    f"Row count mismatch: expected {expected_result['row_count']}, "
                    f"got {generated_result['row_count']}"
                ),
            )

        # Compare on shared columns (tolerates SELECT * or extra columns)
        proj_expected, proj_generated = _project_results(expected_result, generated_result)
        correct = proj_expected == proj_generated
        return Score(
            value=CORRECT if correct else INCORRECT,
            answer=generated_sql,
            explanation=(
                "Result data matched on shared columns."
                if correct
                else f"Expected rows: {proj_expected[:3]}; generated rows: {proj_generated[:3]}"
            ),
        )

    return score


def _normalize_sql_structure(sql: str) -> dict[str, str] | None:
    """Extract the structural components of a SELECT for comparison.

    Returns a dict with normalised WHERE, GROUP BY, ORDER BY, LIMIT, and FROM
    clauses (as strings), or None if parsing fails.  The SELECT list is
    intentionally excluded so that column-choice differences don't matter.
    """
    from sqlglot import parse_one
    from sqlglot.errors import ParseError
    from sqlglot import exp

    try:
        parsed = parse_one(sql, read="clickhouse")
    except ParseError:
        return None
    if not isinstance(parsed, exp.Select):
        return None

    def _norm(node: exp.Expression | None) -> str:
        return node.sql(dialect="clickhouse", normalize=True).strip() if node else ""

    return {
        "from": _norm(parsed.find(exp.From)),
        "where": _norm(parsed.args.get("where")),
        "group": _norm(parsed.args.get("group")),
        "order": _norm(parsed.args.get("order")),
        "limit": _norm(parsed.args.get("limit")),
    }


@scorer(metrics=[accuracy(), mean()], name="sql_equivalence_pass")
def sql_equivalence_pass():
    """SQL-structure equivalence: same WHERE/GROUP BY/ORDER BY/LIMIT, ignoring SELECT list and aliases."""

    async def score(state: TaskState, target: Target) -> Score:
        payload = _sample_payload(state)
        generated_sql = payload.get("generated_sql", "")
        expected_sql = state.metadata["expected_sql"]

        if payload.get("error"):
            return Score(value=INCORRECT, answer=generated_sql, explanation=payload["error"])

        # Both UNSUPPORTED
        if expected_sql == "UNSUPPORTED":
            correct = generated_sql == "UNSUPPORTED"
            return Score(
                value=CORRECT if correct else INCORRECT,
                answer=generated_sql,
                explanation="Both UNSUPPORTED." if correct else "Expected UNSUPPORTED.",
            )

        if generated_sql == "UNSUPPORTED":
            return Score(
                value=INCORRECT,
                answer=generated_sql,
                explanation="Model returned UNSUPPORTED for a supported question.",
            )

        exp_struct = _normalize_sql_structure(expected_sql)
        gen_struct = _normalize_sql_structure(generated_sql)

        if exp_struct is None or gen_struct is None:
            return Score(
                value=INCORRECT,
                answer=generated_sql,
                explanation="Could not parse SQL for structural comparison.",
            )

        mismatches = []
        for clause in ("from", "where", "group", "order", "limit"):
            if exp_struct[clause] != gen_struct[clause]:
                mismatches.append(
                    f"{clause.upper()}: expected `{exp_struct[clause]}`, got `{gen_struct[clause]}`"
                )

        if not mismatches:
            return Score(
                value=CORRECT,
                answer=generated_sql,
                explanation="SQL structure matches (WHERE, GROUP BY, ORDER BY, LIMIT, FROM).",
            )

        return Score(
            value=INCORRECT,
            answer=generated_sql,
            explanation="Structural mismatches: " + "; ".join(mismatches),
        )

    return score


@scorer(metrics=[accuracy(), mean()], name="hallucination_pass")
def hallucination_pass():
    async def score(state: TaskState, target: Target) -> Score:
        payload = _sample_payload(state)
        expected_sql = state.metadata["expected_sql"]
        generated_sql = payload.get("generated_sql", "")
        error = payload.get("error")

        if expected_sql == "UNSUPPORTED":
            passed = generated_sql == "UNSUPPORTED"
            explanation = (
                "Model correctly abstained on an unsupported prompt."
                if passed
                else "Model fabricated a SQL answer for an unsupported prompt."
            )
            return Score(
                value=CORRECT if passed else INCORRECT,
                answer=generated_sql,
                explanation=explanation,
            )

        if generated_sql == "UNSUPPORTED" and not error:
            return Score(
                value=CORRECT,
                answer=generated_sql,
                explanation="Model abstained instead of fabricating unsupported schema.",
            )

        if _is_schema_hallucination_error(error):
            return Score(
                value=INCORRECT,
                answer=generated_sql,
                explanation=f"Schema hallucination detected: {error}",
            )

        if error:
            return Score(
                value=INCORRECT,
                answer=generated_sql,
                explanation=f"Generation failed before execution: {error}",
            )

        return Score(
            value=CORRECT,
            answer=generated_sql,
            explanation="Generated SQL stayed within the allowed schema and supported query surface.",
        )

    return score


@scorer(metrics=[accuracy(), mean()], name="latency_budget_pass")
def latency_budget_pass():
    async def score(state: TaskState, target: Target) -> Score:
        payload = _sample_payload(state)
        latency = float(payload.get("latency_seconds", 0.0))
        passed = latency <= LATENCY_THRESHOLD_SECONDS and not payload.get("error")
        return Score(
            value=CORRECT if passed else INCORRECT,
            answer=f"{latency:.3f}",
            explanation=f"Latency threshold: {LATENCY_THRESHOLD_SECONDS:.1f}s",
        )

    return score


@scorer(metrics=[accuracy(), mean()], name="cost_budget_pass")
def cost_budget_pass():
    async def score(state: TaskState, target: Target) -> Score:
        payload = _sample_payload(state)
        usage = payload.get("usage") or {}
        cost = usage.get("estimated_cost_usd")
        if cost is None:
            return Score(
                value=INCORRECT,
                answer="n/a",
                explanation="Cost unavailable for this model. Provide Inspect model pricing via model_cost_config.",
            )
        passed = float(cost) <= COST_THRESHOLD_USD and not payload.get("error")
        return Score(
            value=CORRECT if passed else INCORRECT,
            answer=f"{float(cost):.6f}",
            explanation=f"Cost threshold: ${COST_THRESHOLD_USD:.4f}",
        )

    return score


@scorer(metrics=[mean(), p95()], name="latency_seconds")
def latency_seconds():
    async def score(state: TaskState, target: Target) -> Score:
        latency = float(_sample_payload(state).get("latency_seconds", 0.0))
        return Score(value=latency, answer=f"{latency:.3f}")

    return score


@scorer(metrics=[mean(), p95()], name="cost_usd")
def cost_usd():
    async def score(state: TaskState, target: Target) -> Score:
        usage = _sample_payload(state).get("usage") or {}
        cost = usage.get("estimated_cost_usd")
        if cost is None:
            return Score(value=0.0, answer="n/a", explanation="Cost unavailable")
        return Score(value=float(cost), answer=f"{float(cost):.6f}")

    return score


def _quake_sql_benchmark(use_cfg: bool) -> Task:
    return Task(
        dataset=build_dataset(),
        solver=generate_sql(use_cfg=use_cfg),
        scorer=[
            accuracy_pass(),
            sql_equivalence_pass(),
            hallucination_pass(),
            latency_budget_pass(),
            cost_budget_pass(),
            latency_seconds(),
            cost_usd(),
        ],
        name=_task_name(use_cfg),
    )


@task
def quake_sql_benchmark() -> Task:
    return _quake_sql_benchmark(use_cfg=True)


@task
def quake_sql_benchmark_no_cfg() -> Task:
    return _quake_sql_benchmark(use_cfg=False)


def run_eval_suite(
    log_dir: str = "logs/inspect",
    model: str | None = None,
    limit: int | tuple[int, int] | None = None,
    model_cost_config: str | dict[str, Any] | None = None,
    use_cfg: bool = True,
) -> EvalLog:
    benchmark_task = quake_sql_benchmark() if use_cfg else quake_sql_benchmark_no_cfg()
    logs = inspect_eval(
        benchmark_task,
        model=model or f"openai/{get_settings().openai_model}",
        log_dir=log_dir,
        limit=limit,
        model_cost_config=model_cost_config or str(_model_costs_path()),
        log_samples=True,
        display="plain",
        score=True,
    )
    return logs[0]


def _sync_read_eval_log(path: Path) -> EvalLog:
    """Call read_eval_log and handle the case where it returns a coroutine
    (which happens inside a running asyncio event loop such as uvicorn)."""
    import asyncio
    import inspect as _inspect

    result = read_eval_log(path)
    if _inspect.isawaitable(result):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(asyncio.run, result).result()
        else:
            result = asyncio.run(result)
    return result


def load_eval_logs(log_paths: Sequence[str | Path]) -> list[EvalLog]:
    return [_sync_read_eval_log(Path(log_path)) for log_path in log_paths]


def latest_successful_log_paths(
    log_dir: str = "logs/inspect",
    models: Sequence[str] | None = None,
    generation_modes: Sequence[str] | None = None,
    min_samples: int | None = None,
) -> list[Path]:
    expected_samples = benchmark_sample_count() if min_samples is None else min_samples
    requested_models = list(models) if models is not None else None
    requested_generation_modes = list(generation_modes) if generation_modes is not None else None
    requested_model_set = set(requested_models) if requested_models is not None else None
    requested_generation_mode_set = (
        set(requested_generation_modes) if requested_generation_modes is not None else None
    )
    selected: dict[tuple[str, str], Path] = {}

    for path in sorted(Path(log_dir).glob("*.eval"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            eval_log = _sync_read_eval_log(path)
        except Exception:
            continue

        model = str(eval_log.eval.model)
        generation_mode = generation_mode_for_eval_log(eval_log)
        if requested_model_set is not None and model not in requested_model_set:
            continue
        if requested_generation_mode_set is not None and generation_mode not in requested_generation_mode_set:
            continue
        if eval_log.status != "success":
            continue
        if len(eval_log.samples or []) < expected_samples:
            continue
        key = (model, generation_mode)
        if key in selected:
            continue
        selected[key] = path

        if requested_models is not None and requested_generation_modes is not None:
            requested_keys = {
                (requested_model, requested_generation_mode)
                for requested_model in requested_models
                for requested_generation_mode in requested_generation_modes
            }
            if set(selected) >= requested_keys:
                break

    if requested_models is not None and requested_generation_modes is not None:
        ordered_keys = [
            (requested_model, requested_generation_mode)
            for requested_model in requested_models
            for requested_generation_mode in requested_generation_modes
        ]
    elif requested_models is not None:
        ordered_keys = [
            key
            for requested_model in requested_models
            for key in sorted(selected)
            if key[0] == requested_model
        ]
    elif requested_generation_modes is not None:
        ordered_keys = [
            key
            for requested_generation_mode in requested_generation_modes
            for key in sorted(selected)
            if key[1] == requested_generation_mode
        ]
    else:
        ordered_keys = sorted(selected)

    return [selected[key] for key in ordered_keys if key in selected]


def _coerce_eval_logs(eval_logs_or_paths: Sequence[EvalLog | str | Path]) -> list[EvalLog]:
    if not eval_logs_or_paths:
        return []
    first = eval_logs_or_paths[0]
    if isinstance(first, EvalLog):
        return list(eval_logs_or_paths)  # type: ignore[arg-type]
    return load_eval_logs(eval_logs_or_paths)  # type: ignore[arg-type]


def samples_dataframe(eval_log: EvalLog) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sample in eval_log.samples or []:
        payload = sample.output.metadata or {}
        model = str(payload.get("model") or "")
        generation_mode = _generation_mode_from_payload(payload)
        row = {
            "id": sample.id,
            "model": model,
            "generation_mode": generation_mode,
            "run_label": run_label(model, generation_mode),
            "question": sample.input,
            "category": sample.metadata.get("category"),
            "expected_sql": sample.metadata.get("expected_sql"),
            "expected_result": sample.store.get("expected_result"),
            "generated_sql": payload.get("generated_sql"),
            "generated_result": sample.store.get("generated_result"),
            "unsupported": payload.get("unsupported"),
            "latency_seconds": payload.get("latency_seconds"),
            "estimated_cost_usd": (payload.get("usage") or {}).get("estimated_cost_usd"),
            "error": payload.get("error"),
        }
        for score_name, score in (sample.scores or {}).items():
            row[score_name] = score.value
        rows.append(row)
    return pd.DataFrame(rows)


def summary_dataframe(eval_log: EvalLog) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for score in eval_log.results.scores if eval_log.results else []:
        metric_map = {metric.name: metric.value for metric in score.metrics.values()}
        rows.append({"scorer": score.name, **metric_map})
    return pd.DataFrame(rows)


def comparison_samples_dataframe(
    eval_logs_or_paths: Sequence[EvalLog | str | Path],
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for item, eval_log in zip(eval_logs_or_paths, _coerce_eval_logs(eval_logs_or_paths), strict=False):
        frame = samples_dataframe(eval_log)
        frame.insert(0, "log_path", str(item if not isinstance(item, EvalLog) else ""))
        if not frame.empty and not frame["log_path"].iloc[0]:
            frame["log_path"] = str(getattr(eval_log, "location", "") or "")
        rows.append(frame)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def comparison_summary_dataframe(
    eval_logs_or_paths: Sequence[EvalLog | str | Path],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item, eval_log in zip(eval_logs_or_paths, _coerce_eval_logs(eval_logs_or_paths), strict=False):
        samples = samples_dataframe(eval_log)
        generation_mode = generation_mode_for_eval_log(eval_log)
        model = str(eval_log.eval.model)
        row: dict[str, Any] = {
            "model": model,
            "generation_mode": generation_mode,
            "run_label": run_label(model, generation_mode),
            "status": eval_log.status,
            "sample_count": len(samples),
            "error_count": int(samples["error"].notna().sum()),
            "log_path": str(item) if not isinstance(item, EvalLog) else "",
        }
        for metric_name in [
            "accuracy_pass",
            "sql_equivalence_pass",
            "hallucination_pass",
            "latency_budget_pass",
            "cost_budget_pass",
        ]:
            if metric_name in samples.columns:
                row[metric_name] = float((samples[metric_name] == "C").mean())
            else:
                row[metric_name] = 0.0
        row["latency_mean_seconds"] = float(samples["latency_seconds"].dropna().mean())
        row["latency_p95_seconds"] = float(samples["latency_seconds"].dropna().quantile(0.95))
        cost_series = samples["estimated_cost_usd"].dropna()
        row["cost_mean_usd"] = float(cost_series.mean()) if not cost_series.empty else None
        row["cost_p95_usd"] = float(cost_series.quantile(0.95)) if not cost_series.empty else None
        rows.append(row)

    return pd.DataFrame(rows).sort_values(["model", "generation_mode"]).reset_index(drop=True)


def category_pass_rate_dataframe(
    eval_logs_or_paths: Sequence[EvalLog | str | Path],
    metrics: Sequence[str] | None = None,
) -> pd.DataFrame:
    metric_names = list(
        metrics
        or [
            "accuracy_pass",
            "sql_equivalence_pass",
            "hallucination_pass",
            "latency_budget_pass",
            "cost_budget_pass",
        ]
    )
    samples = comparison_samples_dataframe(eval_logs_or_paths)
    if samples.empty:
        return pd.DataFrame(columns=["model", "generation_mode", "run_label", "category", "metric", "pass_rate"])

    rows: list[dict[str, Any]] = []
    for (model, generation_mode, category), frame in samples.groupby(
        ["model", "generation_mode", "category"],
        sort=True,
    ):
        for metric_name in metric_names:
            rows.append(
                {
                    "model": model,
                    "generation_mode": generation_mode,
                    "run_label": run_label(str(model), str(generation_mode)),
                    "category": category,
                    "metric": metric_name,
                    "pass_rate": float((frame[metric_name] == "C").mean()),
                }
            )
    return pd.DataFrame(rows)
