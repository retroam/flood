from __future__ import annotations

from functools import lru_cache
import math
import os
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.requests import Request

from quake_sql.config import Settings, get_settings
from quake_sql.evals import (
    GENERATION_MODE_CFG,
    GENERATION_MODE_NO_CFG,
    benchmark_sample_count,
    comparison_samples_dataframe,
    comparison_summary_dataframe,
    latest_successful_log_paths,
    run_eval_suite,
)
from quake_sql.schema import schema_html, schema_markdown
from quake_sql.service import AppResponse, QueryExecutionError, QueryService


BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Quake SQL")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class QueryRequest(BaseModel):
    question: str
    use_cfg: bool = True


_DEFAULT_EVAL_SAMPLE_COUNT = 3
_MAX_EVAL_SAMPLE_COUNT = benchmark_sample_count()


class EvalRunRequest(BaseModel):
    sample_count: int = Field(
        default=_DEFAULT_EVAL_SAMPLE_COUNT,
        ge=1,
        le=_MAX_EVAL_SAMPLE_COUNT,
    )


def _example_questions() -> list[str]:
    return [
        "How many earthquakes happened in the last 24 hours?",
        "Show the top 10 regions by average magnitude in the last 7 days.",
        "List the 5 strongest earthquakes from the last 72 hours.",
        "How many earthquakes with a recorded magnitude error happened yesterday?",
        "What is the daily earthquake count for the last 14 days?",
        "Show earthquakes above magnitude 6 in Alaska over the last month.",
    ]


@lru_cache(maxsize=1)
def get_query_service() -> QueryService:
    return QueryService(get_settings())


@app.get("/", response_class=HTMLResponse)
def index(request: Request, settings: Settings = Depends(get_settings)) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "app_name": settings.app_name,
            "examples": _example_questions(),
            "schema_markdown": schema_markdown(),
            "schema_html": schema_html(),
            "eval_model": _eval_model(settings),
        },
    )


@app.on_event("startup")
def _warm_clickhouse() -> None:
    """Send a lightweight query to wake ClickHouse Cloud from idle."""
    try:
        service = get_query_service()
        service.client.command("SELECT 1")
    except Exception:
        pass


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _resolve_project_root() -> Path:
    env = os.environ.get("PROJECT_ROOT")
    if env:
        return Path(env)
    candidate = Path(__file__).resolve().parents[2]
    if "site-packages" in str(candidate) or not (candidate / "logs").exists():
        return Path.cwd()
    return candidate


_EVAL_LOG_DIR = str(_resolve_project_root() / "logs" / "inspect")
_EVAL_GENERATION_MODES = [GENERATION_MODE_CFG, GENERATION_MODE_NO_CFG]


def _eval_model(settings: Settings) -> str:
    return f"openai/{settings.openai_model}"


def _empty_eval_data(
    settings: Settings,
    run_errors: list[dict[str, str]] | None = None,
    selected_sample_count: int | None = None,
) -> dict[str, Any]:
    active_model = _eval_model(settings)
    return {
        "active_model": active_model,
        "expected_modes": list(_EVAL_GENERATION_MODES),
        "available_modes": [],
        "complete": False,
        "default_sample_count": _DEFAULT_EVAL_SAMPLE_COUNT,
        "max_sample_count": _MAX_EVAL_SAMPLE_COUNT,
        "selected_sample_count": selected_sample_count,
        "models": [],
        "summary": [],
        "samples": [],
        "run_errors": run_errors or [],
    }


def _safe_optional_float(value: Any, digits: int | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, digits) if digits is not None else number


def _safe_float(value: Any, digits: int, default: float = 0.0) -> float:
    return _safe_optional_float(value, digits=digits) or default


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    text = str(value)
    return text if text else None


def _build_eval_data(
    settings: Settings,
    summary_df: Any,
    samples_df: Any,
    run_errors: list[dict[str, str]] | None = None,
    selected_sample_count: int | None = None,
) -> dict[str, Any]:
    active_model = _eval_model(settings)
    if summary_df.empty:
        return _empty_eval_data(
            settings,
            run_errors=run_errors,
            selected_sample_count=selected_sample_count,
        )

    summary_df = summary_df[summary_df["model"] == active_model].reset_index(drop=True)
    samples_df = samples_df[samples_df["model"] == active_model].reset_index(drop=True)
    if summary_df.empty:
        return _empty_eval_data(
            settings,
            run_errors=run_errors,
            selected_sample_count=selected_sample_count,
        )

    mode_order = {mode: index for index, mode in enumerate(_EVAL_GENERATION_MODES)}
    summary_df["mode_order"] = summary_df["generation_mode"].map(mode_order).fillna(len(mode_order))
    samples_df["mode_order"] = samples_df["generation_mode"].map(mode_order).fillna(len(mode_order))
    summary_df = summary_df.sort_values(["mode_order", "generation_mode"]).reset_index(drop=True)
    samples_df = samples_df.sort_values(["mode_order", "id"]).reset_index(drop=True)

    models = summary_df["run_label"].tolist()
    available_modes = summary_df["generation_mode"].tolist()
    if selected_sample_count is None and not summary_df.empty:
        selected_sample_count = int(summary_df["sample_count"].min())

    summary_records = []
    for _, row in summary_df.iterrows():
        summary_records.append({
            "model": row["model"],
            "generation_mode": row["generation_mode"],
            "label": row["run_label"],
            "accuracy": _safe_float(float(row["accuracy_pass"]) * 100, 1),
            "sql_equivalence": _safe_float(float(row.get("sql_equivalence_pass", 0)) * 100, 1),
            "hallucination": _safe_float(float(row["hallucination_pass"]) * 100, 1),
            "latency_budget": _safe_float(float(row["latency_budget_pass"]) * 100, 1),
            "cost_budget": _safe_float(float(row["cost_budget_pass"]) * 100, 1),
            "latency_mean": _safe_float(row["latency_mean_seconds"], 2),
            "latency_p95": _safe_float(row["latency_p95_seconds"], 2),
            "cost_mean": _safe_float(row.get("cost_mean_usd"), 6),
            "cost_p95": _safe_float(row.get("cost_p95_usd"), 6),
            "sample_count": int(row["sample_count"]),
            "error_count": int(row["error_count"]),
        })

    sample_records = []
    for _, row in samples_df.iterrows():
        sample_records.append({
            "model": row["model"],
            "generation_mode": row["generation_mode"],
            "label": row["run_label"],
            "id": row["id"],
            "question": row["question"],
            "category": row["category"],
            "expected_sql": row["expected_sql"],
            "generated_sql": row["generated_sql"],
            "accuracy": row.get("accuracy_pass") == "C",
            "sql_equivalence": row.get("sql_equivalence_pass") == "C",
            "hallucination": row.get("hallucination_pass") == "C",
            "latency": _safe_float(row.get("latency_seconds"), 2),
            "cost": _safe_float(row.get("estimated_cost_usd"), 6),
            "error": _safe_text(row.get("error")),
        })

    return {
        "active_model": active_model,
        "expected_modes": list(_EVAL_GENERATION_MODES),
        "available_modes": available_modes,
        "complete": set(available_modes) == set(_EVAL_GENERATION_MODES),
        "default_sample_count": _DEFAULT_EVAL_SAMPLE_COUNT,
        "max_sample_count": _MAX_EVAL_SAMPLE_COUNT,
        "selected_sample_count": selected_sample_count,
        "models": models,
        "summary": summary_records,
        "samples": sample_records,
        "run_errors": run_errors or [],
    }


def _eval_data(
    settings: Settings,
    run_errors: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    active_model = _eval_model(settings)
    paths = latest_successful_log_paths(
        log_dir=_EVAL_LOG_DIR,
        models=[active_model],
        generation_modes=_EVAL_GENERATION_MODES,
        min_samples=1,
    )
    if not paths:
        return _empty_eval_data(settings, run_errors=run_errors)

    summary_df = comparison_summary_dataframe(paths)
    samples_df = comparison_samples_dataframe(paths)
    return _build_eval_data(settings, summary_df, samples_df, run_errors=run_errors)


@app.get("/api/evals")
def evals(settings: Settings = Depends(get_settings)) -> JSONResponse:
    try:
        data = _eval_data(settings)
    except Exception as exc:
        data = _empty_eval_data(
            settings,
            run_errors=[{"generation_mode": "load", "error": str(exc)}],
        )
    return JSONResponse(data)


@app.post("/api/evals/run")
def run_evals(
    payload: EvalRunRequest,
    settings: Settings = Depends(get_settings),
) -> JSONResponse:
    active_model = _eval_model(settings)
    run_errors: list[dict[str, str]] = []
    eval_logs = []

    for generation_mode in _EVAL_GENERATION_MODES:
        try:
            eval_logs.append(
                run_eval_suite(
                log_dir=_EVAL_LOG_DIR,
                model=active_model,
                limit=payload.sample_count,
                use_cfg=generation_mode == GENERATION_MODE_CFG,
            )
            )
        except Exception as exc:
            run_errors.append({
                "generation_mode": generation_mode,
                "error": str(exc),
            })

    if not eval_logs:
        return JSONResponse(
            _empty_eval_data(
                settings,
                run_errors=run_errors,
                selected_sample_count=payload.sample_count,
            )
        )

    return JSONResponse(
        _build_eval_data(
            settings,
            comparison_summary_dataframe(eval_logs),
            comparison_samples_dataframe(eval_logs),
            run_errors=run_errors,
            selected_sample_count=payload.sample_count,
        )
    )


@app.get("/api/evals/debug")
def evals_debug() -> JSONResponse:
    from quake_sql.evals import latest_successful_log_paths as _lsp
    try:
        log_dir = _EVAL_LOG_DIR
        log_path = Path(log_dir)
        exists = log_path.exists()
        glob_count = len(list(log_path.glob("*.eval"))) if exists else 0
        paths = _lsp(log_dir=log_dir)
        return JSONResponse({
            "log_dir": log_dir,
            "exists": exists,
            "glob_count": glob_count,
            "path_count": len(paths),
            "paths": [str(p) for p in paths],
        })
    except Exception as exc:
        return JSONResponse({"error": str(exc), "type": type(exc).__name__}, status_code=500)


@app.post("/api/query")
def query(
    payload: QueryRequest,
    _: Settings = Depends(get_settings),
) -> AppResponse:
    try:
        service = get_query_service()
        return service.run(payload.question, use_cfg=payload.use_cfg)
    except QueryExecutionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Query service is temporarily unavailable.",
        ) from exc
