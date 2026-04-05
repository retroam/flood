from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any

from inspect_ai.model import get_model_info
from openai import APIError, BadRequestError, OpenAI

from quake_sql.config import Settings
from quake_sql.grammar import GRAMMAR, SYSTEM_PROMPT
from quake_sql.schema import schema_prompt
from quake_sql.sql import SqlValidationError, validate_sql


class SqlGenerationError(RuntimeError):
    pass


@dataclass
class UsageBreakdown:
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_tokens: int
    total_tokens: int
    estimated_cost_usd: float


@dataclass
class SqlGenerationResult:
    model: str
    sql: str
    latency_seconds: float
    usage: UsageBreakdown | None
    unsupported: bool = False


def _fallback_cost(settings: Settings, usage: Any) -> float:
    cached_tokens = getattr(usage.input_tokens_details, "cached_tokens", 0)
    uncached_input_tokens = max(usage.input_tokens - cached_tokens, 0)
    return (
        uncached_input_tokens * settings.openai_input_cost_per_1m
        + cached_tokens * settings.openai_cached_input_cost_per_1m
        + usage.output_tokens * settings.openai_output_cost_per_1m
    ) / 1_000_000


def _estimate_cost(settings: Settings, model: str, usage: Any) -> float:
    cached_tokens = getattr(usage.input_tokens_details, "cached_tokens", 0)
    uncached_input_tokens = max(usage.input_tokens - cached_tokens, 0)

    model_info = get_model_info(f"openai/{model}")
    if model_info is not None and model_info.cost is not None:
        return (
            uncached_input_tokens * model_info.cost.input
            + cached_tokens * model_info.cost.input_cache_read
            + usage.output_tokens * model_info.cost.output
        ) / 1_000_000

    return _fallback_cost(settings, usage)


def _build_usage(
    settings: Settings,
    model: str,
    usage: Any | None,
) -> UsageBreakdown | None:
    if usage is None:
        return None
    return UsageBreakdown(
        input_tokens=usage.input_tokens,
        cached_input_tokens=getattr(usage.input_tokens_details, "cached_tokens", 0),
        output_tokens=usage.output_tokens,
        reasoning_tokens=getattr(usage.output_tokens_details, "reasoning_tokens", 0),
        total_tokens=usage.total_tokens,
        estimated_cost_usd=_estimate_cost(settings, model, usage),
    )


class OpenAISqlGenerator:
    def __init__(self, settings: Settings):
        if not settings.openai_api_key:
            raise SqlGenerationError(
                "OPENAI_API_KEY is not configured. Set it in the environment before running the app or evals."
            )
        self.settings = settings
        client_kwargs: dict[str, Any] = {
            "api_key": settings.openai_api_key,
            "timeout": settings.openai_timeout_seconds,
        }
        if settings.openai_base_url:
            client_kwargs["base_url"] = settings.openai_base_url
        self.client = OpenAI(**client_kwargs)
        self.instructions = f"{SYSTEM_PROMPT}\n\n{schema_prompt()}"

    def _retry_prompt(self, question: str, previous_sql: str, error: str) -> str:
        return (
            f"Original question:\n{question}\n\n"
            f"Previous invalid SQL:\n{previous_sql}\n\n"
            f"Validation error:\n{error}\n\n"
            "Return corrected ClickHouse SQL only.\n"
            "Use the simplest valid query.\n"
            "Never use OFFSET or LIMIT-with-offset syntax.\n"
            "Do not add `event_type = 'earthquake'` unless the user explicitly asks for an event type filter.\n"
            "Do not add LIMIT unless the user explicitly requests a bounded list or row lookup.\n"
            "If the request truly cannot be answered from this dataset, return UNSUPPORTED."
        )

    def _request_once(
        self, prompt: str, model: str, use_cfg: bool, temperature: float | None = None,
    ) -> tuple[Any, float]:
        started_at = time.perf_counter()
        request_kwargs: dict[str, Any] = {
            "model": model,
            "instructions": self.instructions,
            "input": prompt,
            "reasoning": {"effort": self.settings.openai_reasoning_effort},
        }
        if temperature is not None and "reasoning" not in request_kwargs:
            request_kwargs["temperature"] = temperature
        if use_cfg:
            request_kwargs.update(
                {
                    "tools": [
                        {
                            "type": "custom",
                            "name": "generate_clickhouse_sql",
                            "description": "Generate one ClickHouse SQL query for the earthquakes table, or UNSUPPORTED.",
                            "format": {
                                "type": "grammar",
                                "syntax": "lark",
                                "definition": GRAMMAR,
                            },
                        }
                    ],
                    "tool_choice": {"type": "custom", "name": "generate_clickhouse_sql"},
                    "parallel_tool_calls": False,
                }
            )
        response = self.client.responses.create(**request_kwargs)
        return response, time.perf_counter() - started_at

    def _merge_usage(
        self,
        current: UsageBreakdown | None,
        new: UsageBreakdown | None,
    ) -> UsageBreakdown | None:
        if current is None:
            return new
        if new is None:
            return current
        return UsageBreakdown(
            input_tokens=current.input_tokens + new.input_tokens,
            cached_input_tokens=current.cached_input_tokens + new.cached_input_tokens,
            output_tokens=current.output_tokens + new.output_tokens,
            reasoning_tokens=current.reasoning_tokens + new.reasoning_tokens,
            total_tokens=current.total_tokens + new.total_tokens,
            estimated_cost_usd=current.estimated_cost_usd + new.estimated_cost_usd,
        )

    def _resolve_model_name(self, model: str | None) -> str:
        if not model or model == "none/none":
            return self.settings.openai_model
        if "/" not in model:
            return model
        provider, model_name = model.split("/", 1)
        if provider != "openai":
            raise SqlGenerationError(
                f"Unsupported eval model provider '{provider}'. Use an OpenAI model like 'openai/gpt-5-mini'."
            )
        return model_name

    def _extract_sql(self, response: Any) -> str:
        tool_sql: str | None = None
        message_sql: str | None = None

        for item in getattr(response, "output", []):
            if getattr(item, "type", None) == "custom_tool_call" and tool_sql is None:
                tool_sql = item.input
            elif getattr(item, "type", None) == "message" and message_sql is None:
                for content in getattr(item, "content", []):
                    text = getattr(content, "text", "")
                    if text:
                        message_sql = text
                        break

        if tool_sql:
            return tool_sql
        if message_sql:
            return message_sql
        if getattr(response, "output_text", "").strip():
            return response.output_text
        raise SqlGenerationError(
            "The model response did not contain SQL output."
        )

    _MAX_ATTEMPTS = 3
    _BACKOFF_SECONDS = (0.0, 0.5, 1.5)
    _RETRY_TEMPERATURES = (None, 0.3, 0.6)

    def _request(
        self,
        question: str,
        model: str | None = None,
        use_cfg: bool = True,
    ) -> SqlGenerationResult:
        resolved_model = self._resolve_model_name(model)
        prompt = question
        total_latency_seconds = 0.0
        usage_totals: UsageBreakdown | None = None
        last_error: SqlValidationError | None = None

        for attempt in range(self._MAX_ATTEMPTS):
            backoff = self._BACKOFF_SECONDS[attempt] if attempt < len(self._BACKOFF_SECONDS) else 1.5
            if backoff > 0:
                time.sleep(backoff)

            temperature = self._RETRY_TEMPERATURES[attempt] if attempt < len(self._RETRY_TEMPERATURES) else 0.6
            response, attempt_latency_seconds = self._request_once(
                prompt,
                resolved_model,
                use_cfg=use_cfg,
                temperature=temperature,
            )
            total_latency_seconds += attempt_latency_seconds
            usage_totals = self._merge_usage(
                usage_totals,
                _build_usage(self.settings, resolved_model, response.usage),
            )
            raw_sql = self._extract_sql(response)
            try:
                validated = validate_sql(
                    raw_sql,
                    max_limit=self.settings.max_generated_limit,
                    question=question,
                )
                return SqlGenerationResult(
                    model=f"openai/{resolved_model}",
                    sql=validated.sql,
                    unsupported=validated.unsupported,
                    latency_seconds=total_latency_seconds,
                    usage=usage_totals,
                )
            except SqlValidationError as exc:
                last_error = exc
                if attempt == self._MAX_ATTEMPTS - 1:
                    raise
                prompt = self._retry_prompt(question, raw_sql, str(exc))

        if last_error is not None:
            raise last_error

        return SqlGenerationResult(
            model=f"openai/{resolved_model}",
            sql="UNSUPPORTED",
            unsupported=True,
            latency_seconds=total_latency_seconds,
            usage=usage_totals,
        )

    def generate(
        self,
        question: str,
        model: str | None = None,
        use_cfg: bool = True,
    ) -> SqlGenerationResult:
        try:
            return self._request(question, model=model, use_cfg=use_cfg)
        except (BadRequestError, APIError) as exc:
            raise SqlGenerationError(f"SQL generation failed: {exc}") from exc
