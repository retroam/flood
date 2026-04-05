from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import clickhouse_connect.driver.exceptions

from quake_sql.clickhouse import QueryResult, execute_query, get_client
from quake_sql.config import Settings
from quake_sql.openai_sql import OpenAISqlGenerator, SqlGenerationError
from quake_sql.sql import SqlValidationError


class QueryExecutionError(RuntimeError):
    pass


@dataclass
class AppResponse:
    question: str
    sql: str
    generation_mode: str
    unsupported: bool
    result: QueryResult | None
    latency_seconds: float
    usage: dict[str, Any] | None
    error: str | None = None


class QueryService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.generator = OpenAISqlGenerator(settings)
        self.client = get_client(settings)

    def run(self, question: str, use_cfg: bool = True) -> AppResponse:
        text = question.strip()
        if not text:
            raise QueryExecutionError("Enter a question before submitting.")
        if len(text) > 1000:
            raise QueryExecutionError("Question is too long. Please keep it under 1000 characters.")

        try:
            generation = self.generator.generate(text, use_cfg=use_cfg)
        except (SqlGenerationError, SqlValidationError) as exc:
            mode_label = "CFG" if use_cfg else "No CFG"
            raise QueryExecutionError(
                f"SQL generation failed in {mode_label} mode: {exc}"
            ) from exc
        usage = asdict(generation.usage) if generation.usage else None
        generation_mode = "cfg" if use_cfg else "no_cfg"

        if generation.unsupported:
            return AppResponse(
                question=text,
                sql=generation.sql,
                generation_mode=generation_mode,
                unsupported=True,
                result=None,
                latency_seconds=generation.latency_seconds,
                usage=usage,
                error="The request is outside the supported earthquake schema, so the model returned `UNSUPPORTED`.",
            )

        try:
            result = execute_query(self.client, generation.sql, max_rows=self.settings.max_result_rows)
        except (clickhouse_connect.driver.exceptions.DatabaseError, OSError) as exc:
            raise QueryExecutionError(
                f"ClickHouse could not execute the generated SQL: {exc}"
            ) from exc

        return AppResponse(
            question=text,
            sql=generation.sql,
            generation_mode=generation_mode,
            unsupported=False,
            result=result,
            latency_seconds=generation.latency_seconds,
            usage=usage,
        )
