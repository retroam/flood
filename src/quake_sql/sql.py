from __future__ import annotations

from dataclasses import dataclass
import re

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

from quake_sql.schema import ALL_COLUMNS, TABLE_NAME


ALLOWED_COLUMNS = set(ALL_COLUMNS)


class SqlValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedSql:
    sql: str
    unsupported: bool = False


def normalize_output(raw_text: str) -> str:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`").strip()
        if "\n" in text:
            text = text.split("\n", maxsplit=1)[1].strip()
    return text.rstrip(";").strip()


def is_unsupported(raw_text: str) -> bool:
    return normalize_output(raw_text).upper() == "UNSUPPORTED"


def _question_requests_limit(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    if re.search(r"\b(top|list|show|give me)\s+\d+\b", lowered):
        return True
    return any(
        phrase in lowered
        for phrase in (
            "top ",
            "list ",
            "most recent",
            "latest",
            "strongest",
            "deepest",
            "highest",
            "lowest",
            "first ",
        )
    )


def _question_allows_default_event_type_filter(question: str | None) -> bool:
    if not question:
        return False
    lowered = question.lower()
    return "event type" in lowered or "event types" in lowered or "type of event" in lowered


def _sql_filters_default_earthquake_event_type(sql: str) -> bool:
    return bool(
        re.search(
            r"\bevent_type\b\s*(=|LIKE|ILIKE)\s*'earthquake'",
            sql,
            flags=re.IGNORECASE,
        )
    )


def validate_sql(
    raw_text: str,
    max_limit: int = 500,
    question: str | None = None,
) -> ValidatedSql:
    normalized = normalize_output(raw_text)
    if normalized.upper() == "UNSUPPORTED":
        return ValidatedSql(sql="UNSUPPORTED", unsupported=True)

    if not normalized:
        raise SqlValidationError("The model returned an empty response.")
    if ";" in normalized:
        raise SqlValidationError("Only a single SELECT statement is allowed.")

    try:
        parsed = parse_one(normalized, read="clickhouse")
    except ParseError as exc:
        raise SqlValidationError(f"Generated SQL could not be parsed: {exc}") from exc

    if not isinstance(parsed, exp.Select):
        raise SqlValidationError("The generated statement is not a SELECT query.")

    if parsed.args.get("offset") is not None:
        raise SqlValidationError(
            "OFFSET or LIMIT-with-offset syntax is not allowed. Use plain LIMIT <n> only."
        )

    tables = {table.name for table in parsed.find_all(exp.Table)}
    if tables != {TABLE_NAME}:
        raise SqlValidationError(
            f"Only the `{TABLE_NAME}` table is allowed. Found: {sorted(tables)}"
        )

    aliases = {
        expression.alias
        for expression in parsed.expressions
        if isinstance(expression, exp.Expression) and expression.alias
    }
    columns = {column.name for column in parsed.find_all(exp.Column)}
    disallowed = sorted(columns - ALLOWED_COLUMNS - aliases)
    if disallowed:
        raise SqlValidationError(
            f"Generated SQL references unknown columns: {', '.join(disallowed)}"
        )

    limit = parsed.args.get("limit")
    if limit is not None and isinstance(limit.expression, exp.Literal):
        limit_value = int(limit.expression.name)
        if limit_value > max_limit:
            raise SqlValidationError(
                f"Generated SQL exceeds the maximum allowed LIMIT of {max_limit}."
            )
        if not _question_requests_limit(question):
            raise SqlValidationError(
                "Do not add LIMIT unless the user explicitly requested a bounded list or row lookup."
            )

    if _sql_filters_default_earthquake_event_type(normalized) and not _question_allows_default_event_type_filter(question):
        raise SqlValidationError(
            "Do not add `event_type = 'earthquake'` unless the user explicitly asks for an event type filter."
        )

    return ValidatedSql(sql=normalized, unsupported=False)
