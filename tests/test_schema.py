"""Tests for schema definitions and helpers (quake_sql.schema)."""
from __future__ import annotations

from quake_sql.schema import (
    ALL_COLUMNS,
    COLUMN_DOCS,
    GROUPABLE_COLUMNS,
    NUMERIC_COLUMNS,
    STRING_COLUMNS,
    TABLE_NAME,
    TIME_COLUMNS,
    schema_html,
    schema_markdown,
    schema_prompt,
)


class TestSchemaConsistency:
    """Verify schema tuples are internally consistent."""

    def test_all_columns_is_union(self):
        assert set(ALL_COLUMNS) == set(TIME_COLUMNS) | set(NUMERIC_COLUMNS) | set(STRING_COLUMNS)

    def test_no_duplicate_columns(self):
        assert len(ALL_COLUMNS) == len(set(ALL_COLUMNS))

    def test_groupable_columns_are_subset_of_string(self):
        assert set(GROUPABLE_COLUMNS).issubset(set(STRING_COLUMNS))

    def test_column_docs_covers_all_columns(self):
        documented = {col.name for col in COLUMN_DOCS}
        assert documented == set(ALL_COLUMNS)

    def test_table_name(self):
        assert TABLE_NAME == "earthquakes"


class TestSchemaRenderers:
    def test_markdown_has_header(self):
        md = schema_markdown()
        assert "| Column | Type | Description |" in md

    def test_markdown_has_all_columns(self):
        md = schema_markdown()
        for col in ALL_COLUMNS:
            assert f"`{col}`" in md

    def test_html_has_table_tag(self):
        html = schema_html()
        assert "<table" in html
        assert "</table>" in html

    def test_html_has_all_columns(self):
        html = schema_html()
        for col in ALL_COLUMNS:
            assert col in html

    def test_prompt_mentions_table_name(self):
        prompt = schema_prompt()
        assert TABLE_NAME in prompt

    def test_prompt_mentions_all_columns(self):
        prompt = schema_prompt()
        for col in ALL_COLUMNS:
            assert col in prompt

    def test_prompt_contains_sql_rules(self):
        prompt = schema_prompt()
        assert "SQL rules:" in prompt
        assert "UNSUPPORTED" in prompt
