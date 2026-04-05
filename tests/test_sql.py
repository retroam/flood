"""Tests for SQL validation and normalization (quake_sql.sql)."""
from __future__ import annotations

import pytest

from quake_sql.sql import (
    SqlValidationError,
    ValidatedSql,
    is_unsupported,
    normalize_output,
    validate_sql,
)


# ---------------------------------------------------------------------------
# normalize_output
# ---------------------------------------------------------------------------

class TestNormalizeOutput:
    def test_strips_whitespace(self):
        assert normalize_output("  SELECT 1  ") == "SELECT 1"

    def test_strips_trailing_semicolon(self):
        assert normalize_output("SELECT 1;") == "SELECT 1"

    def test_strips_markdown_fences(self):
        raw = "```sql\nSELECT 1\n```"
        assert normalize_output(raw) == "SELECT 1"

    def test_strips_plain_backtick_fence(self):
        raw = "```\nSELECT 1\n```"
        assert normalize_output(raw) == "SELECT 1"

    def test_empty_string(self):
        assert normalize_output("") == ""

    def test_only_semicolons(self):
        assert normalize_output(";;;") == ""  # strips all after rstrip(";").strip()


# ---------------------------------------------------------------------------
# is_unsupported
# ---------------------------------------------------------------------------

class TestIsUnsupported:
    def test_literal_unsupported(self):
        assert is_unsupported("UNSUPPORTED") is True

    def test_case_insensitive(self):
        assert is_unsupported("unsupported") is True

    def test_with_whitespace(self):
        assert is_unsupported("  UNSUPPORTED  ") is True

    def test_wrapped_in_markdown(self):
        assert is_unsupported("```\nUNSUPPORTED\n```") is True

    def test_not_unsupported(self):
        assert is_unsupported("SELECT 1") is False


# ---------------------------------------------------------------------------
# validate_sql — happy paths
# ---------------------------------------------------------------------------

class TestValidateSqlValid:
    def test_simple_count(self):
        result = validate_sql("SELECT count(*) FROM earthquakes")
        assert isinstance(result, ValidatedSql)
        assert result.unsupported is False

    def test_unsupported_passthrough(self):
        result = validate_sql("UNSUPPORTED")
        assert result.unsupported is True
        assert result.sql == "UNSUPPORTED"

    def test_with_where_clause(self):
        sql = "SELECT count(*) FROM earthquakes WHERE magnitude > 3"
        result = validate_sql(sql)
        assert result.sql == sql

    def test_with_group_by(self):
        sql = "SELECT region, count(*) FROM earthquakes GROUP BY region"
        result = validate_sql(sql)
        assert result.sql == sql

    def test_with_alias(self):
        sql = "SELECT count(*) AS cnt FROM earthquakes"
        result = validate_sql(sql)
        assert result.sql == sql

    def test_limit_with_matching_question(self):
        sql = "SELECT event_time, place FROM earthquakes ORDER BY magnitude DESC LIMIT 10"
        result = validate_sql(sql, question="Show the top 10 earthquakes")
        assert result.sql == sql

    def test_all_valid_columns(self):
        sql = (
            "SELECT event_time, updated_at, magnitude, depth_km, latitude, longitude, "
            "station_count, azimuthal_gap, distance_to_station_deg, rms_residual, "
            "horizontal_error_km, depth_error_km, magnitude_error, magnitude_station_count, "
            "event_id, magnitude_type, event_type, status, source_net, place, region, "
            "location_source, magnitude_source FROM earthquakes"
        )
        result = validate_sql(sql, question="List all columns")
        assert result.unsupported is False


# ---------------------------------------------------------------------------
# validate_sql — rejection paths
# ---------------------------------------------------------------------------

class TestValidateSqlRejections:
    def test_empty_response(self):
        with pytest.raises(SqlValidationError, match="empty response"):
            validate_sql("")

    def test_multiple_statements(self):
        with pytest.raises(SqlValidationError, match="single SELECT"):
            validate_sql("SELECT 1; SELECT 2")

    def test_not_a_select(self):
        with pytest.raises(SqlValidationError, match="not a SELECT"):
            validate_sql("DROP TABLE earthquakes")

    def test_insert_rejected(self):
        with pytest.raises(SqlValidationError, match="not a SELECT"):
            validate_sql("INSERT INTO earthquakes VALUES ('x', now())")

    def test_wrong_table(self):
        with pytest.raises(SqlValidationError, match="Only the `earthquakes` table"):
            validate_sql("SELECT * FROM users")

    def test_unknown_columns(self):
        with pytest.raises(SqlValidationError, match="unknown columns"):
            validate_sql("SELECT phone_number FROM earthquakes")

    def test_offset_disallowed(self):
        with pytest.raises(SqlValidationError, match="OFFSET"):
            validate_sql(
                "SELECT event_time FROM earthquakes LIMIT 10 OFFSET 5",
                question="Show top 10",
            )

    def test_limit_exceeds_max(self):
        with pytest.raises(SqlValidationError, match="maximum allowed LIMIT"):
            validate_sql(
                "SELECT event_time FROM earthquakes LIMIT 9999",
                question="Show top 9999 earthquakes",
            )

    def test_limit_without_user_request(self):
        with pytest.raises(SqlValidationError, match="Do not add LIMIT"):
            validate_sql(
                "SELECT count(*) FROM earthquakes LIMIT 10",
                question="How many earthquakes happened?",
            )

    def test_event_type_filter_without_request(self):
        with pytest.raises(SqlValidationError, match="event_type"):
            validate_sql(
                "SELECT count(*) FROM earthquakes WHERE event_type = 'earthquake'",
                question="How many earthquakes?",
            )

    def test_event_type_filter_allowed_when_asked(self):
        sql = "SELECT count(*) FROM earthquakes WHERE event_type = 'earthquake'"
        result = validate_sql(sql, question="How many have event type earthquake?")
        assert result.unsupported is False

    def test_sql_injection_union(self):
        with pytest.raises(SqlValidationError):
            validate_sql("SELECT * FROM earthquakes UNION SELECT * FROM system.users")

    def test_sql_injection_subquery(self):
        with pytest.raises(SqlValidationError):
            validate_sql(
                "SELECT * FROM earthquakes WHERE event_id IN (SELECT name FROM system.tables)"
            )

    def test_unparseable_sql(self):
        with pytest.raises(SqlValidationError, match="could not be parsed"):
            validate_sql("NOT VALID SQL AT ALL !@#$")


# ---------------------------------------------------------------------------
# _question_requests_limit edge cases
# ---------------------------------------------------------------------------

class TestQuestionRequestsLimit:
    """Exercises the internal _question_requests_limit heuristic via validate_sql."""

    def test_top_n_triggers_limit(self):
        sql = "SELECT region FROM earthquakes ORDER BY magnitude DESC LIMIT 5"
        result = validate_sql(sql, question="Show the top 5 regions")
        assert result.unsupported is False

    def test_list_n_triggers_limit(self):
        sql = "SELECT region FROM earthquakes LIMIT 3"
        result = validate_sql(sql, question="List 3 earthquakes")
        assert result.unsupported is False

    def test_most_recent_triggers_limit(self):
        sql = "SELECT event_time FROM earthquakes ORDER BY event_time DESC LIMIT 1"
        result = validate_sql(sql, question="Show the most recent earthquake")
        assert result.unsupported is False

    def test_strongest_triggers_limit(self):
        sql = "SELECT magnitude FROM earthquakes ORDER BY magnitude DESC LIMIT 1"
        result = validate_sql(sql, question="What was the strongest earthquake?")
        assert result.unsupported is False

    def test_plain_question_rejects_limit(self):
        with pytest.raises(SqlValidationError, match="Do not add LIMIT"):
            validate_sql(
                "SELECT count(*) FROM earthquakes LIMIT 100",
                question="How many earthquakes were there?",
            )
