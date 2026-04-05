"""Tests for the Lark CFG grammar (quake_sql.grammar).

These tests parse SQL strings against the grammar to verify that the grammar
accepts valid ClickHouse queries and rejects invalid ones — independent of
the LLM.  This is exactly what the review flagged as missing: validation that
the grammar itself does what it claims.
"""
from __future__ import annotations

import pytest
from lark import Lark, exceptions as lark_exc

from quake_sql.grammar import GRAMMAR


@pytest.fixture(scope="module")
def parser() -> Lark:
    return Lark(GRAMMAR, start="start", parser="earley", ambiguity="resolve")


# ---------------------------------------------------------------------------
# Valid SQL that the grammar MUST accept
# ---------------------------------------------------------------------------

class TestGrammarAccepts:
    def test_simple_count(self, parser):
        parser.parse("SELECT count(*) FROM earthquakes")

    def test_unsupported_literal(self, parser):
        parser.parse("UNSUPPORTED")

    def test_select_columns(self, parser):
        parser.parse("SELECT event_time, magnitude, place FROM earthquakes")

    def test_where_numeric(self, parser):
        parser.parse("SELECT count(*) FROM earthquakes WHERE magnitude > 3")

    def test_where_time(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes WHERE event_time >= now() - INTERVAL 24 HOUR"
        )

    def test_where_string_eq(self, parser):
        parser.parse("SELECT count(*) FROM earthquakes WHERE status = 'reviewed'")

    def test_where_ilike(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes WHERE region ILIKE '%Alaska%'"
        )

    def test_where_is_null(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes WHERE magnitude IS NULL"
        )

    def test_where_is_not_null(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes WHERE station_count IS NOT NULL"
        )

    def test_group_by(self, parser):
        parser.parse(
            "SELECT region, count(*) FROM earthquakes GROUP BY region"
        )

    def test_order_by_asc(self, parser):
        parser.parse(
            "SELECT event_time, magnitude FROM earthquakes ORDER BY magnitude ASC"
        )

    def test_order_by_desc(self, parser):
        parser.parse(
            "SELECT event_time, magnitude FROM earthquakes ORDER BY magnitude DESC"
        )

    def test_limit(self, parser):
        parser.parse(
            "SELECT event_time FROM earthquakes LIMIT 10"
        )

    def test_limit_at_500(self, parser):
        parser.parse(
            "SELECT event_time FROM earthquakes LIMIT 500"
        )

    def test_aggregate_avg(self, parser):
        parser.parse("SELECT avg(magnitude) FROM earthquakes")

    def test_aggregate_sum(self, parser):
        parser.parse("SELECT sum(depth_km) FROM earthquakes")

    def test_aggregate_min_max(self, parser):
        parser.parse("SELECT min(magnitude), max(magnitude) FROM earthquakes")

    def test_time_bucket_todate(self, parser):
        parser.parse(
            "SELECT toDate(event_time), count(*) FROM earthquakes GROUP BY toDate(event_time)"
        )

    def test_time_bucket_start_of_hour(self, parser):
        parser.parse(
            "SELECT toStartOfHour(event_time) AS hour_bucket, count(*) FROM earthquakes "
            "GROUP BY toStartOfHour(event_time)"
        )

    def test_alias(self, parser):
        parser.parse("SELECT count(*) AS cnt FROM earthquakes")

    def test_boolean_and(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes WHERE magnitude > 3 AND depth_km < 100"
        )

    def test_boolean_or_with_parens(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes "
            "WHERE (region ILIKE '%Alaska%' OR region ILIKE '%California%') AND magnitude > 3"
        )

    def test_relative_time_today_minus(self, parser):
        parser.parse(
            "SELECT count(*) FROM earthquakes WHERE event_time >= today() - 7"
        )

    def test_all_numeric_columns(self, parser):
        parser.parse(
            "SELECT magnitude, depth_km, latitude, longitude, station_count, "
            "azimuthal_gap, distance_to_station_deg, rms_residual, "
            "horizontal_error_km, depth_error_km, magnitude_error, magnitude_station_count "
            "FROM earthquakes"
        )

    def test_all_string_columns(self, parser):
        parser.parse(
            "SELECT event_id, magnitude_type, event_type, status, source_net, "
            "place, region, location_source, magnitude_source FROM earthquakes"
        )

    def test_full_query(self, parser):
        parser.parse(
            "SELECT region, count(*) AS cnt, avg(magnitude) AS avg_mag "
            "FROM earthquakes "
            "WHERE event_time >= now() - INTERVAL 30 DAY AND magnitude > 2 "
            "GROUP BY region "
            "ORDER BY cnt DESC "
            "LIMIT 10"
        )


# ---------------------------------------------------------------------------
# Invalid SQL that the grammar MUST reject
# ---------------------------------------------------------------------------

class TestGrammarRejects:
    def test_drop_table(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("DROP TABLE earthquakes")

    def test_insert(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("INSERT INTO earthquakes VALUES ('x')")

    def test_delete(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("DELETE FROM earthquakes WHERE 1=1")

    def test_update(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("UPDATE earthquakes SET magnitude = 10")

    def test_wrong_table(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT * FROM users")

    def test_union(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT 1 FROM earthquakes UNION SELECT 2 FROM earthquakes")

    def test_subquery(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse(
                "SELECT * FROM earthquakes WHERE event_id IN (SELECT name FROM system.tables)"
            )

    def test_limit_above_500(self, parser):
        """Grammar-level LIMIT cap at 500: 501+ should not parse."""
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT event_time FROM earthquakes LIMIT 501")

    def test_limit_above_999(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT event_time FROM earthquakes LIMIT 1000")

    def test_offset_syntax(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT event_time FROM earthquakes LIMIT 10 OFFSET 5")

    def test_show_tables(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SHOW TABLES")

    def test_create_table(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("CREATE TABLE evil (id Int32) ENGINE = MergeTree()")

    def test_semicolon_injection(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT 1 FROM earthquakes; DROP TABLE earthquakes")

    def test_unknown_column_in_select(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT phone_number FROM earthquakes")

    def test_star_select(self, parser):
        """SELECT * is not in the grammar — only named columns and aggregates."""
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT * FROM earthquakes")

    def test_unknown_function(self, parser):
        with pytest.raises(lark_exc.UnexpectedInput):
            parser.parse("SELECT sleep(10) FROM earthquakes")
