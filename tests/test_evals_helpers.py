"""Tests for eval helper functions (sql equivalence, result projection)."""
from __future__ import annotations

import pytest

from quake_sql.evals import _normalize_sql_structure, _project_results


class TestNormalizeSqlStructure:
    def test_same_query_matches(self):
        sql = "SELECT count(*) FROM earthquakes WHERE magnitude > 3"
        a = _normalize_sql_structure(sql)
        b = _normalize_sql_structure(sql)
        assert a == b

    def test_different_select_list_same_structure(self):
        a = _normalize_sql_structure(
            "SELECT event_time, place, magnitude FROM earthquakes WHERE magnitude > 3 ORDER BY magnitude DESC LIMIT 5"
        )
        b = _normalize_sql_structure(
            "SELECT * FROM earthquakes WHERE magnitude > 3 ORDER BY magnitude DESC LIMIT 5"
        )
        # WHERE, ORDER BY, LIMIT, FROM should match; SELECT is not compared
        assert a is not None and b is not None
        assert a["where"] == b["where"]
        assert a["order"] == b["order"]
        assert a["limit"] == b["limit"]
        assert a["from"] == b["from"]

    def test_different_aliases_same_structure(self):
        a = _normalize_sql_structure(
            "SELECT count(*) AS cnt FROM earthquakes WHERE event_time >= now() - INTERVAL 24 HOUR"
        )
        b = _normalize_sql_structure(
            "SELECT count(*) AS earthquake_count FROM earthquakes WHERE event_time >= now() - INTERVAL 24 HOUR"
        )
        assert a is not None and b is not None
        assert a["where"] == b["where"]
        assert a["from"] == b["from"]

    def test_different_where_doesnt_match(self):
        a = _normalize_sql_structure(
            "SELECT count(*) FROM earthquakes WHERE magnitude > 3"
        )
        b = _normalize_sql_structure(
            "SELECT count(*) FROM earthquakes WHERE magnitude > 5"
        )
        assert a is not None and b is not None
        assert a["where"] != b["where"]

    def test_different_limit_doesnt_match(self):
        a = _normalize_sql_structure(
            "SELECT event_time FROM earthquakes LIMIT 5"
        )
        b = _normalize_sql_structure(
            "SELECT event_time FROM earthquakes LIMIT 10"
        )
        assert a is not None and b is not None
        assert a["limit"] != b["limit"]

    def test_unparseable_returns_none(self):
        assert _normalize_sql_structure("NOT SQL AT ALL") is None

    def test_unsupported_returns_none(self):
        assert _normalize_sql_structure("UNSUPPORTED") is None

    def test_group_by_matches(self):
        a = _normalize_sql_structure(
            "SELECT region, count(*) AS cnt FROM earthquakes GROUP BY region ORDER BY cnt DESC"
        )
        b = _normalize_sql_structure(
            "SELECT region, count(*) AS event_count FROM earthquakes GROUP BY region ORDER BY event_count DESC"
        )
        assert a is not None and b is not None
        assert a["group"] == b["group"]
        assert a["from"] == b["from"]


class TestProjectResults:
    def test_identical_columns(self):
        expected = {"columns": ["a", "b"], "rows": [[1, 2], [3, 4]]}
        generated = {"columns": ["a", "b"], "rows": [[1, 2], [3, 4]]}
        pe, pg = _project_results(expected, generated)
        assert pe == pg

    def test_extra_columns_in_generated(self):
        expected = {"columns": ["a", "b"], "rows": [[1, 2]]}
        generated = {"columns": ["a", "b", "c"], "rows": [[1, 2, 99]]}
        pe, pg = _project_results(expected, generated)
        assert pe == [[1, 2]]
        assert pg == [[1, 2]]

    def test_select_star_superset(self):
        expected = {"columns": ["magnitude", "place"], "rows": [[5.2, "Alaska"]]}
        generated = {"columns": ["event_id", "magnitude", "depth_km", "place"], "rows": [["us123", 5.2, 10.0, "Alaska"]]}
        pe, pg = _project_results(expected, generated)
        assert pe == [[5.2, "Alaska"]]
        assert pg == [[5.2, "Alaska"]]

    def test_no_overlapping_columns(self):
        expected = {"columns": ["a", "b"], "rows": [[1, 2]]}
        generated = {"columns": ["x", "y"], "rows": [[9, 8]]}
        pe, pg = _project_results(expected, generated)
        # Falls back to full comparison
        assert pe == [[1, 2]]
        assert pg == [[9, 8]]

    def test_reordered_columns(self):
        expected = {"columns": ["a", "b"], "rows": [[1, 2], [3, 4]]}
        generated = {"columns": ["b", "a"], "rows": [[2, 1], [4, 3]]}
        pe, pg = _project_results(expected, generated)
        assert pe == pg
