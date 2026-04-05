"""Tests for data fetching and transformation (quake_sql.data)."""
from __future__ import annotations

import pytest

from quake_sql.data import derive_region


class TestDeriveRegion:
    def test_of_pattern(self):
        assert derive_region("10km NW of Anchorage, Alaska") == "Anchorage, Alaska"

    def test_comma_fallback(self):
        assert derive_region("Anchorage, Alaska") == "Alaska"

    def test_no_delimiter(self):
        assert derive_region("Yellowstone") == "Yellowstone"

    def test_none_returns_unknown(self):
        assert derive_region(None) == "Unknown"

    def test_empty_string_returns_unknown(self):
        assert derive_region("") == "Unknown"

    def test_float_nan_returns_unknown(self):
        assert derive_region(float("nan")) == "Unknown"

    def test_multiple_of_uses_first(self):
        assert derive_region("5km S of 10km N of SomePlace") == "10km N of SomePlace"

    def test_whitespace_collapsed(self):
        assert derive_region("  lots   of   spaces  ") == "spaces"

    def test_of_at_start_no_space_before(self):
        # "of X" without leading space — no " of " match, falls to comma/plain
        assert derive_region("of SomeRegion") == "of SomeRegion"
