"""
Tests for pipeline utilities and transformation logic.
These tests validate cleaning functions without needing a database.
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.utils import (
    parse_date,
    normalize_category,
    normalize_priority,
    clean_cost,
    clean_sla,
    normalize_submitter,
    clean_status,
    clean_building,
    fix_encoding,
    compute_row_hash,
)
import pandas as pd


# ---------------------------------------------------------------------------
# Date Parsing Tests
# ---------------------------------------------------------------------------

class TestParseDates:
    def test_iso_8601_with_time(self):
        result = parse_date("2024-10-24 14:32:10")
        assert result == datetime(2024, 10, 24, 14, 32, 10)

    def test_iso_8601_with_t(self):
        result = parse_date("2024-07-25T18:27:44")
        assert result == datetime(2024, 7, 25, 18, 27, 44)

    def test_us_format(self):
        result = parse_date("02/03/2025")
        assert result is not None
        assert result.year == 2025

    def test_us_format_with_ampm(self):
        result = parse_date("02/14/2025 08:16 AM")
        assert result is not None
        assert result.hour == 8

    def test_european_text(self):
        result = parse_date("28-Feb-2024 17:03")
        assert result is not None
        assert result.month == 2
        assert result.day == 28

    def test_unix_epoch(self):
        result = parse_date("1726547768")
        assert result is not None
        assert result.year in (2024, 2025)

    def test_none_value(self):
        assert parse_date(None) is None

    def test_empty_string(self):
        assert parse_date("") is None

    def test_na_string(self):
        assert parse_date("N/A") is None

    def test_date_only(self):
        result = parse_date("2025-07-01")
        assert result == datetime(2025, 7, 1)


# ---------------------------------------------------------------------------
# Category Normalization Tests
# ---------------------------------------------------------------------------

class TestNormalizeCategory:
    def test_hvac_variants(self):
        assert normalize_category("hvac") == "HVAC"
        assert normalize_category("A/C") == "HVAC"
        assert normalize_category("AC") == "HVAC"
        assert normalize_category("air conditioning") == "HVAC"
        assert normalize_category("HVAC System") == "HVAC"
        assert normalize_category("heating cooling") == "HVAC"
        assert normalize_category("Climate Control") == "HVAC"

    def test_electrical_variants(self):
        assert normalize_category("electrical") == "Electrical"
        assert normalize_category("ELECTRICAL") == "Electrical"
        assert normalize_category("Elec") == "Electrical"
        assert normalize_category("power issue") == "Electrical"
        assert normalize_category("Power") == "Electrical"

    def test_plumbing_variants(self):
        assert normalize_category("Plumbing") == "Plumbing"
        assert normalize_category("PLUMBING") == "Plumbing"
        assert normalize_category("water issue") == "Plumbing"
        assert normalize_category("Water/Plumbing") == "Plumbing"

    def test_junk_values(self):
        assert normalize_category("DELETE ME") is None
        assert normalize_category("test") is None
        assert normalize_category("asdf") is None
        assert normalize_category("???") is None
        assert normalize_category("unknown") is None

    def test_none_and_empty(self):
        assert normalize_category(None) is None
        assert normalize_category("") is None

    def test_long_description_swap(self):
        # A description accidentally in the category field
        long_val = "Breaker keeps tripping in server room 391. Critical affects production systems."
        assert normalize_category(long_val) is None  # Too long, flagged for review


# ---------------------------------------------------------------------------
# Priority Normalization Tests
# ---------------------------------------------------------------------------

class TestNormalizePriority:
    def test_critical_variants(self):
        assert normalize_priority("CRITICAL") == "Critical"
        assert normalize_priority("critical") == "Critical"
        assert normalize_priority("crit") == "Critical"
        assert normalize_priority("urgent!!!") == "Critical"

    def test_high_variants(self):
        assert normalize_priority("HIGH") == "High"
        assert normalize_priority("high") == "High"
        assert normalize_priority("hi") == "High"

    def test_medium_variants(self):
        assert normalize_priority("Medium") == "Medium"
        assert normalize_priority("med") == "Medium"
        assert normalize_priority("MED") == "Medium"
        assert normalize_priority("Normal") == "Medium"

    def test_low_variants(self):
        assert normalize_priority("Low") == "Low"
        assert normalize_priority("LOW") == "Low"
        assert normalize_priority("lo") == "Low"

    def test_invalid_values(self):
        assert normalize_priority("???") is None
        assert normalize_priority(None) is None
        assert normalize_priority("") is None


# ---------------------------------------------------------------------------
# Cost Cleaning Tests
# ---------------------------------------------------------------------------

class TestCleanCost:
    def test_normal_value(self):
        assert clean_cost("4569.86") == 4569.86

    def test_dollar_prefix(self):
        assert clean_cost("$3065.86") == 3065.86

    def test_tbd(self):
        assert clean_cost("TBD") is None

    def test_error(self):
        assert clean_cost("error") is None

    def test_negative_sentinel(self):
        assert clean_cost("-1") is None

    def test_none(self):
        assert clean_cost(None) is None

    def test_zero_is_valid(self):
        assert clean_cost("0") == 0.0


# ---------------------------------------------------------------------------
# SLA Cleaning Tests
# ---------------------------------------------------------------------------

class TestCleanSla:
    def test_valid_values(self):
        assert clean_sla(4) == 4
        assert clean_sla(8) == 8
        assert clean_sla(24) == 24
        assert clean_sla(48) == 48
        assert clean_sla(72) == 72

    def test_sentinels(self):
        assert clean_sla(-1) is None
        assert clean_sla(0) is None
        assert clean_sla(999) is None

    def test_none(self):
        assert clean_sla(None) is None


# ---------------------------------------------------------------------------
# Submitter Normalization Tests
# ---------------------------------------------------------------------------

class TestNormalizeSubmitter:
    def test_canonical_forms(self):
        assert normalize_submitter("john smith") == "John Smith"
        assert normalize_submitter("J. Smith") == "John Smith"
        assert normalize_submitter("jane doe") == "Jane Doe"
        assert normalize_submitter("J. Doe") == "Jane Doe"

    def test_variant_forms(self):
        assert normalize_submitter("bob martinez") == "Robert Martinez"
        assert normalize_submitter("mike johnson") == "Michael Johnson"
        assert normalize_submitter("D. Kim") == "David Kim"

    def test_junk_submitters(self):
        assert normalize_submitter("test") is None
        assert normalize_submitter("admin") is None
        assert normalize_submitter("system") is None


# ---------------------------------------------------------------------------
# Encoding Fix Tests
# ---------------------------------------------------------------------------

class TestFixEncoding:
    def test_em_dash_fix(self):
        # Test cleaning raw UTF-8 bytes read as Latin-1 (mojibake)
        raw_mojibake = "Critical \u00e2\u0080\u0094 affects production"
        result = fix_encoding(raw_mojibake)
        assert result == "Critical \u2014 affects production"

    def test_none(self):
        assert fix_encoding(None) is None


# ---------------------------------------------------------------------------
# Row Hash Tests
# ---------------------------------------------------------------------------

class TestRowHash:
    def test_deterministic(self):
        row = pd.Series({"a": "1", "b": "2"})
        h1 = compute_row_hash(row)
        h2 = compute_row_hash(row)
        assert h1 == h2

    def test_different_data_different_hash(self):
        row1 = pd.Series({"a": "1", "b": "2"})
        row2 = pd.Series({"a": "1", "b": "3"})
        assert compute_row_hash(row1) != compute_row_hash(row2)
