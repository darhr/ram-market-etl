"""Tests for ETL extraction and normalization helpers in etl.py."""

from __future__ import annotations

from etl import (
    extract_brand,
    extract_part_number,
    extract_ram_series_and_brand,
    extract_series,
    normalize_part_number,
)


class TestExtractPartNumber:
    """Tests for extract_part_number - regex-based PN extraction."""

    def test_sercoplus_html_pattern(self) -> None:
        name = "RAM DDR5 16GB<h4>N\u00famero de Parte: KF560C36BBE16</h4>"
        assert extract_part_number(name) == "KF560C36BBE16"

    def test_sercoplus_html_with_accent(self) -> None:
        name = "RAM DDR5 16GB<h4>N\u00famero de Parte: ABC-123</h4>"
        assert extract_part_number(name) == "ABC-123"

    def test_cyc_parentheses_with_colon(self) -> None:
        name = "KINGSTON FURY 16GB DDR5 (PN: KF560C36BBE16)"
        assert extract_part_number(name) == "KF560C36BBE16"

    def test_cyc_parentheses_without_colon(self) -> None:
        name = "KINGSTON FURY 16GB DDR5 (PN KF560C36BBE16)"
        assert extract_part_number(name) == "KF560C36BBE16"

    def test_compuvision_trailing_token(self) -> None:
        name = "KINGSTON FURY 16GB DDR5 6000 KF560C36BBE16"
        assert extract_part_number(name) == "KF560C36BBE16"

    def test_compuvision_strips_trailing_ean(self) -> None:
        name = "KINGSTON FURY 16GB DDR5 KF560C36BBE16 1234567890123"
        assert extract_part_number(name) == "KF560C36BBE16"

    def test_no_match_returns_none(self) -> None:
        assert extract_part_number("RANDOM TEXT NO PN HERE") is None

    def test_empty_string_returns_none(self) -> None:
        assert extract_part_number("") is None

    def test_whitespace_is_stripped(self) -> None:
        name = "RAM DDR5 16GB<h4>N\u00famero de Parte:  KF560C36  </h4>"
        assert extract_part_number(name) == "KF560C36"

    def test_result_is_uppercased(self) -> None:
        name = "RAM (pn: abc-123)"
        assert extract_part_number(name) == "ABC-123"


class TestNormalizePartNumber:
    """Tests for normalize_part_number - PN dedup normalization."""

    def test_removes_separators(self) -> None:
        assert normalize_part_number("KVR-56S4/16-8") == "KVR56S4168"

    def test_strips_trailing_digits_in_parens(self) -> None:
        assert normalize_part_number("ABC (1)") == "ABC"

    def test_strips_separators_after_parens(self) -> None:
        assert normalize_part_number("ABC-123 (2)") == "ABC123"

    def test_uppercases(self) -> None:
        assert normalize_part_number("abc-123") == "ABC123"

    def test_none_returns_none(self) -> None:
        assert normalize_part_number(None) is None

    def test_empty_returns_none(self) -> None:
        assert normalize_part_number("") is None

    def test_pure_separators_returns_none(self) -> None:
        assert normalize_part_number("---") is None

    def test_whitespace_only_returns_none(self) -> None:
        assert normalize_part_number("   ") is None

    def test_already_clean(self) -> None:
        assert normalize_part_number("KF560C36BBE16") == "KF560C36BBE16"


class TestExtractSeries:
    """Tests for extract_series - fuzzy matching against known series."""

    def test_exact_match(self) -> None:
        assert extract_series("KINGSTON FURY 16GB DDR5 6000") == "FURY"

    def test_alias_match(self) -> None:
        assert extract_series("KINGSTON FURIA 16GB DDR5") == "FURY"

    def test_no_match_returns_none(self) -> None:
        assert extract_series("UNKNOWN BRAND 8GB DDR4") is None

    def test_partial_match_with_noise(self) -> None:
        assert extract_series("TRIDENT Z5 RGB 32GB DDR5") == "TRIDENT Z5"


class TestExtractBrand:
    """Tests for extract_brand - fuzzy matching against known brands."""

    def test_exact_match(self) -> None:
        assert extract_brand("KINGSTON FURY 16GB DDR5") == "KINGSTON"

    def test_no_match_returns_none(self) -> None:
        assert extract_brand("UNKNOWN PRODUCT 8GB") is None

    def test_partial_match_brand_typo(self) -> None:
        assert extract_brand("CORSIR 32GB DDR4") == "CORSAIR"


class TestExtractRamSeriesAndBrand:
    """Tests for extract_ram_series_and_brand - combined extraction."""

    def test_series_found_returns_both(self) -> None:
        brand, series = extract_ram_series_and_brand("KINGSTON FURY 16GB DDR5")
        assert brand == "KINGSTON"
        assert series == "FURY"

    def test_brand_only_no_series(self) -> None:
        brand, series = extract_ram_series_and_brand("KINGSTON RAM 8GB DDR4")
        assert brand == "KINGSTON"
        assert series is None

    def test_nothing_found(self) -> None:
        brand, series = extract_ram_series_and_brand("RANDOM TEXT 8GB")
        assert brand is None
        assert series is None
