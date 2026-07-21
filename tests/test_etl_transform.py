"""Tests for transform_data in etl.py."""

from __future__ import annotations

import pandas as pd
import pytest
from prefect.logging import disable_run_logger

from etl import transform_data


def _make_raw_df(rows: list[dict]) -> pd.DataFrame:
    """Build a raw DataFrame matching scraper output format."""
    return pd.DataFrame(rows)


class TestTransformDataEmpty:
    """Tests for transform_data with empty input."""

    def test_empty_df_returns_empty(self) -> None:
        with disable_run_logger():
            result = transform_data.fn(pd.DataFrame())
        assert result.empty

    def test_empty_df_returns_same_columns(self) -> None:
        with disable_run_logger():
            result = transform_data.fn(pd.DataFrame())
        assert list(result.columns) == []


class TestTransformDataOutput:
    """Tests for transform_data output structure and column extraction."""

    def test_output_columns(self) -> None:
        df = _make_raw_df(
            [
                {"name": "KINGSTON FURY 16GB DDR5 6000MHZ RGB", "price": 89.99, "store": "cyc"},
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        expected = {
            "raw_name",
            "total_capacity_gb",
            "ddr_gen",
            "speed_mts",
            "has_rgb",
            "kit_modules",
            "brand",
            "series",
            "part_number",
            "price",
            "store",
        }
        assert set(result.columns) == expected

    @pytest.mark.parametrize(
        "kit_string, expected_modules", [("2x16GB", 2), ("(2x32GB)", 2), ("8GB", 1), ("16x2GB", 2)]
    )
    def test_kit_modules(self, kit_string: str, expected_modules: int) -> None:
        df = _make_raw_df(
            [
                {
                    "name": f"KINGSTON FURY {kit_string} DDR5 6000MHZ",
                    "price": 179.99,
                    "store": "cyc",
                },
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["kit_modules"] == expected_modules

    @pytest.mark.parametrize(
        "ddr_string, expected_ddr_gen",
        [
            ("DDR3", 3),
            ("DDR4", 4),
            ("DDR5", 5),
        ],
    )
    def test_ddr_gen_extraction(self, ddr_string: str, expected_ddr_gen: int) -> None:
        df = _make_raw_df(
            [
                {
                    "name": f"KINGSTON FURY 16GB {ddr_string} 6000MHZ",
                    "price": 89.99,
                    "store": "cyc",
                },
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["ddr_gen"] == expected_ddr_gen

    @pytest.mark.parametrize(
        "speed_string, expected_speed",
        [
            ("5200MHz", 5200),
            ("5200Mhz", 5200),
            ("5200MHZ", 5200),
            ("5200MT/S", 5200),
            ("5200Mt/s", 5200),
        ],
    )
    def test_speed_extraction(self, speed_string: str, expected_speed: int) -> None:
        df = _make_raw_df(
            [
                {"name": f"KINGSTON FURY 16GB DDR5 {speed_string}", "price": 89.99, "store": "cyc"},
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["speed_mts"] == expected_speed

    def test_rgb_detected(self) -> None:
        df = _make_raw_df(
            [
                {"name": "KINGSTON FURY 16GB DDR5 6000MHZ RGB", "price": 89.99, "store": "cyc"},
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["has_rgb"] == True  # noqa: E712

    def test_rgb_not_detected(self) -> None:
        df = _make_raw_df(
            [
                {"name": "KINGSTON FURY 16GB DDR5 6000MHZ", "price": 89.99, "store": "cyc"},
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["has_rgb"] == False  # noqa: E712

    @pytest.mark.parametrize(
        "capacity_string, expected_capacity",
        [("16GB", 16), ("24GB", 24), ("32GB", 32), ("48GB", 48)],
    )
    def test_capacity_extraction(self, capacity_string: str, expected_capacity: int) -> None:
        df = _make_raw_df(
            [
                {
                    "name": f"KINGSTON FURY {capacity_string} DDR5 6000MHZ",
                    "price": 159.99,
                    "store": "cyc",
                },
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["total_capacity_gb"] == expected_capacity

    def test_name_is_uppercased(self) -> None:
        df = _make_raw_df(
            [
                {"name": "kingston fury 16gb ddr5 6000mhz", "price": 89.99, "store": "cyc"},
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["raw_name"] == "KINGSTON FURY 16GB DDR5 6000MHZ"

    def test_brand_and_series_extracted(self) -> None:
        df = _make_raw_df(
            [
                {"name": "KINGSTON FURY 16GB DDR5 6000MHZ", "price": 89.99, "store": "cyc"},
            ]
        )
        with disable_run_logger():
            result = transform_data.fn(df)
        assert result.iloc[0]["brand"] == "KINGSTON"
        assert result.iloc[0]["series"] == "FURY"
