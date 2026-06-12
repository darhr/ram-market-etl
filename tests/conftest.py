"""Shared fixtures for ETL tests.

Sets up temporary brand/series CSV data and reloads the etl module so that
module-level globals (all_brands, all_series, etc.) use test data instead
of the production Google Sheets.
"""

from __future__ import annotations

import atexit
import importlib
import os
import sys
import tempfile
from decimal import Decimal
from typing import Any

import pytest

# Environment setup - runs at conftest import time, which pytest handles
# before collecting any test modules.  This ensures the env vars are
# present when etl.py reads them at import time.
#
# Module-level code is necessary because test files import from etl at
# module level (e.g. from etl import transform_data), which triggers
# etl.py's CSV reads before any fixture can run.
#
# Pytest's pythonpath = ["."] (pyproject.toml) already adds the project
# root to sys.path, so no manual manipulation is needed.

# CSV column layout: first row = brand names, subsequent rows = series
# per brand (aligned by column).  This matches what etl.py expects from
# the production Google Sheet.
_BRAND_SERIES_CSV = """\
KINGSTON,G.SKILL,CORSAIR
FURY,Trident Z5,VENGEANCE
FURY Beast,Ripjaws V,Dominator
"""

_SERIES_ALIASES_CSV = """\
FURY,Trident Z5,VENGEANCE
FURIA,Trident,Vengeance LPX
"""

# Create temp CSV files for brand/series data.
_etl_tmpdir_obj = tempfile.TemporaryDirectory(prefix="etl_csvs_")
_etl_tmpdir = _etl_tmpdir_obj.name
_bs_path = os.path.join(_etl_tmpdir, "brand_series.csv")
_sa_path = os.path.join(_etl_tmpdir, "series_aliases.csv")

with open(_bs_path, "w", encoding="utf-8") as f:
    f.write(_BRAND_SERIES_CSV)
with open(_sa_path, "w", encoding="utf-8") as f:
    f.write(_SERIES_ALIASES_CSV)

os.environ["BRAND_SERIES_MAP_URL"] = _bs_path
os.environ["SERIES_ALIASES_MAP_URL"] = _sa_path

# Reload etl so it re-reads the CSVs from the temp files.
if "etl" in sys.modules:
    importlib.reload(sys.modules["etl"])
else:
    import etl  # noqa: F401 - triggers module-level CSV reads


@atexit.register
def _cleanup_etl_env() -> None:
    """Remove temporary CSV files and env vars."""
    os.environ.pop("BRAND_SERIES_MAP_URL", None)
    os.environ.pop("SERIES_ALIASES_MAP_URL", None)
    _etl_tmpdir_obj.cleanup()


@pytest.fixture()
def sample_valid_record() -> dict[str, Any]:
    """A single valid record as returned by split_valid_invalid."""
    return {
        "raw_name": "KINGSTON FURY 16GB DDR5 6000MHZ RGB",
        "total_capacity_gb": 16,
        "ddr_gen": 5,
        "speed_mts": 6000,
        "has_rgb": True,
        "kit_modules": 1,
        "brand": "KINGSTON",
        "series": "FURY",
        "part_number": "KF560C36BBE16",
        "price": Decimal("89.99"),
        "store": "cyc",
    }


@pytest.fixture()
def sample_raw_record() -> dict[str, Any]:
    """A raw record as returned by a scraper."""
    return {
        "name": "KINGSTON FURY 16GB DDR5 6000MHZ RGB",
        "price": 89.99,
        "store": "cyc",
    }


@pytest.fixture()
def sample_invalid_record() -> dict[str, Any]:
    """A record that failed Pydantic validation."""
    return {
        "raw_name": "UNKNOWN BRAND 8GB DDR4",
        "price": 29.99,
        "store": "compuvision",
        "error_reason": "brand: Field required",
    }
