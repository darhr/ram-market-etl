"""Tests for ETL load functions with mocked database connections."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, call

NOW = datetime(2026, 6, 11, 12, 0, 0, tzinfo=timezone.utc)

import pytest
from sqlalchemy import text

from etl import (
    _insert_invalid_records,
    _upsert_price_snapshots,
    _upsert_products,
    _upsert_stores,
    load_data,
    register_etl_run_end,
    register_etl_run_start,
)

def _mock_execute_result(fetchone_val: Any = None, fetchall_val: list | None = None, yield_rows: list | None = None):
    """Build a mock result object for conn.execute()."""
    result = MagicMock()
    if fetchone_val is not None:
        result.fetchone.return_value = fetchone_val
    if fetchall_val is not None:
        result.fetchall.return_value = fetchall_val
    if yield_rows is not None:
        result.yield_per.return_value = iter(yield_rows)
    return result


class TestUpsertStores:
    """Tests for _upsert_stores."""

    def test_empty_set_returns_empty_dict(self) -> None:
        conn = MagicMock()
        assert _upsert_stores(conn, set()) == {}
        conn.execute.assert_not_called()

    def test_all_new_stores(self) -> None:
        conn = MagicMock()
        conn.execute.side_effect = [
            _mock_execute_result(fetchone_val=[0]),       # COUNT before
            MagicMock(),                                   # INSERT
            _mock_execute_result(fetchone_val=[3]),       # COUNT after
            _mock_execute_result(fetchall_val=[("cyc", 1), ("compuvision", 2), ("sercoplus", 3)]),
        ]
        result = _upsert_stores(conn, {"cyc", "compuvision", "sercoplus"})
        assert result == {"cyc": 1, "compuvision": 2, "sercoplus": 3}
        assert conn.execute.call_count == 4

    def test_no_new_stores(self) -> None:
        conn = MagicMock()
        conn.execute.side_effect = [
            _mock_execute_result(fetchone_val=[3]),       # COUNT before
            MagicMock(),                                   # INSERT (all conflict)
            _mock_execute_result(fetchone_val=[3]),       # COUNT after - same
            _mock_execute_result(fetchall_val=[("cyc", 1), ("compuvision", 2), ("sercoplus", 3)]),
        ]
        result = _upsert_stores(conn, {"cyc", "compuvision"})
        assert result == {"cyc": 1, "compuvision": 2, "sercoplus": 3}
        assert conn.execute.call_count == 4


class TestUpsertProducts:
    """Tests for _upsert_products."""

    def test_no_new_products_only_selects(self) -> None:
        conn = MagicMock()
        # Row tuple: (id, part_number, capacity_gb, kit_modules)
        existing_rows = [(100, "KF560C36BBE16", 16, 1)]
        conn.execute.side_effect = [
            _mock_execute_result(yield_rows=existing_rows),  # SELECT existing products
            _mock_execute_result(fetchone_val=[NOW]),         # SELECT NOW()
            MagicMock(),                                      # INSERT ON CONFLICT
            _mock_execute_result(fetchone_val=[0]),           # SELECT COUNT (0 updates)
        ]

        records = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "brand": "KINGSTON",
            "series": "FURY",
            "speed_mts": 6000,
            "ddr_gen": 5,
            "has_rgb": True,
        }]
        result = _upsert_products(conn, records)
        assert result == {("KF560C36BBE16", 16, 1): 100}
        assert conn.execute.call_count == 4

    def test_new_products_trigger_insert_and_reread(self) -> None:
        conn = MagicMock()
        conn.execute.side_effect = [
            _mock_execute_result(yield_rows=[]),                        # SELECT existing products
            _mock_execute_result(fetchone_val=[NOW]),                    # SELECT NOW()
            MagicMock(),                                                 # INSERT ON CONFLICT
            _mock_execute_result(fetchone_val=[1]),                      # SELECT COUNT (1 insert)
            _mock_execute_result(yield_rows=[(200, "KF560C36BBE16", 16, 1)]),  # SELECT re-read
        ]

        records = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "brand": "KINGSTON",
            "series": "FURY",
            "speed_mts": 6000,
            "ddr_gen": 5,
            "has_rgb": True,
        }]
        result = _upsert_products(conn, records)
        assert result == {("KF560C36BBE16", 16, 1): 200}
        assert conn.execute.call_count == 5

    def test_existing_product_with_updated_brand(self) -> None:
        """Product exists but brand changed -> ON CONFLICT updates the row."""
        conn = MagicMock()
        existing_rows = [(100, "KF560C36BBE16", 16, 1)]
        conn.execute.side_effect = [
            _mock_execute_result(yield_rows=existing_rows),  # SELECT existing
            _mock_execute_result(fetchone_val=[NOW]),         # SELECT NOW()
            MagicMock(),                                      # INSERT ON CONFLICT (update)
            _mock_execute_result(fetchone_val=[1]),           # SELECT COUNT (1 update)
        ]

        records = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "brand": "HYPERX",
            "series": "FURY",
            "speed_mts": 6000,
            "ddr_gen": 5,
            "has_rgb": True,
        }]
        result = _upsert_products(conn, records)
        assert result == {("KF560C36BBE16", 16, 1): 100}
        assert conn.execute.call_count == 4

    def test_mixed_new_and_unchanged_existing_products(self) -> None:
        """New product + existing product with no changes -> re-read captures both."""
        conn = MagicMock()
        existing_rows = [(100, "KF560C36BBE16", 16, 1)]
        conn.execute.side_effect = [
            _mock_execute_result(yield_rows=existing_rows),   # SELECT existing
            _mock_execute_result(fetchone_val=[NOW]),          # SELECT NOW()
            MagicMock(),                                       # INSERT ON CONFLICT
            _mock_execute_result(fetchone_val=[1]),            # SELECT COUNT (1 new)
            _mock_execute_result(yield_rows=[                  # SELECT re-read
                (100, "KF560C36BBE16", 16, 1),
                (200, "CMH32GX5M2B6000Z30", 32, 2),
            ]),
        ]

        records = [
            {
                "part_number": "KF560C36BBE16",
                "total_capacity_gb": 16,
                "kit_modules": 1,
                "brand": "KINGSTON",
                "series": "FURY",
                "speed_mts": 6000,
                "ddr_gen": 5,
                "has_rgb": True,
            },
            {
                "part_number": "CMH32GX5M2B6000Z30",
                "total_capacity_gb": 32,
                "kit_modules": 2,
                "brand": "CORSAIR",
                "series": "VENGEANCE",
                "speed_mts": 6000,
                "ddr_gen": 5,
                "has_rgb": True,
            },
        ]
        result = _upsert_products(conn, records)
        assert result == {
            ("KF560C36BBE16", 16, 1): 100,
            ("CMH32GX5M2B6000Z30", 32, 2): 200,
        }
        assert conn.execute.call_count == 5


class TestUpsertPriceSnapshots:
    """Tests for _upsert_price_snapshots."""

    def test_no_changes_no_execution(self) -> None:
        """Same price in current snapshot -> no UPDATE or INSERT."""
        conn = MagicMock()
        # Current snapshot: same price as new record
        current = [("KF560C36BBE16", 16, 1, "cyc", 89.99)]
        conn.execute.return_value = _mock_execute_result(fetchall_val=current)

        product_map = {("KF560C36BBE16", 16, 1): 100}
        store_map = {"cyc": 1}
        records = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "store": "cyc",
            "price": Decimal("89.99"),
        }]
        _upsert_price_snapshots(conn, records, product_map, store_map, 1)
        # Only the SELECT was called
        assert conn.execute.call_count == 1

    def test_new_price_inserts_snapshot(self) -> None:
        """No current snapshot -> INSERT only (no close)."""
        conn = MagicMock()
        conn.execute.return_value = _mock_execute_result(fetchall_val=[])

        product_map = {("KF560C36BBE16", 16, 1): 100}
        store_map = {"cyc": 1}
        records = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "store": "cyc",
            "price": Decimal("89.99"),
        }]
        _upsert_price_snapshots(conn, records, product_map, store_map, 1)
        assert conn.execute.call_count == 2  # SELECT + INSERT

    def test_price_changed_closes_and_inserts(self) -> None:
        """Different price -> UPDATE (close) + INSERT (new)."""
        conn = MagicMock()
        current = [("KF560C36BBE16", 16, 1, "cyc", 79.99)]
        conn.execute.side_effect = [
            _mock_execute_result(fetchall_val=current),   # SELECT current
            MagicMock(),                                   # UPDATE close
            MagicMock(),                                   # INSERT new
        ]

        product_map = {("KF560C36BBE16", 16, 1): 100}
        store_map = {"cyc": 1}
        records = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "store": "cyc",
            "price": Decimal("89.99"),
        }]
        _upsert_price_snapshots(conn, records, product_map, store_map, 1)
        assert conn.execute.call_count == 3  # SELECT + UPDATE + INSERT

    def test_dedup_same_product_store_keeps_first(self) -> None:
        """Two records for same product-store -> only first is inserted."""
        conn = MagicMock()
        conn.execute.return_value = _mock_execute_result(fetchall_val=[])

        product_map = {("KF560C36BBE16", 16, 1): 100}
        store_map = {"cyc": 1}
        records = [
            {
                "part_number": "KF560C36BBE16",
                "total_capacity_gb": 16,
                "kit_modules": 1,
                "store": "cyc",
                "price": Decimal("89.99"),
            },
            {
                "part_number": "KF560C36BBE16",
                "total_capacity_gb": 16,
                "kit_modules": 1,
                "store": "cyc",
                "price": Decimal("95.00"),
            },
        ]
        _upsert_price_snapshots(conn, records, product_map, store_map, 1)
        # SELECT + 1 INSERT (second record deduped)
        assert conn.execute.call_count == 2


class TestInsertInvalidRecords:
    """Tests for _insert_invalid_records."""

    def test_with_records_executes_insert(self) -> None:
        conn = MagicMock()
        records = [
            {"raw_name": "BAD RAM", "store": "cyc", "price": 29.99, "error_reason": "brand: required"},
        ]
        _insert_invalid_records(conn, records, {"cyc": 1}, 1)
        conn.execute.assert_called_once()
        # Verify the SQL contains ON CONFLICT
        sql_call = conn.execute.call_args
        assert "ON CONFLICT" in str(sql_call[0][0])

    def test_empty_records_no_execution(self) -> None:
        conn = MagicMock()
        _insert_invalid_records(conn, [], {"cyc": 1}, 1)
        conn.execute.assert_not_called()


class TestRegisterEtlRunStart:
    """Tests for register_etl_run_start."""

    def test_returns_run_id(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # First execute: SET search_path, second: INSERT RETURNING id
        conn.execute.side_effect = [
            MagicMock(),                                # SET search_path
            _mock_execute_result(fetchone_val=[42]),    # INSERT RETURNING id
        ]
        result = register_etl_run_start(engine)
        assert result == 42


class TestRegisterEtlRunEnd:
    """Tests for register_etl_run_end."""

    def test_executes_update(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        conn.execute.side_effect = [
            MagicMock(),  # SET search_path
            MagicMock(),  # UPDATE etl_runs
        ]
        register_etl_run_end(engine, 1, valid_count=98, invalid_count=4,
                             raw_count=102, stores_success=["cyc"],
                             stores_failed=["sercoplus"])
        assert conn.execute.call_count == 2


class TestLoadData:
    """Tests for load_data orchestration."""

    def test_empty_records_returns_early(self) -> None:
        engine = MagicMock()
        load_data([], [], 1, engine, ["cyc"])
        engine.begin.assert_not_called()

    def test_valid_records_calls_upsert_stores_and_products(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        valid = [{
            "part_number": "KF560C36BBE16",
            "total_capacity_gb": 16,
            "kit_modules": 1,
            "brand": "KINGSTON",
            "series": "FURY",
            "speed_mts": 6000,
            "ddr_gen": 5,
            "has_rgb": True,
            "store": "cyc",
            "price": Decimal("89.99"),
        }]

        # Mock all the internal execute calls:
        # SET search_path
        # _upsert_stores: COUNT before, INSERT, COUNT after, SELECT
        # _upsert_products: SELECT, SELECT NOW, ON CONFLICT, SELECT COUNT, re-read
        # _upsert_price_snapshots: SELECT current, INSERT
        results = [
            MagicMock(),                                # SET search_path
            _mock_execute_result(fetchone_val=[0]),     # COUNT stores before
            MagicMock(),                                # INSERT stores
            _mock_execute_result(fetchone_val=[1]),     # COUNT stores after
            _mock_execute_result(fetchall_val=[("cyc", 1)]),  # SELECT stores
            _mock_execute_result(yield_rows=[]),        # SELECT existing products
            _mock_execute_result(fetchone_val=[NOW]),   # SELECT NOW()
            MagicMock(),                                # INSERT ON CONFLICT
            _mock_execute_result(fetchone_val=[1]),     # SELECT COUNT
            _mock_execute_result(yield_rows=[(100, "KF560C36BBE16", 16, 1)]),  # SELECT re-read
            _mock_execute_result(fetchall_val=[]),      # SELECT current prices
            MagicMock(),                                # INSERT price snapshot
        ]
        conn.execute.side_effect = results

        load_data(valid, [], 1, engine, ["cyc"])
        # Verify SET search_path was called
        first_call = conn.execute.call_args_list[0]
        assert "search_path" in str(first_call[0][0])
