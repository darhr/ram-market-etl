"""Tests for Pydantic validators in utils/validators.py."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from utils.validators import RamKitRecord, split_valid_invalid


def _valid_record() -> dict:
    return {
        "total_capacity_gb": 32,
        "ddr_gen": 5,
        "speed_mts": 6000,
        "has_rgb": True,
        "kit_modules": 2,
        "brand": "G.SKILL",
        "series": "Trident Z5",
        "part_number": "F5-6000R3036G32GQ4Z",
        "price": 550.0,
        "store": "compuvision",
    }


class TestRamKitRecord:
    """Tests for the RamKitRecord schema."""

    def test_valid_record_passes(self) -> None:
        record = RamKitRecord(**_valid_record())
        assert record.brand == "G.SKILL"
        assert record.ddr_gen == 5
        assert record.total_capacity_gb == 32

    def test_series_optional_accepts_none(self) -> None:
        data = _valid_record()
        data["series"] = None
        record = RamKitRecord(**data)
        assert record.series is None

    def test_series_optional_accepts_empty_string(self) -> None:
        data = _valid_record()
        data["series"] = ""
        record = RamKitRecord(**data)
        assert record.series == ""

    def test_extra_field_is_ignored(self) -> None:
        data = {**_valid_record(), "extra_column": "scraper_artifact"}
        record = RamKitRecord(**data)
        assert "extra_column" not in record.model_dump()

    def test_str_strip_whitespace(self) -> None:
        data = _valid_record()
        data["brand"] = "  G.SKILL  "
        record = RamKitRecord(**data)
        assert record.brand == "G.SKILL"

    def test_model_is_frozen(self) -> None:
        record = RamKitRecord(**_valid_record())
        with pytest.raises(ValidationError):
            record.brand = "Corsair"

    @pytest.mark.parametrize("value", [3, 4, 5])
    def test_valid_ddr_gen(self, value: int) -> None:
        data = _valid_record()
        data["ddr_gen"] = value
        record = RamKitRecord(**data)
        assert record.ddr_gen == value

    @pytest.mark.parametrize("value", [0, 6, 7])
    def test_invalid_ddr_gen(self, value: int) -> None:
        data = _valid_record()
        data["ddr_gen"] = value
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("value", [1600, 2666, 6000, 10000])
    def test_valid_speed_boundaries(self, value: int) -> None:
        data = _valid_record()
        data["speed_mts"] = value
        record = RamKitRecord(**data)
        assert record.speed_mts == value

    @pytest.mark.parametrize("value", [1599, 10001, 0, -100])
    def test_invalid_speed(self, value: int) -> None:
        data = _valid_record()
        data["speed_mts"] = value
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("value", [4, 8, 16, 32, 128, 256])
    def test_valid_capacity_boundaries(self, value: int) -> None:
        data = _valid_record()
        data["total_capacity_gb"] = value
        record = RamKitRecord(**data)
        assert record.total_capacity_gb == value

    @pytest.mark.parametrize("value", [0, 4096, -1])
    def test_invalid_capacity(self, value: int) -> None:
        data = _valid_record()
        data["total_capacity_gb"] = value
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("value", [0.01, 1.0, 550.0, 99999.99])
    def test_valid_price_positive(self, value: float) -> None:
        data = _valid_record()
        data["price"] = value
        record = RamKitRecord(**data)
        assert record.price == value

    @pytest.mark.parametrize("value", [0, -1, -0.01])
    def test_invalid_price(self, value: float) -> None:
        data = _valid_record()
        data["price"] = value
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("value", [1, 2, 4])
    def test_valid_kit_modules(self, value: int) -> None:
        data = _valid_record()
        data["kit_modules"] = value
        record = RamKitRecord(**data)
        assert record.kit_modules == value

    @pytest.mark.parametrize("value", [0, -1])
    def test_invalid_kit_modules(self, value: int) -> None:
        data = _valid_record()
        data["kit_modules"] = value
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("field", ["part_number", "brand", "store"])
    def test_required_string_rejects_none(self, field: str) -> None:
        data = _valid_record()
        data[field] = None
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("field", ["part_number", "brand", "store"])
    def test_required_string_rejects_empty(self, field: str) -> None:
        data = _valid_record()
        data[field] = ""
        with pytest.raises(ValidationError):
            RamKitRecord(**data)

    @pytest.mark.parametrize("field", ["part_number", "brand", "store"])
    def test_required_string_rejects_whitespace_only(self, field: str) -> None:
        data = _valid_record()
        data[field] = "   "
        with pytest.raises(ValidationError):
            RamKitRecord(**data)


class TestSplitValidInvalid:
    """Tests for the split_valid_invalid helper."""

    def test_empty_input_returns_empty_lists(self) -> None:
        valid, invalid = split_valid_invalid([])
        assert valid == []
        assert invalid == []

    def test_all_valid_records(self) -> None:
        records = [_valid_record() for _ in range(3)]
        valid, invalid = split_valid_invalid(records)
        assert len(valid) == 3
        assert invalid == []

    def test_all_invalid_records(self) -> None:
        records = []
        for i in range(3):
            record = _valid_record()
            record["price"] = -(i + 1)
            records.append(record)
        valid, invalid = split_valid_invalid(records)
        assert valid == []
        assert len(invalid) == 3
        for inv in invalid:
            assert "error_reason" in inv
            assert "price" in inv["error_reason"]

    def test_mixed_batch_separates_correctly(self) -> None:
        valid_a = _valid_record()
        invalid_a = _valid_record()
        invalid_a["part_number"] = None
        valid_b = _valid_record()
        valid_b["part_number"] = "OTHER-PN-1234"
        invalid_b = _valid_record()
        invalid_b["ddr_gen"] = 6

        records = [valid_a, invalid_a, valid_b, invalid_b]
        valid, invalid = split_valid_invalid(records)
        assert len(valid) == 2
        assert len(invalid) == 2
        for inv in invalid:
            assert "error_reason" in inv
            assert inv["error_reason"]

    def test_invalid_record_preserves_original_fields(self) -> None:
        record = _valid_record()
        record["price"] = -10
        _, invalid = split_valid_invalid([record])
        assert len(invalid) == 1
        inv = invalid[0]
        assert inv["brand"] == record["brand"]
        assert inv["part_number"] == record["part_number"]
        assert inv["store"] == record["store"]
        assert "error_reason" in inv

    def test_nan_value_rejected_as_type_error(self) -> None:
        record = _valid_record()
        record["series"] = float("nan")
        _, invalid = split_valid_invalid([record])
        assert len(invalid) == 1
        assert "series" in invalid[0]["error_reason"]
