"""Pydantic schemas for ETL record validation."""

from __future__ import annotations
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

class RamKitRecord(BaseModel):
    """Schema for a single RAM kit record after transformation."""

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        frozen=True, # Immutable objects but only while validating with this model
    )

    total_capacity_gb: Annotated[int, Field(ge=1, le=256)]
    ddr_gen: Literal[3, 4, 5]
    speed_mts: Annotated[int, Field(ge=1600, le=10000)]
    has_rgb: bool
    kit_modules: Annotated[int, Field(ge=1)]
    brand: Annotated[str, Field(min_length=1)]
    series: str | None = None
    part_number: Annotated[str, Field(min_length=1)]
    price: Annotated[float, Field(gt=0)]
    store: Annotated[str, Field(min_length=1)]

_RAM_ADAPTER = TypeAdapter(RamKitRecord)

def split_valid_invalid(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Validate a batch of raw records, separating valid from invalid.

    Returns:
        A tuple ``(valid, invalid)`` where each invalid record carries an
        ``error_reason`` field with the raw pydantic error message.
    """
    valid: list[dict] = []
    invalid: list[dict] = []

    for record in records:
        try:
            valid.append(_RAM_ADAPTER.validate_python(record).model_dump())
        except ValidationError as exc:
            invalid.append({**record, "error_reason": str(exc)})

    return valid, invalid