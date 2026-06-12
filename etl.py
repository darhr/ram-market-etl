"""
Main entry point for the ETL pipeline.

Extracts data from scrapers, persists the raw snapshot to the R2 bronze layer,
re-reads it from there as the single source of truth, transforms it, and
loads it into the database.
"""

import logging
import re
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from scrapers.factory import get_all_scrapers
import pandas as pd
from rapidfuzz import process, fuzz
import os
from sqlalchemy import text
from utils.storage import upload_dataframe, download_dataframe
from utils.validators import split_valid_invalid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Price quantizer - round half up to 2 decimal places
_PRICE_Q = Decimal("0.01")

# Create the data folder if it doesn't exist
os.makedirs("data", exist_ok=True)

# Load brand and series mappings from environment variables
BRAND_SERIES_MAP_URL = os.getenv("BRAND_SERIES_MAP_URL")
SERIES_ALIASES_MAP_URL = os.getenv("SERIES_ALIASES_MAP_URL")

if not BRAND_SERIES_MAP_URL or not SERIES_ALIASES_MAP_URL:
    raise ValueError(
        "BRAND_SERIES_MAP_URL and SERIES_ALIASES_MAP_URL must be set in the environment"
    )

brand_series_df = pd.read_csv(BRAND_SERIES_MAP_URL)
series_aliases_df = pd.read_csv(SERIES_ALIASES_MAP_URL)

brand_series_dict = brand_series_df.to_dict("list")
raw_series_aliases_dict = series_aliases_df.to_dict("list")

BRAND_SERIES_MAP = {
    brand: [
        str(series).upper().strip()
        for series in series_list
        if pd.notna(series) and str(series).strip() != ""
    ]
    for brand, series_list in brand_series_dict.items()
}
SERIES_ALIASES_MAP = {
    series: [
        str(alias).upper().strip()
        for alias in alias_list
        if pd.notna(alias) and str(alias).strip() != ""
    ]
    for series, alias_list in raw_series_aliases_dict.items()
}

# Create a reverse mapping from series to brand
series_to_brand = {
    series: brand for brand, series_list in BRAND_SERIES_MAP.items() for series in series_list
}

# Get all brands and series as lists
all_brands = list(BRAND_SERIES_MAP.keys())
all_series = list(series_to_brand.keys())

# Part number extraction patterns, applied in order (most specific first).
# - sercoplus embeds the PN inside an HTML <h4> tag.
# - cyc wraps it in parentheses with a "PN:" or "PN " prefix.
# - compuvision appends it at the end of the name, optionally followed by
#   EAN barcodes (>=8 consecutive digits) which are ignored
PART_NUMBER_PATTERNS: List[re.Pattern] = [
    re.compile(r"<h4>N[u\u00fa]mero de Parte:\s*([^<]+?)\s*</h4>", re.IGNORECASE),
    re.compile(r"\(PN:?\s*([^\)]+?)\s*\)", re.IGNORECASE),
    re.compile(r"\s+([A-Z][A-Z0-9_\-/()+]{4,})\s*(?:\s+\d{8,})*\s*$"),
]


def extract_part_number(name: str) -> Optional[str]:
    """
    Extracts the manufacturer part number from the raw product name.

    Tries three patterns in order of specificity:
    1. Sercoplus HTML tag ``<h4>Número de Parte: ...</h4>``.
    2. Cyc parenthetical notation ``(PN:...)`` or ``(PN ...)``.
    3. Compuvision end-of-string alphanumeric token (letters-led, 5+ chars),
       ignoring trailing EAN barcodes if present.

    Args:
        name (str): The raw product name.

    Returns:
        Optional[str]: The part number in uppercase with stripped whitespace,
        or ``None`` if no pattern matches.
    """
    for pattern in PART_NUMBER_PATTERNS:
        match = pattern.search(name)
        if match:
            return match.group(1).strip().upper()
    return None


def normalize_part_number(pn: Optional[str]) -> Optional[str]:
    """
    Normalize a part number for deduplication.

    Applies a general-rules pipeline that strips all separator characters
    so that equivalent PNs from different stores resolve to the same key.

    Args:
        pn (Optional[str]): The extracted part number.

    Returns:
        Optional[str]: The normalized part number or None.
    """
    if not isinstance(pn, str) or not pn:
        return None
    pn = pn.strip().upper()
    pn = re.sub(r"\(\d+\)$", "", pn)
    pn = re.sub(r"[/\-_\s]+", "", pn)
    return pn or None


def extract_series(name: str) -> Optional[str]:
    """
    Extracts the RAM series from the product name.

    Args:
        name (str): The raw product name.

    Returns:
        Optional[str]: The extracted series if found, else None.
    """
    # partial_ratio covers substrings; token_set_ratio covers typos and extra noise.
    # We take the best of both for each candidate.
    scorer = lambda a, b, **kw: max(fuzz.partial_ratio(a, b), fuzz.token_set_ratio(a, b))
    match = process.extractOne(name, all_series, scorer=scorer, score_cutoff=80)

    if match:
        return match[0]

    for series, aliases in SERIES_ALIASES_MAP.items():
        for alias in aliases:
            if alias in name.split():
                return series

    return None


def extract_brand(name: str) -> Optional[str]:
    """
    Extracts the RAM brand from the product name.

    Args:
        name (str): The raw product name.

    Returns:
        Optional[str]: The extracted brand if found, else None.
    """
    # Same dual approach for consistency.
    scorer = lambda a, b, **kw: max(fuzz.partial_ratio(a, b), fuzz.token_set_ratio(a, b))
    match = process.extractOne(name, all_brands, scorer=scorer, score_cutoff=80)

    return match[0] if match else None


def extract_ram_series_and_brand(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts both the brand and series from the RAM product name.

    Args:
        name (str): The raw product name.

    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing the brand and series.
    """
    series = extract_series(name)
    if series:
        return series_to_brand[series], series

    return extract_brand(name), None


def extract_data() -> tuple[str, list[str], list[str]]:
    """
    Extracts data from all scrapers, persists the raw snapshot to the R2 bronze
    layer, and returns the object key for the transform stage.

    The local CSV (``data/raw_data.csv``) is kept as a development fallback
    only; the canonical raw record set lives in R2.

    Returns:
        A tuple of (bronze_key, stores_success, stores_failed) where:
        - bronze_key: The R2 object key where the bronze Parquet was uploaded.
        - stores_success: Store names that were scraped successfully.
        - stores_failed: Store names that failed during scraping.

    Raises:
        botocore.exceptions.BotoCoreError: If the upload to R2 fails.
        botocore.exceptions.ClientError: If the upload to R2 fails.
    """
    scrapers = get_all_scrapers()
    raw_data: List[Dict[str, Any]] = []
    stores_success: list[str] = []
    stores_failed: list[str] = []

    for scraper in scrapers:
        logger.info("Running scraper: %s", scraper.store_name)
        try:
            raw_data.extend(scraper.scrape_all())
            stores_success.append(scraper.store_name)
        except Exception:
            stores_failed.append(scraper.store_name)
            logger.exception("Scraper %s failed", scraper.store_name)

    logger.info(
        "Total raw records extracted: %d (success: %s, failed: %s)",
        len(raw_data),
        stores_success,
        stores_failed,
    )
    raw_data_df = pd.DataFrame(raw_data)

    # Local fallback (dev/debug only; canonical raw lives in R2)
    if os.getenv("ENVIRONMENT") == "development":
        raw_data_df.to_csv("data/raw_data.csv", index=False)
        logger.info("Local fallback saved in data/raw_data.csv")

    # Bronze layer: critical step. Re-raises on failure.
    bronze_key = upload_dataframe(raw_data_df)
    return bronze_key, stores_success, stores_failed


def transform_data(raw_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw data into a structured format.

    Args:
        raw_data_df (pd.DataFrame): The raw data to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame with all records.
    """
    if raw_data_df.empty:
        logger.warning("No data extracted to transform. Returning empty DataFrame.")
        return pd.DataFrame()

    # Upper case for consistent matching across all extractors and fuzzy logic.
    raw_data_df["name"] = raw_data_df["name"].str.upper()

    # Extract both numbers around the multiplier (e.g., 2x16 or 16x2).
    # The minimum is taken as kit_modules because symmetric listings are
    # normalized to the lower count (e.g., 2x16 and 16x2 → 2).
    extracted_modules = raw_data_df["name"].str.extract(
        r"(?:^|\s|\()(\d+)\s*(?:GB?|G)?\s*(?:X|\*)\s*(\d+)\s*(?:GB?|G)?(?:$|\s|\))"
    )

    # Deduplicate brand/series lookups by unique name to avoid redundant
    # fuzzy matching on repeated product names.
    unique_names = raw_data_df["name"].unique()
    brand_series_mapping = {name: extract_ram_series_and_brand(name) for name in unique_names}
    brand_series_df = pd.DataFrame(raw_data_df["name"].map(brand_series_mapping).tolist())

    raw_data_df["part_number"] = raw_data_df["name"].map(extract_part_number)
    raw_data_df["part_number"] = raw_data_df["part_number"].map(normalize_part_number)

    transformed_ram_kit_df = pd.DataFrame(
        {
            "raw_name": raw_data_df["name"].astype("string"),
            "total_capacity_gb": raw_data_df["name"]
            .str.extract(r"\s(\d+)GB*", expand=False)
            .astype("Int64"),
            "ddr_gen": raw_data_df["name"].str.extract(r"\sDDR(\d)", expand=False).astype("Int64"),
            "speed_mts": raw_data_df["name"]
            .str.extract(r"\s(\d{4})\s*(?:MHZ|MT/S)*", expand=False)
            .astype("Int64"),
            "has_rgb": raw_data_df["name"].str.contains("RGB"),
            "kit_modules": extracted_modules.astype(float).min(axis=1).fillna(1).astype("Int64"),
            "brand": brand_series_df[0].astype("string"),
            "series": brand_series_df[1].astype("string"),
            "part_number": raw_data_df["part_number"].astype("string"),
            "price": raw_data_df["price"].astype("Float64"),
            "store": raw_data_df["store"].astype("string"),
        }
    )

    logger.info("Transformed %d records.", len(transformed_ram_kit_df))
    return transformed_ram_kit_df


def load_data(
    valid_records: list[Dict[str, Any]],
    invalid_records: list[Dict[str, Any]],
    etl_run_id: int,
    engine: Any,
    stores_success: list[str],
) -> None:
    """
    Loads validated records into the silver schema.

    Args:
        valid_records: Records that passed Pydantic validation.
        invalid_records: Records that failed validation (carry error_reason).
        etl_run_id: The current ETL run id for traceability.
        engine: SQLAlchemy engine to use for the connection.
        stores_success: Store names that were scraped successfully.
    """
    if not valid_records and not invalid_records:
        logger.info("No data to load into the database.")
        return

    logger.info("Starting load process into the silver schema...")

    with engine.begin() as conn:
        conn.execute(text("SET search_path TO silver"))

        store_to_id = _upsert_stores(conn, set(stores_success))

        if valid_records:
            product_id_map = _upsert_products(conn, valid_records)
            _upsert_price_snapshots(conn, valid_records, product_id_map, store_to_id, etl_run_id)

        if invalid_records:
            _insert_invalid_records(conn, invalid_records, store_to_id, etl_run_id)

    logger.info("Load process completed successfully.")


def _upsert_stores(
    conn: Any,
    stores: set[str],
) -> Dict[str, int]:
    """Insert new stores and return a name→id mapping.

    Args:
        conn: Active database connection.
        stores: Set of store names to ensure exist in the database.
    """
    if not stores:
        return {}

    # Count before insert to derive the actual number of newly added stores.
    count_before = conn.execute(text("SELECT COUNT(*) FROM store")).fetchone()[0]

    conn.execute(
        text(
            "INSERT INTO store (name, country) "
            "VALUES (:name, 'Peru') ON CONFLICT (name) DO NOTHING"
        ),
        [{"name": s} for s in stores],
    )

    # Delta between counts gives only the stores that didn't exist before.
    count_after = conn.execute(text("SELECT COUNT(*) FROM store")).fetchone()[0]
    new_stores = count_after - count_before

    result = conn.execute(text("SELECT name, id FROM store"))
    store_to_id = {row[0]: row[1] for row in result.fetchall()}
    logger.info("Stores: %d total, %d new.", len(store_to_id), new_stores)
    return store_to_id


def _upsert_products(
    conn: Any,
    valid_records: list[Dict[str, Any]],
) -> Dict[Tuple[str, int, int], int]:
    """
    Upsert products and return (part_number, capacity_gb, kit_modules) -> id
    to ensure the same PN does not necessarily mean the same product.

    Uses INSERT ON CONFLICT DO UPDATE so that ALL records (both new and
    existing) are processed. This ensures that changes in brand/series
    mappings (e.g., from an updated BRAND_SERIES_MAP) propagate to
    products that already exist in the database.
    """
    # Load existing products to detect genuinely new ones.
    result = conn.execute(text("SELECT id, part_number, capacity_gb, kit_modules FROM product"))
    product_key_to_id: Dict[Tuple[str, int, int], int] = {}
    for row in result.yield_per(500):
        product_key_to_id[(row[1], row[2], row[3])] = row[0]

    existing_keys = set(product_key_to_id.keys())
    new_product_keys: set[Tuple[str, int, int]] = set()
    all_records_params: list[Dict[str, Any]] = []

    for rec in valid_records:
        key = (rec["part_number"], rec["total_capacity_gb"], rec["kit_modules"])
        if key not in existing_keys:
            new_product_keys.add(key)
        all_records_params.append(
            {
                "brand": rec["brand"],
                "series": rec.get("series"),
                "capacity_gb": rec["total_capacity_gb"],
                "speed_mts": rec["speed_mts"],
                "ddr_gen": rec["ddr_gen"],
                "kit_modules": rec["kit_modules"],
                "has_rgb": rec["has_rgb"],
                "part_number": rec["part_number"],
            }
        )

    if all_records_params:
        # Capture the DB timestamp before the upsert to later count how many
        # products were actually inserted or had their spec fields changed.
        before_ts = conn.execute(text("SELECT NOW()")).fetchone()[0]

        # ON CONFLICT handles both new inserts and updates for existing rows.
        conn.execute(
            text(
                "INSERT INTO product (brand, series, capacity_gb, speed_mts, "
                "ddr_gen, kit_modules, has_rgb, part_number) "
                "VALUES (:brand, :series, :capacity_gb, :speed_mts, "
                ":ddr_gen, :kit_modules, :has_rgb, :part_number) "
                "ON CONFLICT (part_number, capacity_gb, kit_modules) DO UPDATE SET "
                "brand = EXCLUDED.brand, "
                "series = EXCLUDED.series, "
                "speed_mts = EXCLUDED.speed_mts, "
                "ddr_gen = EXCLUDED.ddr_gen, "
                "has_rgb = EXCLUDED.has_rgb, "
                "updated_at = CASE WHEN product.brand IS DISTINCT FROM EXCLUDED.brand "
                "OR product.series IS DISTINCT FROM EXCLUDED.series "
                "OR product.speed_mts IS DISTINCT FROM EXCLUDED.speed_mts "
                "OR product.ddr_gen IS DISTINCT FROM EXCLUDED.ddr_gen "
                "OR product.has_rgb IS DISTINCT FROM EXCLUDED.has_rgb "
                "THEN NOW() ELSE product.updated_at END"
            ),
            all_records_params,
        )

        # Count how many products were actually inserted or had spec changes
        # (the CASE WHEN above leaves updated_at unchanged when nothing
        # changed, so they won't be counted).
        inserted_count = conn.execute(
            text("SELECT COUNT(*) FROM product WHERE updated_at >= :ts"),
            {"ts": before_ts},
        ).fetchone()[0]

        # Re-read only when genuinely new products were inserted.
        if new_product_keys:
            result = conn.execute(
                text("SELECT id, part_number, capacity_gb, kit_modules FROM product")
            )
            product_key_to_id = {}
            for row in result.yield_per(500):
                product_key_to_id[(row[1], row[2], row[3])] = row[0]

        # "inserted_count" refers to products that have been updated or inserted
        logger.info(
            "Products: %d upserted (inserted=%d, updated=%d)",
            inserted_count,
            len(new_product_keys),
            inserted_count - len(new_product_keys)
        )
    else:
        logger.info("Products: no changes.")

    return product_key_to_id


def _upsert_price_snapshots(
    conn: Any,
    valid_records: list[Dict[str, Any]],
    product_key_to_id: Dict[Tuple[str, int, int], int],
    store_to_id: Dict[str, int],
    etl_run_id: int,
) -> None:
    """Implement SCD-2: close old snapshot if price changed, insert new.

    Uses (part_number, capacity_gb, kit_modules) + store_name as lookup keys.
    Deduplicates using (product_id, store_id) to handle multiple records for
    the same product-store pair within a single run.
    """
    # Load all current snapshots to compare against new prices.
    result = conn.execute(
        text(
            "SELECT p.part_number, p.capacity_gb, p.kit_modules, s.name, ps.price "
            "FROM price_snapshot ps "
            "JOIN product p ON ps.product_id = p.id "
            "JOIN store s ON ps.store_id = s.id "
            "WHERE ps.is_current = TRUE"
        )
    )
    current_prices: Dict[Tuple[str, int, int, str], Decimal] = {
        (row[0], row[1], row[2], row[3]): Decimal(str(row[4])).quantize(_PRICE_Q)
        for row in result.fetchall()
    }

    # to_close and to_insert use (product_id, store_id) as key for
    # deduplication within the same run.
    to_close: Dict[Tuple[int, int], Dict[str, Any]] = {}
    to_insert: Dict[Tuple[int, int], Dict[str, Any]] = {}

    for rec in valid_records:
        pn = rec["part_number"]
        cap = rec["total_capacity_gb"]
        kit = rec["kit_modules"]
        store = rec["store"]
        product_id = product_key_to_id.get((pn, cap, kit))
        store_id = store_to_id.get(store)
        if not product_id or not store_id:
            continue

        # Quantize both prices to 2 decimals for exact comparison.
        new_price = Decimal(str(rec["price"])).quantize(_PRICE_Q)
        current_price = current_prices.get((pn, cap, kit, store))

        # No change — skip entirely.
        if current_price is not None and current_price == new_price:
            continue

        key = (product_id, store_id)

        # Mark the current snapshot for closing (only once per product-store).
        if current_price is not None and key not in to_close:
            to_close[key] = {"product_id": product_id, "store_id": store_id}

        # Keep only the first record for each product-store pair in a run.
        if key not in to_insert:
            to_insert[key] = {
                "product_id": product_id,
                "store_id": store_id,
                "etl_run_id": etl_run_id,
                "price": new_price,
            }

    if to_close:
        conn.execute(
            text(
                "UPDATE price_snapshot "
                "SET valid_to = NOW(), is_current = FALSE "
                "WHERE product_id = :product_id AND store_id = :store_id AND is_current = TRUE"
            ),
            list(to_close.values()),
        )

    if to_insert:
        conn.execute(
            text(
                "INSERT INTO price_snapshot (product_id, store_id, price, etl_run_id) "
                "VALUES (:product_id, :store_id, :price, :etl_run_id)"
            ),
            list(to_insert.values()),
        )
        logger.info("Price snapshots: %d closed, %d opened.", len(to_close), len(to_insert))
    else:
        logger.info("Price snapshots: no price changes detected.")


def _insert_invalid_records(
    conn: Any,
    invalid_records: list[Dict[str, Any]],
    store_to_id: Dict[str, int],
    etl_run_id: int,
) -> None:
    """Insert invalid records into the quarantine table.

    Uses ON CONFLICT (raw_name, store_name) to avoid duplicate rows for
    the same invalid record across runs; updates last_seen and error_reason
    instead.
    """
    inserts = []
    for rec in invalid_records:
        inserts.append(
            {
                "raw_name": rec.get("raw_name", ""),
                "store_name": rec.get("store", ""),
                "price": rec.get("price"),
                "error_reason": rec.get("error_reason", ""),
                "etl_run_id": etl_run_id,
            }
        )

    if inserts:
        conn.execute(
            text(
                "INSERT INTO invalid_records (raw_name, store_name, price, error_reason, etl_run_id) "
                "VALUES (:raw_name, :store_name, :price, :error_reason, :etl_run_id) "
                "ON CONFLICT (raw_name, store_name) DO UPDATE SET "
                "last_seen = NOW(), price = EXCLUDED.price, error_reason = EXCLUDED.error_reason, "
                "etl_run_id = EXCLUDED.etl_run_id"
            ),
            inserts,
        )
        logger.info("Invalid records: %d upserted.", len(inserts))


def register_etl_run_start(engine: Any) -> int:
    """Insert a new row in silver.etl_runs with status 'running'.

    Args:
        engine: SQLAlchemy engine to use for the connection.

    Returns:
        The id of the newly created ETL run.
    """
    with engine.begin() as conn:
        conn.execute(text("SET search_path TO silver"))
        result = conn.execute(
            text(
                "INSERT INTO etl_runs (status, triggered_by) "
                "VALUES ('running', 'schedule') RETURNING id"
            )
        )
        run_id = result.fetchone()[0]
    logger.info("ETL run started: id=%d", run_id)
    return run_id


def register_etl_run_end(
    engine: Any,
    run_id: int,
    valid_count: int,
    invalid_count: int,
    raw_count: int,
    stores_success: list[str],
    stores_failed: list[str],
) -> None:
    """Update the ETL run row with final metrics.

    Args:
        engine: SQLAlchemy engine to use for the connection.
        run_id: The id returned by ``register_etl_run_start``.
        valid_count: Number of records that passed validation.
        invalid_count: Number of records that failed validation.
        raw_count: Total number of raw records extracted.
        stores_success: Store names that were scraped successfully.
        stores_failed: Store names that failed during scraping.
    """
    with engine.begin() as conn:
        conn.execute(text("SET search_path TO silver"))
        conn.execute(
            text(
                "UPDATE etl_runs "
                "SET finished_at = NOW(), "
                "    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)), "
                "    status = 'success', "
                "    raw_records = :raw_records, "
                "    valid_records = :valid_records, "
                "    invalid_records = :invalid_records, "
                "    stores_success = :stores_success, "
                "    stores_failed = :stores_failed "
                "WHERE id = :run_id"
            ),
            {
                "run_id": run_id,
                "raw_records": raw_count,
                "valid_records": valid_count,
                "invalid_records": invalid_count,
                "stores_success": stores_success,
                "stores_failed": stores_failed,
            },
        )
    logger.info(
        "ETL run %d finished: raw=%d (valid=%d, invalid=%d)",
        run_id,
        raw_count,
        valid_count,
        invalid_count,
    )


if __name__ == "__main__":
    from db.connection import get_db_engine

    logger.info("Starting ETL process...")
    engine = get_db_engine()

    # Register the run before any work; marked as 'failed' on exception.
    run_id = register_etl_run_start(engine)
    try:
        bronze_key, stores_success, stores_failed = extract_data()

        # Re-read the bronze Parquet as the single source of truth.
        raw_data_df = download_dataframe(bronze_key)
        transformed_df = transform_data(raw_data_df)
        valid, invalid = split_valid_invalid(transformed_df.to_dict("records"))
        load_data(valid, invalid, run_id, engine, stores_success)
        register_etl_run_end(
            engine,
            run_id,
            valid_count=len(valid),
            invalid_count=len(invalid),
            raw_count=len(transformed_df),
            stores_success=stores_success,
            stores_failed=stores_failed,
        )
        logger.info("ETL process completed")
    except Exception:
        # Mark the run as failed so it's visible in etl_runs history.
        with engine.begin() as conn:
            conn.execute(text("SET search_path TO silver"))
            conn.execute(
                text("UPDATE etl_runs SET status = 'failed', finished_at = NOW() WHERE id = :id"),
                {"id": run_id},
            )
        logger.exception("ETL process failed")
        raise
