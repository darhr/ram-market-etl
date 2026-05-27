"""
Main entry point for the ETL pipeline.

Extracts data from scrapers, transforms it, and loads it into the database.
"""
import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from scrapers.factory import get_all_scrapers
import pandas as pd
from rapidfuzz import process, utils, fuzz
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the data folder if it doesn't exist
os.makedirs("data", exist_ok=True)

# Load brand and series mappings from environment variables
BRAND_SERIES_MAP_URL = os.getenv("BRAND_SERIES_MAP_URL")
SERIES_ALIASES_MAP_URL = os.getenv("SERIES_ALIASES_MAP_URL")

if not BRAND_SERIES_MAP_URL or not SERIES_ALIASES_MAP_URL:
    raise ValueError("BRAND_SERIES_MAP_URL and SERIES_ALIASES_MAP_URL must be set in the environment")

brand_series_df = pd.read_csv(BRAND_SERIES_MAP_URL)
series_aliases_df = pd.read_csv(SERIES_ALIASES_MAP_URL)

brand_series_dict = brand_series_df.to_dict("list")
raw_series_aliases_dict = series_aliases_df.to_dict("list")

BRAND_SERIES_MAP = {
    brand: [str(series).upper().strip() for series in series_list if pd.notna(series) and str(series).strip() != ""]
    for brand, series_list in brand_series_dict.items()
}
SERIES_ALIASES_MAP = {
    series: [str(alias).upper().strip() for alias in alias_list if pd.notna(alias) and str(alias).strip() != ""]
    for series, alias_list in raw_series_aliases_dict.items()
}

# Create a reverse mapping from series to brand
series_to_brand = {
    series: brand
    for brand, series_list in BRAND_SERIES_MAP.items()
    for series in series_list
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
    re.compile(r'<h4>N[u\u00fa]mero de Parte:\s*([^<]+?)\s*</h4>', re.IGNORECASE),
    re.compile(r'\(PN:?\s*([^\)]+?)\s*\)', re.IGNORECASE),
    re.compile(r'\s+([A-Z][A-Z0-9_\-/()+]{4,})\s*(?:\s+\d{8,})*\s*$'),
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
    pn = re.sub(r'\(\d+\)$', '', pn)
    pn = re.sub(r'[/\-_\s]+', '', pn)
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


def extract_data() -> pd.DataFrame:
    """
    Extracts data from all scrapers.

    Returns:
        pd.DataFrame: A DataFrame containing the raw data.
    """
    scrapers = get_all_scrapers()
    raw_data: List[Dict[str, Any]] = []
    for scraper in scrapers:
        logger.info(f"Running scraper: {scraper.__class__.__name__}")
        raw_data.extend(scraper.scrape_all())
    
    logger.info(f"Total raw records extracted: {len(raw_data)}")
    
    # Save the raw data to a CSV file
    raw_data_df = pd.DataFrame(raw_data)
    raw_data_df.to_csv("data/raw_data.csv", index=False)
    logger.info("Raw data saved in data/raw_data.csv")
    
    return raw_data_df


def transform_data(raw_data_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transforms raw data into a structured format.
    
    Args:
        raw_data_df (pd.DataFrame): The raw data to transform.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the consistent and inconsistent DataFrames.
    """
    if raw_data_df.empty:
        logger.warning("No data extracted to transform. Returning empty DataFrames.")
        return pd.DataFrame(), pd.DataFrame()

    # Set name column to upper case for consistent processing
    raw_data_df["name"] = raw_data_df["name"].str.upper()
    
    # Extract both numbers around the multiplier (e.g., 2x16 or 16x2) to later take the minimum value (kit modules)
    extracted_modules = raw_data_df["name"].str.extract(r'(?:^|\s|\()(\d+)\s*(?:GB?|G)?\s*(?:X|\*)\s*(\d+)\s*(?:GB?|G)?(?:$|\s|\))')

    # Map on unique names to extract both brand and series from the product name
    unique_names = raw_data_df["name"].unique()
    brand_series_mapping = {name: extract_ram_series_and_brand(name) for name in unique_names}
    brand_series_df = pd.DataFrame(raw_data_df["name"].map(brand_series_mapping).tolist())

    raw_data_df["part_number"] = raw_data_df["name"].map(extract_part_number)
    raw_data_df["part_number"] = raw_data_df["part_number"].map(normalize_part_number)

    transformed_ram_kit_df = pd.DataFrame({
        "raw_name": raw_data_df["name"].astype("string"),
        "total_capacity_gb": raw_data_df["name"].str.extract(r'\s(\d+)GB*', expand=False).astype("Int64"),
        "ddr_gen": raw_data_df["name"].str.extract(r'\sDDR(\d)', expand=False).astype("Int64"),
        "speed_mts": raw_data_df["name"].str.extract(r'\s(\d{4})\s*(?:MHZ|MT/S)*', expand=False).astype("Int64"),
        "has_rgb": raw_data_df["name"].str.contains("RGB"),
        "kit_modules": extracted_modules.astype(float).min(axis=1).fillna(1).astype("Int64"),
        "brand": brand_series_df[0].astype("string"),
        "series": brand_series_df[1].astype("string"),
        "part_number": raw_data_df["part_number"].astype("string"),
        "price": raw_data_df["price"].astype("Float64"),
        "store": raw_data_df["store"].astype("string"),
    })
    
    # Define critical columns that must not contain null/NaN values.
    # series is allowed to be null/NaN for downstream analysis.
    critical_cols = [
        "total_capacity_gb",
        "ddr_gen",
        "speed_mts",
        "brand",
        "price",
        "store",
    ]

    inconsistent_data = transformed_ram_kit_df[transformed_ram_kit_df[critical_cols].isna().any(axis=1)]
    consistent_data = transformed_ram_kit_df.dropna(subset=critical_cols)
    
    # Save the consistent data to a CSV file
    consistent_data.drop(columns=["raw_name"]).to_csv("data/consistent_data.csv", index=False)
    logger.info("Transformed data saved in data/consistent_data.csv")
    
    # Save the inconsistent data to a CSV file
    inconsistent_data.drop(columns=["raw_name"]).to_csv("data/inconsistent_data.csv", index=False)
    logger.info("Inconsistent data saved in data/inconsistent_data.csv")
    
    return consistent_data, inconsistent_data


def load_data(consistent_df: pd.DataFrame, inconsistent_df: pd.DataFrame) -> None:
    """
    Loads transformed data into the database.
    
    Args:
        consistent_df (pd.DataFrame): The DataFrame containing the consistent data to load.
        inconsistent_df (pd.DataFrame): The DataFrame containing the inconsistent data to load.
    """
    from db.connection import get_db_engine
    from sqlalchemy import text

    if consistent_df.empty and inconsistent_df.empty:
        logger.info("No data to load into the database.")
        return

    logger.info("Starting load process into the database...")
    engine = get_db_engine()

    with engine.begin() as conn:
        # Get unique store names from both consistent and inconsistent data
        stores_in_data = set()
        if not consistent_df.empty:
            stores_in_data.update(consistent_df["store"].dropna().unique())
        if not inconsistent_df.empty:
            stores_in_data.update(inconsistent_df["store"].dropna().unique())

        # Register stores and cache their IDs
        store_to_id_map: Dict[str, int] = {}
        
        # Retrieve existing stores
        store_result = conn.execute(text("SELECT name, id FROM store"))
        for name, store_id in store_result.fetchall():
            store_to_id_map[name] = store_id

        new_stores = [s for s in stores_in_data if s not in store_to_id_map]
        if new_stores:
            for store_name in new_stores:
                conn.execute(
                    text("INSERT INTO store (name, country) VALUES (:name, 'Peru')"),
                    {"name": store_name}
                )
            # Re-fetch store IDs after inserting new ones
            store_result = conn.execute(text("SELECT name, id FROM store"))
            for name, store_id in store_result.fetchall():
                store_to_id_map[name] = store_id
            logger.info(f"Successfully registered {len(new_stores)} new stores.")
        else:
            logger.info("No new stores detected.")

        # Load consistent products
        if not consistent_df.empty:
            logger.info(f"Processing {len(consistent_df)} consistent records...")

            # Resolve ram_id for each unique specification set (cache-first registration)
            ram_id_map: Dict[Tuple[str, Optional[str], int, int, int, int, bool, Optional[str]], int] = {}
            
            # Retrieve all existing RAM specs to build the cache
            ram_specs_result = conn.execute(text("""
                SELECT id, brand, series, total_capacity_gb, ddr_gen, speed_mts, kit_modules, has_rgb, part_number
                FROM ram_kit_specs
            """))
            for r in ram_specs_result.fetchall():
                existing_key = (
                    str(r[1]),
                    None if r[2] is None else str(r[2]),
                    int(r[3]),
                    int(r[4]),
                    int(r[5]),
                    int(r[6]),
                    bool(r[7]),
                    None if r[8] is None else str(r[8]),
                )
                ram_id_map[existing_key] = r[0]

            new_specs_count = 0

            # Register unique RAM specifications and cache their IDs
            for _, row in consistent_df.iterrows():
                series_val = None if pd.isna(row["series"]) else str(row["series"])
                pn_val = None if pd.isna(row["part_number"]) else str(row["part_number"])
                key = (
                    str(row["brand"]),
                    series_val,
                    int(row["total_capacity_gb"]),
                    int(row["ddr_gen"]),
                    int(row["speed_mts"]),
                    int(row["kit_modules"]),
                    bool(row["has_rgb"]),
                    pn_val,
                )
                
                if key in ram_id_map:
                    continue

                ram_spec_query = text("""
                    INSERT INTO ram_kit_specs (brand, series, total_capacity_gb, ddr_gen, speed_mts, kit_modules, has_rgb, part_number)
                    VALUES (:brand, :series, :total_capacity_gb, :ddr_gen, :speed_mts, :kit_modules, :has_rgb, :part_number)
                    RETURNING id;
                """)

                ram_id_result = conn.execute(
                    ram_spec_query,
                    {
                        "brand": key[0],
                        "series": key[1],
                        "total_capacity_gb": key[2],
                        "ddr_gen": key[3],
                        "speed_mts": key[4],
                        "kit_modules": key[5],
                        "has_rgb": key[6],
                        "part_number": key[7],
                    }
                ).fetchone()

                if ram_id_result:
                    ram_id_map[key] = ram_id_result[0]
                    new_specs_count += 1

            if new_specs_count > 0:
                logger.info(f"Successfully registered {new_specs_count} new RAM specs.")
            else:
                logger.info("No new RAM specs detected.")

            # Fetch the latest prices for all existing ram_id and store_id combinations to compare with new prices
            latest_prices_query = text("""
                SELECT DISTINCT ON (ram_id, store_id) ram_id, store_id, price 
                FROM price_history 
                ORDER BY ram_id, store_id, extraction_date DESC;
            """)
            latest_prices_res = conn.execute(latest_prices_query).fetchall()
            latest_prices_cache: Dict[Tuple[int, int], float] = {
                (row[0], row[1]): float(row[2]) for row in latest_prices_res
            }

            # Filter out identical prices and prepare multi-row insert list
            price_inserts: List[Dict[str, Any]] = []
            for _, row in consistent_df.iterrows():
                store_id = store_to_id_map.get(row["store"])
                if not store_id:
                    continue

                series_val = None if pd.isna(row["series"]) else str(row["series"])
                pn_val = None if pd.isna(row["part_number"]) else str(row["part_number"])
                key = (
                    str(row["brand"]),
                    series_val,
                    int(row["total_capacity_gb"]),
                    int(row["ddr_gen"]),
                    int(row["speed_mts"]),
                    int(row["kit_modules"]),
                    bool(row["has_rgb"]),
                    pn_val,
                )
                ram_id = ram_id_map.get(key)
                if not ram_id:
                    continue

                new_price = float(row["price"])
                latest_price = latest_prices_cache.get((ram_id, store_id))
                
                # Check if price has changed or if it's a new listing
                if latest_price is None or abs(latest_price - new_price) >= 0.01:
                    price_inserts.append({
                        "ram_id": ram_id,
                        "store_id": store_id,
                        "price": new_price,
                    })

            # Perform a multi-row insert of all price changes
            if price_inserts:
                insert_price_query = text("""
                    INSERT INTO price_history (ram_id, store_id, price)
                    VALUES (:ram_id, :store_id, :price)
                    ON CONFLICT (ram_id, store_id, extraction_date) DO NOTHING;
                """)
                conn.execute(insert_price_query, price_inserts)
                logger.info(f"Successfully loaded {len(price_inserts)} new price records into price history.")
            else:
                logger.info("No price changes detected. Price history was not updated.")

        # Load inconsistent products to unmapped_product (multi-row load)
        if not inconsistent_df.empty:
            logger.info(f"Processing {len(inconsistent_df)} inconsistent records...")
            unmapped_inserts: List[Dict[str, Any]] = []
            
            for _, row in inconsistent_df.iterrows():
                store_id = store_to_id_map.get(row["store"])
                if not store_id:
                    continue

                raw_name = str(row["raw_name"])
                price_val = None if pd.isna(row["price"]) else float(row["price"])
                unmapped_inserts.append({
                    "raw_name": raw_name,
                    "store_id": store_id,
                    "price": price_val
                })

            if unmapped_inserts:
                unmapped_query = text("""
                    INSERT INTO unmapped_product (raw_name, store_id, price)
                    VALUES (:raw_name, :store_id, :price)
                    ON CONFLICT (raw_name, store_id)
                    DO UPDATE SET
                        price = EXCLUDED.price,
                        last_seen = NOW();
                """)
                conn.execute(unmapped_query, unmapped_inserts)
                logger.info(f"Successfully loaded {len(unmapped_inserts)} unmapped products.")

    logger.info("Load process completed successfully.")


if __name__ == "__main__":
    logger.info("Starting ETL process...")
    raw_data_df = extract_data()
    consistent_data, inconsistent_data = transform_data(raw_data_df)
    load_data(consistent_data, inconsistent_data)
    logger.info("ETL process completed")
