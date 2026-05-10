"""
Main entry point for the ETL pipeline.

Extracts data from scrapers, transforms it, and loads it into the database.
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from scrapers.factory import get_all_scrapers
import pandas as pd
from datetime import datetime
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


def extract_data() -> List[Dict[str, Any]]:
    """
    Extracts data from all scrapers.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing the raw data.
    """
    scrapers = get_all_scrapers()
    raw_data: List[Dict[str, Any]] = []
    for scraper in scrapers:
        logger.info(f"Running scraper: {scraper.__class__.__name__}")
        raw_data.extend(scraper.scrape_all())
    
    logger.info(f"Total raw records extracted: {len(raw_data)}")
    
    return raw_data


def transform_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transforms raw data into a structured format.
    
    Args:
        raw_data (List[Dict[str, Any]]): The raw data to transform.
    
    Returns:
        pd.DataFrame: A DataFrame containing the transformed data.
    """
    transformed_ram_kit_df = pd.DataFrame()

    raw_data_df = pd.DataFrame(raw_data)
    raw_data_df["name"] = raw_data_df["name"].str.upper()
    

    transformed_ram_kit_df["total_capacity_gb"] = raw_data_df["name"].str.extract(r'\s(\d+)GB*', expand=False).astype("Int64")
    transformed_ram_kit_df["ddr_gen"] = raw_data_df["name"].str.extract(r'\sDDR(\d)', expand=False).astype("Int64")
    transformed_ram_kit_df["speed_mts"] = raw_data_df["name"].str.extract(r'\s(\d{4})\s*(?:MHZ|MT/S)*', expand=False)
    transformed_ram_kit_df["has_rgb"] = raw_data_df["name"].str.contains("RGB")
    
    # Extract both numbers around the multiplier (e.g., 2x16 or 16x2) and take the smaller one.
    extracted_modules = raw_data_df["name"].str.extract(r'(?:^|\s|\()(\d+)\s*(?:GB?|G)?\s*(?:X|\*)\s*(\d+)\s*(?:GB?|G)?(?:$|\s|\))')
    transformed_ram_kit_df["kit_modules"] = extracted_modules.astype(float).min(axis=1).fillna(1).astype("Int64")

    # Use map on unique names to extract both brand and series from the product name
    unique_names = raw_data_df["name"].unique()
    brand_series_mapping = {name: extract_ram_series_and_brand(name) for name in unique_names}
    transformed_ram_kit_df[["brand", "series"]] = raw_data_df["name"].map(brand_series_mapping).tolist()

    transformed_ram_kit_df["price"] = raw_data_df["price"]
    transformed_ram_kit_df["store"] = raw_data_df["store"]

    # Save the raw data to a CSV file
    raw_data_df.to_csv("data/raw_data.csv", index=False)
    logger.info("Raw data saved in data/raw_data.csv")
    
    # Save the transformed data to a CSV file
    transformed_ram_kit_df.to_csv("data/transformed_data.csv", index=False)
    logger.info("Transformed data saved in data/transformed_data.csv")
    
    return transformed_ram_kit_df


def load_data(df: pd.DataFrame) -> None:
    """
    Loads transformed data into the database.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the transformed data to load.
    """
    logger.info("Load process completed")


if __name__ == "__main__":
    logger.info("Starting ETL process...")
    raw_data_extracted = extract_data()
    transformed_data = transform_data(raw_data_extracted)
    load_data(transformed_data)
    logger.info("ETL process completed")
