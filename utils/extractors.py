"""
RAM product data extraction and normalization helpers.

Pure functions for extracting brand, series, and part numbers from raw
product names. No pipeline or Prefect dependencies.
"""

import re
from functools import lru_cache
from typing import Any, List, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process

from utils.config import get_brand_series_map_url, get_series_aliases_map_url


@lru_cache(maxsize=1)
def _load_brand_series_maps() -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, str]]:
    """Load brand and series mappings from CSV files referenced by env vars.

    Cached so the CSVs are read only once per process lifetime.

    Returns:
        A tuple of (brand_series_map, series_aliases_map, series_to_brand).
    """
    brand_series_url = get_brand_series_map_url()
    series_aliases_url = get_series_aliases_map_url()

    if not brand_series_url or not series_aliases_url:
        raise ValueError(
            "brand-series-map-url and series-aliases-map-url Secret Blocks "
            "or their env var equivalents (BRAND_SERIES_MAP_URL, "
            "SERIES_ALIASES_MAP_URL) must be configured"
        )

    brand_series_df = pd.read_csv(brand_series_url)
    series_aliases_df = pd.read_csv(series_aliases_url)

    brand_series_dict = brand_series_df.to_dict("list")
    raw_series_aliases_dict = series_aliases_df.to_dict("list")

    brand_series_map = {
        brand: [
            str(series).upper().strip()
            for series in series_list
            if pd.notna(series) and str(series).strip() != ""
        ]
        for brand, series_list in brand_series_dict.items()
    }
    series_aliases_map = {
        series: [
            str(alias).upper().strip()
            for alias in alias_list
            if pd.notna(alias) and str(alias).strip() != ""
        ]
        for series, alias_list in raw_series_aliases_dict.items()
    }

    series_to_brand = {
        series: brand for brand, series_list in brand_series_map.items() for series in series_list
    }

    return brand_series_map, series_aliases_map, series_to_brand


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


def _series_scorer(a: str, b: str, **kw: Any) -> int:
    """Dual scorer: partial_ratio for substrings, token_set_ratio for typos."""
    return max(fuzz.partial_ratio(a, b), fuzz.token_set_ratio(a, b))


def extract_series(name: str, candidates: Optional[List[str]] = None) -> Optional[str]:
    """
    Extracts the RAM series from the product name.

    Args:
        name (str): The raw product name.
        candidates (Optional[List[str]]): Specific series candidates to match
            against. If None, matches against all known series.

    Returns:
        Optional[str]: The extracted series if found, else None.
    """
    _, series_aliases_map, series_to_brand = _load_brand_series_maps()
    if candidates is None:
        candidates = list(series_to_brand.keys())

    matches = process.extract(name, candidates, scorer=_series_scorer, score_cutoff=80)

    if matches:
        if len(matches) == 1:
            return matches[0][0]

        # if more than one match, return the one with the highest score
        # if there is a tie in score, return the one with the longest name
        best_score = max(m[1] for m in matches)
        top = [m for m in matches if m[1] == best_score]
        match = max(top, key=lambda m: len(m[0]))
        return match[0]

    # if still no match, check for aliases (only within candidates)
    candidate_set = set(candidates)
    for series, aliases in series_aliases_map.items():
        if series not in candidate_set:
            continue
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
    brand_series_map, _, _ = _load_brand_series_maps()
    all_brands = list(brand_series_map.keys())

    match = process.extractOne(name, all_brands, scorer=_series_scorer, score_cutoff=80)

    return match[0] if match else None


def extract_ram_series_and_brand(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts both the brand and series from the RAM product name.

    Uses a brand-first strategy: extract brand, then search for series
    within that brand's series list. Falls back to series-first with
    brand derivation via O(1) lookup if brand extraction fails.

    Args:
        name (str): The raw product name.

    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing the brand and series.
    """
    brand_series_map, _, series_to_brand = _load_brand_series_maps()

    # Brand-first: extract brand, then series within that brand.
    brand = extract_brand(name)
    if brand:
        series = extract_series(name, candidates=brand_series_map[brand])
        if series:
            return brand, series

    # Fallback: series-first, derive brand from series via O(1) lookup.
    series = extract_series(name)
    if series:
        return series_to_brand[series], series

    return brand, None
