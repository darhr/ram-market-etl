"""
Module for the scraper factory.

Registers and instantiates the available scrapers.
"""

from typing import List
from .base_scraper import BaseScraper


def get_all_scrapers() -> List[BaseScraper]:
    """
    Returns instances of all registered scrapers.

    Returns:
        List[BaseScraper]: A list containing instances of all configured scrapers.
    """
    return []
