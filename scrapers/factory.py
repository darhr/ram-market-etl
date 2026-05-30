"""
Module for the scraper factory.

Registers and instantiates the available scrapers.
"""

from typing import List
from .base_scraper import BaseScraper
from .compuvision_scraper import CompuvisionScraper
from .cyc_scraper import CyCScraper
from .sercoplus_scraper import SercoplusScraper

def get_all_scrapers() -> List[BaseScraper]:
    """
    Returns instances of all registered scrapers.

    Returns:
        List[BaseScraper]: A list containing instances of all configured scrapers.
    """
    
    return [
        CompuvisionScraper(),
        CyCScraper(),
        SercoplusScraper(),
    ]
