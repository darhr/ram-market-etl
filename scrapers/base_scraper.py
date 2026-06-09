"""
Module providing the base class for all scrapers.

Defines the interface that all specific store scrapers must implement.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseScraper(ABC):
    """
    Abstract base class for web scrapers.
    """

    @property
    @abstractmethod
    def store_name(self) -> str:
        """Return the canonical store name used in scraped records."""
        ...

    @abstractmethod
    def scrape_all(self) -> List[Dict[str, Any]]:
        """
        Executes the scraping process and returns a list of products.

        Returns:
            List[Dict[str, Any]]: List of dictionary representations of products.
        """
        pass
