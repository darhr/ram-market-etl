"""
Sercoplus Web Scraper Module.

This module provides a class to handle RAM memories information, from "Sercoplus" store website.
"""

from .base_scraper import BaseScraper
import requests
from typing import List, Dict, Any
import re
import cloudscraper


class SercoplusScraper(BaseScraper):
    """
    Scraper class for Sercoplus website.
    """

    def scrape_all(self) -> List[Dict[str, Any]]:
        """
        Scrape RAM memories information from Sercoplus website.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing
            a formatted 'name' and 'price' of the extracted products.
        """
        page_number = 1
        extracted_products = []

        # Initialize cloudscraper once to preserve Cloudflare clearance cookies across pages
        scraper = cloudscraper.create_scraper()

        while True:
            # Endpoint used internally by the site to load products dynamically
            url = f"https://sercoplus.com/87-memoria-ram-pc?page={page_number}&from-xhr"

            # Instruct the server to return JSON instead of HTML
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            }

            try:
                response = scraper.get(url, timeout=10, headers=headers)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving page: {e}")
                break

            products_elements = response.json().get("products", [])

            # Empty list means the last page has been consumed
            if not products_elements:
                break

            for product in products_elements:
                name = product["name"]
                price = product["price"]
                extracted_products.append(
                    {
                        "name": format_name(name),
                        "price": format_price(price),
                    }
                )

            page_number += 1

        return extracted_products


def format_price(price: str) -> float:
    """
    Format the price string to a float.

    Args:
        price (str): Raw price string (e.g., "$\u00a01.270,86").

    Returns:
        float: The numeric price, or 0.0 if no number is found.
    """
    # Search for numbers with optional comma and dot
    pattern = r"([\d.,]+)"
    match = re.search(pattern, price)
    if match:
        number_str = match.group(1)
        cleaned_price = number_str.replace(".", "").replace(",", ".")
        return float(cleaned_price)
    return 0.0


def format_name(name: str) -> str:
    """
    Remove commas in the product name.

    Args:
        name (str): Raw product name.

    Returns:
        str: Cleaned name.
    """
    return name.replace(",", "")


if __name__ == "__main__":
    scraper = SercoplusScraper()
    products = scraper.scrape_all()
    print(products)
