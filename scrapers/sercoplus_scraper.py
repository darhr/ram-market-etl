"""
Sercoplus Web Scraper Module.

This module provides a class to handle RAM memories information, from "Sercoplus" store website.
"""

from base_scraper import BaseScraper
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import re
import cloudscraper

class SercoplusScraper(BaseScraper):
    """
    Scraper class for Sercoplus website.
    """

    def scrape_all(self) -> List[Dict[str, Any]]:
        """
        Scrape hardware products from the target category page.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing
            a formatted 'name' and 'price' of the extracted products.
        """
        page_number = 1
        extracted_products = []

        while True:
            url = (
                f"https://sercoplus.com/87-memoria-ram-pc?page={page_number}"
            )

            # Request to the URL
            try:
                scraper = cloudscraper.create_scraper()
                response = scraper.get(url, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving page: {e}")
                break

            # Parse the HTML content
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all products
            product_elements = soup.find_all("article", class_="item")

            # If no products are found, break the loop
            if not product_elements:
                break

            for item in product_elements:
                # Extract name
                name_tag = item.find("h6", itemprop="name")
                name = name_tag.get_text(strip=True) if name_tag else "n/a"

                # Extract price
                price_tag = item.find("span", class_="price")
                price = price_tag.get_text(strip=True) if price_tag else "n/a"

                extracted_products.append(
                    {"name": format_name(name), "price": format_price(price)}
                )

            page_number += 1

        return extracted_products

def format_price(price: str) -> float:
    """
    Format the price string to a float.
    """
    # Extract the price from the string
    pattern = r"S\/(?:\s|\xa0)*([\d.,]+)"
    match = re.search(pattern, price)
    if match:
        number_str = match.group(1)
        # Remove commas (assuming they are thousands separators: 1,270.86 -> 1270.86)
        cleaned_price = number_str.replace(".", "").replace(",", ".")
        try:
            return float(cleaned_price)
        except ValueError:
            pass
    return 0.0

def format_name(name: str) -> str:
    """
    Clean the product name string.
    """
    return name.replace(",", "")

if __name__ == "__main__":
    scraper = SercoplusScraper()
    products = scraper.scrape_all()
    print(products)
