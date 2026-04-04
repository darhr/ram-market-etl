"""
Compuvision Scraper Module.

This module provides a class to handle RAM memories information, from "Compuvision" store website.
"""

from base_scraper import BaseScraper
import requests
from typing import List, Dict, Any
import json

class CompuvisionScraper(BaseScraper):
    """
    Scraper class for Compuvision website.
    """

    def scrape_all(self) -> List[Dict[str, Any]]:
        """
        Scrape RAM memories information from Compuvision website.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing
            a formatted 'name' and 'price' of the extracted products.
        """
        page_number = 0
        extracted_products = []

        while True:
            # Endpoint used internally by the site to load products dynamically
            url = "https://compuvisionperu.pe/ajax/ajs_productos.php"

            # Payload mirrors the request the browser sends
            data = {
                "tipo": "pag-search2P",
                "pal": "MEMORIA",
                "forma": "all",
                "cntPH": 12,
                "hojaActual": page_number,
                "minUSD": 0,
                "maxUSD": 10000,
                "marcasFilt": []
            }

            # Instruct the server to return JSON instead of HTML
            headers = {
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01"
            }

            try:
                response = requests.post(url, headers=headers, data=data)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving page: {e}")
                break

            full_text = response.text
            json_start_index = full_text.find('{')

            product_elements = []

            if json_start_index != -1:
                # Strip any non-JSON content before the JSON object
                clean_text = full_text[json_start_index:]
                product_elements_json = json.loads(clean_text)
                product_elements = product_elements_json["datas"]

                for item in product_elements:
                    name = item["nombre"]
                    price = item["precio"]
                    extracted_products.append(
                        {"name": name, "price": float(price)}
                    )

            # Empty list means the last page has been consumed
            if not product_elements:
                break

            page_number += 1

        return extracted_products

if __name__ == "__main__":
    scraper = CompuvisionScraper()
    products = scraper.scrape_all()
    print (products)