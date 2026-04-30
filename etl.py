"""
Main entry point for the ETL pipeline.

Extracts data from scrapers, transforms it, and loads it into the database.
"""
import logging
from typing import List, Dict, Any
from scrapers.factory import get_all_scrapers

logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)

def main():
    """
    Executes the main ETL process.
    """
    logger.info("Starting ETL process...")
    scrapers: List[Any] = get_all_scrapers()
    
    raw_data: List[Dict[str, Any]] = []
    for scraper in scrapers:
        logger.info(f"Running scraper: {scraper.__class__.__name__}")
        raw_data.extend(scraper.scrape_all())
    
    logger.info(f"Total raw records extracted: {len(raw_data)}")
    
    logger.info("ETL process completed")

if __name__ == "__main__":
    main()
