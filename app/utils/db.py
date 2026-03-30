"""
Database utilities for the Streamlit app.

Provides helper functions for querying the database.
"""
from sqlalchemy.engine import Engine
from typing import List, Any

def get_products(engine: Engine, search_query: str | None = None) -> List[Any]:
    """
    Retrieves products from the database, optionally filtered by a search query.

    Args:
        engine (Engine): The database connection engine.
        search_query (Optional[str]): The string to filter product names by.

    Returns:
        List[Any]: A list of products matching the query.
    """
    return []
