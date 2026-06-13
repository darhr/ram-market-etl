"""
Module for database connection management.

Provides utilities to connect to the PostgreSQL database.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine instance.

    Returns:
        Engine: The SQLAlchemy engine for database connections.
    """
    db_url: str = os.getenv("DATABASE_URL", "sqlite:///:memory:")
    return create_engine(db_url)
