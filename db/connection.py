"""
Module for database connection management.

Provides utilities to connect to the PostgreSQL database.
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from utils.config import get_database_url


def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine instance.

    Returns:
        Engine: The SQLAlchemy engine for database connections.
    """
    return create_engine(get_database_url())
