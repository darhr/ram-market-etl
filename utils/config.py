"""Centralized secret loading for Prefect Cloud and local development.

Each getter tries to load from a Prefect Secret Block first. If the block
does not exist or Prefect is not reachable, it falls back to the equivalent
environment variable. This keeps local dev (``poe dev`` / ``poe prod``)
unchanged while enabling clean secret injection in Prefect Cloud deployments.
"""

import os

_FALLBACK_DB_URL = "sqlite:///:memory:"


def _get_secret(name: str, env_var: str, fallback: str = "") -> str:
    """Load a secret from a Prefect Secret Block, falling back to an env var.

    Args:
        name: The Prefect Secret Block name (e.g. ``"database-url"``).
        env_var: The environment variable to check as fallback.
        fallback: Default value if neither the block nor the env var exist.

    Returns:
        The resolved secret value.
    """
    try:
        from prefect.blocks.system import Secret

        return Secret.load(name).get()
    except Exception:
        return os.getenv(env_var, fallback)


def get_database_url() -> str:
    """Return the PostgreSQL connection URL."""
    return _get_secret("database-url", "DATABASE_URL", _FALLBACK_DB_URL)


def get_r2_config() -> dict[str, str]:
    """Return all Cloudflare R2 credentials as a dictionary.

    Keys: ``endpoint_url``, ``access_key``, ``secret_key``, ``bucket``.
    """
    return {
        "endpoint_url": _get_secret("r2-endpoint-url", "R2_ENDPOINT_URL"),
        "access_key": _get_secret("r2-access-key", "R2_ACCESS_KEY_ID"),
        "secret_key": _get_secret("r2-secret-key", "R2_SECRET_ACCESS_KEY"),
        "bucket": _get_secret("r2-bucket-name", "R2_BUCKET_NAME"),
    }


def get_brand_series_map_url() -> str:
    """Return the public CSV URL for the brand-to-series mapping."""
    return _get_secret("brand-series-map-url", "BRAND_SERIES_MAP_URL")


def get_series_aliases_map_url() -> str:
    """Return the public CSV URL for the series aliases mapping."""
    return _get_secret("series-aliases-map-url", "SERIES_ALIASES_MAP_URL")
