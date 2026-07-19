"""Cloudflare R2 client for the bronze layer of the data lake.

All R2 interactions go through this module so the rest of the codebase stays
S3-compatible and provider-agnostic. The bronze layer is the single source of
truth for raw scraped data: the transform step always re-reads the persisted
Parquet snapshot from R2 rather than operating on an in-memory DataFrame.
"""

import io
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from utils.config import get_r2_config

logger = logging.getLogger(__name__)


def _get_client():
    """Build a stateless boto3 S3 client targeting Cloudflare R2.

    The client is rebuilt on every call to keep the module stateless and safe
    for parallel / orchestrated execution (e.g. Prefect flows).

    Raises:
        RuntimeError: If any required R2 credential is missing.
    """
    r2 = get_r2_config()
    missing = [key for key, value in r2.items() if not value]
    if missing:
        raise RuntimeError(f"Missing R2 credentials: {', '.join(missing)}")

    return boto3.client(
        "s3",
        endpoint_url=r2["endpoint_url"],
        aws_access_key_id=r2["access_key"],
        aws_secret_access_key=r2["secret_key"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def build_bronze_key(run_at: datetime | None = None) -> str:
    """Build the medallion-style object key for a bronze-layer raw snapshot.

    The key embeds a UTC timestamp in the sub-folder to prevent overwrites
    when the pipeline runs multiple times per day. Format:

        bronze/raw_data/YYYY-MM-DD/YYYY-MM-DDTHH-MM-SSZ/raw_ram_data.parquet

    The date partition at the first level enables date-based S3 list scans;
    the timestamp sub-folder keeps each run unique.

    Args:
        run_at: Snapshot timestamp. Defaults to ``now`` in UTC.

    Returns:
        The full object key under the bronze prefix.
    """
    run_at = run_at or datetime.now(timezone.utc)
    date_part = run_at.strftime("%Y-%m-%d")
    time_part = run_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"bronze/raw_data/{date_part}/{time_part}/raw_ram_data.parquet"


def upload_dataframe(df: pd.DataFrame, run_at: datetime | None = None) -> str:
    """Upload a DataFrame as Parquet to the R2 bronze layer.

    The function is the single write path for raw data and is considered
    a critical step: any failure is logged and re-raised so callers can
    decide whether to abort the pipeline.

    Args:
        df: The DataFrame to serialize and upload.
        run_at: Snapshot timestamp. Defaults to ``now`` in UTC.

    Returns:
        The full R2 object key written.

    Raises:
        botocore.exceptions.BotoCoreError: For low-level boto3 errors.
        botocore.exceptions.ClientError: For service-side errors.
    """
    client = _get_client()
    key = build_bronze_key(run_at)
    bucket = get_r2_config()["bucket"]

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    body = buffer.getvalue()

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/vnd.apache.parquet",
        )
    except (BotoCoreError, ClientError) as exc:
        logger.error("R2 upload failed for r2://%s/%s: %s", bucket, key, exc)
        raise

    logger.info(
        "Uploaded %d rows (%d bytes) to r2://%s/%s",
        len(df),
        len(body),
        bucket,
        key,
    )
    return key


def download_dataframe(key: str) -> pd.DataFrame:
    """Download a bronze-layer Parquet object from R2 into a DataFrame.

    Used by the transform step to read the persisted bronze snapshot rather
    than relying on an in-memory DataFrame. This guarantees the transform
    operates on the official raw record set.

    Args:
        key: The full R2 object key to fetch.

    Returns:
        The parsed DataFrame.

    Raises:
        botocore.exceptions.ClientError: If the object does not exist
            or the request fails.
    """
    client = _get_client()
    bucket = get_r2_config()["bucket"]

    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        logger.error("R2 download failed for r2://%s/%s: %s", bucket, key, exc)
        raise

    body = response["Body"].read()
    logger.info("Downloaded %d bytes from r2://%s/%s", len(body), bucket, key)
    return pd.read_parquet(io.BytesIO(body), engine="pyarrow")
