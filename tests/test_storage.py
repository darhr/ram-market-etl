"""Tests for the R2 storage client in utils/storage.py."""

from __future__ import annotations

import io
import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import BotoCoreError, ClientError

from utils.storage import build_bronze_key, download_dataframe, upload_dataframe


_R2_VARS = {
    "R2_BUCKET_NAME": "ram-market-lake",
    "R2_ENDPOINT_URL": "https://example.r2.cloudflarestorage.com",
    "R2_ACCESS_KEY_ID": "AKIA-test",
    "R2_SECRET_ACCESS_KEY": "secret-test",
}


@pytest.fixture(autouse=True)
def _set_r2_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _R2_VARS.items():
        monkeypatch.setenv(key, value)


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "name": ["KINGSTON FURY 16GB DDR5 6000", "CORSAIR VENGEANCE 32GB DDR4 3600"],
            "price": [89.99, 119.50],
            "store": ["cyc", "compuvision"],
        }
    )


class TestBuildBronzeKey:
    """Tests for the medallion-style key builder."""

    def test_default_uses_now_utc(self) -> None:
        before = datetime.now(timezone.utc)
        key = build_bronze_key()
        after = datetime.now(timezone.utc)

        match = re.fullmatch(
            r"bronze/raw_data/(\d{4}-\d{2}-\d{2})/(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)/raw_ram_data\.parquet",
            key,
        )
        assert match is not None, f"Key does not match expected format: {key!r}"
        assert match.group(1) == before.strftime("%Y-%m-%d")
        assert match.group(1) == after.strftime("%Y-%m-%d")

    def test_explicit_timestamp(self) -> None:
        ts = datetime(2026, 6, 6, 14, 30, 0, tzinfo=timezone.utc)
        key = build_bronze_key(ts)
        assert (
            key
            == "bronze/raw_data/2026-06-06/2026-06-06T14-30-00Z/raw_ram_data.parquet"
        )

    def test_naive_datetime_treated_as_utc_for_format_only(self) -> None:
        ts = datetime(2026, 1, 1, 0, 0, 0)
        key = build_bronze_key(ts)
        assert (
            key
            == "bronze/raw_data/2026-01-01/2026-01-01T00-00-00Z/raw_ram_data.parquet"
        )

    def test_two_runs_same_day_produce_different_keys(self) -> None:
        ts_a = datetime(2026, 6, 6, 0, 0, 0, tzinfo=timezone.utc)
        ts_b = datetime(2026, 6, 6, 6, 0, 0, tzinfo=timezone.utc)
        assert build_bronze_key(ts_a) != build_bronze_key(ts_b)


class TestUploadDataframe:
    """Tests for the upload_dataframe function."""

    def test_upload_calls_put_object_with_parquet_body(self) -> None:
        mock_client = MagicMock()
        df = _sample_df()
        ts = datetime(2026, 6, 6, 14, 30, 0, tzinfo=timezone.utc)

        with patch("utils.storage.boto3.client", return_value=mock_client):
            key = upload_dataframe(df, run_at=ts)

        assert (
            key
            == "bronze/raw_data/2026-06-06/2026-06-06T14-30-00Z/raw_ram_data.parquet"
        )
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == _R2_VARS["R2_BUCKET_NAME"]
        assert call_kwargs["Key"] == key
        assert call_kwargs["ContentType"] == "application/vnd.apache.parquet"
        assert isinstance(call_kwargs["Body"], bytes)
        # Body should be a valid Parquet file: magic bytes 'PAR1' at start and end.
        assert call_kwargs["Body"][:4] == b"PAR1"
        assert call_kwargs["Body"][-4:] == b"PAR1"

    def test_upload_default_uses_current_utc_date(self) -> None:
        mock_client = MagicMock()

        with patch("utils.storage.boto3.client", return_value=mock_client):
            key = upload_dataframe(_sample_df())

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert f"bronze/raw_data/{today}/" in key
        assert key.endswith("/raw_ram_data.parquet")

    def test_upload_reraises_client_error(self) -> None:
        mock_client = MagicMock()
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
            "PutObject",
        )

        with patch("utils.storage.boto3.client", return_value=mock_client):
            with pytest.raises(ClientError):
                upload_dataframe(_sample_df())

    def test_upload_reraises_boto_core_error(self) -> None:
        mock_client = MagicMock()
        mock_client.put_object.side_effect = BotoCoreError()

        with patch("utils.storage.boto3.client", return_value=mock_client):
            with pytest.raises(BotoCoreError):
                upload_dataframe(_sample_df())

    def test_upload_raises_when_env_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("R2_BUCKET_NAME", raising=False)
        with patch("utils.storage.boto3.client") as mock_boto:
            with pytest.raises(RuntimeError, match="R2_BUCKET_NAME"):
                upload_dataframe(_sample_df())
        mock_boto.assert_not_called()


class TestDownloadDataframe:
    """Tests for the download_dataframe function."""

    def test_download_parses_parquet_body(self) -> None:
        df = _sample_df()
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        body = buffer.getvalue()

        mock_body = MagicMock()
        mock_body.read.return_value = body
        mock_client = MagicMock()
        mock_client.get_object.return_value = {"Body": mock_body}

        key = "bronze/raw_data/2026-06-06/2026-06-06T14-30-00Z/raw_ram_data.parquet"
        with patch("utils.storage.boto3.client", return_value=mock_client):
            result = download_dataframe(key)

        assert list(result.columns) == ["name", "price", "store"]
        assert len(result) == 2
        assert result.iloc[0]["name"] == "KINGSTON FURY 16GB DDR5 6000"
        mock_client.get_object.assert_called_once_with(
            Bucket=_R2_VARS["R2_BUCKET_NAME"],
            Key=key,
        )

    def test_download_reraises_client_error(self) -> None:
        mock_client = MagicMock()
        mock_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
            "GetObject",
        )

        with patch("utils.storage.boto3.client", return_value=mock_client):
            with pytest.raises(ClientError):
                download_dataframe(
                    "bronze/raw_data/2026-06-06/2026-06-06T14-30-00Z/raw_ram_data.parquet"
                )

    def test_download_raises_when_env_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("R2_ENDPOINT_URL", raising=False)
        with patch("utils.storage.boto3.client") as mock_boto:
            with pytest.raises(RuntimeError, match="R2_ENDPOINT_URL"):
                download_dataframe(
                    "bronze/raw_data/2026-06-06/2026-06-06T14-30-00Z/raw_ram_data.parquet"
                )
        mock_boto.assert_not_called()
