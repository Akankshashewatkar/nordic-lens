"""
Generic S3 uploader for NordicLens raw data lake.

Supports local Parquet → S3 uploads with Hive-style date partitioning:
  s3://<bucket>/<prefix>/year=YYYY/month=MM/<filename>.parquet
"""

import logging
import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Uploader:
    """Uploads local Parquet files to S3 with optional date partitioning."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
    ) -> None:
        self.bucket = bucket or os.environ["S3_BUCKET_NAME"]
        self.region = region or os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")
        self._client = boto3.client("s3", region_name=self.region)

    def upload_file(
        self,
        local_path: Path,
        s3_prefix: str,
        partition_by_date: bool = True,
        partition_date: Optional[str] = None,
    ) -> str:
        """
        Upload a single local file to S3.

        Args:
            local_path: Path to the local Parquet file.
            s3_prefix: S3 key prefix, e.g. "macro/ssb_cpi".
            partition_by_date: Whether to add year=/month= partition segments.
            partition_date: ISO date string (YYYY-MM-DD) for partition;
                            defaults to today if partition_by_date is True.

        Returns:
            Full S3 URI of the uploaded object.
        """
        # TODO (Phase 2): implement upload with partitioning logic
        raise NotImplementedError

    def upload_directory(
        self,
        local_dir: Path,
        s3_prefix: str,
        glob_pattern: str = "**/*.parquet",
    ) -> list[str]:
        """
        Recursively upload all matching files in a directory.

        Returns:
            List of S3 URIs for uploaded objects.
        """
        # TODO (Phase 2): implement directory upload
        raise NotImplementedError

    def _object_exists(self, s3_key: str) -> bool:
        """Return True if the S3 object already exists (idempotency check)."""
        try:
            self._client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
