"""
Generic S3 uploader for NordicLens raw data lake.

Supports local Parquet → S3 uploads with Hive-style date partitioning:
  s3://<bucket>/<prefix>/year=YYYY/month=MM/<filename>.parquet

Upload is idempotent: existing objects are skipped unless force=True.
"""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Uploader:
    """Uploads local Parquet files to S3 with optional Hive-style date partitioning."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
    ) -> None:
        self.bucket = bucket or os.environ["S3_BUCKET_NAME"]
        self.region = region or os.environ.get("AWS_DEFAULT_REGION", "eu-north-1")
        self._client = boto3.client("s3", region_name=self.region)
        logger.debug("S3Uploader initialised (bucket=%s, region=%s)", self.bucket, self.region)

    def upload_file(
        self,
        local_path: Path,
        s3_prefix: str,
        partition_by_date: bool = True,
        partition_date: Optional[str] = None,
        force: bool = False,
    ) -> str:
        """
        Upload a single local file to S3.

        Args:
            local_path: Path to the local file (typically .parquet).
            s3_prefix: S3 key prefix, e.g. "macro/ssb_cpi". No leading slash.
            partition_by_date: Append year=/month= Hive partition segments.
            partition_date: ISO date (YYYY-MM-DD) for partition key; defaults to today.
            force: Re-upload even if the object already exists in S3.

        Returns:
            Full S3 URI of the uploaded object, e.g. s3://bucket/prefix/.../file.parquet
        """
        local_path = Path(local_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        if partition_by_date:
            ref_date = date.fromisoformat(partition_date) if partition_date else date.today()
            s3_key = (
                f"{s3_prefix}"
                f"/year={ref_date.year}"
                f"/month={ref_date.month:02d}"
                f"/{local_path.name}"
            )
        else:
            s3_key = f"{s3_prefix}/{local_path.name}"

        if not force and self._object_exists(s3_key):
            logger.info("Skipping (already exists): s3://%s/%s", self.bucket, s3_key)
            return f"s3://{self.bucket}/{s3_key}"

        file_size_mb = local_path.stat().st_size / 1024 / 1024
        logger.info(
            "Uploading %s (%.1f MB) → s3://%s/%s",
            local_path.name,
            file_size_mb,
            self.bucket,
            s3_key,
        )
        self._client.upload_file(
            Filename=str(local_path),
            Bucket=self.bucket,
            Key=s3_key,
        )
        s3_uri = f"s3://{self.bucket}/{s3_key}"
        logger.info("Upload complete: %s", s3_uri)
        return s3_uri

    def upload_directory(
        self,
        local_dir: Path,
        s3_prefix: str,
        glob_pattern: str = "**/*.parquet",
        force: bool = False,
    ) -> list[str]:
        """
        Recursively upload all files matching glob_pattern inside local_dir.

        Args:
            local_dir: Local directory to scan.
            s3_prefix: S3 key prefix for all uploaded files.
            glob_pattern: Glob pattern to match files (default: all .parquet files).
            force: Re-upload existing objects.

        Returns:
            List of S3 URIs for all uploaded objects.
        """
        local_dir = Path(local_dir)
        if not local_dir.is_dir():
            raise NotADirectoryError(f"Not a directory: {local_dir}")

        files = sorted(local_dir.glob(glob_pattern))
        if not files:
            logger.warning("No files matched '%s' in %s", glob_pattern, local_dir)
            return []

        logger.info("Uploading %d files from %s → s3://%s/%s", len(files), local_dir, self.bucket, s3_prefix)
        uris: list[str] = []
        for f in files:
            uri = self.upload_file(
                local_path=f,
                s3_prefix=s3_prefix,
                partition_by_date=False,  # directory structure already encodes partition
                force=force,
            )
            uris.append(uri)

        logger.info("Uploaded %d / %d files", len(uris), len(files))
        return uris

    def _object_exists(self, s3_key: str) -> bool:
        """Return True if the S3 object already exists (idempotency guard)."""
        try:
            self._client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise


def main() -> None:
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    uploader = S3Uploader()

    uploads = [
        (Path("data/raw/transactions"), "transactions"),
        (Path("data/raw/customers"), "customers"),
        (Path("data/raw/macro/ssb_cpi"), "macro/ssb_cpi"),
        (Path("data/raw/macro/norges_bank_rates"), "macro/norges_bank_rates"),
    ]

    total = 0
    for local_dir, s3_prefix in uploads:
        if local_dir.is_dir():
            uris = uploader.upload_directory(local_dir, s3_prefix=s3_prefix)
            total += len(uris)
        else:
            logger.warning("Skipping missing directory: %s", local_dir)

    logger.info("Done — uploaded %d files total", total)


if __name__ == "__main__":
    main()
