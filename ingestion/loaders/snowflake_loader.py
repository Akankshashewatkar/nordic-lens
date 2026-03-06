"""
Snowflake RAW schema loader.

Loads Parquet files from S3 into Snowflake RAW schema using COPY INTO.
Creates external stages, file formats, and tables if they do not exist.

Target tables:
  RAW.TRANSACTIONS   — synthetic banking transactions
  RAW.CUSTOMERS      — synthetic customer master data
  RAW.MACRO_CPI      — SSB Consumer Price Index
  RAW.MACRO_RATES    — Norges Bank key policy rate
"""

import logging
import os
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)

# DDLs are defined in snowflake_setup.sql; this module only COPY INTOs
RAW_TABLES = ["TRANSACTIONS", "CUSTOMERS", "MACRO_CPI", "MACRO_RATES"]


class SnowflakeLoader:
    """Loads data from S3 into Snowflake RAW schema via COPY INTO."""

    def __init__(self) -> None:
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    def connect(self) -> None:
        """Open Snowflake connection using environment variables."""
        logger.info("Connecting to Snowflake account %s", os.environ["SNOWFLAKE_ACCOUNT"])
        self._conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE", "NORDICLENS_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NORDICLENS_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "NORDICLENS_DB"),
            schema=os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW"),
        )

    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed")

    def ensure_infrastructure(self) -> None:
        """
        Idempotently create stage, file format, and raw tables.
        Reads DDL from snowflake_setup.sql sections.
        """
        # TODO (Phase 2): parse and execute DDL blocks from snowflake_setup.sql
        raise NotImplementedError

    def copy_into(self, table: str, s3_path: str) -> int:
        """
        Execute COPY INTO for a single table from an S3 path.

        Args:
            table: Target RAW schema table name (e.g. "TRANSACTIONS").
            s3_path: S3 URI prefix, e.g. "s3://nordiclens-raw/transactions/".

        Returns:
            Number of rows loaded.
        """
        # TODO (Phase 2): implement COPY INTO with error handling
        raise NotImplementedError

    def load_all(self, s3_paths: dict[str, str]) -> dict[str, int]:
        """
        Load all RAW tables from their respective S3 paths.

        Args:
            s3_paths: Mapping of table name → S3 URI prefix.

        Returns:
            Mapping of table name → rows loaded.
        """
        results: dict[str, int] = {}
        for table, path in s3_paths.items():
            logger.info("Loading RAW.%s from %s", table, path)
            results[table] = self.copy_into(table, path)
        return results

    def __enter__(self) -> "SnowflakeLoader":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()
