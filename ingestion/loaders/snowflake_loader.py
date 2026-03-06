"""
Snowflake RAW schema loader.

Loads Parquet files from S3 into Snowflake RAW schema using COPY INTO.
Idempotently creates the external S3 stage, Parquet file format, and
all RAW tables on first run.

Target tables:
  RAW.TRANSACTIONS   — synthetic banking transactions
  RAW.CUSTOMERS      — synthetic customer master data
  RAW.MACRO_CPI      — SSB Consumer Price Index
  RAW.MACRO_RATES    — Norges Bank key policy rate
"""

import logging
import os
from textwrap import dedent
from typing import Optional

import snowflake.connector

logger = logging.getLogger(__name__)

# S3 prefix → target RAW table (used by load_all)
S3_TABLE_MAP: dict[str, str] = {
    "TRANSACTIONS": "transactions/",
    "CUSTOMERS": "customers/",
    "MACRO_CPI": "macro/ssb_cpi/",
    "MACRO_RATES": "macro/norges_bank_rates/",
}

# DDL for all RAW tables. _loaded_at uses a server-side DEFAULT so it is
# populated automatically when COPY INTO omits the column.
_DDL_STATEMENTS = [
    # ── External S3 stage ─────────────────────────────────────────────────────
    """
    CREATE STAGE IF NOT EXISTS RAW.NORDICLENS_S3_STAGE
        URL = 's3://{bucket}/'
        CREDENTIALS = (
            AWS_KEY_ID     = '{aws_key_id}'
            AWS_SECRET_KEY = '{aws_secret_key}'
        )
        COMMENT = 'NordicLens raw data lake';
    """,
    # ── Parquet file format ───────────────────────────────────────────────────
    """
    CREATE FILE FORMAT IF NOT EXISTS RAW.NORDICLENS_PARQUET
        TYPE = 'PARQUET'
        SNAPPY_COMPRESSION = TRUE
        BINARY_AS_TEXT = FALSE
        COMMENT = 'NordicLens Parquet format';
    """,
    # ── RAW.TRANSACTIONS ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS RAW.TRANSACTIONS (
        transaction_id   VARCHAR(36)      NOT NULL,
        customer_id      VARCHAR(36)      NOT NULL,
        merchant_id      VARCHAR(36)      NOT NULL,
        merchant_name    VARCHAR(255),
        mcc_code         VARCHAR(4),
        mcc_category     VARCHAR(100),
        amount_nok       NUMBER(18, 2)    NOT NULL,
        currency         VARCHAR(3)       DEFAULT 'NOK',
        transaction_type VARCHAR(20),
        transaction_date TIMESTAMP_NTZ,
        is_weekend       BOOLEAN,
        hour_of_day      NUMBER(2, 0),
        is_flagged       BOOLEAN          DEFAULT FALSE,
        country_code     VARCHAR(2),
        channel          VARCHAR(20),
        _loaded_at       TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ── RAW.CUSTOMERS ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
        customer_id      VARCHAR(36)   NOT NULL,
        first_name       VARCHAR(100),
        last_name        VARCHAR(100),
        date_of_birth    DATE,
        gender           VARCHAR(1),
        city             VARCHAR(100),
        postal_code      VARCHAR(10),
        is_domestic_only BOOLEAN       DEFAULT TRUE,
        customer_since   DATE,
        _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ── RAW.MACRO_CPI ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS RAW.MACRO_CPI (
        period           DATE            NOT NULL,
        cpi_index        NUMBER(10, 4)   NOT NULL,
        period_code      VARCHAR(10),
        base_year        NUMBER(4, 0)    DEFAULT 1998,
        _source          VARCHAR(20)     DEFAULT 'SSB',
        _loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ── RAW.MACRO_RATES ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS RAW.MACRO_RATES (
        period           DATE            NOT NULL,
        value            NUMBER(10, 4)   NOT NULL,
        rate_type        VARCHAR(30)     DEFAULT 'policy_rate_pct',
        _source          VARCHAR(20)     DEFAULT 'NORGES_BANK',
        _loaded_at       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
    );
    """,
]


class SnowflakeLoader:
    """Loads data from S3 into Snowflake RAW schema via COPY INTO."""

    def __init__(self) -> None:
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    # ── Connection management ──────────────────────────────────────────────────

    def connect(self) -> None:
        """Open a Snowflake connection from environment variables."""
        account = os.environ["SNOWFLAKE_ACCOUNT"]
        logger.info("Connecting to Snowflake (%s)", account)
        self._conn = snowflake.connector.connect(
            account=account,
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE", "NORDICLENS_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "NORDICLENS_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "NORDICLENS_DB"),
            schema=os.environ.get("SNOWFLAKE_RAW_SCHEMA", "RAW"),
        )
        logger.info("Connected to Snowflake")

    def disconnect(self) -> None:
        """Close the Snowflake connection if open."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Snowflake connection closed")

    def _execute(self, sql: str) -> list[tuple]:
        """Execute a single SQL statement and return all result rows."""
        assert self._conn is not None, "Call connect() first"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    # ── Infrastructure ─────────────────────────────────────────────────────────

    def ensure_infrastructure(self) -> None:
        """
        Idempotently create the S3 external stage, Parquet file format,
        and all four RAW tables using CREATE IF NOT EXISTS statements.
        """
        bucket = os.environ["S3_BUCKET_NAME"]
        aws_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

        logger.info("Ensuring Snowflake RAW infrastructure")
        for ddl_template in _DDL_STATEMENTS:
            ddl = dedent(
                ddl_template.format(
                    bucket=bucket,
                    aws_key_id=aws_key_id,
                    aws_secret_key=aws_secret_key,
                )
            ).strip()
            # Log only the first line to avoid logging credentials
            first_line = ddl.split("\n")[0]
            logger.debug("Executing: %s ...", first_line)
            self._execute(ddl)

        logger.info("RAW infrastructure is ready")

    # ── Data loading ───────────────────────────────────────────────────────────

    def copy_into(self, table: str, s3_path: str) -> int:
        """
        Execute COPY INTO <table> from an S3 path using the Parquet stage.

        MATCH_BY_COLUMN_NAME maps Parquet columns to table columns automatically;
        any missing columns (e.g. _loaded_at) fall back to their DEFAULT value.

        Args:
            table: RAW schema table name in UPPER CASE (e.g. "TRANSACTIONS").
            s3_path: S3 URI prefix, e.g. "s3://nordiclens-raw/transactions/".
                     The path after the bucket name is appended to the stage URL.

        Returns:
            Number of rows loaded in this run.
        """
        assert self._conn is not None, "Call connect() first"

        # Strip s3://<bucket>/ to get the stage-relative prefix
        bucket = os.environ["S3_BUCKET_NAME"]
        prefix = s3_path.replace(f"s3://{bucket}/", "").rstrip("/")

        sql = dedent(f"""
            COPY INTO RAW.{table}
            FROM @RAW.NORDICLENS_S3_STAGE/{prefix}/
            FILE_FORMAT = (FORMAT_NAME = 'RAW.NORDICLENS_PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE
            PURGE = FALSE;
        """).strip()

        logger.info("COPY INTO RAW.%s from %s", table, s3_path)
        rows = self._execute(sql)

        # Snowflake COPY INTO returns one row per loaded file with row counts
        total_rows = sum(int(r[3]) for r in rows if len(r) > 3 and r[3] is not None)
        logger.info("RAW.%s — loaded %d rows", table, total_rows)
        return total_rows

    def load_all(self, s3_base_uri: Optional[str] = None) -> dict[str, int]:
        """
        Load all four RAW tables from their respective S3 paths.

        Args:
            s3_base_uri: Base S3 URI, e.g. "s3://nordiclens-raw". Reads
                         S3_BUCKET_NAME env var if not provided.

        Returns:
            Mapping of table name → rows loaded.
        """
        bucket = os.environ["S3_BUCKET_NAME"]
        base = (s3_base_uri or f"s3://{bucket}").rstrip("/")

        results: dict[str, int] = {}
        for table, prefix in S3_TABLE_MAP.items():
            results[table] = self.copy_into(table, f"{base}/{prefix}")
        return results

    # ── Context manager ────────────────────────────────────────────────────────

    def __enter__(self) -> "SnowflakeLoader":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    with SnowflakeLoader() as loader:
        loader.ensure_infrastructure()
        results = loader.load_all()

    logger.info("Load complete: %s", results)


if __name__ == "__main__":
    main()
