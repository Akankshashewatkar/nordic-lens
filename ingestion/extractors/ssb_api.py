"""
Statistics Norway (SSB) CPI extractor.

Fetches monthly Consumer Price Index data from the SSB JSON-stat API
(table 03013) and persists as Parquet for downstream loading to S3.

API reference: https://www.ssb.no/en/statbank/table/03013
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SSB_API_URL = "https://data.ssb.no/api/v0/en/table/03013"
DEFAULT_OUTPUT_DIR = Path("data/raw/macro/ssb_cpi")


class SSBCPIExtractor:
    """Extracts Norwegian CPI data from Statistics Norway JSON-stat API."""

    def __init__(self, output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, start_year: int = 2015, end_year: Optional[int] = None) -> dict:
        """
        POST to SSB JSON-stat API and return the raw JSON response.

        Args:
            start_year: First year to retrieve (inclusive).
            end_year: Last year to retrieve; defaults to current year.

        Returns:
            Raw API response as a dict.
        """
        # TODO (Phase 2): implement SSB query payload and POST request
        raise NotImplementedError

    def parse(self, raw: dict) -> pd.DataFrame:
        """
        Parse JSON-stat response into a tidy DataFrame.

        Returns DataFrame with columns:
            period (str YYYY-MM), cpi_index (float), base_year (int)
        """
        # TODO (Phase 2): implement JSON-stat → DataFrame parsing
        raise NotImplementedError

    def save_parquet(self, df: pd.DataFrame) -> Path:
        """
        Persist DataFrame as a date-partitioned Parquet file.

        Returns:
            Path to the written Parquet file.
        """
        # TODO (Phase 2): implement partitioned Parquet write
        raise NotImplementedError

    def run(self) -> Path:
        """Orchestrate fetch → parse → save. Returns output Parquet path."""
        logger.info("Starting SSB CPI extraction")
        raw = self.fetch()
        df = self.parse(raw)
        path = self.save_parquet(df)
        logger.info("SSB CPI extraction complete — %d rows written to %s", len(df), path)
        return path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    extractor = SSBCPIExtractor()
    extractor.run()


if __name__ == "__main__":
    main()
