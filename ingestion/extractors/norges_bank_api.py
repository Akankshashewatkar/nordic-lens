"""
Norges Bank interest rate extractor.

Fetches the Norwegian key policy rate (styringsrenten) from the
Norges Bank SDMX-JSON API and persists as Parquet.

API reference: https://data.norges-bank.no/api/data/IR/B.KPRA.SD.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

NORGES_BANK_API_URL = (
    "https://data.norges-bank.no/api/data/IR/B.KPRA.SD."
    "?format=sdmx-json&startPeriod={start}&endPeriod={end}&locale=en"
)
DEFAULT_OUTPUT_DIR = Path("data/raw/macro/norges_bank_rates")


class NorgesBankRateExtractor:
    """Extracts Norwegian key policy rate data from Norges Bank SDMX API."""

    def __init__(self, output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, start_period: str = "2015-01-01", end_period: Optional[str] = None) -> dict:
        """
        GET SDMX-JSON response from Norges Bank API.

        Args:
            start_period: ISO date string for start of range.
            end_period: ISO date string for end of range; defaults to today.

        Returns:
            Raw SDMX-JSON response as a dict.
        """
        # TODO (Phase 2): implement GET request with date range substitution
        raise NotImplementedError

    def parse(self, raw: dict) -> pd.DataFrame:
        """
        Parse SDMX-JSON response into a tidy DataFrame.

        Returns DataFrame with columns:
            period (str YYYY-MM-DD), rate_pct (float), rate_type (str)
        """
        # TODO (Phase 2): implement SDMX-JSON → DataFrame parsing
        raise NotImplementedError

    def save_parquet(self, df: pd.DataFrame) -> Path:
        """Persist DataFrame as Parquet. Returns output path."""
        # TODO (Phase 2): implement partitioned Parquet write
        raise NotImplementedError

    def run(self) -> Path:
        """Orchestrate fetch → parse → save. Returns output Parquet path."""
        logger.info("Starting Norges Bank rate extraction")
        raw = self.fetch()
        df = self.parse(raw)
        path = self.save_parquet(df)
        logger.info("Norges Bank extraction complete — %d rows written to %s", len(df), path)
        return path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    extractor = NorgesBankRateExtractor()
    extractor.run()


if __name__ == "__main__":
    main()
