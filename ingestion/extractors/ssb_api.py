"""
Statistics Norway (SSB) CPI extractor.

Fetches monthly Consumer Price Index data from the SSB JSON-stat2 API
(table 03013) and persists as Parquet for downstream loading to S3.

API reference: https://www.ssb.no/en/statbank/table/03013
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SSB_API_URL = "https://data.ssb.no/api/v0/en/table/03013"
DEFAULT_OUTPUT_DIR = Path("data/raw/macro/ssb_cpi")

# Request total CPI across all consumption groups, all available months.
# ContentsCode "KpiIndMnd" = monthly CPI index value.
# Konsumgrp "TOTAL" = all-items aggregate.
_SSB_QUERY: dict = {
    "query": [
        {
            "code": "Konsumgrp",
            "selection": {"filter": "item", "values": ["TOTAL"]},
        },
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["KpiIndMnd"]},
        },
    ],
    "response": {"format": "json-stat2"},
}


class SSBCPIExtractor:
    """Extracts Norwegian CPI data from Statistics Norway JSON-stat2 API."""

    def __init__(self, output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, start_year: int = 2015, end_year: Optional[int] = None) -> dict:
        """
        POST to SSB JSON-stat2 API and return the raw response dict.

        Args:
            start_year: Earliest year to include (rows before this are dropped in parse).
            end_year: Latest year to include; defaults to current year.

        Returns:
            Raw JSON-stat2 response as a dict.
        """
        if end_year is None:
            end_year = datetime.now().year

        logger.info("Fetching SSB CPI (table 03013) for %d–%d", start_year, end_year)
        response = requests.post(
            SSB_API_URL,
            json=_SSB_QUERY,
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        logger.debug("SSB API returned %d values", len(data.get("value", [])))
        return data

    def parse(self, raw: dict, start_year: int = 2015) -> pd.DataFrame:
        """
        Parse a JSON-stat2 response into a tidy DataFrame.

        SSB encodes time as "YYYYMmm" (e.g. "2023M01"). Values are a flat
        array ordered by the last (fastest-varying) dimension — Tid.

        Returns:
            DataFrame with columns: period, cpi_index, period_code, base_year, _source
        """
        time_dim = raw["dimension"]["Tid"]["category"]
        # index maps period_code → position in the value array
        time_index: dict[str, int] = time_dim["index"]
        sorted_codes = sorted(time_index, key=lambda k: time_index[k])

        # values may be a list (dense) or dict {str_index: value} (sparse)
        raw_values = raw["value"]
        if isinstance(raw_values, dict):
            values = [raw_values.get(str(i)) for i in range(len(sorted_codes))]
        else:
            values = raw_values

        records = []
        for code, value in zip(sorted_codes, values):
            if value is None:
                continue
            year_str, month_str = code.split("M")
            year = int(year_str)
            if year < start_year:
                continue
            records.append(
                {
                    "period": pd.Timestamp(f"{year_str}-{month_str}-01").date(),
                    "cpi_index": float(value),
                    "period_code": code,
                    "base_year": 1998,
                    "_source": "SSB",
                }
            )

        df = pd.DataFrame(records)
        if df.empty:
            logger.info("Parsed 0 CPI observations (all filtered by start_year)")
        else:
            logger.info(
                "Parsed %d CPI observations (from %s to %s)",
                len(df), df["period"].min(), df["period"].max(),
            )
        return df

    def save_parquet(self, df: pd.DataFrame) -> Path:
        """Write DataFrame to Parquet. Returns the output path."""
        path = self.output_dir / "cpi.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
        logger.info("Wrote %d rows → %s", len(df), path)
        return path

    def run(self, start_year: int = 2015) -> Path:
        """Orchestrate fetch → parse → save. Returns output Parquet path."""
        logger.info("Starting SSB CPI extraction")
        raw = self.fetch(start_year=start_year)
        df = self.parse(raw, start_year=start_year)
        path = self.save_parquet(df)
        logger.info("SSB CPI extraction complete — %d rows → %s", len(df), path)
        return path


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    SSBCPIExtractor().run()


if __name__ == "__main__":
    main()
