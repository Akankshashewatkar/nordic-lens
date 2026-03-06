"""
Norges Bank interest rate extractor.

Fetches the Norwegian key policy rate (styringsrenten / KPRA) from the
Norges Bank SDMX-JSON API and persists as Parquet.

The rate is published on business days the decision is made. This extractor
stores the raw daily observations; dbt aggregates to monthly via MAX per month.

API reference: https://data.norges-bank.no/api/data/IR/B.KPRA.SD.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_API_BASE = "https://data.norges-bank.no/api/data/IR/B.KPRA.SD."
DEFAULT_OUTPUT_DIR = Path("data/raw/macro/norges_bank_rates")


class NorgesBankRateExtractor:
    """Extracts Norwegian key policy rate data from Norges Bank SDMX-JSON API."""

    def __init__(self, output_dir: Path = DEFAULT_OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, start_period: str = "2015-01-01", end_period: Optional[str] = None) -> dict:
        """
        GET SDMX-JSON response from Norges Bank API.

        Args:
            start_period: ISO date (YYYY-MM-DD) for start of range.
            end_period: ISO date for end of range; defaults to today.

        Returns:
            Raw SDMX-JSON response as a dict.
        """
        if end_period is None:
            end_period = date.today().isoformat()

        url = (
            f"{_API_BASE}"
            f"?format=sdmx-json"
            f"&startPeriod={start_period}"
            f"&endPeriod={end_period}"
            f"&locale=en"
        )
        logger.info("Fetching Norges Bank key policy rate %s → %s", start_period, end_period)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.debug("Norges Bank API response received")
        return data

    def parse(self, raw: dict) -> pd.DataFrame:
        """
        Parse an SDMX-JSON response into a tidy DataFrame.

        SDMX-JSON layout used by Norges Bank:
          structure.dimensions.observation[0] → TIME_PERIOD values list
          dataSets[0].series["0:0:0"].observations → {obs_index: [value, ...]}

        Returns:
            DataFrame with columns: period, value, rate_type, _source
        """
        # API wraps payload under "data" key alongside "meta"
        payload = raw.get("data", raw)
        structure = payload["structure"]
        obs_dims = structure["dimensions"]["observation"]

        # Find the TIME_PERIOD dimension
        time_values: list[dict] = []
        for dim in obs_dims:
            if dim["id"] == "TIME_PERIOD":
                time_values = dim["values"]
                break

        if not time_values:
            raise ValueError("TIME_PERIOD dimension not found in SDMX-JSON response")

        # Extract the first (only) series — key policy rate has one series
        dataset = payload["dataSets"][0]
        series_key = next(iter(dataset["series"]))
        observations: dict[str, list] = dataset["series"][series_key]["observations"]

        records = []
        for obs_idx_str, obs_vals in observations.items():
            obs_idx = int(obs_idx_str)
            value = obs_vals[0]  # first element is the observation value
            if value is None:
                continue
            period_id: str = time_values[obs_idx]["id"]  # e.g. "2023-03-23"
            records.append(
                {
                    "period": pd.Timestamp(period_id).date(),
                    "value": float(value),
                    "rate_type": "policy_rate_pct",
                    "_source": "NORGES_BANK",
                }
            )

        df = pd.DataFrame(records).sort_values("period").reset_index(drop=True)
        logger.info(
            "Parsed %d rate observations (%s → %s)",
            len(df),
            df["period"].min() if len(df) else "n/a",
            df["period"].max() if len(df) else "n/a",
        )
        return df

    def save_parquet(self, df: pd.DataFrame) -> Path:
        """Write DataFrame to Parquet. Returns the output path."""
        path = self.output_dir / "rates.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
        logger.info("Wrote %d rows → %s", len(df), path)
        return path

    def run(self, start_period: str = "2015-01-01") -> Path:
        """Orchestrate fetch → parse → save. Returns output Parquet path."""
        logger.info("Starting Norges Bank rate extraction")
        raw = self.fetch(start_period=start_period)
        df = self.parse(raw)
        path = self.save_parquet(df)
        logger.info("Norges Bank extraction complete — %d rows → %s", len(df), path)
        return path


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    NorgesBankRateExtractor().run()


if __name__ == "__main__":
    main()
