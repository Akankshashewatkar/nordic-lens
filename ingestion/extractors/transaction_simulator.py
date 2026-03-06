"""
Synthetic Norwegian banking transaction simulator.

Generates 100,000 realistic transactions with Norwegian merchants,
real MCC codes, and injected anomaly patterns (PaySim-style).

Anomaly patterns injected:
  - Round-number large transactions (velocity + round amount)
  - Late-night high-value activity (00:00–05:00)
  - Transaction velocity bursts (3+ txns in 10 minutes)
  - Unusual foreign transactions for domestic-only customers
"""

import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("data/raw/transactions")
NUM_TRANSACTIONS = 100_000
FRAUD_RATE = 0.05  # 5 %

# Real Norwegian MCC codes → category label
MCC_MAP: dict[str, str] = {
    "4111": "Transport",
    "4121": "Taxi",
    "4814": "Telecom",
    "5411": "Grocery",
    "5541": "Petrol",
    "5812": "Restaurant",
    "5912": "Pharmacy",
    "5942": "Books",
    "7011": "Hotel",
    "7832": "Cinema",
    "8099": "Healthcare",
    "9311": "Government",
}

# Realistic Norwegian merchant names mapped to MCC
NORWEGIAN_MERCHANTS: list[dict] = [
    {"name": "Rema 1000",          "mcc": "5411"},
    {"name": "Kiwi",               "mcc": "5411"},
    {"name": "Meny",               "mcc": "5411"},
    {"name": "Coop Extra",         "mcc": "5411"},
    {"name": "Vinmonopolet",       "mcc": "5912"},
    {"name": "NSB / Vy",           "mcc": "4111"},
    {"name": "Ruter",              "mcc": "4111"},
    {"name": "Flybussen",          "mcc": "4111"},
    {"name": "Oslo Taxi",          "mcc": "4121"},
    {"name": "Telenor",            "mcc": "4814"},
    {"name": "Telia",              "mcc": "4814"},
    {"name": "Circle K",           "mcc": "5541"},
    {"name": "Uno-X",              "mcc": "5541"},
    {"name": "McDonald's Oslo",    "mcc": "5812"},
    {"name": "Peppes Pizza",       "mcc": "5812"},
    {"name": "Starbucks Aker Brygge", "mcc": "5812"},
    {"name": "Apotek 1",           "mcc": "5912"},
    {"name": "Norli Bokhandel",    "mcc": "5942"},
    {"name": "Scandic Hotels",     "mcc": "7011"},
    {"name": "Filmstaden",         "mcc": "7832"},
    {"name": "Legevakten",         "mcc": "8099"},
    {"name": "Skatteetaten",       "mcc": "9311"},
]

CHANNELS = ["mobile", "atm", "online", "branch"]
TRANSACTION_TYPES = ["debit", "credit", "transfer"]
COUNTRY_CODES = ["NO", "SE", "DK", "DE", "GB", "US", "ES", "FR", "IT", "TH"]
DOMESTIC_WEIGHT = 0.88  # 88 % of transactions are domestic


@dataclass
class SimulatorConfig:
    n_transactions: int = NUM_TRANSACTIONS
    n_customers: int = 5_000
    n_merchants: int = len(NORWEGIAN_MERCHANTS)
    start_date: datetime = field(default_factory=lambda: datetime(2023, 1, 1))
    end_date: datetime = field(default_factory=lambda: datetime(2024, 12, 31))
    fraud_rate: float = FRAUD_RATE
    seed: int = 42


class TransactionSimulator:
    """Generates synthetic Norwegian banking transactions with anomaly patterns."""

    def __init__(self, config: Optional[SimulatorConfig] = None) -> None:
        self.config = config or SimulatorConfig()
        self.rng = np.random.default_rng(self.config.seed)
        self.fake = Faker("no_NO")
        Faker.seed(self.config.seed)

    def _generate_customers(self) -> pd.DataFrame:
        """
        Generate synthetic customer master data.

        Returns DataFrame with columns:
            customer_id, age, gender, city, is_domestic_only
        """
        # TODO (Phase 2): implement customer generation
        raise NotImplementedError

    def _generate_transactions(self, customers: pd.DataFrame) -> pd.DataFrame:
        """
        Generate the main transactions DataFrame.

        Returns DataFrame with columns:
            transaction_id, customer_id, merchant_id, merchant_name, mcc_code,
            mcc_category, amount_nok, currency, transaction_type, transaction_date,
            is_weekend, hour_of_day, is_flagged, country_code, channel
        """
        # TODO (Phase 2): implement transaction generation with anomaly injection
        raise NotImplementedError

    def _inject_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Inject structured anomaly patterns into the transaction dataset.

        Patterns:
          1. Velocity bursts — 3+ transactions within 10 minutes
          2. Large round amounts — multiples of 1000 NOK > 50,000
          3. Late-night high-value — 00:00–05:00 + amount > 5,000 NOK
          4. Unusual foreign — domestic-only customer with foreign transaction
        """
        # TODO (Phase 2): implement anomaly injection
        raise NotImplementedError

    def save_parquet(self, df: pd.DataFrame) -> Path:
        """Persist transactions as date-partitioned Parquet. Returns output path."""
        # TODO (Phase 2): implement partitioned Parquet write
        raise NotImplementedError

    def run(self) -> pd.DataFrame:
        """Orchestrate simulation and return the final transactions DataFrame."""
        logger.info("Starting transaction simulation (n=%d)", self.config.n_transactions)
        customers = self._generate_customers()
        transactions = self._generate_transactions(customers)
        transactions = self._inject_anomalies(transactions)
        logger.info(
            "Simulation complete — %d transactions, %.1f%% flagged",
            len(transactions),
            transactions["is_flagged"].mean() * 100,
        )
        return transactions


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    sim = TransactionSimulator()
    df = sim.run()
    path = DEFAULT_OUTPUT_DIR / "transactions.parquet"
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("Saved %d rows to %s", len(df), path)


if __name__ == "__main__":
    main()
