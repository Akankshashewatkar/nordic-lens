"""
Synthetic Norwegian banking transaction simulator.

Generates 100,000 realistic transactions with Norwegian merchants,
real MCC codes, and injected anomaly patterns (PaySim-style).

Anomaly patterns injected:
  1. Velocity bursts      — 3+ transactions within 10 minutes per customer
  2. Large round amounts  — multiples of 1,000 NOK exceeding 50,000
  3. Late-night activity  — 00:00–04:59 with amount > 5,000 NOK
  4. Unusual geography    — foreign transaction for domestic-only customers
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("data/raw/transactions")
NUM_TRANSACTIONS = 100_000
FRAUD_RATE = 0.05  # 5 %

# Real Norwegian MCC codes → human-readable category
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

# Realistic Norwegian merchants with real MCC codes.
# Weights reflect approximate market share / transaction frequency.
NORWEGIAN_MERCHANTS: list[dict] = [
    {"name": "Rema 1000",             "mcc": "5411", "weight": 0.12},
    {"name": "Kiwi",                  "mcc": "5411", "weight": 0.10},
    {"name": "Meny",                  "mcc": "5411", "weight": 0.07},
    {"name": "Coop Extra",            "mcc": "5411", "weight": 0.06},
    {"name": "Vinmonopolet",          "mcc": "5912", "weight": 0.04},
    {"name": "Vy (NSB)",              "mcc": "4111", "weight": 0.05},
    {"name": "Ruter",                 "mcc": "4111", "weight": 0.06},
    {"name": "Flybussen",             "mcc": "4111", "weight": 0.02},
    {"name": "Oslo Taxi",             "mcc": "4121", "weight": 0.03},
    {"name": "Telenor",               "mcc": "4814", "weight": 0.04},
    {"name": "Telia",                 "mcc": "4814", "weight": 0.03},
    {"name": "Circle K",              "mcc": "5541", "weight": 0.05},
    {"name": "Uno-X",                 "mcc": "5541", "weight": 0.03},
    {"name": "McDonald's Oslo",       "mcc": "5812", "weight": 0.05},
    {"name": "Peppes Pizza",          "mcc": "5812", "weight": 0.03},
    {"name": "Starbucks Aker Brygge", "mcc": "5812", "weight": 0.02},
    {"name": "Apotek 1",              "mcc": "5912", "weight": 0.03},
    {"name": "Norli Bokhandel",       "mcc": "5942", "weight": 0.02},
    {"name": "Scandic Hotels",        "mcc": "7011", "weight": 0.03},
    {"name": "Filmstaden",            "mcc": "7832", "weight": 0.02},
    {"name": "Legevakten",            "mcc": "8099", "weight": 0.02},
    {"name": "Skatteetaten",          "mcc": "9311", "weight": 0.02},
]

# Normalise merchant weights to sum to 1
_merchant_weights = np.array([m["weight"] for m in NORWEGIAN_MERCHANTS], dtype=float)
_merchant_weights /= _merchant_weights.sum()

CHANNELS = ["mobile", "atm", "online", "branch"]
_CHANNEL_WEIGHTS = [0.50, 0.15, 0.25, 0.10]

TRANSACTION_TYPES = ["debit", "credit", "transfer"]
_TYPE_WEIGHTS = [0.70, 0.20, 0.10]

# Country codes; NO always first (used as domestic sentinel)
FOREIGN_COUNTRY_CODES = ["SE", "DK", "DE", "GB", "US", "ES", "FR", "IT", "TH"]
DOMESTIC_WEIGHT = 0.88  # fraction of transactions that are Norwegian


@dataclass
class SimulatorConfig:
    n_transactions: int = NUM_TRANSACTIONS
    n_customers: int = 5_000
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

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _uuid_batch(self, n: int) -> list[str]:
        return [str(uuid.uuid4()) for _ in range(n)]

    # ── Customer generation ────────────────────────────────────────────────────

    def _generate_customers(self) -> pd.DataFrame:
        """
        Generate synthetic Norwegian customer master data.

        Returns:
            DataFrame with columns: customer_id, first_name, last_name,
            date_of_birth, gender, city, postal_code, is_domestic_only,
            customer_since
        """
        n = self.config.n_customers
        logger.info("Generating %d synthetic customers", n)

        # Norwegian cities weighted by approximate population
        cities = ["Oslo", "Bergen", "Trondheim", "Stavanger", "Kristiansand",
                  "Tromsø", "Drammen", "Fredrikstad", "Sandnes", "Bodø",
                  "Asker", "Lillehammer", "Hamar", "Ålesund", "Porsgrunn"]
        city_weights = np.array([0.22, 0.11, 0.08, 0.07, 0.05,
                                  0.05, 0.04, 0.04, 0.04, 0.03,
                                  0.03, 0.03, 0.03, 0.03, 0.03], dtype=float)
        city_weights /= city_weights.sum()

        genders = self.rng.choice(["M", "F"], size=n)
        city_idx = self.rng.choice(len(cities), size=n, p=city_weights)
        is_domestic_only = self.rng.random(size=n) < 0.70

        records = []
        for i in range(n):
            g = genders[i]
            first = self.fake.first_name_male() if g == "M" else self.fake.first_name_female()
            dob = self.fake.date_of_birth(minimum_age=18, maximum_age=80)
            since = self.fake.date_between(start_date="-10y", end_date="-6m")
            records.append(
                {
                    "customer_id": str(uuid.uuid4()),
                    "first_name": first,
                    "last_name": self.fake.last_name(),
                    "date_of_birth": dob,
                    "gender": g,
                    "city": cities[city_idx[i]],
                    "postal_code": str(self.rng.integers(1000, 9999)),
                    "is_domestic_only": bool(is_domestic_only[i]),
                    "customer_since": since,
                }
            )

        df = pd.DataFrame(records)
        logger.info("Generated %d customers across %d cities", len(df), len(cities))
        return df

    # ── Transaction generation ─────────────────────────────────────────────────

    def _generate_transactions(self, customers: pd.DataFrame) -> pd.DataFrame:
        """
        Vectorised transaction generation.

        Amounts follow a log-normal distribution (median ≈ 245 NOK, max 100k NOK),
        matching typical Norwegian retail transaction distributions.
        Includes a temporary _is_domestic_only column consumed by _inject_anomalies.
        """
        n = self.config.n_transactions
        n_customers = len(customers)
        n_merchants = len(NORWEGIAN_MERCHANTS)
        logger.info("Generating %d transactions", n)

        # ── Random draws (vectorised) ──────────────────────────────────────────
        cust_idx = self.rng.integers(0, n_customers, size=n)
        merch_idx = self.rng.choice(n_merchants, size=n, p=_merchant_weights)

        date_range_sec = int((self.config.end_date - self.config.start_date).total_seconds())
        time_offsets_sec = self.rng.integers(0, date_range_sec, size=n)

        # Log-normal amounts: exp(N(5.5, 1.2)) → median ~245 NOK
        amounts = np.clip(
            np.round(np.exp(self.rng.normal(5.5, 1.2, size=n)), 2),
            10.0,
            100_000.0,
        )

        txn_type_idx = self.rng.choice(len(TRANSACTION_TYPES), size=n, p=_TYPE_WEIGHTS)
        channel_idx = self.rng.choice(len(CHANNELS), size=n, p=_CHANNEL_WEIGHTS)
        is_flagged = self.rng.random(size=n) < self.config.fraud_rate

        # ── Build transaction timestamps ───────────────────────────────────────
        start_ts = pd.Timestamp(self.config.start_date)
        txn_dates = pd.to_datetime(
            [start_ts + pd.Timedelta(seconds=int(s)) for s in time_offsets_sec]
        )
        # DatetimeIndex.hour / .dayofweek return numpy.ndarray in pandas 2.x
        hours = np.asarray(txn_dates.hour)
        is_weekend = np.asarray(txn_dates.dayofweek >= 5)

        # ── Customer attributes ────────────────────────────────────────────────
        cust_ids = customers["customer_id"].values[cust_idx]
        is_domestic = customers["is_domestic_only"].values[cust_idx].astype(bool)

        # Country codes: domestic-only customers always transact in NO
        foreign_draw = self.rng.random(size=n)
        foreign_picks = self.rng.choice(FOREIGN_COUNTRY_CODES, size=n)
        country_codes = np.where(is_domestic | (foreign_draw < DOMESTIC_WEIGHT), "NO", foreign_picks)

        # ── Merchant surrogate IDs (deterministic per merchant name) ───────────
        merch_ids = [
            str(uuid.uuid5(uuid.NAMESPACE_DNS, NORWEGIAN_MERCHANTS[i]["name"]))
            for i in merch_idx
        ]

        df = pd.DataFrame(
            {
                "transaction_id": self._uuid_batch(n),
                "customer_id": cust_ids,
                "merchant_id": merch_ids,
                "merchant_name": [NORWEGIAN_MERCHANTS[i]["name"] for i in merch_idx],
                "mcc_code": [NORWEGIAN_MERCHANTS[i]["mcc"] for i in merch_idx],
                "mcc_category": [MCC_MAP[NORWEGIAN_MERCHANTS[i]["mcc"]] for i in merch_idx],
                "amount_nok": amounts,
                "currency": np.where(country_codes == "NO", "NOK", "EUR"),
                "transaction_type": [TRANSACTION_TYPES[i] for i in txn_type_idx],
                "transaction_date": txn_dates,
                "is_weekend": is_weekend,
                "hour_of_day": hours,
                "is_flagged": is_flagged,
                "country_code": country_codes,
                "channel": [CHANNELS[i] for i in channel_idx],
                # Temp column — used in _inject_anomalies, dropped afterwards
                "_is_domestic_only": is_domestic,
            }
        )

        return df.sort_values("transaction_date").reset_index(drop=True)

    # ── Anomaly injection ──────────────────────────────────────────────────────

    def _inject_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Inject four structured anomaly patterns into the dataset.

        Pattern 1 — Velocity bursts:
            For 50 random customers, insert 2 extra transactions within 10 minutes
            of an existing one, creating 3-transaction velocity bursts.

        Pattern 2 — Large round amounts:
            200 transactions are set to round NOK multiples > 50,000
            (50k, 100k, 200k, 500k).

        Pattern 3 — Late-night high-value:
            300 transactions are moved to hours 0–4 with amounts > 5,000 NOK.

        Pattern 4 — Unusual geography:
            150 domestic-only customers receive a foreign transaction.
        """
        df = df.copy()

        # ── Pattern 1: velocity bursts ─────────────────────────────────────────
        all_customers = df["customer_id"].unique()
        burst_customers = self.rng.choice(
            all_customers, size=min(50, len(all_customers)), replace=False
        )
        burst_rows: list[pd.Series] = []
        for cid in burst_customers:
            base = df[df["customer_id"] == cid].iloc[0]
            base_ts = pd.Timestamp(base["transaction_date"])
            for offset_min in (3, 7):  # +3 min and +7 min → 3 txns within 10 min
                row = base.copy()
                row["transaction_id"] = str(uuid.uuid4())
                row["transaction_date"] = base_ts + pd.Timedelta(minutes=offset_min)
                row["is_flagged"] = True
                burst_rows.append(row)

        if burst_rows:
            df = pd.concat([df, pd.DataFrame(burst_rows)], ignore_index=True)
        logger.debug("Injected %d velocity-burst transactions", len(burst_rows))

        # ── Pattern 2: large round amounts ────────────────────────────────────
        n_round = min(200, len(df))
        round_idx = df.sample(n=n_round, random_state=42).index
        round_amounts = self.rng.choice([50_000, 100_000, 200_000, 500_000], size=n_round)
        df.loc[round_idx, "amount_nok"] = round_amounts.astype(float)
        df.loc[round_idx, "is_flagged"] = True
        logger.debug("Injected %d large-round-amount transactions", n_round)

        # ── Pattern 3: late-night high-value ──────────────────────────────────
        n_late = min(300, len(df))
        late_idx = df.sample(n=n_late, random_state=43).index
        late_hours = self.rng.integers(0, 5, size=n_late).astype(df["hour_of_day"].dtype)
        late_amounts = self.rng.uniform(5_001, 30_000, size=n_late).round(2)
        df.loc[late_idx, "hour_of_day"] = late_hours
        df.loc[late_idx, "amount_nok"] = late_amounts
        # Align the timestamp hour to match
        df.loc[late_idx, "transaction_date"] = [
            ts.replace(hour=int(h))
            for ts, h in zip(
                pd.to_datetime(df.loc[late_idx, "transaction_date"]),
                late_hours,
            )
        ]
        df.loc[late_idx, "is_flagged"] = True
        logger.debug("Injected %d late-night transactions", n_late)

        # ── Pattern 4: unusual geography ──────────────────────────────────────
        domestic_candidates = df[df["_is_domestic_only"].astype(bool)]
        n_geo = min(150, len(domestic_candidates))
        if n_geo > 0:
            geo_idx = domestic_candidates.sample(n=n_geo, random_state=44).index
            df.loc[geo_idx, "country_code"] = self.rng.choice(
                FOREIGN_COUNTRY_CODES, size=n_geo
            )
            df.loc[geo_idx, "currency"] = "EUR"
            df.loc[geo_idx, "is_flagged"] = True
            logger.debug("Injected %d unusual-geography transactions", n_geo)

        # Drop temp column and re-sort
        df = df.drop(columns=["_is_domestic_only"])
        df = df.sort_values("transaction_date").reset_index(drop=True)
        return df

    # ── Parquet output ─────────────────────────────────────────────────────────

    def save_parquet(self, df: pd.DataFrame, output_dir: Optional[Path] = None) -> Path:
        """Write DataFrame to Parquet. Returns the output path."""
        out = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
        out.mkdir(parents=True, exist_ok=True)
        path = out / "transactions.parquet"
        df.to_parquet(path, index=False, engine="pyarrow", use_deprecated_int96_timestamps=True)
        logger.info("Wrote %d transactions → %s", len(df), path)
        return path

    # ── Orchestration ──────────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        """Generate customers → transactions → inject anomalies. Returns DataFrame."""
        logger.info(
            "Starting simulation: %d transactions, seed=%d",
            self.config.n_transactions,
            self.config.seed,
        )
        customers = self._generate_customers()
        transactions = self._generate_transactions(customers)
        transactions = self._inject_anomalies(transactions)
        fraud_pct = transactions["is_flagged"].mean() * 100
        logger.info(
            "Simulation complete — %d transactions (%.1f%% flagged)",
            len(transactions),
            fraud_pct,
        )
        return transactions


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    sim = TransactionSimulator()

    # Generate both DataFrames (customers needed for transactions)
    customers = sim._generate_customers()
    transactions = sim._generate_transactions(customers)
    transactions = sim._inject_anomalies(transactions)

    # Save transactions
    sim.save_parquet(transactions)

    # Save customers separately so s3_uploader can upload them
    customers_dir = Path("data/raw/customers")
    customers_dir.mkdir(parents=True, exist_ok=True)
    customers_path = customers_dir / "customers.parquet"
    customers.to_parquet(customers_path, index=False, engine="pyarrow", use_deprecated_int96_timestamps=True)
    logger.info("Wrote %d customers → %s", len(customers), customers_path)


if __name__ == "__main__":
    main()
