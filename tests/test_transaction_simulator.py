"""
Unit tests for ingestion.extractors.transaction_simulator.TransactionSimulator.

Uses a small config (n=500) for fast CI execution. Tests cover output shape,
column contracts, amount bounds, and anomaly injection counts.

Notes:
- _generate_transactions() returns exactly n_transactions rows (pre-anomaly)
- run() returns > n_transactions rows due to velocity burst injection (+2 rows per burst customer)
- Fraud flag column is `is_flagged` (renamed to is_simulated_fraud in the dbt mart layer)
"""

import pandas as pd
import pytest

from ingestion.extractors.transaction_simulator import (
    MCC_MAP,
    NORWEGIAN_MERCHANTS,
    SimulatorConfig,
    TransactionSimulator,
)

SMALL_CFG = SimulatorConfig(n_transactions=500, n_customers=50, seed=0)


@pytest.fixture(scope="module")
def simulator():
    return TransactionSimulator(config=SMALL_CFG)


@pytest.fixture(scope="module")
def customers(simulator):
    return simulator._generate_customers()


@pytest.fixture(scope="module")
def transactions(simulator, customers):
    """Pre-anomaly transactions — exactly n_transactions rows."""
    return simulator._generate_transactions(customers)


@pytest.fixture(scope="module")
def full_transactions(simulator):
    """Full pipeline: customers + transactions + anomaly injection."""
    return simulator.run()


# ── Customer generation ────────────────────────────────────────────────────────

class TestCustomerGeneration:
    def test_row_count_matches_config(self, customers):
        assert len(customers) == SMALL_CFG.n_customers

    def test_required_columns_present(self, customers):
        required = {
            "customer_id", "first_name", "last_name", "date_of_birth",
            "gender", "city", "postal_code", "is_domestic_only", "customer_since",
        }
        assert required <= set(customers.columns)

    def test_no_null_customer_ids(self, customers):
        assert customers["customer_id"].notna().all()

    def test_gender_values_valid(self, customers):
        assert set(customers["gender"].unique()) <= {"M", "F"}

    def test_is_domestic_only_is_bool(self, customers):
        assert customers["is_domestic_only"].dtype == bool


# ── Transaction generation (pre-anomaly) ─────────────────────────────────────

class TestTransactionGeneration:
    def test_row_count_matches_config(self, transactions):
        assert len(transactions) == SMALL_CFG.n_transactions

    def test_required_columns_present(self, transactions):
        required = {
            "transaction_id", "customer_id", "merchant_id", "merchant_name",
            "mcc_code", "mcc_category", "amount_nok", "transaction_date",
            "hour_of_day", "is_weekend", "channel", "country_code", "is_flagged",
        }
        assert required <= set(transactions.columns)

    def test_amounts_within_bounds(self, transactions):
        assert transactions["amount_nok"].min() >= 10.0
        assert transactions["amount_nok"].max() <= 100_000.0

    def test_hour_of_day_range(self, transactions):
        assert transactions["hour_of_day"].between(0, 23).all()

    def test_mcc_categories_are_known(self, transactions):
        known = set(MCC_MAP.values())
        assert set(transactions["mcc_category"].unique()) <= known

    def test_no_null_transaction_ids(self, transactions):
        assert transactions["transaction_id"].notna().all()

    def test_transaction_date_in_config_range(self, transactions):
        dates = pd.to_datetime(transactions["transaction_date"])
        cfg = SMALL_CFG
        assert dates.min() >= pd.Timestamp(cfg.start_date)
        assert dates.max() <= pd.Timestamp(cfg.end_date)

    def test_channels_are_valid(self, transactions):
        valid = {"mobile", "atm", "online", "branch"}
        assert set(transactions["channel"].unique()) <= valid

    def test_merchant_ids_are_deterministic(self, transactions):
        """Merchant UUIDs are derived from merchant name — same name → same UUID."""
        rema_rows = transactions[transactions["merchant_name"] == "Rema 1000"]
        if len(rema_rows) > 1:
            assert rema_rows["merchant_id"].nunique() == 1

    def test_domestic_only_customers_always_country_no(self, transactions):
        """Domestic-only customers must not have foreign transactions before anomaly injection."""
        domestic_txns = transactions[transactions["_is_domestic_only"].astype(bool)]
        assert (domestic_txns["country_code"] == "NO").all()


# ── Anomaly injection (full pipeline) ────────────────────────────────────────

class TestAnomalyInjection:
    def test_is_flagged_column_exists(self, full_transactions):
        assert "is_flagged" in full_transactions.columns

    def test_row_count_exceeds_config_due_to_velocity_bursts(self, full_transactions):
        # run() injects +2 rows per burst customer (up to 50 customers × 2 = 100 extra)
        assert len(full_transactions) >= SMALL_CFG.n_transactions

    def test_domestic_only_col_dropped_after_injection(self, full_transactions):
        assert "_is_domestic_only" not in full_transactions.columns

    def test_fraud_rate_roughly_correct(self, full_transactions):
        actual_rate = full_transactions["is_flagged"].mean()
        # Anomaly injection pushes rate well above base FRAUD_RATE; allow wide range
        assert 0.05 <= actual_rate <= 0.80

    def test_late_night_anomalies_exist(self, full_transactions):
        late_flagged = full_transactions[
            full_transactions["is_flagged"] & full_transactions["hour_of_day"].between(0, 4)
        ]
        assert len(late_flagged) > 0

    def test_large_round_amounts_injected(self, full_transactions):
        large_round = full_transactions[
            full_transactions["is_flagged"]
            & full_transactions["amount_nok"].isin([50_000.0, 100_000.0, 200_000.0, 500_000.0])
        ]
        assert len(large_round) > 0


# ── Merchant catalogue ─────────────────────────────────────────────────────────

class TestMerchantCatalogue:
    def test_all_merchants_have_required_keys(self):
        for m in NORWEGIAN_MERCHANTS:
            assert "name" in m
            assert "mcc" in m
            assert "weight" in m

    def test_all_merchant_mcc_codes_in_mcc_map(self):
        for m in NORWEGIAN_MERCHANTS:
            assert m["mcc"] in MCC_MAP, f"MCC {m['mcc']} not in MCC_MAP"

    def test_weights_are_positive(self):
        for m in NORWEGIAN_MERCHANTS:
            assert m["weight"] > 0
