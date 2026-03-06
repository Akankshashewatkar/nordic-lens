"""
Unit tests for the statistical anomaly detection logic.

Tests the core algorithmic functions (Z-score, IQR fences, Isolation Forest)
in isolation — no Airflow, no Snowflake required.
"""

import numpy as np
import pandas as pd
import pytest
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler


# ── IQR fences ────────────────────────────────────────────────────────────────

def _iqr_outlier(series: pd.Series) -> pd.Series:
    """Exact replica of the function in dag_anomaly_detection._compute_statistical_checks."""
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    fence_lo, fence_hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    return (series < fence_lo) | (series > fence_hi)


class TestIQRFences:
    def test_normal_values_not_flagged(self):
        s = pd.Series([100.0, 110.0, 105.0, 95.0, 108.0, 102.0, 98.0])
        result = _iqr_outlier(s)
        assert not result.any()

    def test_extreme_high_value_flagged(self):
        s = pd.Series([100.0, 102.0, 99.0, 101.0, 103.0, 50_000.0])
        result = _iqr_outlier(s)
        # Only the extreme value at index 5 should be flagged
        assert result.iloc[-1]
        assert not result.iloc[:-1].any()

    def test_extreme_low_value_flagged(self):
        s = pd.Series([100.0, 102.0, 99.0, 101.0, 103.0, 0.01])
        result = _iqr_outlier(s)
        assert result.iloc[-1]

    def test_uniform_distribution_no_outliers(self):
        rng = np.random.default_rng(42)
        s = pd.Series(rng.uniform(50, 150, size=200))
        result = _iqr_outlier(s)
        # Very few or no outliers expected in uniform distribution
        assert result.mean() < 0.05

    def test_returns_boolean_series(self):
        s = pd.Series([1.0, 2.0, 3.0, 100.0])
        result = _iqr_outlier(s)
        assert result.dtype == bool

    def test_result_has_same_index(self):
        s = pd.Series([10.0, 20.0, 30.0], index=[5, 10, 15])
        result = _iqr_outlier(s)
        assert list(result.index) == [5, 10, 15]


# ── Z-score per group ──────────────────────────────────────────────────────────

class TestZscorePerGroup:
    def test_outlier_within_group_flagged(self):
        df = pd.DataFrame({
            "mcc_category": ["Grocery"] * 10 + ["Grocery"],
            "amount_nok": [100.0] * 10 + [50_000.0],
        })
        df["z_score"] = df.groupby("mcc_category")["amount_nok"].transform(
            lambda x: stats.zscore(x, nan_policy="omit") if len(x) > 1 else 0.0
        )
        df["flagged"] = df["z_score"].abs() > 3

        assert df["flagged"].iloc[-1]
        assert not df["flagged"].iloc[:-1].any()

    def test_cross_group_outlier_not_flagged(self):
        """A value that's extreme vs all data but normal within its group should not be flagged."""
        df = pd.DataFrame({
            "mcc_category": ["Hotel"] * 5 + ["Grocery"] * 5,
            "amount_nok":   [1500.0, 1600.0, 1550.0, 1520.0, 1480.0,  # normal for Hotel
                             80.0,   90.0,   85.0,   88.0,   82.0],    # normal for Grocery
        })
        df["z_score"] = df.groupby("mcc_category")["amount_nok"].transform(
            lambda x: stats.zscore(x, nan_policy="omit") if len(x) > 1 else 0.0
        )
        df["flagged"] = df["z_score"].abs() > 3
        assert not df["flagged"].any()

    def test_single_element_group_returns_zero(self):
        df = pd.DataFrame({
            "mcc_category": ["Cinema", "Grocery", "Grocery"],
            "amount_nok":   [150.0, 80.0, 90.0],
        })
        df["z_score"] = df.groupby("mcc_category")["amount_nok"].transform(
            lambda x: stats.zscore(x, nan_policy="omit") if len(x) > 1 else 0.0
        )
        cinema_z = df.loc[df["mcc_category"] == "Cinema", "z_score"].iloc[0]
        assert cinema_z == 0.0


# ── Isolation Forest ───────────────────────────────────────────────────────────

class TestIsolationForest:
    def _build_features(self, df: pd.DataFrame) -> np.ndarray:
        imputer = SimpleImputer(strategy="median")
        scaler = StandardScaler()
        return scaler.fit_transform(imputer.fit_transform(df))

    def test_anomaly_rate_near_contamination(self):
        """IsoForest with contamination=0.05 should flag ~5% of points."""
        rng = np.random.default_rng(42)
        n = 1000
        df = pd.DataFrame({
            "amount_nok":        rng.lognormal(5.5, 1.2, n),
            "hour_of_day":       rng.integers(0, 24, n).astype(float),
            "days_since_last_txn": rng.integers(1, 30, n).astype(float),
        })
        features = self._build_features(df)
        iso = IsolationForest(n_estimators=100, contamination=0.05, random_state=42, n_jobs=-1)
        preds = iso.fit_predict(features)
        anomaly_rate = (preds == -1).mean()
        assert abs(anomaly_rate - 0.05) < 0.01  # within 1% of target

    def test_injected_extreme_point_flagged(self):
        """A clearly extreme synthetic point should be detected by IsoForest."""
        rng = np.random.default_rng(42)
        n = 200
        normal = pd.DataFrame({
            "amount_nok":        rng.lognormal(5.5, 0.5, n),
            "hour_of_day":       rng.integers(8, 20, n).astype(float),
            "days_since_last_txn": rng.integers(1, 10, n).astype(float),
        })
        extreme = pd.DataFrame({
            "amount_nok":        [99_000.0],
            "hour_of_day":       [3.0],
            "days_since_last_txn": [365.0],
        })
        df = pd.concat([normal, extreme], ignore_index=True)
        features = self._build_features(df)
        iso = IsolationForest(n_estimators=100, contamination=0.05, random_state=42, n_jobs=-1)
        preds = iso.fit_predict(features)
        # The last row (extreme point) should be flagged as anomaly
        assert preds[-1] == -1

    def test_null_imputation_does_not_crash(self):
        """days_since_last_txn is NULL for first-ever transactions — imputation required."""
        df = pd.DataFrame({
            "amount_nok":        [100.0, 200.0, 150.0, None],
            "hour_of_day":       [10.0, 14.0, 9.0, 3.0],
            "days_since_last_txn": [5.0, None, 7.0, None],
        })
        features = self._build_features(df)
        iso = IsolationForest(n_estimators=10, contamination=0.05, random_state=42)
        preds = iso.fit_predict(features)
        assert len(preds) == 4
