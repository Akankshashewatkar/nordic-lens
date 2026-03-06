"""
Unit tests for ingestion.extractors.ssb_api.SSBCPIExtractor.parse()

Tests cover JSON-stat2 parsing logic without any HTTP calls.
"""

import pandas as pd
import pytest

from ingestion.extractors.ssb_api import SSBCPIExtractor


def _make_jsonstat2(codes: list[str], values: list[float | None]) -> dict:
    """Build a minimal JSON-stat2 dict matching SSB's schema."""
    return {
        "dimension": {
            "Tid": {
                "category": {
                    "index": {code: i for i, code in enumerate(codes)},
                }
            }
        },
        "value": values,
    }


@pytest.fixture()
def extractor(tmp_path):
    return SSBCPIExtractor(output_dir=tmp_path)


class TestParse:
    def test_basic_parse_returns_dataframe(self, extractor):
        raw = _make_jsonstat2(["2023M01", "2023M02"], [125.0, 126.0])
        df = extractor.parse(raw, start_year=2023)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_expected_columns(self, extractor):
        raw = _make_jsonstat2(["2023M01"], [125.0])
        df = extractor.parse(raw, start_year=2023)

        assert set(df.columns) >= {"period", "cpi_index", "period_code", "base_year", "_source"}

    def test_period_is_timestamp(self, extractor):
        raw = _make_jsonstat2(["2023M06"], [127.5])
        df = extractor.parse(raw, start_year=2023)

        assert pd.api.types.is_datetime64_any_dtype(df["period"])
        assert df["period"].iloc[0] == pd.Timestamp("2023-06-01")

    def test_start_year_filter_excludes_earlier(self, extractor):
        raw = _make_jsonstat2(["2022M12", "2023M01", "2023M02"], [120.0, 125.0, 126.0])
        df = extractor.parse(raw, start_year=2023)

        assert len(df) == 2
        assert all(df["period"].dt.year >= 2023)

    def test_none_values_are_dropped(self, extractor):
        raw = _make_jsonstat2(["2023M01", "2023M02", "2023M03"], [125.0, None, 127.0])
        df = extractor.parse(raw, start_year=2023)

        assert len(df) == 2
        assert df["cpi_index"].notna().all()

    def test_sparse_value_dict(self, extractor):
        """SSB may return values as {str_index: value} instead of a dense list."""
        raw = {
            "dimension": {
                "Tid": {
                    "category": {
                        "index": {"2023M01": 0, "2023M02": 1, "2023M03": 2},
                    }
                }
            },
            "value": {"0": 125.0, "2": 127.0},  # sparse — index 1 is missing
        }
        df = extractor.parse(raw, start_year=2023)

        assert len(df) == 2
        assert 127.0 in df["cpi_index"].values

    def test_base_year_and_source(self, extractor):
        raw = _make_jsonstat2(["2023M01"], [125.0])
        df = extractor.parse(raw, start_year=2023)

        assert df["base_year"].iloc[0] == 1998
        assert df["_source"].iloc[0] == "SSB"

    def test_period_code_preserved(self, extractor):
        raw = _make_jsonstat2(["2023M07"], [130.0])
        df = extractor.parse(raw, start_year=2023)

        assert df["period_code"].iloc[0] == "2023M07"

    def test_empty_result_when_all_filtered(self, extractor):
        raw = _make_jsonstat2(["2020M01", "2020M06"], [110.0, 111.0])
        df = extractor.parse(raw, start_year=2023)

        assert len(df) == 0

    def test_values_are_float(self, extractor):
        raw = _make_jsonstat2(["2023M01"], [125])  # integer value
        df = extractor.parse(raw, start_year=2023)

        assert df["cpi_index"].dtype == float
