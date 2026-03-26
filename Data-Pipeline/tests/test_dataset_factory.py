"""
Unit tests for dataset_factory.py
Run: pytest tests/test_dataset_factory.py -v
"""

import sys

import pandas as pd
import pytest

sys.path.insert(0, "./scripts")

from dataset_factory import (
    NCVotersHandler,
    OFACHandler,
    PseudopeopleHandler,
    get_dataset_handler,
)


@pytest.fixture
def sample_config():
    """Sample dataset configuration matching datasets.yaml structure."""
    return {
        "name": "Test Dataset",
        "entity_type": "PERSON",
        "source_url": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV",
        "base_records": 100,
        "target_records": 500,
        "corruption": {"rate": 0.15},
        "pairs": {"positive": 100, "negative": 100},
    }


class TestPseudopeopleHandler:
    """Test Pseudopeople handler."""

    def test_download_generates_records(self, sample_config):
        """Test handler generates correct number of records."""
        handler = PseudopeopleHandler(sample_config)
        df = handler.download()

        assert len(df) == 100
        assert "person_id" in df.columns
        assert "first_name" in df.columns
        assert "last_name" in df.columns
        assert df["first_name"].notna().all()

    def test_normalize_schema(self, sample_config):
        """Test schema normalization to standard columns."""
        handler = PseudopeopleHandler(sample_config)
        raw_df = handler.download()
        normalized_df = handler.normalize_schema(raw_df)

        assert list(normalized_df.columns) == ["id", "name", "address", "dob"]
        assert "person_id" not in normalized_df.columns
        assert "first_name" not in normalized_df.columns

    def test_name_combines_first_last(self, sample_config):
        """Test that first and last names are combined."""
        handler = PseudopeopleHandler(sample_config)
        raw_df = handler.download()
        normalized_df = handler.normalize_schema(raw_df)

        # Name should contain a space (first + last)
        assert normalized_df["name"].str.contains(" ").all()

    def test_subsample_reduces_size(self, sample_config):
        """Test subsampling when data exceeds target."""
        sample_config["target_records"] = 50

        handler = PseudopeopleHandler(sample_config)
        df = handler.download()
        subsampled_df = handler.subsample(df)

        assert len(subsampled_df) == 50

    def test_subsample_preserves_when_smaller(self, sample_config):
        """Test subsample returns all when data is smaller than target."""
        sample_config["target_records"] = 1000

        handler = PseudopeopleHandler(sample_config)
        df = handler.download()
        subsampled_df = handler.subsample(df)

        assert len(subsampled_df) == 100


class TestNCVotersHandler:
    """Test NC Voters handler."""

    def test_download_generates_records(self, sample_config):
        """Test handler generates test data locally."""
        handler = NCVotersHandler(sample_config)
        df = handler.download()

        assert len(df) > 0
        assert "voter_id" in df.columns
        assert "full_name" in df.columns
        assert "city" in df.columns
        assert "state" in df.columns

    def test_normalize_schema(self, sample_config):
        """Test schema normalization combines address fields."""
        handler = NCVotersHandler(sample_config)
        raw_df = handler.download()
        normalized_df = handler.normalize_schema(raw_df)

        assert list(normalized_df.columns) == ["id", "name", "address", "dob"]
        assert "voter_id" not in normalized_df.columns

    def test_address_combines_components(self, sample_config):
        """Test that address components are combined correctly."""
        handler = NCVotersHandler(sample_config)
        raw_df = handler.download()
        normalized_df = handler.normalize_schema(raw_df)

        # Address should contain city, state, zip
        assert normalized_df["address"].str.contains("NC").all()

    def test_all_nc_state(self, sample_config):
        """Test all generated records have NC state."""
        handler = NCVotersHandler(sample_config)
        df = handler.download()

        assert (df["state"] == "NC").all()


class TestOFACHandler:
    """Test OFAC SDN handler."""

    def test_fallback_generates_records(self, sample_config):
        """Test synthetic fallback generates data."""
        handler = OFACHandler(sample_config)
        df = handler._generate_synthetic_data()

        assert len(df) == 100
        assert "ent_num" in df.columns
        assert "sdn_name" in df.columns
        assert "program" in df.columns

    def test_normalize_schema(self, sample_config):
        """Test schema normalization."""
        handler = OFACHandler(sample_config)
        raw_df = handler._generate_synthetic_data()
        normalized_df = handler.normalize_schema(raw_df)

        assert "id" in normalized_df.columns
        assert "name" in normalized_df.columns
        assert "address" in normalized_df.columns
        assert len(normalized_df.columns) == 3  # id, name, address only (no dob)

    def test_synthetic_data_has_programs(self, sample_config):
        """Test synthetic data includes sanctions programs."""
        handler = OFACHandler(sample_config)
        df = handler._generate_synthetic_data()

        valid_programs = {
            "SDGT",
            "IRAN",
            "SYRIA",
            "DPRK",
            "VENEZUELA",
            "CUBA",
            "RUSSIA",
        }
        assert set(df["program"].unique()).issubset(valid_programs)

    def test_synthetic_data_has_nationalities(self, sample_config):
        """Test synthetic data includes nationality in remarks."""
        handler = OFACHandler(sample_config)
        df = handler._generate_synthetic_data()

        assert df["remarks"].str.contains("Nationality:").all()


class TestFactoryFunction:
    """Test factory function."""

    def test_returns_pseudopeople_handler(self, sample_config):
        """Test factory returns correct handler for pseudopeople."""
        handler = get_dataset_handler("pseudopeople", sample_config)
        assert isinstance(handler, PseudopeopleHandler)

    def test_returns_nc_voters_handler(self, sample_config):
        """Test factory returns correct handler for nc_voters."""
        handler = get_dataset_handler("nc_voters", sample_config)
        assert isinstance(handler, NCVotersHandler)

    def test_returns_ofac_handler(self, sample_config):
        """Test factory returns correct handler for ofac_sdn."""
        handler = get_dataset_handler("ofac_sdn", sample_config)
        assert isinstance(handler, OFACHandler)

    def test_invalid_dataset_raises_error(self, sample_config):
        """Test factory raises error for unknown dataset."""
        with pytest.raises(ValueError, match="Unknown dataset"):
            get_dataset_handler("invalid_dataset", sample_config)

    def test_error_message_lists_available(self, sample_config):
        """Test error message includes available datasets."""
        with pytest.raises(ValueError, match="pseudopeople"):
            get_dataset_handler("invalid_dataset", sample_config)


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_minimal_config(self):
        """Test handler with minimal required config."""
        config = {
            "name": "Minimal Test",
            "entity_type": "PERSON",
            "base_records": 10,
            "target_records": 10,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 5, "negative": 5},
        }
        handler = PseudopeopleHandler(config)
        df = handler.download()

        assert len(df) == 10

    def test_single_record(self):
        """Test handler with single record."""
        config = {
            "name": "Single Record",
            "entity_type": "PERSON",
            "base_records": 1,
            "target_records": 1,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 1, "negative": 1},
        }
        handler = PseudopeopleHandler(config)
        df = handler.download()
        normalized = handler.normalize_schema(df)

        assert len(normalized) == 1
        assert list(normalized.columns) == ["id", "name", "address", "dob"]

    def test_download_with_retry_attribute(self, sample_config):
        """Test that handlers have retry capability."""
        handler = NCVotersHandler(sample_config)
        assert hasattr(handler, "download_with_retry")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
