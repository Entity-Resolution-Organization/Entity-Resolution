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
    """Sample dataset configuration."""
    return {
        "name": "Test Dataset",
        "base_records": 100,
        "target_records": 500,
        "schema": ["id", "name", "address"],
        "corruption": {"rate": 0.15},
        "source_url": "http://test.com",
    }


class TestDatasetHandlers:
    """Test dataset handler classes."""

    def test_pseudopeople_handler(self, sample_config):
        """Test Pseudopeople handler downloads data."""
        handler = PseudopeopleHandler(sample_config)
        df = handler.download()

        assert len(df) == 100
        assert "person_id" in df.columns
        assert "first_name" in df.columns
        assert df["first_name"].notna().all()

    def test_pseudopeople_normalize_schema(self, sample_config):
        """Test schema normalization."""
        handler = PseudopeopleHandler(sample_config)
        raw_df = handler.download()
        normalized_df = handler.normalize_schema(raw_df)

        assert "id" in normalized_df.columns
        assert "name" in normalized_df.columns
        assert "address" in normalized_df.columns
        assert "person_id" not in normalized_df.columns

    def test_nc_voters_handler(self, sample_config):
        """Test NC Voters handler."""
        handler = NCVotersHandler(sample_config)
        df = handler.download()

        assert len(df) > 0
        assert "voter_id" in df.columns
        assert "full_name" in df.columns

    def test_ofac_handler(self, sample_config):
        """Test OFAC handler."""
        handler = OFACHandler(sample_config)
        df = handler.download()

        assert len(df) > 0
        assert "ent_num" in df.columns or "sdn_name" in df.columns

    def test_ofac_expansion(self, sample_config):
        """Test OFAC synthetic expansion."""
        sample_config["expansion_factor"] = 10
        sample_config["target_records"] = 1000

        handler = OFACHandler(sample_config)
        df = handler.download()
        expanded_df = handler.expand(df)

        assert len(expanded_df) == 1000
        assert "id" in expanded_df.columns

    def test_subsample(self, sample_config):
        """Test subsampling large datasets."""
        sample_config["target_records"] = 50

        handler = PseudopeopleHandler(sample_config)
        df = handler.download()
        subsampled_df = handler.subsample(df)

        assert len(subsampled_df) == 50


class TestFactoryFunction:
    """Test factory function."""

    def test_get_dataset_handler_pseudopeople(self, sample_config):
        """Test factory returns correct handler."""
        handler = get_dataset_handler("pseudopeople", sample_config)
        assert isinstance(handler, PseudopeopleHandler)

    def test_get_dataset_handler_nc_voters(self, sample_config):
        """Test factory returns NC Voters handler."""
        handler = get_dataset_handler("nc_voters", sample_config)
        assert isinstance(handler, NCVotersHandler)

    def test_get_dataset_handler_ofac(self, sample_config):
        """Test factory returns OFAC handler."""
        handler = get_dataset_handler("ofac_sdn", sample_config)
        assert isinstance(handler, OFACHandler)

    def test_get_dataset_handler_invalid(self, sample_config):
        """Test factory raises error for invalid dataset."""
        with pytest.raises(ValueError, match="Unknown dataset"):
            get_dataset_handler("invalid_dataset", sample_config)


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_config(self):
        """Test handler with minimal config."""
        config = {
            "name": "Test",
            "base_records": 10,
            "target_records": 10,
            "schema": [],
        }
        handler = PseudopeopleHandler(config)
        df = handler.download()

        assert len(df) == 10

    def test_subsample_smaller_than_original(self, sample_config):
        """Test subsample when target > original."""
        sample_config["target_records"] = 1000

        handler = PseudopeopleHandler(sample_config)
        df = handler.download()  # Only 100 records
        subsampled_df = handler.subsample(df)

        assert len(subsampled_df) == 100  # Returns all


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
