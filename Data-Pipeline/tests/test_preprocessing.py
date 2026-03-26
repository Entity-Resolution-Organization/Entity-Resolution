"""
Unit tests for preprocessing.py
Run: pytest tests/test_preprocessing.py -v
"""

import sys

import pandas as pd
import pytest

sys.path.insert(0, "./scripts")

from preprocessing import (AddressCorruptor, DataCorruptor, DataNormalizer,
                           NameCorruptor, PairGenerator, preprocess_dataset)


@pytest.fixture
def sample_record():
    """Sample person record."""
    return pd.Series(
        {
            "id": "1",
            "name": "Robert Smith",
            "address": "123 Main St, New York",
            "dob": "1980-01-01",
        }
    )


@pytest.fixture
def sample_dataframe():
    """Sample dataset with unique cluster IDs."""
    return pd.DataFrame(
        {
            "id": ["1", "2", "3", "4", "5"],
            "name": [
                "Robert Smith",
                "William Jones",
                "James Brown",
                "Michael Davis",
                "Richard Wilson",
            ],
            "address": [
                "123 Main St",
                "456 Oak Ave",
                "789 Elm Rd",
                "321 Pine Dr",
                "654 Maple Ln",
            ],
            "cluster_id": ["1", "2", "3", "4", "5"],
        }
    )


@pytest.fixture
def clustered_dataframe():
    """Sample dataset with shared cluster IDs for pair generation."""
    return pd.DataFrame(
        {
            "id": ["1", "1_var1", "2", "2_var1", "3"],
            "name": [
                "Robert Smith",
                "Rob Smith",
                "William Jones",
                "Will Jones",
                "James Brown",
            ],
            "address": [
                "123 Main St",
                "123 Main Street",
                "456 Oak Ave",
                "456 Oak Avenue",
                "789 Elm Rd",
            ],
            "cluster_id": ["1", "1", "2", "2", "3"],
        }
    )


@pytest.fixture
def preprocessing_config():
    """Preprocessing configuration matching datasets.yaml structure."""
    return {
        "target_records": 20,
        "corruption": {"rate": 0.15},
        "pairs": {"positive": 5, "negative": 5},
    }


class TestDataNormalizer:
    """Test data normalization functions."""

    def test_normalize_name_whitespace(self):
        """Test name normalization removes extra whitespace."""
        assert DataNormalizer.normalize_name("john  smith") == "John Smith"

    def test_normalize_name_case(self):
        """Test name normalization applies title case."""
        assert DataNormalizer.normalize_name("JOHN SMITH") == "John Smith"

    def test_normalize_name_null(self):
        """Test name normalization handles null."""
        assert DataNormalizer.normalize_name(None) == ""

    def test_normalize_address_whitespace(self):
        """Test address normalization removes extra whitespace."""
        assert DataNormalizer.normalize_address("  123  Main  St  ") == "123 Main St"

    def test_normalize_address_null(self):
        """Test address normalization handles null."""
        assert DataNormalizer.normalize_address(None) == ""

    def test_normalize_date_standard_format(self):
        """Test date normalization with standard format."""
        result = DataNormalizer.normalize_date("01/15/1980")
        assert result == "1980-01-15"

    def test_normalize_date_already_normalized(self):
        """Test date normalization with already-correct format."""
        assert DataNormalizer.normalize_date("1980-01-01") == "1980-01-01"

    def test_normalize_date_null(self):
        """Test date normalization handles null."""
        assert DataNormalizer.normalize_date(None) == ""

    def test_normalize_record(self, sample_record):
        """Test full record normalization."""
        normalized = DataNormalizer.normalize_record(sample_record)

        assert normalized["name"] == "Robert Smith"
        assert normalized["address"] == "123 Main St, New York"
        assert normalized["dob"] == "1980-01-01"

    def test_normalize_record_strips_whitespace(self):
        """Test record normalization strips all string fields."""
        record = pd.Series({"id": " 1 ", "name": " John ", "address": " 123 Main "})
        normalized = DataNormalizer.normalize_record(record)

        assert normalized["id"] == "1"
        assert normalized["name"] == "John"


class TestNameCorruptor:
    """Test name corruption functions."""

    def test_apply_nickname_known_name(self):
        """Test nickname application for known name."""
        result = NameCorruptor.apply_nickname("Robert Smith")
        assert result in ["Rob Smith", "Bobby Smith", "Robbie Smith"]

    def test_apply_nickname_unknown_name(self):
        """Test nickname application leaves unknown names unchanged."""
        assert NameCorruptor.apply_nickname("Zephyr Person") == "Zephyr Person"

    def test_swap_first_last(self):
        """Test name swapping."""
        assert NameCorruptor.swap_first_last("John Smith") == "Smith, John"

    def test_swap_single_name(self):
        """Test name swapping with single name."""
        assert NameCorruptor.swap_first_last("SingleName") == "SingleName"

    def test_add_middle_initial(self):
        """Test middle initial addition."""
        result = NameCorruptor.add_middle_initial("John Smith")
        parts = result.split()
        assert len(parts) == 3
        assert "." in parts[1]

    def test_add_middle_initial_already_three_parts(self):
        """Test middle initial not added when name already has three parts."""
        result = NameCorruptor.add_middle_initial("John A Smith")
        assert result == "John A Smith"

    def test_apply_typo_changes_text(self):
        """Test typo introduction modifies text."""
        result = NameCorruptor.apply_typo("Smith")
        assert len(result) in [4, 5]

    def test_apply_typo_short_text_unchanged(self):
        """Test very short text is not corrupted."""
        result = NameCorruptor.apply_typo("AB")
        assert result == "AB"


class TestAddressCorruptor:
    """Test address corruption functions."""

    def test_abbreviate_street(self):
        """Test street abbreviation."""
        result = AddressCorruptor.abbreviate("123 Main Street")
        assert "Street" not in result
        assert any(abbr in result for abbr in ["St", "St.", "Str"])

    def test_expand_abbreviations(self):
        """Test abbreviation expansion."""
        result = AddressCorruptor.expand_abbreviations("123 Main St")
        assert "Street" in result

    def test_apply_typo_high_rate(self):
        """Test address typos at high rate."""
        result = AddressCorruptor.apply_typo("Main Street Avenue", rate=1.0)
        assert result != "Main Street Avenue"

    def test_apply_typo_zero_rate(self):
        """Test no typos at zero rate."""
        result = AddressCorruptor.apply_typo("Main Street", rate=0.0)
        assert result == "Main Street"

    def test_reorder_components_with_comma(self):
        """Test component reordering with comma-separated parts."""
        result = AddressCorruptor.reorder_components("123 Main, Apt 4")
        assert result == "Apt 4, 123 Main"

    def test_reorder_components_without_comma(self):
        """Test reordering returns unchanged text without commas."""
        result = AddressCorruptor.reorder_components("123 Main Street")
        assert result == "123 Main Street"


class TestDataCorruptor:
    """Test main corruption orchestrator."""

    def test_corrupt_record_with_full_rate(self, sample_record):
        """Test corruption at 100% rate changes at least one field."""
        corruptor = DataCorruptor(corruption_rate=1.0)
        corrupted = corruptor.corrupt_record(sample_record)

        assert "name" in corrupted
        assert "address" in corrupted
        assert (
            corrupted["name"] != sample_record["name"]
            or corrupted["address"] != sample_record["address"]
        )

    def test_corrupt_record_with_zero_rate(self, sample_record):
        """Test corruption at 0% rate preserves all fields."""
        corruptor = DataCorruptor(corruption_rate=0.0)
        corrupted = corruptor.corrupt_record(sample_record)

        assert corrupted["name"] == sample_record["name"]
        assert corrupted["address"] == sample_record["address"]

    def test_expand_dataset_reaches_target(self, sample_dataframe):
        """Test dataset expansion reaches target size."""
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)

        assert len(expanded) == 20

    def test_expand_dataset_has_cluster_ids(self, sample_dataframe):
        """Test expanded dataset has cluster_id column."""
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)

        assert "cluster_id" in expanded.columns
        assert expanded["cluster_id"].notna().all()

    def test_expand_dataset_preserves_clusters(self, sample_dataframe):
        """Test cluster_ids map back to original records."""
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)

        unique_clusters = expanded["cluster_id"].unique()
        assert len(unique_clusters) == len(sample_dataframe)

    def test_expand_dataset_variant_ids(self, sample_dataframe):
        """Test variant records get _var suffix IDs."""
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)

        variant_ids = expanded[expanded["id"].astype(str).str.contains("_var")]
        assert len(variant_ids) > 0

    def test_expand_empty_dataset(self):
        """Test expansion of empty dataset."""
        df = pd.DataFrame(columns=["id", "name", "address", "cluster_id"])
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(df, target_size=0)

        assert len(expanded) == 0

    def test_corrupt_record_handles_null_name(self):
        """Test corruption handles null name gracefully."""
        record = pd.Series({"id": "1", "name": None, "address": "123 Main St"})
        corruptor = DataCorruptor(corruption_rate=1.0)
        corrupted = corruptor.corrupt_record(record)

        assert corrupted["name"] is None

    def test_corrupt_record_handles_null_address(self):
        """Test corruption handles null address gracefully."""
        record = pd.Series({"id": "1", "name": "John Smith", "address": None})
        corruptor = DataCorruptor(corruption_rate=1.0)
        corrupted = corruptor.corrupt_record(record)

        assert corrupted["address"] is None


class TestPairGenerator:
    """Test pair generation functions."""

    def test_generate_positive_pairs(self, clustered_dataframe):
        """Test positive pair generation from clustered data."""
        pairs = PairGenerator.generate_positive_pairs(clustered_dataframe, n_pairs=3)

        assert len(pairs) <= 3
        assert (pairs["label"] == 1).all()
        assert "id1" in pairs.columns
        assert "name1" in pairs.columns
        assert "address1" in pairs.columns

    def test_generate_negative_pairs(self, sample_dataframe):
        """Test negative pair generation from different clusters."""
        pairs = PairGenerator.generate_negative_pairs(sample_dataframe, n_pairs=5)

        assert len(pairs) == 5
        assert (pairs["label"] == 0).all()

    def test_negative_pairs_from_different_clusters(self, sample_dataframe):
        """Test negative pairs come from different clusters."""
        pairs = PairGenerator.generate_negative_pairs(sample_dataframe, n_pairs=5)

        for _, row in pairs.iterrows():
            cluster1 = sample_dataframe[sample_dataframe["id"] == row["id1"]][
                "cluster_id"
            ].iloc[0]
            cluster2 = sample_dataframe[sample_dataframe["id"] == row["id2"]][
                "cluster_id"
            ].iloc[0]
            assert cluster1 != cluster2

    def test_generate_balanced_pairs(self, clustered_dataframe):
        """Test balanced pair generation."""
        pairs = PairGenerator.generate_pairs(
            clustered_dataframe, n_positive=5, n_negative=5
        )

        assert len(pairs) <= 10
        assert len(pairs) >= 2

    def test_missing_fields_raises_error(self):
        """Test error on missing required fields."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with pytest.raises(ValueError, match="Missing required fields"):
            PairGenerator.generate_positive_pairs(df, n_pairs=1)

    def test_missing_fields_negative_pairs_raises_error(self):
        """Test error on missing required fields for negative pairs."""
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with pytest.raises(ValueError, match="Missing required fields"):
            PairGenerator.generate_negative_pairs(df, n_pairs=1)

    def test_insufficient_clusters_returns_empty(self):
        """Test single cluster returns empty negative pairs."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["A", "B"],
                "address": ["X", "Y"],
                "cluster_id": ["1", "1"],
            }
        )

        result = PairGenerator.generate_negative_pairs(df, n_pairs=1)
        assert len(result) == 0

    def test_pairs_are_shuffled(self, clustered_dataframe):
        """Test that generated pairs are shuffled."""
        pairs = PairGenerator.generate_pairs(
            clustered_dataframe, n_positive=10, n_negative=10
        )

        if len(pairs) > 1:
            # Not all positive first, then all negative (shuffled)
            labels = pairs["label"].tolist()
            # Check it's not perfectly sorted
            assert labels != sorted(labels, reverse=True) or len(set(labels)) == 1


class TestPreprocessDataset:
    """Test main preprocessing pipeline."""

    def test_full_pipeline(self, sample_dataframe, preprocessing_config):
        """Test full preprocessing pipeline produces expected output."""
        accounts, pairs = preprocess_dataset(sample_dataframe, preprocessing_config)

        assert len(accounts) == 20
        assert len(pairs) == 10
        assert "cluster_id" in accounts.columns
        assert "label" in pairs.columns

    def test_preserves_original_records(self, sample_dataframe, preprocessing_config):
        """Test that original records are preserved in expanded dataset."""
        accounts, _ = preprocess_dataset(sample_dataframe, preprocessing_config)

        original_ids = set(sample_dataframe["id"].values)
        expanded_clusters = set(accounts["cluster_id"].values)

        assert original_ids.issubset(expanded_clusters)

    def test_zero_corruption_rate(self, sample_dataframe):
        """Test preprocessing with no corruption."""
        config = {
            "target_records": 10,
            "corruption": {"rate": 0.0},
            "pairs": {"positive": 2, "negative": 2},
        }

        accounts, pairs = preprocess_dataset(sample_dataframe, config)

        assert len(accounts) >= 2
        assert len(pairs) <= 4

    def test_default_config_values(self, sample_dataframe):
        """Test preprocessing with missing config keys uses defaults."""
        config = {}

        accounts, pairs = preprocess_dataset(sample_dataframe, config)

        # Should use defaults: target_records=len(df), rate=0.15, pairs=1000/1000
        assert len(accounts) == len(sample_dataframe)

    def test_pairs_have_correct_columns(self, sample_dataframe, preprocessing_config):
        """Test pairs output has all required columns."""
        _, pairs = preprocess_dataset(sample_dataframe, preprocessing_config)

        expected_cols = {
            "id1",
            "id2",
            "name1",
            "name2",
            "address1",
            "address2",
            "label",
        }
        assert expected_cols.issubset(set(pairs.columns))

    def test_label_distribution(self, sample_dataframe, preprocessing_config):
        """Test pairs have both positive and negative labels."""
        _, pairs = preprocess_dataset(sample_dataframe, preprocessing_config)

        if len(pairs) > 0:
            assert (pairs["label"] == 1).sum() > 0 or preprocessing_config["pairs"][
                "positive"
            ] == 0
            assert (pairs["label"] == 0).sum() > 0 or preprocessing_config["pairs"][
                "negative"
            ] == 0


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        df = pd.DataFrame(columns=["id", "name", "address", "cluster_id"])
        config = {
            "target_records": 0,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 0, "negative": 0},
        }

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 0
        assert len(pairs) == 0

    def test_single_record(self):
        """Test with single record."""
        df = pd.DataFrame(
            {"id": ["1"], "name": ["John Smith"], "address": ["123 Main St"]}
        )
        config = {
            "target_records": 5,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 1, "negative": 0},
        }

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 5
        assert accounts["cluster_id"].nunique() == 1

    def test_null_handling(self):
        """Test handling of null values in records."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John", None],
                "address": ["123 Main", "456 Oak"],
            }
        )
        config = {
            "target_records": 4,
            "corruption": {"rate": 0.1},
            "pairs": {"positive": 1, "negative": 1},
        }

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 4

    def test_large_expansion_factor(self):
        """Test expansion with large factor."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John Smith", "Jane Doe"],
                "address": ["123 Main St", "456 Oak Ave"],
            }
        )
        config = {
            "target_records": 100,
            "corruption": {"rate": 0.25},
            "pairs": {"positive": 10, "negative": 10},
        }

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 100
        assert accounts["cluster_id"].nunique() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
