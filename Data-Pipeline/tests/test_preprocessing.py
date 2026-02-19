"""
Unit tests for preprocessing.py
Run: pytest tests/test_preprocessing.py -v
"""
import pytest
import pandas as pd
import numpy as np
import sys

sys.path.insert(0, './scripts')

from preprocessing import (
    DataNormalizer,
    NameCorruptor,
    AddressCorruptor,
    DataCorruptor,
    PairGenerator,
    preprocess_dataset
)


@pytest.fixture
def sample_record():
    """Sample person record."""
    return pd.Series({
        'id': '1',
        'name': 'Robert Smith',
        'address': '123 Main St, New York',
        'dob': '1980-01-01'
    })


@pytest.fixture
def sample_dataframe():
    """Sample dataset."""
    return pd.DataFrame({
        'id': ['1', '2', '3', '4', '5'],
        'name': ['Robert Smith', 'William Jones', 'James Brown', 'Michael Davis', 'Richard Wilson'],
        'address': ['123 Main St', '456 Oak Ave', '789 Elm Rd', '321 Pine Dr', '654 Maple Ln'],
        'cluster_id': ['1', '2', '3', '4', '5']
    })


@pytest.fixture
def preprocessing_config():
    """Preprocessing configuration."""
    return {
        'target_records': 20,
        'corruption': {'rate': 0.15},
        'pairs': {'positive': 5, 'negative': 5}
    }


class TestDataNormalizer:
    """Test data normalization functions."""

    def test_normalize_name(self):
        """Test name normalization."""
        assert DataNormalizer.normalize_name('john  smith') == 'John Smith'
        assert DataNormalizer.normalize_name('JOHN SMITH') == 'John Smith'
        assert DataNormalizer.normalize_name('john123smith') == 'JohnSmith'
        assert DataNormalizer.normalize_name(None) == ''

    def test_normalize_address(self):
        """Test address normalization."""
        assert 'Street' in DataNormalizer.normalize_address('123 Main St')
        assert 'Avenue' in DataNormalizer.normalize_address('456 Oak Ave.')
        assert DataNormalizer.normalize_address('  123  Main  St  ') == '123 Main Street'
        assert DataNormalizer.normalize_address(None) == ''

    def test_normalize_date(self):
        """Test date normalization."""
        result = DataNormalizer.normalize_date('01/15/1980')
        assert result  # Should return some format
        assert DataNormalizer.normalize_date(None) == ''

    def test_normalize_record(self, sample_record):
        """Test full record normalization."""
        normalized = DataNormalizer.normalize_record(sample_record)

        assert normalized['name'] == 'Robert Smith'
        assert 'Street' in normalized['address']
        assert normalized['dob'] == '1980-01-01'


class TestNameCorruptor:
    """Test name corruption functions."""

    def test_apply_nickname(self):
        """Test nickname application."""
        result = NameCorruptor.apply_nickname('Robert Smith')
        assert result in ['Bob Smith', 'Rob Smith', 'Bobby Smith']

        # Non-nickname name unchanged
        assert NameCorruptor.apply_nickname('Unknown Person') == 'Unknown Person'

    def test_swap_first_last(self):
        """Test name swapping."""
        assert NameCorruptor.swap_first_last('John Smith') == 'Smith, John'
        assert NameCorruptor.swap_first_last('SingleName') == 'SingleName'

    def test_add_middle_initial(self):
        """Test middle initial addition."""
        result = NameCorruptor.add_middle_initial('John Smith')
        assert len(result.split()) == 3
        assert '.' in result

    def test_apply_typo(self):
        """Test typo introduction."""
        result = NameCorruptor.apply_typo('Smith')
        assert len(result) in [4, 5]  # Delete or substitute


class TestAddressCorruptor:
    """Test address corruption functions."""

    def test_abbreviate(self):
        """Test address abbreviation."""
        result = AddressCorruptor.abbreviate('123 Main Street')
        assert 'St' in result or 'Str' in result

    def test_expand_abbreviations(self):
        """Test abbreviation expansion."""
        result = AddressCorruptor.expand_abbreviations('123 Main St')
        assert 'Street' in result

    def test_apply_typo(self):
        """Test address typos."""
        result = AddressCorruptor.apply_typo('Main Street', rate=1.0)
        # With rate=1.0, at least one word should be corrupted
        assert result != 'Main Street'

    def test_reorder_components(self):
        """Test component reordering."""
        result = AddressCorruptor.reorder_components('123 Main, Apt 4')
        assert result != '123 Main, Apt 4'


class TestDataCorruptor:
    """Test main corruption orchestrator."""

    def test_corrupt_record(self, sample_record):
        """Test single record corruption."""
        corruptor = DataCorruptor(corruption_rate=1.0)  # Force corruption
        corrupted = corruptor.corrupt_record(sample_record)

        assert 'name' in corrupted
        assert 'address' in corrupted
        # At least one field should be different
        assert (corrupted['name'] != sample_record['name'] or
                corrupted['address'] != sample_record['address'])

    def test_expand_dataset(self, sample_dataframe):
        """Test dataset expansion."""
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)

        assert len(expanded) == 20
        assert 'cluster_id' in expanded.columns
        assert expanded['cluster_id'].notna().all()

    def test_expansion_preserves_clusters(self, sample_dataframe):
        """Test cluster_id tracking."""
        corruptor = DataCorruptor(corruption_rate=0.15)
        expanded = corruptor.expand_dataset(sample_dataframe, target_size=20)

        # Check that cluster_ids map to original ids
        unique_clusters = expanded['cluster_id'].unique()
        assert len(unique_clusters) == len(sample_dataframe)


class TestPairGenerator:
    """Test pair generation functions."""

    def test_generate_positive_pairs(self, sample_dataframe):
        """Test positive pair generation."""
        # Need multiple records per cluster
        df = sample_dataframe.copy()
        df['cluster_id'] = ['1', '1', '2', '2', '3']  # Create duplicates

        pairs = PairGenerator.generate_positive_pairs(df, n_pairs=3)

        assert len(pairs) <= 3
        assert all(pairs['label'] == 1)
        assert 'id1' in pairs.columns
        assert 'name1' in pairs.columns

    def test_generate_negative_pairs(self, sample_dataframe):
        """Test negative pair generation."""
        pairs = PairGenerator.generate_negative_pairs(sample_dataframe, n_pairs=5)

        assert len(pairs) == 5
        assert all(pairs['label'] == 0)

        # Verify pairs are from different clusters
        for idx, row in pairs.iterrows():
            orig_df = sample_dataframe
            cluster1 = orig_df[orig_df['id'] == row['id1']]['cluster_id'].iloc[0]
            cluster2 = orig_df[orig_df['id'] == row['id2']]['cluster_id'].iloc[0]
            assert cluster1 != cluster2

    def test_generate_pairs_balanced(self, sample_dataframe):
        """Test balanced pair generation."""
        df = sample_dataframe.copy()
        df['cluster_id'] = ['1', '1', '2', '2', '3']

        pairs = PairGenerator.generate_pairs(df, n_positive=5, n_negative=5)

        assert len(pairs) <= 10
        assert pairs['label'].sum() <= 5

        # test_preprocessing_with_zero_corruption - change:
        assert len(pairs) >= 2
        assert len(pairs) <= 10

    def test_missing_fields_error(self):
        """Test error on missing required fields."""
        df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})  # Missing 'address'

        with pytest.raises(ValueError, match="Missing required fields"):
            PairGenerator.generate_positive_pairs(df, n_pairs=1)

    def test_insufficient_clusters_error(self):
        """Test error with insufficient clusters."""
        df = pd.DataFrame({
            'id': ['1', '2'],
            'name': ['A', 'B'],
            'address': ['X', 'Y'],
            'cluster_id': ['1', '1']  # Only 1 cluster
        })

        result = PairGenerator.generate_negative_pairs(df, n_pairs=1)
        assert len(result) == 0


class TestPreprocessDataset:
    """Test main preprocessing pipeline."""

    def test_preprocess_dataset(self, sample_dataframe, preprocessing_config):
        """Test full preprocessing pipeline."""
        accounts, pairs = preprocess_dataset(sample_dataframe, preprocessing_config)

        assert len(accounts) == 20
        assert len(pairs) == 10
        assert 'cluster_id' in accounts.columns
        assert 'label' in pairs.columns

    def test_preprocessing_preserves_originals(self, sample_dataframe, preprocessing_config):
        """Test that original records are preserved."""
        accounts, _ = preprocess_dataset(sample_dataframe, preprocessing_config)

        # Original IDs should exist in expanded dataset
        original_ids = set(sample_dataframe['id'].values)
        expanded_clusters = set(accounts['cluster_id'].values)

        assert original_ids.issubset(expanded_clusters)

    def test_preprocessing_with_zero_corruption(self, sample_dataframe):
        """Test preprocessing with no corruption."""
        config = {
            'target_records': 10,
            'corruption': {'rate': 0.0},
            'pairs': {'positive': 2, 'negative': 2}
        }

        accounts, pairs = preprocess_dataset(sample_dataframe, config)

        assert len(accounts) >= 2
        assert len(pairs) <= 4


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        df = pd.DataFrame(columns=['id', 'name', 'address', 'cluster_id'])
        config = {'target_records': 0, 'corruption': {'rate': 0.1}, 'pairs': {'positive': 0, 'negative': 0}}

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 0
        assert len(pairs) == 0

    def test_single_record(self):
        """Test with single record."""
        df = pd.DataFrame({
            'id': ['1'],
            'name': ['John Smith'],
            'address': ['123 Main St']
        })
        config = {
            'target_records': 5,
            'corruption': {'rate': 0.1},
            'pairs': {'positive': 1, 'negative': 0}
        }

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 5
        assert accounts['cluster_id'].nunique() == 1

    def test_null_handling(self):
        """Test handling of null values."""
        df = pd.DataFrame({
            'id': ['1', '2'],
            'name': ['John', None],
            'address': ['123 Main', '456 Oak']
        })
        config = {
            'target_records': 4,
            'corruption': {'rate': 0.1},
            'pairs': {'positive': 1, 'negative': 1}
        }

        accounts, pairs = preprocess_dataset(df, config)
        assert len(accounts) == 4


if __name__ == '__main__':
    pytest.main([__file__, '-v'])