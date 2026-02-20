"""
Unit tests for schema_validation.py
Run: pytest tests/test_schema_validation.py -v
"""
import pytest
import pandas as pd
import numpy as np
import tempfile
import json
from pathlib import Path

import sys
sys.path.insert(0, './scripts')

from schema_validation import SchemaValidator, validate_data


@pytest.fixture
def validator():
    """Create SchemaValidator with temp output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield SchemaValidator(output_dir=tmpdir)


@pytest.fixture
def valid_accounts():
    """Valid accounts dataframe."""
    return pd.DataFrame({
        'id': ['1', '2', '3', '4', '5'],
        'name': ['John Smith', 'Jane Doe', 'Bob Wilson', 'Alice Brown', 'Charlie Davis'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm St', '654 Cedar Ln']
    })


@pytest.fixture
def valid_pairs():
    """Valid pairs dataframe."""
    return pd.DataFrame({
        'id1': ['1', '2', '3', '4', '5'],
        'id2': ['1_var1', '2_var1', '6', '7', '8'],
        'label': [1, 1, 0, 0, 1]
    })


class TestAccountsValidation:
    """Test accounts dataframe validation."""

    def test_valid_accounts_passes(self, validator, valid_accounts):
        """Valid accounts should pass all expectations."""
        result = validator.validate_accounts(valid_accounts)

        assert result['success'] == True
        assert result['statistics']['failed_expectations'] == 0
        assert result['statistics']['success_rate'] == 100.0

    def test_missing_id_column_fails(self, validator):
        """Missing id column should fail."""
        df = pd.DataFrame({
            'name': ['John', 'Jane'],
            'address': ['123 Main', '456 Oak']
        })

        result = validator.validate_accounts(df)

        assert result['success'] == False
        assert 'id' in result['failed_expectations'][0]['details']['missing_columns']

    def test_missing_name_column_fails(self, validator):
        """Missing name column should fail."""
        df = pd.DataFrame({
            'id': ['1', '2'],
            'address': ['123 Main', '456 Oak']
        })

        result = validator.validate_accounts(df)

        assert result['success'] == False
        assert 'name' in result['failed_expectations'][0]['details']['missing_columns']

    def test_null_ids_fail(self, validator):
        """Null ID values should fail."""
        df = pd.DataFrame({
            'id': ['1', None, '3'],
            'name': ['John', 'Jane', 'Bob'],
            'address': ['123 Main', '456 Oak', '789 Pine']
        })

        result = validator.validate_accounts(df)

        assert result['success'] == False
        # Find the null check expectation
        null_check = [e for e in result['failed_expectations']
                      if e['expectation'] == 'expect_column_values_to_not_be_null' and e['column'] == 'id']
        assert len(null_check) == 1
        assert null_check[0]['details']['null_count'] == 1

    def test_duplicate_ids_fail(self, validator):
        """Duplicate ID values should fail."""
        df = pd.DataFrame({
            'id': ['1', '1', '2'],  # '1' is duplicated
            'name': ['John', 'John Copy', 'Jane'],
            'address': ['123 Main', '123 Main', '456 Oak']
        })

        result = validator.validate_accounts(df)

        assert result['success'] == False
        # Find the uniqueness check
        unique_check = [e for e in result['failed_expectations']
                        if e['expectation'] == 'expect_column_values_to_be_unique']
        assert len(unique_check) == 1
        assert unique_check[0]['details']['duplicate_count'] == 1

    def test_high_null_names_fail(self, validator):
        """More than 5% null names should fail."""
        # Create 100 records with 10 null names (10%)
        df = pd.DataFrame({
            'id': [str(i) for i in range(100)],
            'name': ['John'] * 90 + [None] * 10,
            'address': ['123 Main'] * 100
        })

        result = validator.validate_accounts(df)

        assert result['success'] == False
        null_check = [e for e in result['failed_expectations']
                      if e['expectation'] == 'expect_column_values_to_not_be_null'
                      and e.get('column') == 'name']
        assert len(null_check) == 1

    def test_empty_dataframe(self, validator):
        """Empty dataframe should fail row count check."""
        df = pd.DataFrame(columns=['id', 'name', 'address'])

        result = validator.validate_accounts(df)

        assert result['success'] == False
        row_check = [e for e in result['failed_expectations']
                     if e['expectation'] == 'expect_table_row_count_to_be_between']
        assert len(row_check) == 1


class TestPairsValidation:
    """Test pairs dataframe validation."""

    def test_valid_pairs_passes(self, validator, valid_pairs):
        """Valid pairs should pass all expectations."""
        result = validator.validate_pairs(valid_pairs)

        assert result['success'] == True
        assert result['statistics']['failed_expectations'] == 0
        assert result['statistics']['success_rate'] == 100.0

    def test_missing_id1_column_fails(self, validator):
        """Missing id1 column should fail."""
        df = pd.DataFrame({
            'id2': ['1', '2'],
            'label': [1, 0]
        })

        result = validator.validate_pairs(df)

        assert result['success'] == False
        assert 'id1' in result['failed_expectations'][0]['details']['missing_columns']

    def test_missing_label_column_fails(self, validator):
        """Missing label column should fail."""
        df = pd.DataFrame({
            'id1': ['1', '2'],
            'id2': ['3', '4']
        })

        result = validator.validate_pairs(df)

        assert result['success'] == False
        assert 'label' in result['failed_expectations'][0]['details']['missing_columns']

    def test_invalid_label_values_fail(self, validator):
        """Label values not in {0, 1} should fail."""
        df = pd.DataFrame({
            'id1': ['1', '2', '3'],
            'id2': ['4', '5', '6'],
            'label': [0, 1, 2]  # 2 is invalid
        })

        result = validator.validate_pairs(df)

        assert result['success'] == False
        label_check = [e for e in result['failed_expectations']
                       if e['expectation'] == 'expect_column_values_to_be_in_set']
        assert len(label_check) == 1
        assert label_check[0]['details']['invalid_count'] == 1

    def test_duplicate_pairs_fail(self, validator):
        """Duplicate (id1, id2) pairs should fail."""
        df = pd.DataFrame({
            'id1': ['1', '1', '2'],  # (1, 4) is duplicated
            'id2': ['4', '4', '5'],
            'label': [1, 0, 1]
        })

        result = validator.validate_pairs(df)

        assert result['success'] == False
        unique_check = [e for e in result['failed_expectations']
                        if e['expectation'] == 'expect_compound_columns_to_be_unique']
        assert len(unique_check) == 1
        assert unique_check[0]['details']['duplicate_pairs'] == 1

    def test_null_labels_fail(self, validator):
        """Null label values should fail."""
        df = pd.DataFrame({
            'id1': ['1', '2', '3'],
            'id2': ['4', '5', '6'],
            'label': [1, None, 0]
        })

        result = validator.validate_pairs(df)

        assert result['success'] == False
        null_check = [e for e in result['failed_expectations']
                      if e['expectation'] == 'expect_column_values_to_not_be_null'
                      and e.get('column') == 'label']
        assert len(null_check) == 1

    def test_label_distribution_tracked(self, validator, valid_pairs):
        """Label distribution should be tracked in results."""
        result = validator.validate_pairs(valid_pairs)

        assert result['statistics']['positive_pairs'] == 3
        assert result['statistics']['negative_pairs'] == 2


class TestCombinedValidation:
    """Test combined validation of accounts and pairs."""

    def test_all_valid_passes(self, validator, valid_accounts, valid_pairs):
        """Valid accounts and pairs should pass."""
        result = validator.validate_all(valid_accounts, valid_pairs)

        assert result['overall_success'] == True
        assert result['summary']['failed_expectations'] == 0
        assert result['summary']['overall_success_rate'] == 100.0

    def test_invalid_accounts_fails_overall(self, validator, valid_pairs):
        """Invalid accounts should fail overall validation."""
        invalid_accounts = pd.DataFrame({
            'id': ['1', '1'],  # duplicate
            'name': ['John', 'Jane'],
            'address': ['123 Main', '456 Oak']
        })

        result = validator.validate_all(invalid_accounts, valid_pairs)

        assert result['overall_success'] == False
        assert result['accounts_validation']['success'] == False
        assert result['pairs_validation']['success'] == True

    def test_invalid_pairs_fails_overall(self, validator, valid_accounts):
        """Invalid pairs should fail overall validation."""
        invalid_pairs = pd.DataFrame({
            'id1': ['1', '2'],
            'id2': ['3', '4'],
            'label': [0, 5]  # 5 is invalid
        })

        result = validator.validate_all(valid_accounts, invalid_pairs)

        assert result['overall_success'] == False
        assert result['accounts_validation']['success'] == True
        assert result['pairs_validation']['success'] == False


class TestResultsSaving:
    """Test saving validation results."""

    def test_save_results_creates_file(self, valid_accounts, valid_pairs):
        """Results should be saved to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            validator = SchemaValidator(output_dir=tmpdir)
            result = validator.validate_all(valid_accounts, valid_pairs)
            output_path = validator.save_results(result)

            assert Path(output_path).exists()

            with open(output_path) as f:
                saved_data = json.load(f)

            assert saved_data['overall_success'] == True
            assert 'accounts_validation' in saved_data
            assert 'pairs_validation' in saved_data

    def test_validate_data_function(self, valid_accounts, valid_pairs):
        """Test the validate_data convenience function."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save test data
            accounts_path = Path(tmpdir) / 'accounts.csv'
            pairs_path = Path(tmpdir) / 'pairs.csv'
            output_dir = Path(tmpdir) / 'metrics'

            valid_accounts.to_csv(accounts_path, index=False)
            valid_pairs.to_csv(pairs_path, index=False)

            result = validate_data(str(accounts_path), str(pairs_path), str(output_dir))

            assert result['overall_success'] == True
            assert (output_dir / 'schema_validation_results.json').exists()


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_extra_columns_allowed(self, validator):
        """Extra columns beyond required should be allowed."""
        df = pd.DataFrame({
            'id': ['1', '2'],
            'name': ['John', 'Jane'],
            'address': ['123 Main', '456 Oak'],
            'extra_col': ['a', 'b']  # Extra column
        })

        result = validator.validate_accounts(df)

        assert result['success'] == True

    def test_numeric_ids_handled(self, validator):
        """Numeric IDs should be handled correctly."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'address': ['123 Main', '456 Oak', '789 Pine']
        })

        result = validator.validate_accounts(df)

        assert result['success'] == True

    def test_very_long_names_fail(self, validator):
        """Names longer than 500 chars should fail (mostly check)."""
        long_name = 'A' * 600
        df = pd.DataFrame({
            'id': ['1'],
            'name': [long_name],
            'address': ['123 Main']
        })

        result = validator.validate_accounts(df)

        # Should fail because 100% of names exceed length limit (needs 99% valid)
        assert result['success'] == False

    def test_float_labels_converted(self, validator):
        """Float labels (0.0, 1.0) should be handled."""
        df = pd.DataFrame({
            'id1': ['1', '2'],
            'id2': ['3', '4'],
            'label': [0.0, 1.0]
        })

        result = validator.validate_pairs(df)

        # Float 0.0 and 1.0 should match int 0 and 1
        assert result['success'] == True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
