"""
Unit tests for schema_validation.py
Run: pytest tests/test_schema_validation.py -v
"""

import json
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, "./scripts")

from schema_validation import SchemaValidator


@pytest.fixture
def validator():
    """Create SchemaValidator with temp output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield SchemaValidator(output_dir=tmpdir)


@pytest.fixture
def valid_accounts():
    """Valid accounts dataframe."""
    return pd.DataFrame(
        {
            "id": ["1", "2", "3", "4", "5"],
            "name": [
                "John Smith",
                "Jane Doe",
                "Bob Wilson",
                "Alice Brown",
                "Charlie Davis",
            ],
            "address": [
                "123 Main St",
                "456 Oak Ave",
                "789 Pine Rd",
                "321 Elm St",
                "654 Cedar Ln",
            ],
        }
    )


@pytest.fixture
def valid_pairs():
    """Valid pairs dataframe."""
    return pd.DataFrame(
        {
            "id1": ["1", "2", "3", "4", "5"],
            "id2": ["1_var1", "2_var1", "6", "7", "8"],
            "label": [1, 1, 0, 0, 1],
        }
    )


class TestAccountsValidation:
    """Test accounts dataframe validation."""

    def test_valid_accounts_passes(self, validator, valid_accounts):
        """Valid accounts should pass all expectations."""
        result = validator.validate_accounts(valid_accounts)

        assert result["success"] is True
        assert result["statistics"]["failed_expectations"] == 0
        assert result["statistics"]["success_rate"] == 100.0

    def test_missing_id_column_fails(self, validator):
        """Missing id column should fail."""
        df = pd.DataFrame(
            {"name": ["John", "Jane"], "address": ["123 Main", "456 Oak"]}
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False
        assert "id" in result["failed_expectations"][0]["details"]["missing_columns"]

    def test_missing_name_column_fails(self, validator):
        """Missing name column should fail."""
        df = pd.DataFrame({"id": ["1", "2"], "address": ["123 Main", "456 Oak"]})

        result = validator.validate_accounts(df)

        assert result["success"] is False
        assert "name" in result["failed_expectations"][0]["details"]["missing_columns"]

    def test_missing_address_column_fails(self, validator):
        """Missing address column should fail."""
        df = pd.DataFrame({"id": ["1", "2"], "name": ["John", "Jane"]})

        result = validator.validate_accounts(df)

        assert result["success"] is False
        assert (
            "address" in result["failed_expectations"][0]["details"]["missing_columns"]
        )

    def test_null_ids_fail(self, validator):
        """Null ID values should fail."""
        df = pd.DataFrame(
            {
                "id": ["1", None, "3"],
                "name": ["John", "Jane", "Bob"],
                "address": ["123 Main", "456 Oak", "789 Pine"],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False
        null_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_column_values_to_not_be_null"
            and e["column"] == "id"
        ]
        assert len(null_check) == 1
        assert null_check[0]["details"]["null_count"] == 1

    def test_duplicate_ids_fail(self, validator):
        """Duplicate ID values should fail."""
        df = pd.DataFrame(
            {
                "id": ["1", "1", "2"],
                "name": ["John", "John Copy", "Jane"],
                "address": ["123 Main", "123 Main", "456 Oak"],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False
        unique_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_column_values_to_be_unique"
        ]
        assert len(unique_check) == 1
        assert unique_check[0]["details"]["duplicate_count"] == 1

    def test_high_null_names_fail(self, validator):
        """More than 5% null names should fail."""
        df = pd.DataFrame(
            {
                "id": [str(i) for i in range(100)],
                "name": ["John"] * 90 + [None] * 10,
                "address": ["123 Main"] * 100,
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False
        null_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_column_values_to_not_be_null"
            and e.get("column") == "name"
        ]
        assert len(null_check) == 1

    def test_high_null_addresses_fail(self, validator):
        """More than 10% null addresses should fail."""
        df = pd.DataFrame(
            {
                "id": [str(i) for i in range(100)],
                "name": ["John"] * 100,
                "address": ["123 Main"] * 85 + [None] * 15,
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False
        null_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_column_values_to_not_be_null"
            and e.get("column") == "address"
        ]
        assert len(null_check) == 1

    def test_empty_dataframe_fails(self, validator):
        """Empty dataframe should fail row count check."""
        df = pd.DataFrame(columns=["id", "name", "address"])

        result = validator.validate_accounts(df)

        assert result["success"] is False
        row_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_table_row_count_to_be_between"
        ]
        assert len(row_check) == 1

    def test_very_long_names_fail(self, validator):
        """Names longer than 500 chars should fail."""
        long_name = "A" * 600
        df = pd.DataFrame({"id": ["1"], "name": [long_name], "address": ["123 Main"]})

        result = validator.validate_accounts(df)

        assert result["success"] is False

    def test_entity_type_validation_valid(self, validator):
        """Valid entity types should pass."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John", "Jane"],
                "address": ["123 Main", "456 Oak"],
                "entity_type": ["PERSON", "PERSON"],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is True
        assert result["statistics"]["entity_types"] == 1

    def test_entity_type_validation_invalid(self, validator):
        """Invalid entity types should fail."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John", "Jane"],
                "address": ["123 Main", "456 Oak"],
                "entity_type": ["PERSON", "INVALID_TYPE"],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False

    def test_source_dataset_validation(self, validator):
        """Source dataset column should not have nulls."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John", "Jane"],
                "address": ["123 Main", "456 Oak"],
                "source_dataset": ["pseudopeople", None],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is False

    def test_extra_columns_allowed(self, validator):
        """Extra columns beyond required should be allowed."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "name": ["John", "Jane"],
                "address": ["123 Main", "456 Oak"],
                "extra_col": ["a", "b"],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is True

    def test_numeric_ids_handled(self, validator):
        """Numeric IDs should be handled correctly."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["John", "Jane", "Bob"],
                "address": ["123 Main", "456 Oak", "789 Pine"],
            }
        )

        result = validator.validate_accounts(df)

        assert result["success"] is True


class TestPairsValidation:
    """Test pairs dataframe validation."""

    def test_valid_pairs_passes(self, validator, valid_pairs):
        """Valid pairs should pass all expectations."""
        result = validator.validate_pairs(valid_pairs)

        assert result["success"] is True
        assert result["statistics"]["failed_expectations"] == 0
        assert result["statistics"]["success_rate"] == 100.0

    def test_missing_id1_column_fails(self, validator):
        """Missing id1 column should fail."""
        df = pd.DataFrame({"id2": ["1", "2"], "label": [1, 0]})

        result = validator.validate_pairs(df)

        assert result["success"] is False
        assert "id1" in result["failed_expectations"][0]["details"]["missing_columns"]

    def test_missing_label_column_fails(self, validator):
        """Missing label column should fail."""
        df = pd.DataFrame({"id1": ["1", "2"], "id2": ["3", "4"]})

        result = validator.validate_pairs(df)

        assert result["success"] is False
        assert "label" in result["failed_expectations"][0]["details"]["missing_columns"]

    def test_invalid_label_values_fail(self, validator):
        """Label values not in {0, 1} should fail."""
        df = pd.DataFrame(
            {
                "id1": ["1", "2", "3"],
                "id2": ["4", "5", "6"],
                "label": [0, 1, 2],
            }
        )

        result = validator.validate_pairs(df)

        assert result["success"] is False
        label_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_column_values_to_be_in_set"
        ]
        assert len(label_check) == 1
        assert label_check[0]["details"]["invalid_count"] == 1

    def test_duplicate_pairs_fail(self, validator):
        """Duplicate (id1, id2) pairs should fail."""
        df = pd.DataFrame(
            {
                "id1": ["1", "1", "2"],
                "id2": ["4", "4", "5"],
                "label": [1, 0, 1],
            }
        )

        result = validator.validate_pairs(df)

        assert result["success"] is False
        unique_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_compound_columns_to_be_unique"
        ]
        assert len(unique_check) == 1
        assert unique_check[0]["details"]["duplicate_pairs"] == 1

    def test_null_labels_fail(self, validator):
        """Null label values should fail."""
        df = pd.DataFrame(
            {"id1": ["1", "2", "3"], "id2": ["4", "5", "6"], "label": [1, None, 0]}
        )

        result = validator.validate_pairs(df)

        assert result["success"] is False
        null_check = [
            e
            for e in result["failed_expectations"]
            if e["expectation"] == "expect_column_values_to_not_be_null"
            and e.get("column") == "label"
        ]
        assert len(null_check) == 1

    def test_null_id1_fails(self, validator):
        """Null id1 values should fail."""
        df = pd.DataFrame({"id1": [None, "2"], "id2": ["3", "4"], "label": [1, 0]})

        result = validator.validate_pairs(df)

        assert result["success"] is False

    def test_null_id2_fails(self, validator):
        """Null id2 values should fail."""
        df = pd.DataFrame({"id1": ["1", "2"], "id2": [None, "4"], "label": [1, 0]})

        result = validator.validate_pairs(df)

        assert result["success"] is False

    def test_label_distribution_tracked(self, validator, valid_pairs):
        """Label distribution should be tracked in statistics."""
        result = validator.validate_pairs(valid_pairs)

        assert result["statistics"]["positive_pairs"] == 3
        assert result["statistics"]["negative_pairs"] == 2

    def test_float_labels_accepted(self, validator):
        """Float labels (0.0, 1.0) should be handled."""
        df = pd.DataFrame({"id1": ["1", "2"], "id2": ["3", "4"], "label": [0.0, 1.0]})

        result = validator.validate_pairs(df)

        assert result["success"] is True

    def test_entity_type_in_pairs(self, validator):
        """Entity type validation in pairs data."""
        df = pd.DataFrame(
            {
                "id1": ["1", "2"],
                "id2": ["3", "4"],
                "label": [1, 0],
                "entity_type": ["PERSON", "PERSON"],
            }
        )

        result = validator.validate_pairs(df)

        assert result["success"] is True


class TestCombinedValidation:
    """Test combined validation of accounts and pairs."""

    def test_all_valid_passes(self, validator, valid_accounts, valid_pairs):
        """Valid accounts and pairs should pass."""
        result = validator.validate_all(valid_accounts, valid_pairs)

        assert result["overall_success"] is True
        assert result["summary"]["failed_expectations"] == 0
        assert result["summary"]["overall_success_rate"] == 100.0

    def test_invalid_accounts_fails_overall(self, validator, valid_pairs):
        """Invalid accounts should fail overall validation."""
        invalid_accounts = pd.DataFrame(
            {
                "id": ["1", "1"],
                "name": ["John", "Jane"],
                "address": ["123 Main", "456 Oak"],
            }
        )

        result = validator.validate_all(invalid_accounts, valid_pairs)

        assert result["overall_success"] is False
        assert result["accounts_validation"]["success"] is False
        assert result["pairs_validation"]["success"] is True

    def test_invalid_pairs_fails_overall(self, validator, valid_accounts):
        """Invalid pairs should fail overall validation."""
        invalid_pairs = pd.DataFrame(
            {"id1": ["1", "2"], "id2": ["3", "4"], "label": [0, 5]}
        )

        result = validator.validate_all(valid_accounts, invalid_pairs)

        assert result["overall_success"] is False
        assert result["accounts_validation"]["success"] is True
        assert result["pairs_validation"]["success"] is False

    def test_both_invalid_fails(self, validator):
        """Both invalid should fail with details from each."""
        invalid_accounts = pd.DataFrame(
            {"id": ["1", "1"], "name": ["John", "Jane"], "address": ["123", "456"]}
        )
        invalid_pairs = pd.DataFrame(
            {"id1": ["1", "2"], "id2": ["3", "4"], "label": [0, 5]}
        )

        result = validator.validate_all(invalid_accounts, invalid_pairs)

        assert result["overall_success"] is False
        assert result["accounts_validation"]["success"] is False
        assert result["pairs_validation"]["success"] is False
        assert result["summary"]["failed_expectations"] >= 2

    def test_summary_counts_correct(self, validator, valid_accounts, valid_pairs):
        """Summary should correctly aggregate expectation counts."""
        result = validator.validate_all(valid_accounts, valid_pairs)

        total = result["summary"]["total_expectations"]
        passed = result["summary"]["successful_expectations"]
        failed = result["summary"]["failed_expectations"]

        assert total == passed + failed
        assert total > 0


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

            assert saved_data["overall_success"] is True
            assert "accounts_validation" in saved_data
            assert "pairs_validation" in saved_data

    def test_save_results_custom_filename(self, valid_accounts, valid_pairs):
        """Results should be saved with custom filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            validator = SchemaValidator(output_dir=tmpdir)
            result = validator.validate_all(valid_accounts, valid_pairs)
            output_path = validator.save_results(result, filename="custom_results.json")

            assert Path(output_path).exists()
            assert "custom_results" in output_path


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
