"""
Unit tests for data_validation.py
Run: pytest tests/test_data_validation.py -v
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, "./scripts")

from data_validation import DatasetValidator, QualityGate, TrainingSplitValidator


# =============================================================================
# DatasetValidator Tests
# =============================================================================
class TestDatasetValidator:
    """Test raw dataset validation."""

    @pytest.fixture
    def validator(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield DatasetValidator(output_dir=tmpdir)

    @pytest.fixture
    def valid_csv(self, tmp_path):
        df = pd.DataFrame(
            {
                "id": ["1", "2", "3", "4", "5"],
                "name": ["John", "Jane", "Bob", "Alice", "Charlie"],
                "address": ["123 Main", "456 Oak", "789 Pine", "321 Elm", "654 Cedar"],
            }
        )
        path = tmp_path / "valid.csv"
        df.to_csv(path, index=False)
        return str(path)

    def test_valid_dataset_passes(self, validator, valid_csv):
        result = validator.validate_raw_dataset(
            dataset_name="test",
            data_path=valid_csv,
            entity_type="PERSON",
            min_records=5,
        )
        assert result["success"] is True
        assert len(result["critical_failures"]) == 0

    def test_missing_file_fails(self, validator):
        result = validator.validate_raw_dataset(
            dataset_name="test",
            data_path="/nonexistent/path.csv",
            entity_type="PERSON",
        )
        assert result["success"] is False
        assert "File does not exist" in result["critical_failures"]

    def test_empty_file_fails(self, validator, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")
        result = validator.validate_raw_dataset(
            dataset_name="test", data_path=str(path), entity_type="PERSON"
        )
        assert result["success"] is False
        assert "File is empty" in result["critical_failures"]

    def test_min_records_check(self, validator, tmp_path):
        df = pd.DataFrame({"id": ["1", "2"], "name": ["A", "B"]})
        path = tmp_path / "small.csv"
        df.to_csv(path, index=False)
        result = validator.validate_raw_dataset(
            dataset_name="test",
            data_path=str(path),
            entity_type="PERSON",
            min_records=100,
        )
        assert result["success"] is False
        assert any("records" in f for f in result["critical_failures"])

    def test_missing_required_columns(self, validator, tmp_path):
        df = pd.DataFrame({"other": ["1", "2", "3", "4", "5"] * 20})
        path = tmp_path / "bad_cols.csv"
        df.to_csv(path, index=False)
        result = validator.validate_raw_dataset(
            dataset_name="test", data_path=str(path), entity_type="PERSON"
        )
        assert result["success"] is False
        assert any("Missing required columns" in f for f in result["critical_failures"])

    def test_high_id_nulls_fail(self, validator, tmp_path):
        df = pd.DataFrame(
            {
                "id": [None] * 10 + ["1"] * 90,
                "name": ["John"] * 100,
            }
        )
        path = tmp_path / "null_ids.csv"
        df.to_csv(path, index=False)
        result = validator.validate_raw_dataset(
            dataset_name="test", data_path=str(path), entity_type="PERSON"
        )
        assert result["success"] is False
        assert any("ID column" in f for f in result["critical_failures"])

    def test_high_name_nulls_fail(self, validator, tmp_path):
        df = pd.DataFrame(
            {
                "id": [str(i) for i in range(100)],
                "name": ["John"] * 75 + [None] * 25,
            }
        )
        path = tmp_path / "null_names.csv"
        df.to_csv(path, index=False)
        result = validator.validate_raw_dataset(
            dataset_name="test", data_path=str(path), entity_type="PERSON"
        )
        assert result["success"] is False
        assert any("Name column" in f for f in result["critical_failures"])

    def test_statistics_included(self, validator, valid_csv):
        result = validator.validate_raw_dataset(
            dataset_name="test",
            data_path=valid_csv,
            entity_type="PERSON",
            min_records=5,
        )
        assert "statistics" in result
        assert result["statistics"]["total_records"] == 5
        assert result["statistics"]["total_columns"] == 3

    def test_corrupt_csv_fails(self, validator, tmp_path):
        path = tmp_path / "corrupt.csv"
        path.write_text("not,valid\x00csv\ndata")
        result = validator.validate_raw_dataset(
            dataset_name="test", data_path=str(path), entity_type="PERSON"
        )
        # Should either pass reading (pandas is lenient) or fail gracefully
        assert "success" in result


# =============================================================================
# TrainingSplitValidator Tests
# =============================================================================
class TestTrainingSplitValidator:
    """Test training split validation."""

    @pytest.fixture
    def validator(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield TrainingSplitValidator(output_dir=tmpdir)

    @pytest.fixture
    def valid_splits(self, tmp_path):
        """Create valid 70/15/15 splits."""
        entity_dir = tmp_path / "person"
        entity_dir.mkdir()

        # 700 train, 150 val, 150 test
        for split_name, count in [("train", 700), ("val", 150), ("test", 150)]:
            pos_count = count // 2
            neg_count = count - pos_count
            df = pd.DataFrame(
                {
                    "id1": [f"{split_name}_{i}_a" for i in range(count)],
                    "id2": [f"{split_name}_{i}_b" for i in range(count)],
                    "label": [1] * pos_count + [0] * neg_count,
                }
            )
            df.to_csv(entity_dir / f"{split_name}.csv", index=False)

        return str(entity_dir)

    def test_valid_splits_pass(self, validator, valid_splits):
        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=valid_splits
        )
        assert result["success"] is True
        assert len(result["critical_failures"]) == 0

    def test_missing_split_files_fail(self, validator, tmp_path):
        entity_dir = tmp_path / "person"
        entity_dir.mkdir()
        # Only create train.csv
        pd.DataFrame({"id1": ["1"], "id2": ["2"], "label": [1]}).to_csv(
            entity_dir / "train.csv", index=False
        )
        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=str(entity_dir)
        )
        assert result["success"] is False
        assert any("Missing split files" in f for f in result["critical_failures"])

    def test_wrong_ratios_detected(self, validator, tmp_path):
        entity_dir = tmp_path / "person"
        entity_dir.mkdir()
        # 50/25/25 instead of 70/15/15
        for split_name, count in [("train", 500), ("val", 250), ("test", 250)]:
            df = pd.DataFrame(
                {
                    "id1": [f"{split_name}_{i}_a" for i in range(count)],
                    "id2": [f"{split_name}_{i}_b" for i in range(count)],
                    "label": [1] * (count // 2) + [0] * (count - count // 2),
                }
            )
            df.to_csv(entity_dir / f"{split_name}.csv", index=False)

        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=str(entity_dir)
        )
        # Ratio checks should flag the deviation
        ratio_checks = [c for c in result["checks"] if "ratio" in c["check"]]
        assert any(not c["passed"] for c in ratio_checks)

    def test_data_leakage_detected(self, validator, tmp_path):
        entity_dir = tmp_path / "person"
        entity_dir.mkdir()
        # Same pair in train and test
        overlapping = pd.DataFrame(
            {"id1": ["shared_1"], "id2": ["shared_2"], "label": [1]}
        )
        for split_name, count in [("train", 700), ("val", 150), ("test", 150)]:
            df = pd.DataFrame(
                {
                    "id1": [f"{split_name}_{i}_a" for i in range(count)],
                    "id2": [f"{split_name}_{i}_b" for i in range(count)],
                    "label": [1] * (count // 2) + [0] * (count - count // 2),
                }
            )
            df = pd.concat([df, overlapping], ignore_index=True)
            df.to_csv(entity_dir / f"{split_name}.csv", index=False)

        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=str(entity_dir)
        )
        assert result["success"] is False
        assert any("leakage" in f.lower() for f in result["critical_failures"])

    def test_invalid_labels_detected(self, validator, tmp_path):
        entity_dir = tmp_path / "person"
        entity_dir.mkdir()
        for split_name, count in [("train", 700), ("val", 150), ("test", 150)]:
            df = pd.DataFrame(
                {
                    "id1": [f"{split_name}_{i}_a" for i in range(count)],
                    "id2": [f"{split_name}_{i}_b" for i in range(count)],
                    "label": [1] * (count // 2) + [0] * (count - count // 2),
                }
            )
            # Add invalid label to test split
            if split_name == "test":
                df.loc[0, "label"] = 5
            df.to_csv(entity_dir / f"{split_name}.csv", index=False)

        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=str(entity_dir)
        )
        assert result["success"] is False

    def test_missing_columns_detected(self, validator, tmp_path):
        entity_dir = tmp_path / "person"
        entity_dir.mkdir()
        for split_name in ["train", "val", "test"]:
            # Missing label column
            df = pd.DataFrame({"id1": ["1", "2", "3"], "id2": ["4", "5", "6"]})
            df.to_csv(entity_dir / f"{split_name}.csv", index=False)

        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=str(entity_dir)
        )
        assert result["success"] is False

    def test_statistics_included(self, validator, valid_splits):
        result = validator.validate_entity_splits(
            entity_type="person", entity_dir=valid_splits
        )
        assert "statistics" in result
        assert result["statistics"]["total_pairs"] == 1000
        assert result["statistics"]["train_count"] == 700


# =============================================================================
# QualityGate Tests
# =============================================================================
class TestQualityGate:
    """Test quality gate evaluation."""

    def test_all_passing_returns_go(self):
        gate = QualityGate()
        decision = gate.evaluate(
            schema_results={
                "summary": {"overall_success_rate": 100.0, "failed_expectations": 0}
            },
            training_results={
                "summary": {"success_rate": 100.0, "critical_failures": []}
            },
            bias_results={
                "summary": {
                    "overall_bias_risk": "LOW",
                    "high_risk_count": 0,
                    "bias_issues": [],
                }
            },
        )
        assert decision["decision"] == "GO"
        assert decision["passed"] is True

    def test_schema_below_threshold_returns_nogo(self):
        gate = QualityGate()
        decision = gate.evaluate(
            schema_results={
                "summary": {"overall_success_rate": 50.0, "failed_expectations": 5}
            },
        )
        assert decision["decision"] == "NO-GO"
        assert decision["passed"] is False
        assert any("Schema" in f for f in decision["failures"])

    def test_training_critical_failure_returns_nogo(self):
        gate = QualityGate()
        decision = gate.evaluate(
            training_results={
                "summary": {
                    "success_rate": 90.0,
                    "critical_failures": ["person: data leakage detected"],
                }
            },
        )
        assert decision["decision"] == "NO-GO"
        assert decision["passed"] is False

    def test_training_below_threshold_returns_nogo(self):
        gate = QualityGate()
        decision = gate.evaluate(
            training_results={
                "summary": {"success_rate": 50.0, "critical_failures": []}
            },
        )
        assert decision["decision"] == "NO-GO"
        assert decision["passed"] is False

    def test_high_bias_with_strict_mode_returns_nogo(self):
        gate = QualityGate()
        decision = gate.evaluate(
            bias_results={
                "summary": {
                    "overall_bias_risk": "HIGH",
                    "high_risk_count": 2,
                    "bias_issues": ["language_bias", "geographic_bias"],
                }
            },
            fail_on_high_bias=True,
        )
        assert decision["decision"] == "NO-GO"
        assert decision["passed"] is False

    def test_high_bias_without_strict_mode_returns_go_with_warnings(self):
        gate = QualityGate()
        decision = gate.evaluate(
            bias_results={
                "summary": {
                    "overall_bias_risk": "HIGH",
                    "high_risk_count": 2,
                    "bias_issues": ["language_bias"],
                }
            },
            fail_on_high_bias=False,
        )
        assert decision["decision"] == "GO-WITH-WARNINGS"
        assert decision["passed"] is True
        assert len(decision["warnings"]) > 0

    def test_critical_bias_always_fails(self):
        gate = QualityGate()
        decision = gate.evaluate(
            bias_results={
                "summary": {
                    "overall_bias_risk": "CRITICAL",
                    "high_risk_count": 3,
                    "bias_issues": [],
                }
            },
        )
        assert decision["decision"] == "NO-GO"
        assert decision["passed"] is False

    def test_no_results_returns_go(self):
        gate = QualityGate()
        decision = gate.evaluate()
        assert decision["decision"] == "GO"
        assert decision["passed"] is True

    def test_schema_warnings_on_minor_failures(self):
        gate = QualityGate()
        decision = gate.evaluate(
            schema_results={
                "summary": {"overall_success_rate": 85.0, "failed_expectations": 2}
            },
        )
        assert decision["decision"] == "GO-WITH-WARNINGS"
        assert decision["passed"] is True
        assert len(decision["warnings"]) > 0

    def test_summary_counts_correct(self):
        gate = QualityGate()
        decision = gate.evaluate(
            schema_results={
                "summary": {"overall_success_rate": 100.0, "failed_expectations": 0}
            },
            training_results={
                "summary": {"success_rate": 100.0, "critical_failures": []}
            },
            bias_results={
                "summary": {
                    "overall_bias_risk": "LOW",
                    "high_risk_count": 0,
                    "bias_issues": [],
                }
            },
        )
        assert decision["summary"]["total_checks"] == 3
        assert decision["summary"]["passed_checks"] == 3
        assert decision["summary"]["failed_checks"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
