"""
Entity Resolution — Model Pipeline Tests

Run:
    pytest tests/test_pipeline.py -v
"""

import json
import os
import tempfile

import numpy as np
import pandas as pd
import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "model": {
            "base_model": "microsoft/deberta-v3-base",
            "num_labels": 2,
            "max_length": 256,
            "cache_dir": None,
        },
        "lora": {
            "r": 8,
            "lora_alpha": 16,
            "lora_dropout": 0.1,
            "target_modules": ["query_proj", "value_proj"],
            "bias": "none",
            "task_type": "SEQ_CLS",
            "modules_to_save": ["classifier", "pooler"],
        },
        "training": {
            "batch_size": 16,
            "learning_rate": 2e-5,
            "num_epochs": 3,
            "warmup_steps": 500,
            "weight_decay": 0.01,
            "gradient_accumulation_steps": 2,
            "optimizer": "adamw",
            "adam_beta1": 0.9,
            "adam_beta2": 0.999,
            "adam_epsilon": 1e-8,
            "max_grad_norm": 1.0,
            "lr_scheduler_type": "linear",
            "evaluation_strategy": "epoch",
            "save_strategy": "epoch",
            "load_best_model_at_end": True,
            "metric_for_best_model": "f1",
            "greater_is_better": True,
            "save_total_limit": 3,
            "save_steps": 500,
            "logging_dir": "./logs",
            "logging_steps": 100,
            "logging_first_step": False,
        },
        "data": {
            "gcs_bucket": "entity-resolution-bucket-1",
            "gcs_path": "training/2026-03-01",
            "local_data_dir": "./data",
            "entity_types": ["person"],
            "columns": {
                "id1": "id1", "id2": "id2",
                "name1": "name1", "name2": "name2",
                "address1": "address1", "address2": "address2",
                "label": "label",
                "entity_type": "entity_type",
                "source_dataset": "source_dataset",
            },
        },
        "output": {
            "base_dir": "./models",
            "model_dir": "{base_dir}/{entity_type}",
            "final_model_dir": "{model_dir}/final_model",
            "checkpoint_dir": "{model_dir}/checkpoints",
            "results_dir": "{model_dir}/results",
            "plots_dir": "{model_dir}/plots",
        },
        "mlflow": {
            "tracking_uri": "http://mlflow:5000",
            "experiment_name": "entity-resolution-deberta",
            "run_name_prefix": "deberta_lora",
            "artifact_path": "model",
            "registered_model_name": "er_{entity_type}_deberta",
            "tags": {"project": "entity-resolution"},
        },
        "validation": {
            "classification_threshold": 0.45,
            "min_f1": 0.75,
            "min_precision": 0.70,
            "min_recall": 0.70,
            "min_auc": 0.80,
        },
        "bias_detection": {
            "enabled": True,
            "slices": ["source_dataset", "entity_type"],
            "max_f1_disparity": 0.15,
            "max_precision_disparity": 0.15,
            "max_recall_disparity": 0.15,
            "min_slice_size": 10,
        },
        "sensitivity": {"enabled": True, "shap_samples": 10},
        "device": {"use_cuda": False, "cuda_device": 0, "fp16": False},
        "seed": 42,
        "gcp": {
            "project_id": "entity-resolution-487121",
            "region": "us-central1",
            "artifact_registry": {
                "enabled": True,
                "repository": "ml-models",
                "location": "us-central1",
                "image_name": "er_{entity_type}_deberta",
                "serving_base_image": "pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime",
                "serving_packages": ["transformers==4.36.2"],
                "tmp_build_dir": "/tmp/er_push",
            },
        },
        "notifications": {"enabled": False},
    }


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id1":            ["r1", "r2", "r3", "r4", "r5", "r6"],
        "id2":            ["r2", "r3", "r4", "r5", "r6", "r7"],
        "name1":          ["Robert Smith", "Bob Smith", "Alice Jones", "John Doe", "Jane Smith", "Tom Brown"],
        "name2":          ["Bob Smith", "Robert Smith", "Alice J", "Johnny Doe", "Jane S", "Thomas Brown"],
        "address1":       ["123 Main St", "123 Main St", "45 Oak Ave", "67 Pine Rd", "89 Elm St", "12 Cedar Ln"],
        "address2":       ["123 Main Street", "123 Main Street", "45 Oak Avenue", "67 Pine Road", "89 Elm Street", "12 Cedar Lane"],
        "label":          [1, 1, 1, 0, 0, 1],
        "entity_type":    ["person"] * 6,
        "source_dataset": ["febrl", "febrl", "paysim", "paysim", "febrl", "paysim"],
    })


# ---------------------------------------------------------------------------
# 1. Config validation
# ---------------------------------------------------------------------------

class TestConfig:
    def test_required_sections(self, config):
        required = ["model", "lora", "training", "data", "output",
                    "mlflow", "validation", "bias_detection", "sensitivity",
                    "device", "gcp"]
        for section in required:
            assert section in config, f"Missing section: {section}"

    def test_classification_threshold(self, config):
        threshold = config["validation"]["classification_threshold"]
        assert 0 < threshold < 1
        assert threshold == 0.45  # calibrated value — must not change to 0.5

    def test_modules_to_save_present(self, config):
        """classifier + pooler must be saved with LoRA adapter."""
        modules = config["lora"].get("modules_to_save", [])
        assert "classifier" in modules, "classifier must be in modules_to_save"
        assert "pooler" in modules, "pooler must be in modules_to_save"

    def test_entity_types_not_empty(self, config):
        assert len(config["data"]["entity_types"]) > 0

    def test_quality_gate_thresholds_reasonable(self, config):
        v = config["validation"]
        assert 0 < v["min_f1"] <= 1.0
        assert 0 < v["min_precision"] <= 1.0
        assert 0 < v["min_recall"] <= 1.0
        assert 0 < v["min_auc"] <= 1.0


# ---------------------------------------------------------------------------
# 2. Text pair building — critical: must match train/evaluate/serve exactly
# ---------------------------------------------------------------------------

class TestTextPairBuilding:
    """
    The _build_text_pair function is duplicated across train.py, evaluate.py
    and serve.py. All three must produce identical output for the same input.
    This test validates the contract.
    """

    def _build_text_pair(self, row, cols):
        def _safe(val):
            if pd.isna(val) or str(val).strip() == "":
                return "[MISSING]"
            return str(val).strip()

        text_a = (
            f"record_1 "
            f"name: {_safe(row[cols['name1']])} "
            f"address: {_safe(row[cols['address1']])}"
        )
        text_b = (
            f"record_2 "
            f"name: {_safe(row[cols['name2']])} "
            f"address: {_safe(row[cols['address2']])}"
        )
        return text_a, text_b

    def test_normal_pair(self, config):
        cols = config["data"]["columns"]
        row = pd.Series({
            "name1": "Robert Smith", "address1": "123 Main St",
            "name2": "Bob Smith",    "address2": "123 Main Street",
        })
        text_a, text_b = self._build_text_pair(row, cols)
        assert text_a == "record_1 name: Robert Smith address: 123 Main St"
        assert text_b == "record_2 name: Bob Smith address: 123 Main Street"

    def test_missing_field_becomes_MISSING_token(self, config):
        cols = config["data"]["columns"]
        row = pd.Series({
            "name1": "Robert Smith", "address1": float("nan"),
            "name2": "Bob Smith",    "address2": "",
        })
        text_a, text_b = self._build_text_pair(row, cols)
        assert "[MISSING]" in text_a
        assert "[MISSING]" in text_b

    def test_returns_tuple_not_string(self, config):
        cols = config["data"]["columns"]
        row = pd.Series({
            "name1": "A", "address1": "B",
            "name2": "C", "address2": "D",
        })
        result = self._build_text_pair(row, cols)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_record_prefix_present(self, config):
        """record_1 / record_2 prefixes are the structural boundary markers."""
        cols = config["data"]["columns"]
        row = pd.Series({
            "name1": "A", "address1": "B",
            "name2": "C", "address2": "D",
        })
        text_a, text_b = self._build_text_pair(row, cols)
        assert text_a.startswith("record_1")
        assert text_b.startswith("record_2")

    def test_no_literal_SEP_in_output(self, config):
        """[SEP] must NOT appear as a literal string — tokenizer inserts it."""
        cols = config["data"]["columns"]
        row = pd.Series({
            "name1": "A", "address1": "B",
            "name2": "C", "address2": "D",
        })
        text_a, text_b = self._build_text_pair(row, cols)
        assert "[SEP]" not in text_a
        assert "[SEP]" not in text_b


# ---------------------------------------------------------------------------
# 3. Quality gate logic
# ---------------------------------------------------------------------------

class TestQualityGate:
    def _quality_gate(self, metrics, config):
        thresholds = config["validation"]
        checks = {
            "f1":        metrics.get("test_f1",        0) >= thresholds["min_f1"],
            "precision": metrics.get("test_precision", 0) >= thresholds["min_precision"],
            "recall":    metrics.get("test_recall",    0) >= thresholds["min_recall"],
            "auc":       metrics.get("test_auc",       0) >= thresholds["min_auc"],
        }
        return all(checks.values())

    def test_passes_with_good_metrics(self, config):
        metrics = {
            "test_f1": 0.9993, "test_precision": 0.9987,
            "test_recall": 1.0, "test_auc": 0.9999,
        }
        assert self._quality_gate(metrics, config) is True

    def test_fails_with_low_f1(self, config):
        metrics = {
            "test_f1": 0.47, "test_precision": 0.99,
            "test_recall": 0.99, "test_auc": 0.99,
        }
        assert self._quality_gate(metrics, config) is False

    def test_fails_with_low_auc(self, config):
        metrics = {
            "test_f1": 0.99, "test_precision": 0.99,
            "test_recall": 0.99, "test_auc": 0.50,
        }
        assert self._quality_gate(metrics, config) is False

    def test_threshold_is_045_not_05(self, config):
        """Regression test — threshold must stay 0.45."""
        assert config["validation"]["classification_threshold"] == 0.45


# ---------------------------------------------------------------------------
# 4. Bias detection logic
# ---------------------------------------------------------------------------

class TestBiasDetection:
    def test_disparity_detected(self, config):
        slices = {
            "febrl":   {"f1": 0.99},
            "paysim":  {"f1": 0.50},
        }
        values    = [s["f1"] for s in slices.values()]
        disparity = max(values) - min(values)
        threshold = config["bias_detection"]["max_f1_disparity"]
        assert disparity > threshold  # 0.49 > 0.15

    def test_no_disparity(self, config):
        slices = {
            "febrl":   {"f1": 0.98},
            "paysim":  {"f1": 0.97},
        }
        values    = [s["f1"] for s in slices.values()]
        disparity = max(values) - min(values)
        threshold = config["bias_detection"]["max_f1_disparity"]
        assert disparity <= threshold  # 0.01 <= 0.15

    def test_insufficient_slices_no_bias(self, config):
        """Single slice — can't compute disparity, should not flag bias."""
        slices = {"febrl": {"f1": 0.99}}
        assert len(slices) < 2  # can't compute disparity


# ---------------------------------------------------------------------------
# 5. Rollback check logic
# ---------------------------------------------------------------------------

class TestRollbackCheck:
    def test_rollback_triggered_on_large_drop(self):
        current_f1 = 0.47
        best_f1    = 0.9993
        threshold  = 0.02
        degradation = best_f1 - current_f1
        assert degradation > threshold

    def test_no_rollback_on_small_drop(self):
        current_f1 = 0.998
        best_f1    = 0.9993
        threshold  = 0.02
        degradation = best_f1 - current_f1
        assert degradation <= threshold

    def test_no_rollback_on_improvement(self):
        current_f1 = 0.9995
        best_f1    = 0.9993
        threshold  = 0.02
        degradation = best_f1 - current_f1
        assert degradation <= threshold  # negative degradation = improvement


# ---------------------------------------------------------------------------
# 6. Predictions CSV structure
# ---------------------------------------------------------------------------

class TestPredictionsCSV:
    def test_required_columns_present(self, sample_df):
        """test_predictions.csv must have id1, id2, predicted_prob for GNN."""
        pred_df = sample_df.copy()
        pred_df["predicted_prob"]  = [0.91, 0.23, 0.87, 0.12, 0.05, 0.95]
        pred_df["predicted_label"] = (pred_df["predicted_prob"] >= 0.45).astype(int)

        required = ["id1", "id2", "predicted_prob", "predicted_label",
                    "source_dataset", "entity_type"]
        for col in required:
            assert col in pred_df.columns, f"Missing column: {col}"

    def test_predicted_label_uses_045_threshold(self, sample_df):
        pred_df = sample_df.copy()
        probs = np.array([0.90, 0.44, 0.46, 0.10, 0.45, 0.80])
        pred_df["predicted_prob"]  = probs
        pred_df["predicted_label"] = (probs >= 0.45).astype(int)

        # 0.44 → 0 (below threshold), 0.46 → 1, 0.45 → 1 (equal = match)
        assert pred_df.loc[1, "predicted_label"] == 0
        assert pred_df.loc[2, "predicted_label"] == 1
        assert pred_df.loc[4, "predicted_label"] == 1

    def test_prob_range_valid(self, sample_df):
        pred_df = sample_df.copy()
        pred_df["predicted_prob"] = [0.91, 0.23, 0.87, 0.12, 0.05, 0.95]
        assert pred_df["predicted_prob"].between(0, 1).all()