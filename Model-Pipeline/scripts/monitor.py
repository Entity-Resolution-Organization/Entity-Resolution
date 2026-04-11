"""
Entity Resolution — Model Monitoring
=====================================

Core monitoring logic for detecting model decay and data drift.

What it does:
    1. Evaluates deployed model on holdout test set (F1, precision, recall, AUC)
    2. Detects data drift by comparing current data against reference profile
    3. Checks thresholds and decides: HEALTHY / RETRAIN
    4. Logs all metrics to BigQuery
    5. Returns monitoring results

Usage:
    python scripts/monitor.py
"""

import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage
from sklearn.metrics import (
    accuracy_score,
    precision_recall_fscore_support,
    roc_auc_score,
)

DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class ModelMonitor:
    """Monitors deployed entity resolution model."""

    def __init__(self, config: dict, entity_type: str):
        self.config = config
        self.entity_type = entity_type
        self.gcs_client = storage.Client()
        self.bq_client = bigquery.Client(project=config["gcp"]["project_id"])
        self.bucket_name = config["data"]["gcs_bucket"]
        self.project_id = config["gcp"]["project_id"]
        self.region = config["gcp"]["region"]
        self.results = {}

    # ------------------------------------------------------------------
    # Step 1: Evaluate model on holdout test set
    # ------------------------------------------------------------------

    def evaluate_holdout(self) -> dict:
        """Run deployed model on test set, compute metrics."""
        print("[Monitor] Step 1: Evaluating model on holdout test set...")

        # Download test data
        gcs_path = self.config["data"]["gcs_path"]
        blob_path = f"{gcs_path}/{self.entity_type}/test.csv"
        local_path = f"/tmp/test_{self.entity_type}.csv"

        self.gcs_client.bucket(self.bucket_name).blob(
            blob_path
        ).download_to_filename(local_path)
        df = pd.read_csv(local_path)
        print(f"  Loaded {len(df)} test pairs")

        # Get endpoint URL
        endpoint_info = json.loads(
            self.gcs_client.bucket(self.bucket_name)
            .blob("pipeline-results/endpoint_info.json")
            .download_as_text()
        )
        predict_url = endpoint_info["predict_url"]

        # Call deployed model in batches
        cols = self.config["data"]["columns"]
        batch_size = 32
        all_probs = []

        import google.auth
        import google.auth.transport.requests

        credentials, _ = google.auth.default()
        credentials.refresh(google.auth.transport.requests.Request())
        token = credentials.token

        import requests

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            instances = []
            for _, row in batch.iterrows():
                instances.append({
                    "name1": str(row[cols["name1"]]) if pd.notna(row[cols["name1"]]) else "",
                    "address1": str(row[cols["address1"]]) if pd.notna(row[cols["address1"]]) else "",
                    "name2": str(row[cols["name2"]]) if pd.notna(row[cols["name2"]]) else "",
                    "address2": str(row[cols["address2"]]) if pd.notna(row[cols["address2"]]) else "",
                })

            response = requests.post(
                predict_url,
                json={"instances": instances},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )

            if response.status_code == 200:
                preds = response.json()["predictions"]
                all_probs.extend([p["probability"] for p in preds])
            else:
                print(f"  ERROR batch {i}: {response.status_code} {response.text[:200]}")
                return None

            if (i // batch_size) % 10 == 0:
                print(f"  Batch {i // batch_size + 1}/{len(df) // batch_size + 1}")

        # Compute metrics
        threshold = self.config["validation"]["classification_threshold"]
        true_labels = df[cols["label"]].values
        pred_probs = np.array(all_probs)
        pred_labels = (pred_probs >= threshold).astype(int)

        precision, recall, f1, _ = precision_recall_fscore_support(
            true_labels, pred_labels, average="binary"
        )
        accuracy = accuracy_score(true_labels, pred_labels)
        auc_score = roc_auc_score(true_labels, pred_probs)

        metrics = {
            "test_accuracy": float(accuracy),
            "test_precision": float(precision),
            "test_recall": float(recall),
            "test_f1": float(f1),
            "test_auc": float(auc_score),
            "threshold": float(threshold),
            "total_samples": int(len(true_labels)),
        }

        print(f"  Results:")
        print(f"    F1:        {metrics['test_f1']:.4f}")
        print(f"    Precision: {metrics['test_precision']:.4f}")
        print(f"    Recall:    {metrics['test_recall']:.4f}")
        print(f"    AUC:       {metrics['test_auc']:.4f}")

        self.results["performance"] = metrics
        return metrics

    # ------------------------------------------------------------------
    # Step 2: Detect data drift
    # ------------------------------------------------------------------

    def detect_drift(self) -> dict:
        """Compare current data features against reference profile."""
        print("[Monitor] Step 2: Detecting data drift...")

        cols = self.config["data"]["columns"]

        # Download reference data
        ref_blob = f"monitoring/reference/{self.entity_type}/reference_features.csv"
        ref_local = f"/tmp/reference_{self.entity_type}.csv"
        self.gcs_client.bucket(self.bucket_name).blob(ref_blob).download_to_filename(
            ref_local
        )
        ref_df = pd.read_csv(ref_local)
        print(f"  Reference data: {len(ref_df)} records")

        # Download current test data as proxy for production data
        gcs_path = self.config["data"]["gcs_path"]
        test_blob = f"{gcs_path}/{self.entity_type}/test.csv"
        test_local = f"/tmp/test_{self.entity_type}.csv"
        self.gcs_client.bucket(self.bucket_name).blob(test_blob).download_to_filename(
            test_local
        )
        curr_df = pd.read_csv(test_local)

        # Build feature dataframe matching reference format
        curr_features = pd.DataFrame()
        curr_features["name1"] = curr_df[cols["name1"]].fillna("")
        curr_features["name2"] = curr_df[cols["name2"]].fillna("")
        curr_features["address1"] = curr_df[cols["address1"]].fillna("")
        curr_features["address2"] = curr_df[cols["address2"]].fillna("")
        curr_features["name1_length"] = curr_df[cols["name1"]].fillna("").str.len()
        curr_features["name2_length"] = curr_df[cols["name2"]].fillna("").str.len()
        curr_features["address1_length"] = curr_df[cols["address1"]].fillna("").str.len()
        curr_features["address2_length"] = curr_df[cols["address2"]].fillna("").str.len()
        curr_features["name1_has_missing"] = curr_df[cols["name1"]].isna().astype(int)
        curr_features["address1_has_missing"] = curr_df[cols["address1"]].isna().astype(int)
        curr_features["label"] = curr_df[cols["label"]]

        # Compare numerical features using Kolmogorov-Smirnov test
        from scipy import stats

        numeric_cols = [
            "name1_length", "name2_length",
            "address1_length", "address2_length",
            "name1_has_missing", "address1_has_missing",
            "label",
        ]

        drift_results = []
        total_drifted = 0

        for col in numeric_cols:
            ref_values = ref_df[col].dropna().values
            curr_values = curr_features[col].dropna().values

            ks_stat, p_value = stats.ks_2samp(ref_values, curr_values)
            is_drifted = p_value < 0.05

            if is_drifted:
                total_drifted += 1

            drift_results.append({
                "feature": col,
                "ks_statistic": float(ks_stat),
                "p_value": float(p_value),
                "is_drifted": is_drifted,
            })

            status = "DRIFTED" if is_drifted else "OK"
            print(f"  {col}: KS={ks_stat:.4f} p={p_value:.4f} -> {status}")

        drift_summary = {
            "total_features": len(numeric_cols),
            "drifted_features": total_drifted,
            "drift_ratio": float(total_drifted / len(numeric_cols)),
            "feature_details": drift_results,
        }

        print(f"  Summary: {total_drifted}/{len(numeric_cols)} features drifted")

        self.results["drift"] = drift_summary
        return drift_summary

    # ------------------------------------------------------------------
    # Step 3: Check thresholds
    # ------------------------------------------------------------------

    def check_thresholds(self) -> dict:
        """Check if metrics breach monitoring thresholds."""
        print("[Monitor] Step 3: Checking thresholds...")

        perf = self.results.get("performance", {})
        drift = self.results.get("drift", {})

        # Performance thresholds from config
        thresholds = self.config["validation"]
        min_f1 = thresholds["min_f1"]
        min_precision = thresholds["min_precision"]
        min_recall = thresholds["min_recall"]
        min_auc = thresholds["min_auc"]

        # Drift threshold
        max_drift_ratio = 0.3  # retrain if >30% features drifted

        checks = {
            "f1_ok": perf.get("test_f1", 0) >= min_f1,
            "precision_ok": perf.get("test_precision", 0) >= min_precision,
            "recall_ok": perf.get("test_recall", 0) >= min_recall,
            "auc_ok": perf.get("test_auc", 0) >= min_auc,
            "drift_ok": drift.get("drift_ratio", 0) <= max_drift_ratio,
        }

        all_ok = all(checks.values())
        decision = "HEALTHY" if all_ok else "RETRAIN"

        for check, passed in checks.items():
            print(f"  {check}: {'PASS' if passed else 'FAIL'}")

        print(f"  Decision: {decision}")

        result = {
            "decision": decision,
            "checks": checks,
            "thresholds_used": {
                "min_f1": min_f1,
                "min_precision": min_precision,
                "min_recall": min_recall,
                "min_auc": min_auc,
                "max_drift_ratio": max_drift_ratio,
            },
        }

        self.results["thresholds"] = result
        return result

    # ------------------------------------------------------------------
    # Step 4: Log metrics to BigQuery
    # ------------------------------------------------------------------

    def log_to_bigquery(self, pipeline_run_id: str = None):
        """Write monitoring results to BigQuery tables."""
        print("[Monitor] Step 4: Logging metrics to BigQuery...")

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        run_id = pipeline_run_id or f"manual_{today}"
        dataset = "entity_resolution"

        # Log performance metrics
        perf = self.results.get("performance", {})
        perf_rows = []
        for metric_name in ["test_f1", "test_precision", "test_recall", "test_auc", "test_accuracy"]:
            if metric_name in perf:
                perf_rows.append({
                    "date": today,
                    "metric_name": metric_name,
                    "metric_value": perf[metric_name],
                    "entity_type": self.entity_type,
                    "triggered_retrain": self.results.get("thresholds", {}).get("decision") == "RETRAIN",
                    "pipeline_run_id": run_id,
                })

        if perf_rows:
            table_ref = f"{self.project_id}.{dataset}.monitoring_metrics"
            errors = self.bq_client.insert_rows_json(table_ref, perf_rows)
            if errors:
                print(f"  ERROR writing metrics: {errors}")
            else:
                print(f"  Logged {len(perf_rows)} performance metrics")

        # Log drift results
        drift = self.results.get("drift", {})
        drift_rows = []
        for feature in drift.get("feature_details", []):
            drift_rows.append({
                "date": today,
                "feature_name": feature["feature"],
                "drift_score": float(feature["ks_statistic"]),
                "is_drifted": bool(feature["is_drifted"]),
                "test_method": "kolmogorov_smirnov",
                "entity_type": self.entity_type,
                "pipeline_run_id": run_id,
            })

        if drift_rows:
            table_ref = f"{self.project_id}.{dataset}.drift_reports"
            errors = self.bq_client.insert_rows_json(table_ref, drift_rows)
            if errors:
                print(f"  ERROR writing drift: {errors}")
            else:
                print(f"  Logged {len(drift_rows)} drift results")

    # ------------------------------------------------------------------
    # Run all steps
    # ------------------------------------------------------------------

    def run(self, pipeline_run_id: str = None) -> dict:
        """Run full monitoring pipeline."""
        print(f"\n{'=' * 60}")
        print(f"MODEL MONITORING — {self.entity_type.upper()}")
        print(f"{'=' * 60}\n")

        # Step 1: Evaluate
        perf = self.evaluate_holdout()
        if perf is None:
            print("[Monitor] FAILED — could not evaluate model")
            return {"decision": "ERROR", "error": "evaluation_failed"}

        # Step 2: Drift
        self.detect_drift()

        # Step 3: Thresholds
        result = self.check_thresholds()

        # Step 4: Log to BigQuery
        self.log_to_bigquery(pipeline_run_id)

        print(f"\n{'=' * 60}")
        print(f"MONITORING COMPLETE — Decision: {result['decision']}")
        print(f"{'=' * 60}\n")

        return self.results


def main():
    config_path = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config = load_config(config_path)

    print("=" * 60)
    print("ENTITY RESOLUTION — MODEL MONITORING")
    print("=" * 60)

    for entity_type in config["data"]["entity_types"]:
        monitor = ModelMonitor(config=config, entity_type=entity_type)
        results = monitor.run()

        if results.get("thresholds", {}).get("decision") == "RETRAIN":
            print("\n*** RETRAINING RECOMMENDED ***")
            print("Run: python pipeline.py --run")


if __name__ == "__main__":
    main()
