"""
Entity Resolution Bias Detection Script

Loads test_predictions.csv from evaluate.py, slices by source_dataset
and entity_type, computes per-slice metrics, checks F1 disparity threshold,
and generates bias_report.json + plots.

Config-driven — no CLI arguments:
    CONFIG_PATH=config/training_config.yaml python scripts/bias_detection.py

Outputs:
    models/{entity_type}/results/bias_report.json
    models/{entity_type}/plots/bias_f1_by_slice.png
    models/{entity_type}/plots/bias_disparity_heatmap.png
"""

import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import seaborn as sns
import yaml
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score


DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class BiasDetector:
    """
    Slices test predictions by source_dataset and entity_type,
    computes per-slice metrics, checks disparity thresholds.
    """

    def __init__(self, config: dict, entity_type: str):
        self.config      = config
        self.entity_type = entity_type.lower()

        base_dir    = config["output"]["base_dir"]
        model_dir   = f"{base_dir}/{entity_type}"
        self.results_dir = f"{model_dir}/results"
        self.plots_dir   = f"{model_dir}/plots"
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.plots_dir,   exist_ok=True)

        self.bias_cfg = config["bias_detection"]

    # ------------------------------------------------------------------
    # Load predictions
    # ------------------------------------------------------------------

    def load_predictions(self) -> pd.DataFrame:
        path = os.path.join(self.results_dir, "test_predictions.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"test_predictions.csv not found at {path}. "
                "Run evaluate.py first."
            )
        df = pd.read_csv(path)
        print(f"[Bias] Loaded {len(df)} predictions from {path}")
        return df

    # ------------------------------------------------------------------
    # Per-slice metrics
    # ------------------------------------------------------------------

    def compute_slice_metrics(
        self, df: pd.DataFrame, slice_col: str
    ) -> dict:
        """Compute F1/precision/recall per unique value of slice_col."""
        slices     = {}
        min_size   = self.bias_cfg["min_slice_size"]
        col_label  = self.config["data"]["columns"]["label"]

        for value in df[slice_col].unique():
            subset = df[df[slice_col] == value]
            if len(subset) < min_size:
                print(f"[Bias]   Skipping {slice_col}={value} "
                      f"— only {len(subset)} samples (min={min_size})")
                continue

            true  = subset[col_label].values
            pred  = subset["predicted_label"].values

            # Skip slices with only one class (can't compute meaningful metrics)
            if len(np.unique(true)) < 2:
                print(f"[Bias]   Skipping {slice_col}={value} — single class")
                continue

            slices[str(value)] = {
                "n_samples":  int(len(subset)),
                "n_positive": int(true.sum()),
                "accuracy":   float(accuracy_score(true, pred)),
                "f1":         float(f1_score(true, pred, zero_division=0)),
                "precision":  float(precision_score(true, pred, zero_division=0)),
                "recall":     float(recall_score(true, pred, zero_division=0)),
            }
            print(
                f"[Bias]   {slice_col}={value:20s} | "
                f"n={len(subset):5d} | "
                f"F1={slices[str(value)]['f1']:.4f} | "
                f"P={slices[str(value)]['precision']:.4f} | "
                f"R={slices[str(value)]['recall']:.4f}"
            )

        return slices

    # ------------------------------------------------------------------
    # Disparity check
    # ------------------------------------------------------------------

    def check_disparity(self, slices: dict, metric: str = "f1") -> dict:
        """
        Check if max - min metric across slices exceeds threshold.
        Returns disparity info dict.
        """
        values = [s[metric] for s in slices.values() if s[metric] > 0]
        if len(values) < 2:
            return {"bias_detected": False, "reason": "insufficient slices"}

        max_val   = max(values)
        min_val   = min(values)
        disparity = max_val - min_val
        threshold = self.bias_cfg[f"max_{metric}_disparity"]

        return {
            "bias_detected":   disparity > threshold,
            f"max_{metric}":   float(max_val),
            f"min_{metric}":   float(min_val),
            f"{metric}_disparity": float(disparity),
            f"threshold":      float(threshold),
        }

    # ------------------------------------------------------------------
    # Plots
    # ------------------------------------------------------------------

    def plot_f1_by_slice(self, slices: dict, slice_col: str) -> str:
        names  = list(slices.keys())
        f1s    = [slices[n]["f1"] for n in names]
        colors = ["#1D9E75" if f >= 0.75 else "#D85A30" for f in f1s]

        fig, ax = plt.subplots(figsize=(max(8, len(names) * 1.2), 5))
        bars = ax.bar(names, f1s, color=colors, edgecolor="none")
        ax.axhline(0.75, color="#888780", linestyle="--",
                   linewidth=1, label="min F1 threshold (0.75)")
        ax.set_ylim(0, 1.05)
        ax.set_xlabel(slice_col)
        ax.set_ylabel("F1 score")
        ax.set_title(f"F1 by {slice_col} — {self.entity_type}")
        ax.legend(fontsize=10)
        plt.xticks(rotation=30, ha="right")

        for bar, val in zip(bars, f1s):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.01,
                    f"{val:.3f}", ha="center", va="bottom", fontsize=10)

        plt.tight_layout()
        path = os.path.join(self.plots_dir, f"bias_f1_{slice_col}.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Bias] Plot saved → {path}")
        return path

    def plot_metrics_heatmap(self, slices: dict, slice_col: str) -> str:
        metrics = ["f1", "precision", "recall", "accuracy"]
        names   = list(slices.keys())
        data    = [[slices[n][m] for m in metrics] for n in names]

        fig, ax = plt.subplots(figsize=(8, max(4, len(names) * 0.6 + 2)))
        sns.heatmap(
            data, annot=True, fmt=".3f", cmap="RdYlGn",
            xticklabels=metrics, yticklabels=names,
            vmin=0, vmax=1, ax=ax, linewidths=0.5,
        )
        ax.set_title(f"Metrics by {slice_col} — {self.entity_type}")
        plt.tight_layout()
        path = os.path.join(self.plots_dir, f"bias_heatmap_{slice_col}.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Bias] Heatmap saved → {path}")
        return path

    # ------------------------------------------------------------------
    # Main detect()
    # ------------------------------------------------------------------

    def detect(self) -> dict:
        """
        Full bias detection pipeline for one entity type.
        Returns bias_report dict.
        """
        df = self.load_predictions()

        # Apply threshold from config (same as evaluate.py)
        threshold = self.config["validation"]["classification_threshold"]
        if "predicted_label" not in df.columns:
            df["predicted_label"] = (
                df["predicted_prob"] >= threshold
            ).astype(int)

        slice_cols  = self.bias_cfg["slices"]
        report      = {
            "entity_type":  self.entity_type,
            "n_samples":    len(df),
            "threshold":    threshold,
            "slices":       {},
            "disparity":    {},
            "bias_detected": False,
            "mitigation_suggestions": [],
        }
        plot_paths = []

        for slice_col in slice_cols:
            if slice_col not in df.columns:
                print(f"[Bias] Column '{slice_col}' not in predictions — skipping")
                continue

            print(f"\n[Bias] Slicing by: {slice_col}")
            slices = self.compute_slice_metrics(df, slice_col)

            if not slices:
                print(f"[Bias] No valid slices for {slice_col}")
                continue

            disparity = self.check_disparity(slices, "f1")

            report["slices"][slice_col]   = slices
            report["disparity"][slice_col] = disparity

            if disparity["bias_detected"]:
                report["bias_detected"] = True
                report["mitigation_suggestions"].append(
                    f"F1 disparity of {disparity['f1_disparity']:.3f} across "
                    f"{slice_col} exceeds threshold {disparity['threshold']}. "
                    f"Consider re-sampling under-performing slices or adjusting "
                    f"per-slice decision thresholds."
                )
                print(
                    f"[Bias] BIAS DETECTED in {slice_col}: "
                    f"disparity={disparity['f1_disparity']:.3f} "
                    f"> threshold={disparity['threshold']}"
                )
            else:
                print(
                    f"[Bias] OK — {slice_col} disparity="
                    f"{disparity.get('f1_disparity', 0):.3f} "
                    f"<= threshold={disparity['threshold']}"
                )

            # Plots
            plot_paths.append(self.plot_f1_by_slice(slices, slice_col))
            plot_paths.append(self.plot_metrics_heatmap(slices, slice_col))

        # Save report
        report_path = os.path.join(self.results_dir, "bias_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n[Bias] Report saved → {report_path}")

        # Log to MLflow
        mlflow.set_tracking_uri(self.config["mlflow"]["tracking_uri"])
        mlflow.set_experiment(self.config["mlflow"]["experiment_name"])

        with mlflow.start_run(
            run_name=f"bias_detection_{self.entity_type}",
            tags={"entity_type": self.entity_type, "stage": "bias_detection"},
        ):
            mlflow.log_artifact(report_path, artifact_path="bias")
            for p in plot_paths:
                mlflow.log_artifact(p, artifact_path="bias/plots")

            # Log per-slice F1s as metrics
            for slice_col, slices in report["slices"].items():
                for slice_val, metrics in slices.items():
                    key = f"f1_{slice_col}_{slice_val}".replace(" ", "_")[:250]
                    mlflow.log_metric(key, metrics["f1"])

            mlflow.log_metric(
                "bias_detected", int(report["bias_detected"])
            )

        return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config_path  = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config       = load_config(config_path)
    entity_types = config["data"]["entity_types"]

    print("=" * 70)
    print("ENTITY RESOLUTION — BIAS DETECTION")
    print("=" * 70)

    for entity_type in entity_types:
        print(f"\n{'=' * 70}")
        print(f"Entity type: {entity_type.upper()}")
        print(f"{'=' * 70}")

        detector = BiasDetector(config=config, entity_type=entity_type)
        report   = detector.detect()

        print(f"\n[Summary] bias_detected: {report['bias_detected']}")
        if report["mitigation_suggestions"]:
            print("[Summary] Mitigation suggestions:")
            for s in report["mitigation_suggestions"]:
                print(f"  - {s}")

    print("\n" + "=" * 70)
    print("BIAS DETECTION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()