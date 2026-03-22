"""
Outputs:
    models/{entity_type}/results/sensitivity_report.json
    models/{entity_type}/plots/shap_summary.png
    models/{entity_type}/plots/shap_bar.png
"""

import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import torch
import yaml
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from peft import PeftModel


DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class SensitivityAnalyzer:
    """
    SHAP-based sensitivity analysis for the DeBERTa entity resolution model.
    Uses a text masking approach compatible with transformer models.
    """

    def __init__(self, config: dict, entity_type: str):
        self.config      = config
        self.entity_type = entity_type.lower()

        base_dir    = config["output"]["base_dir"]
        model_dir   = f"{base_dir}/{entity_type}"
        self.final_model_dir = f"{model_dir}/final_model"
        self.results_dir     = f"{model_dir}/results"
        self.plots_dir       = f"{model_dir}/plots"
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.plots_dir,   exist_ok=True)

        use_cuda  = config["device"]["use_cuda"]
        cuda_id   = config["device"]["cuda_device"]
        self.device = (
            torch.device(f"cuda:{cuda_id}")
            if use_cuda and torch.cuda.is_available()
            else torch.device("cpu")
        )
        self.n_samples = config["sensitivity"]["shap_samples"]

        print(f"[Sensitivity] entity_type : {self.entity_type}")
        print(f"[Sensitivity] device      : {self.device}")
        print(f"[Sensitivity] shap_samples: {self.n_samples}")

    # ------------------------------------------------------------------
    # Load model
    # ------------------------------------------------------------------

    def load_model_and_tokenizer(self):
        print(f"[Sensitivity] Loading model from {self.final_model_dir}")
        cache_dir = self.config["model"].get("cache_dir")

        base_model = AutoModelForSequenceClassification.from_pretrained(
            self.config["model"]["base_model"],
            num_labels=self.config["model"]["num_labels"],
            cache_dir=cache_dir,
        )
        model     = PeftModel.from_pretrained(base_model, self.final_model_dir)
        model     = model.merge_and_unload()
        tokenizer = AutoTokenizer.from_pretrained(
            self.final_model_dir, cache_dir=cache_dir
        )
        model.to(self.device)
        model.eval()
        return model, tokenizer

    # ------------------------------------------------------------------
    # Load sample
    # ------------------------------------------------------------------

    def load_sample(self) -> pd.DataFrame:
        preds_path = os.path.join(self.results_dir, "test_predictions.csv")
        if not os.path.exists(preds_path):
            raise FileNotFoundError(
                f"test_predictions.csv not found at {preds_path}. "
                "Run evaluate.py first."
            )
        df = pd.read_csv(preds_path)

        # Balanced sample — equal matches and non-matches
        n_each  = self.n_samples // 2
        matches = df[df["label"] == 1].sample(
            min(n_each, len(df[df["label"] == 1])), random_state=42
        )
        non_matches = df[df["label"] == 0].sample(
            min(n_each, len(df[df["label"] == 0])), random_state=42
        )
        sample = pd.concat([matches, non_matches]).reset_index(drop=True)
        print(f"[Sensitivity] Sample: {len(sample)} pairs "
              f"({len(matches)} matches, {len(non_matches)} non-matches)")
        return sample

    # ------------------------------------------------------------------
    # Feature importance via field masking
    # ------------------------------------------------------------------

    def compute_field_importance(
        self,
        model,
        tokenizer,
        sample: pd.DataFrame,
    ) -> dict:
        """
        Compute importance of each input field by masking it out and
        measuring the drop in prediction confidence.

        Fields: name1, address1, name2, address2
        """
        cols       = self.config["data"]["columns"]
        max_length = self.config["model"]["max_length"]
        threshold  = self.config["validation"]["classification_threshold"]

        fields = {
            "name1":    cols["name1"],
            "address1": cols["address1"],
            "name2":    cols["name2"],
            "address2": cols["address2"],
        }
        MASK_TOKEN = "[MASK]"

        def build_text(row, masked_field=None):
            vals = {k: str(row[v]) if masked_field != k else MASK_TOKEN
                    for k, v in fields.items()}
            return (
                f"{vals['name1']} [SEP] {vals['address1']} "
                f"[SEP] {vals['name2']} [SEP] {vals['address2']}"
            )

        def get_probs(texts):
            enc = tokenizer(
                texts,
                truncation=True,
                padding="max_length",
                max_length=max_length,
                return_tensors="pt",
            )
            enc = {k: v.to(self.device)
                   for k, v in enc.items() if k != "token_type_ids"}
            with torch.no_grad():
                logits = model(**enc).logits
            probs = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
            return probs

        print("[Sensitivity] Computing baseline predictions...")
        baseline_texts = [build_text(row) for _, row in sample.iterrows()]
        baseline_probs = get_probs(baseline_texts)

        importance = {}
        for field_key in fields:
            print(f"[Sensitivity] Masking field: {field_key}")
            masked_texts  = [build_text(row, masked_field=field_key)
                             for _, row in sample.iterrows()]
            masked_probs  = get_probs(masked_texts)
            drop          = baseline_probs - masked_probs
            importance[field_key] = {
                "mean_drop":   float(np.mean(drop)),
                "std_drop":    float(np.std(drop)),
                "mean_abs_drop": float(np.mean(np.abs(drop))),
                "importance_score": float(np.mean(np.abs(drop))),
            }
            print(
                f"  {field_key:12s} | mean drop={importance[field_key]['mean_drop']:+.4f} "
                f"| importance={importance[field_key]['importance_score']:.4f}"
            )

        return importance, baseline_probs

    # ------------------------------------------------------------------
    # Plots
    # ------------------------------------------------------------------

    def plot_feature_importance(self, importance: dict) -> str:
        fields = list(importance.keys())
        scores = [importance[f]["importance_score"] for f in fields]
        colors = ["#1D9E75" if s == max(scores) else "#9FE1CB" for s in scores]

        idx    = np.argsort(scores)[::-1]
        fields = [fields[i] for i in idx]
        scores = [scores[i] for i in idx]
        colors = [colors[i] for i in idx]

        fig, ax = plt.subplots(figsize=(8, 4))
        bars = ax.barh(fields[::-1], scores[::-1], color=colors[::-1], edgecolor="none")
        ax.set_xlabel("Importance score (mean |prob drop| when masked)")
        ax.set_title(f"Field importance — {self.entity_type}")
        ax.set_xlim(0, max(scores) * 1.3)

        for bar, val in zip(bars, scores[::-1]):
            ax.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height() / 2,
                    f"{val:.4f}", va="center", fontsize=10)

        plt.tight_layout()
        path = os.path.join(self.plots_dir, "shap_bar.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Sensitivity] Bar chart → {path}")
        return path

    def plot_drop_distribution(
        self, importance: dict, baseline_probs: np.ndarray
    ) -> str:
        fig, axes = plt.subplots(1, 4, figsize=(14, 4))
        for ax, (field, info) in zip(axes, importance.items()):
            ax.axhline(0, color="#888780", linewidth=0.8, linestyle="--")
            ax.bar(range(len(baseline_probs)),
                   [info["mean_drop"]] * len(baseline_probs),
                   color="#378ADD", alpha=0.6)
            ax.set_title(field, fontsize=11)
            ax.set_xlabel("sample index", fontsize=9)
            if ax == axes[0]:
                ax.set_ylabel("prob drop when masked", fontsize=9)
            ax.set_ylim(-0.3, 0.3)

        plt.suptitle(f"Prediction drop by masked field — {self.entity_type}",
                     fontsize=12)
        plt.tight_layout()
        path = os.path.join(self.plots_dir, "shap_summary.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Sensitivity] Summary plot → {path}")
        return path

    # ------------------------------------------------------------------
    # Main analyze()
    # ------------------------------------------------------------------

    def analyze(self) -> dict:
        # Load
        model, tokenizer = self.load_model_and_tokenizer()
        sample           = self.load_sample()

        # Compute importance
        importance, baseline_probs = self.compute_field_importance(
            model, tokenizer, sample
        )

        # Rank fields
        ranked = sorted(
            importance.items(),
            key=lambda x: x[1]["importance_score"],
            reverse=True,
        )

        print("\n[Sensitivity] Field ranking:")
        for rank, (field, info) in enumerate(ranked, 1):
            print(f"  {rank}. {field:12s} importance={info['importance_score']:.4f}")

        # Plots
        bar_path     = self.plot_feature_importance(importance)
        summary_path = self.plot_drop_distribution(importance, baseline_probs)

        # Report
        report = {
            "entity_type":    self.entity_type,
            "n_samples":      len(sample),
            "method":         "field_masking",
            "field_ranking":  [f for f, _ in ranked],
            "importance":     importance,
            "most_important": ranked[0][0],
            "least_important": ranked[-1][0],
        }

        report_path = os.path.join(self.results_dir, "sensitivity_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"[Sensitivity] Report → {report_path}")

        # Log to MLflow
        mlflow.set_tracking_uri(self.config["mlflow"]["tracking_uri"])
        mlflow.set_experiment(self.config["mlflow"]["experiment_name"])

        with mlflow.start_run(
            run_name=f"sensitivity_{self.entity_type}",
            tags={"entity_type": self.entity_type, "stage": "sensitivity"},
        ):
            for field, info in importance.items():
                mlflow.log_metric(
                    f"importance_{field}", info["importance_score"]
                )
            mlflow.log_artifact(report_path,   artifact_path="sensitivity")
            mlflow.log_artifact(bar_path,      artifact_path="sensitivity/plots")
            mlflow.log_artifact(summary_path,  artifact_path="sensitivity/plots")

        return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config_path  = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config       = load_config(config_path)
    entity_types = config["data"]["entity_types"]

    if not config["sensitivity"]["enabled"]:
        print("[Sensitivity] Disabled in config — skipping")
        return

    print("=" * 70)
    print("ENTITY RESOLUTION — SENSITIVITY ANALYSIS")
    print("=" * 70)

    for entity_type in entity_types:
        print(f"\n{'=' * 70}")
        print(f"Entity type: {entity_type.upper()}")
        print(f"{'=' * 70}")

        analyzer = SensitivityAnalyzer(config=config, entity_type=entity_type)
        report   = analyzer.analyze()

        print(f"\n[Summary] Most important field : {report['most_important']}")
        print(f"[Summary] Least important field: {report['least_important']}")

    print("\n" + "=" * 70)
    print("SENSITIVITY ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()