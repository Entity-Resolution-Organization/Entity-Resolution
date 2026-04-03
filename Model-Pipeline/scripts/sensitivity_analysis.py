"""
Entity Resolution Sensitivity Analysis

SHAP-style field masking to measure each input field's contribution
to model predictions.

Outputs:
    models/{entity_type}/results/sensitivity_report.json
    models/{entity_type}/plots/shap_summary.png
    models/{entity_type}/plots/shap_bar.png

Fixes vs previous version:
    1. _build_text_pair() returns (text_a, text_b) and tokenizer is called
       with two separate lists — produces real [SEP] tokens matching train.py.
       Previous version wrote '[SEP]' as literal string in a single f-string,
       meaning baseline and masked probabilities were both computed on the
       wrong input format, making importance scores unreliable.
    2. MLflow tracking URI respects MLFLOW_TRACKING_URI env var — required
       for Vertex AI components which cannot resolve 'mlflow' hostname.
    3. _safe() applied to all field accesses — NaN fields become [MISSING]
       instead of 'nan', matching train.py behaviour exactly.
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
from peft import PeftModel
from transformers import AutoModelForSequenceClassification, AutoTokenizer

DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _safe(val) -> str:
    """
    Replace NaN / empty with [MISSING].
    Must match train.py _safe() exactly — same token, same conditions.
    """
    if pd.isna(val) or str(val).strip() == "":
        return "[MISSING]"
    return str(val).strip()


class SensitivityAnalyzer:
    """
    SHAP-style field masking for DeBERTa entity resolution model.
    Measures importance of each input field by observing the drop in
    prediction confidence when that field is replaced with [MASK].
    """

    def __init__(self, config: dict, entity_type: str):
        self.config      = config
        self.entity_type = entity_type.lower()

        base_dir             = config["output"]["base_dir"]
        model_dir            = f"{base_dir}/{entity_type}"
        self.final_model_dir = f"{model_dir}/final_model"
        self.results_dir     = f"{model_dir}/results"
        self.plots_dir       = f"{model_dir}/plots"
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.plots_dir,   exist_ok=True)

        # Single source of truth for threshold
        self.threshold = config["validation"]["classification_threshold"]
        self.n_samples = config["sensitivity"]["shap_samples"]

        use_cuda  = config["device"]["use_cuda"]
        cuda_id   = config["device"]["cuda_device"]
        self.device = (
            torch.device(f"cuda:{cuda_id}")
            if use_cuda and torch.cuda.is_available()
            else torch.device("cpu")
        )

        print(f"[Sensitivity] entity_type : {self.entity_type}")
        print(f"[Sensitivity] device      : {self.device}")
        print(f"[Sensitivity] threshold   : {self.threshold}")
        print(f"[Sensitivity] shap_samples: {self.n_samples}")

    # ------------------------------------------------------------------
    # Model loading
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
    # Data
    # ------------------------------------------------------------------

    def load_sample(self) -> pd.DataFrame:
        preds_path = os.path.join(self.results_dir, "test_predictions.csv")
        if not os.path.exists(preds_path):
            raise FileNotFoundError(
                f"test_predictions.csv not found at {preds_path}. "
                "Run evaluate.py first."
            )
        df = pd.read_csv(preds_path)

        n_each      = self.n_samples // 2
        matches     = df[df["label"] == 1].sample(
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
    # Text pair builder
    # ------------------------------------------------------------------

    def _build_text_pair(
        self,
        row: pd.Series,
        cols: dict,
        masked_field: str | None = None,
    ) -> tuple[str, str]:
        """
        FIX 1: Return (text_a, text_b) matching train.py format exactly.

        When masked_field is set, that field is replaced with [MASK] in
        the appropriate record. The tokenizer receives two lists so it
        inserts real [SEP] tokens — the previous single-string approach
        with literal '[SEP]' meant baseline and masked probs were both
        computed on inputs the model had never seen during training,
        making all importance scores meaningless.

        FIX 3: All field values go through _safe() — NaN → [MISSING].
        """
        MASK = "[MASK]"

        name1    = MASK if masked_field == "name1"    else _safe(row[cols["name1"]])
        address1 = MASK if masked_field == "address1" else _safe(row[cols["address1"]])
        name2    = MASK if masked_field == "name2"    else _safe(row[cols["name2"]])
        address2 = MASK if masked_field == "address2" else _safe(row[cols["address2"]])

        text_a = f"record_1 name: {name1} address: {address1}"
        text_b = f"record_2 name: {name2} address: {address2}"
        return text_a, text_b

    # ------------------------------------------------------------------
    # Inference helper
    # ------------------------------------------------------------------

    def _get_probs(
        self,
        tokenizer,
        model,
        texts_a: list,
        texts_b: list,
    ) -> np.ndarray:
        """
        FIX 1 cont: text-pair tokenizer call — real [SEP] tokens.
        Returns softmax P(match) for each pair.
        """
        max_length = self.config["model"]["max_length"]
        batch_size = self.config["training"]["batch_size"]
        all_probs  = []

        with torch.no_grad():
            for i in range(0, len(texts_a), batch_size):
                batch_a = texts_a[i : i + batch_size]
                batch_b = texts_b[i : i + batch_size]

                enc = tokenizer(
                    batch_a,
                    batch_b,
                    truncation=True,
                    padding="max_length",
                    max_length=max_length,
                    return_tensors="pt",
                )
                # DeBERTa-v3 does not use token_type_ids
                enc = {k: v.to(self.device)
                       for k, v in enc.items()
                       if k != "token_type_ids"}

                logits = model(**enc).logits
                probs  = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
                all_probs.extend(probs)

        return np.array(all_probs)

    # ------------------------------------------------------------------
    # Field importance
    # ------------------------------------------------------------------

    def compute_field_importance(
        self,
        model,
        tokenizer,
        sample: pd.DataFrame,
    ) -> tuple[dict, np.ndarray]:
        """
        Compute per-field importance by masking one field at a time and
        measuring the drop in P(match) vs the unmasked baseline.

        Fields analysed: name1, address1, name2, address2.
        """
        cols = self.config["data"]["columns"]

        # Baseline — all fields present
        print("[Sensitivity] Computing baseline predictions…")
        baseline_pairs = [
            self._build_text_pair(row, cols) for _, row in sample.iterrows()
        ]
        baseline_probs = self._get_probs(
            tokenizer, model,
            [p[0] for p in baseline_pairs],
            [p[1] for p in baseline_pairs],
        )

        fields     = ["name1", "address1", "name2", "address2"]
        importance = {}

        for field in fields:
            print(f"[Sensitivity] Masking: {field}")
            masked_pairs = [
                self._build_text_pair(row, cols, masked_field=field)
                for _, row in sample.iterrows()
            ]
            masked_probs = self._get_probs(
                tokenizer, model,
                [p[0] for p in masked_pairs],
                [p[1] for p in masked_pairs],
            )

            drop = baseline_probs - masked_probs
            importance[field] = {
                "mean_drop":        float(np.mean(drop)),
                "std_drop":         float(np.std(drop)),
                "mean_abs_drop":    float(np.mean(np.abs(drop))),
                "importance_score": float(np.mean(np.abs(drop))),
            }
            print(
                f"  {field:12s} | mean drop={importance[field]['mean_drop']:+.4f} "
                f"| importance={importance[field]['importance_score']:.4f}"
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
        ax.set_xlabel("Importance (mean |prob drop| when masked)")
        ax.set_title(f"Field importance — {self.entity_type}")
        ax.set_xlim(0, max(scores) * 1.3)

        for bar, val in zip(bars, scores[::-1]):
            ax.text(
                bar.get_width() + 0.001, bar.get_y() + bar.get_height() / 2,
                f"{val:.4f}", va="center", fontsize=10,
            )

        plt.tight_layout()
        path = os.path.join(self.plots_dir, "shap_bar.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Sensitivity] Bar chart → {path}")
        return path

    def plot_drop_distribution(
        self, importance: dict, baseline_probs: np.ndarray
    ) -> str:
        fields = list(importance.keys())
        fig, axes = plt.subplots(1, len(fields), figsize=(14, 4))

        for ax, field in zip(axes, fields):
            info = importance[field]
            ax.axhline(0, color="#888780", linewidth=0.8, linestyle="--")
            ax.bar(
                range(len(baseline_probs)),
                [info["mean_drop"]] * len(baseline_probs),
                color="#378ADD", alpha=0.6,
            )
            ax.set_title(field, fontsize=11)
            ax.set_xlabel("sample index", fontsize=9)
            ax.set_ylim(-0.3, 0.3)
            if ax is axes[0]:
                ax.set_ylabel("prob drop when masked", fontsize=9)

        plt.suptitle(
            f"Prediction drop by masked field — {self.entity_type}", fontsize=12
        )
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
        model, tokenizer = self.load_model_and_tokenizer()
        sample           = self.load_sample()

        importance, baseline_probs = self.compute_field_importance(
            model, tokenizer, sample
        )

        ranked = sorted(
            importance.items(),
            key=lambda x: x[1]["importance_score"],
            reverse=True,
        )

        print("\n[Sensitivity] Field ranking:")
        for rank, (field, info) in enumerate(ranked, 1):
            print(f"  {rank}. {field:12s} importance={info['importance_score']:.4f}")

        bar_path     = self.plot_feature_importance(importance)
        summary_path = self.plot_drop_distribution(importance, baseline_probs)

        report = {
            "entity_type":     self.entity_type,
            "n_samples":       len(sample),
            "method":          "field_masking",
            "threshold":       self.threshold,
            "field_ranking":   [f for f, _ in ranked],
            "importance":      importance,
            "most_important":  ranked[0][0],
            "least_important": ranked[-1][0],
        }

        report_path = os.path.join(self.results_dir, "sensitivity_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"[Sensitivity] Report → {report_path}")

        # FIX 2: env var takes priority over config
        mlflow.set_tracking_uri(
            os.environ.get("MLFLOW_TRACKING_URI") or self.config["mlflow"]["tracking_uri"]
        )
        mlflow.set_experiment(self.config["mlflow"]["experiment_name"])

        with mlflow.start_run(
            run_name=f"sensitivity_{self.entity_type}",
            tags={"entity_type": self.entity_type, "stage": "sensitivity"},
        ):
            for field, info in importance.items():
                mlflow.log_metric(f"importance_{field}", info["importance_score"])
            mlflow.log_artifact(report_path,  artifact_path="sensitivity")
            mlflow.log_artifact(bar_path,     artifact_path="sensitivity/plots")
            mlflow.log_artifact(summary_path, artifact_path="sensitivity/plots")

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

        print(f"\n[Summary] Most important  : {report['most_important']}")
        print(f"[Summary] Least important : {report['least_important']}")

    print("\n" + "=" * 70)
    print("SENSITIVITY ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()