"""
Entity Resolution Model Evaluation Script

Loads a trained model from MLflow registry or local path, runs inference
on the test set, computes all metrics, and saves artefacts.

Outputs (per entity type):
    models/{entity_type}/results/test_metrics.json
    models/{entity_type}/results/test_predictions.csv
    models/{entity_type}/plots/confusion_matrix.png
    models/{entity_type}/plots/roc_curve.png
    models/{entity_type}/plots/precision_recall_curve.png
"""

import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
import seaborn as sns
import torch
import yaml
from google.cloud import storage
from sklearn.metrics import (
    accuracy_score,
    auc,
    confusion_matrix,
    precision_recall_curve,
    precision_recall_fscore_support,
    roc_auc_score,
    roc_curve,
)
from transformers import AutoTokenizer, AutoModelForSequenceClassification

DEFAULT_CONFIG_PATH = "config/training_config.yaml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def resolve(template: str, **kwargs) -> str:
    return template.format(**kwargs)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class EntityResolutionEvaluator:
    """
    Loads a trained DeBERTa+LoRA model and evaluates it on the test set.
    All params read from config — no CLI args.
    """

    def __init__(self, config: dict, entity_type: str):
        self.config      = config
        self.entity_type = entity_type.lower()

        # Paths
        base_dir       = config["output"]["base_dir"]
        model_dir      = resolve(config["output"]["model_dir"],
                                 base_dir=base_dir, entity_type=self.entity_type)
        self.model_dir      = model_dir
        self.final_model_dir = resolve(config["output"]["final_model_dir"],
                                       base_dir=base_dir, model_dir=model_dir,
                                       entity_type=self.entity_type)
        self.results_dir = resolve(config["output"]["results_dir"],
                                   base_dir=base_dir, model_dir=model_dir,
                                   entity_type=self.entity_type)
        self.plots_dir   = resolve(config["output"]["plots_dir"],
                                   base_dir=base_dir, model_dir=model_dir,
                                   entity_type=self.entity_type)
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.plots_dir,   exist_ok=True)

        # Device
        use_cuda  = config["device"]["use_cuda"]
        cuda_id   = config["device"]["cuda_device"]
        self.device = (
            torch.device(f"cuda:{cuda_id}")
            if use_cuda and torch.cuda.is_available()
            else torch.device("cpu")
        )

        print(f"[Evaluator] entity_type : {self.entity_type}")
        print(f"[Evaluator] device      : {self.device}")
        print(f"[Evaluator] model path  : {self.final_model_dir}")

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------

    def download_test_data(self) -> pd.DataFrame:
        """Download test.csv from GCS and return as DataFrame."""
        print(f"[Data] Downloading test set for '{self.entity_type}' from GCS…")

        bucket_name = self.config["data"]["gcs_bucket"]
        gcs_path    = self.config["data"]["gcs_path"]
        local_dir   = self.config["data"]["local_data_dir"]
        os.makedirs(local_dir, exist_ok=True)

        blob_path  = f"{gcs_path}/{self.entity_type}/test.csv"
        local_path = os.path.join(local_dir, f"{self.entity_type}_test.csv")

        # Skip re-download if already present (e.g. train.py ran first)
        if os.path.exists(local_path):
            print(f"[Data]   Found local copy: {local_path}")
        else:
            client = storage.Client()
            client.bucket(bucket_name).blob(blob_path).download_to_filename(local_path)
            print(f"[Data]   Downloaded → {local_path}")

        df = pd.read_csv(local_path)
        print(f"[Data]   {len(df)} test pairs loaded")
        return df

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------

    def load_model_and_tokenizer(self):
        """
        Load LoRA fine-tuned model + tokenizer.
        Loads base model first then applies saved LoRA adapters on top.
        Falls back to MLflow Model Registry if local path not found.
        """
        from peft import PeftModel

        if os.path.isdir(self.final_model_dir):
            print(f"[Model] Loading from local path: {self.final_model_dir}")

            cache_dir = self.config["model"].get("cache_dir")

            # Step 1 — load base model from HuggingFace (or cache)
            base_model = AutoModelForSequenceClassification.from_pretrained(
                self.config["model"]["base_model"],
                num_labels=self.config["model"]["num_labels"],
                cache_dir=cache_dir,
            )

            # Step 2 — apply saved LoRA adapters on top of base model
            model = PeftModel.from_pretrained(base_model, self.final_model_dir)

            # Step 3 — merge adapters into base weights for faster inference
            model = model.merge_and_unload()

            tokenizer = AutoTokenizer.from_pretrained(
                self.final_model_dir, cache_dir=cache_dir
            )
        else:
            registered_name = self.config["mlflow"]["registered_model_name"].format(
                entity_type=self.entity_type
            )
            model_uri = f"models:/{registered_name}/latest"
            print(f"[Model] Local path not found — loading from MLflow: {model_uri}")
            pyfunc_model = mlflow.pyfunc.load_model(model_uri)
            model     = pyfunc_model._model_impl.python_model.model
            tokenizer = pyfunc_model._model_impl.python_model.tokenizer

        model.to(self.device)
        model.eval()
        print(f"[Model] Loaded successfully on {self.device}")
        return model, tokenizer

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def run_inference(
        self,
        model,
        tokenizer,
        test_df: pd.DataFrame,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Run batch inference over test_df.

        Returns:
            true_labels   : shape (N,)
            pred_labels   : shape (N,)  — argmax predictions
            pred_probs    : shape (N,)  — probability of class 1 (match)
        """
        print("[Inference] Running inference on test set…")

        cols       = self.config["data"]["columns"]
        max_length = self.config["model"]["max_length"]
        batch_size = self.config["training"]["batch_size"]
        threshold  = self.config["validation"]["classification_threshold"]

        def _build_text(row: pd.Series) -> str:
            return (
                f"{row[cols['name1']]} [SEP] {row[cols['address1']]} "
                f"[SEP] {row[cols['name2']]} [SEP] {row[cols['address2']]}"
            )

        texts       = [_build_text(row) for _, row in test_df.iterrows()]
        true_labels = test_df[cols["label"]].values

        all_probs = []

        with torch.no_grad():
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i : i + batch_size]
                enc = tokenizer(
                    batch_texts,
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

                if (i // batch_size) % 10 == 0:
                    print(f"[Inference]   batch {i // batch_size + 1} / "
                          f"{len(texts) // batch_size + 1}")

        pred_probs  = np.array(all_probs)
        pred_labels = (pred_probs >= threshold).astype(int)

        print(f"[Inference] Done — {len(pred_labels)} predictions")
        return true_labels, pred_labels, pred_probs

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def compute_metrics(
        self,
        true_labels: np.ndarray,
        pred_labels: np.ndarray,
        pred_probs: np.ndarray,
    ) -> dict:
        """Compute all metrics defined in config.validation.metrics."""
        precision, recall, f1, _ = precision_recall_fscore_support(
            true_labels, pred_labels, average="binary"
        )
        accuracy  = accuracy_score(true_labels, pred_labels)
        auc_score = roc_auc_score(true_labels, pred_probs)

        metrics = {
            "test_accuracy":  float(accuracy),
            "test_precision": float(precision),
            "test_recall":    float(recall),
            "test_f1":        float(f1),
            "test_auc":       float(auc_score),
            "total_samples":  int(len(true_labels)),
            "positive_rate":  float(true_labels.mean()),
            "predicted_positive_rate": float(pred_labels.mean()),
        }

        print("[Metrics] Test results:")
        for k, v in metrics.items():
            print(f"  {k}: {v:.4f}" if isinstance(v, float) else f"  {k}: {v}")

        return metrics

    # ------------------------------------------------------------------
    # Plots
    # ------------------------------------------------------------------

    def save_confusion_matrix(
        self, true_labels: np.ndarray, pred_labels: np.ndarray
    ) -> str:
        cm = confusion_matrix(true_labels, pred_labels)
        plt.figure(figsize=(6, 5))
        sns.heatmap(
            cm, annot=True, fmt="d", cmap="Blues",
            xticklabels=["no-match", "match"],
            yticklabels=["no-match", "match"],
        )
        plt.title(f"Confusion Matrix — {self.entity_type}")
        plt.ylabel("True label")
        plt.xlabel("Predicted label")
        plt.tight_layout()
        path = os.path.join(self.plots_dir, "confusion_matrix.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Plots] Confusion matrix → {path}")
        return path

    def save_roc_curve(
        self, true_labels: np.ndarray, pred_probs: np.ndarray
    ) -> str:
        fpr, tpr, _ = roc_curve(true_labels, pred_probs)
        roc_auc     = auc(fpr, tpr)
        plt.figure(figsize=(6, 5))
        plt.plot(fpr, tpr, color="#378ADD", lw=2,
                 label=f"ROC curve (AUC = {roc_auc:.3f})")
        plt.plot([0, 1], [0, 1], color="#B4B2A9", lw=1, linestyle="--")
        plt.xlabel("False positive rate")
        plt.ylabel("True positive rate")
        plt.title(f"ROC Curve — {self.entity_type}")
        plt.legend(loc="lower right")
        plt.tight_layout()
        path = os.path.join(self.plots_dir, "roc_curve.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Plots] ROC curve → {path}")
        return path

    def save_precision_recall_curve(
        self, true_labels: np.ndarray, pred_probs: np.ndarray
    ) -> str:
        prec, rec, _ = precision_recall_curve(true_labels, pred_probs)
        pr_auc       = auc(rec, prec)
        plt.figure(figsize=(6, 5))
        plt.plot(rec, prec, color="#1D9E75", lw=2,
                 label=f"PR curve (AUC = {pr_auc:.3f})")
        plt.xlabel("Recall")
        plt.ylabel("Precision")
        plt.title(f"Precision-Recall Curve — {self.entity_type}")
        plt.legend(loc="upper right")
        plt.tight_layout()
        path = os.path.join(self.plots_dir, "precision_recall_curve.png")
        plt.savefig(path, dpi=150)
        plt.close()
        print(f"[Plots] PR curve → {path}")
        return path

    # ------------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------------

    def save_metrics_json(self, metrics: dict) -> str:
        path = os.path.join(self.results_dir, "test_metrics.json")
        with open(path, "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"[Output] Metrics JSON → {path}")
        return path

    def save_predictions_csv(
        self,
        test_df: pd.DataFrame,
        pred_labels: np.ndarray,
        pred_probs: np.ndarray,
    ) -> str:
        """
        Save test_predictions.csv — includes original columns plus
        predicted_label and predicted_prob for downstream bias_detection.py.
        """
        out = test_df.copy()
        out["predicted_label"] = pred_labels
        out["predicted_prob"]  = pred_probs
        path = os.path.join(self.results_dir, "test_predictions.csv")
        out.to_csv(path, index=False)
        print(f"[Output] Predictions CSV → {path}")
        return path

    # ------------------------------------------------------------------
    # Quality gate
    # ------------------------------------------------------------------

    def quality_gate(self, metrics: dict) -> bool:
        """Check metrics against thresholds in config.validation."""
        thresholds = self.config["validation"]
        checks = {
            "f1":        (metrics["test_f1"],        thresholds["min_f1"]),
            "precision": (metrics["test_precision"], thresholds["min_precision"]),
            "recall":    (metrics["test_recall"],    thresholds["min_recall"]),
            "auc":       (metrics["test_auc"],       thresholds["min_auc"]),
        }
        passed = True
        print("[QualityGate] Threshold checks:")
        for metric, (value, threshold) in checks.items():
            ok     = value >= threshold
            status = "PASS" if ok else "FAIL"
            print(f"  {metric}: {value:.4f} >= {threshold} → {status}")
            if not ok:
                passed = False
        return passed

    # ------------------------------------------------------------------
    # Main evaluate() — called from main()
    # ------------------------------------------------------------------

    def evaluate(self, run_id: str | None = None) -> dict:
        """
        Full evaluation pipeline for one entity type.
        Logs everything to the provided MLflow run_id (or active run).

        Returns the metrics dict.
        """
        # 1. Data
        test_df = self.download_test_data()

        # 2. Model
        model, tokenizer = self.load_model_and_tokenizer()

        # 3. Inference
        true_labels, pred_labels, pred_probs = self.run_inference(
            model, tokenizer, test_df
        )

        # 4. Metrics
        metrics = self.compute_metrics(true_labels, pred_labels, pred_probs)

        # 5. Plots
        cm_path  = self.save_confusion_matrix(true_labels, pred_labels)
        roc_path = self.save_roc_curve(true_labels, pred_probs)
        pr_path  = self.save_precision_recall_curve(true_labels, pred_probs)

        # 6. Save outputs
        metrics_path = self.save_metrics_json(metrics)
        preds_path   = self.save_predictions_csv(test_df, pred_labels, pred_probs)

        # 7. Log to MLflow (attach to existing run if run_id provided)
        log_ctx = (
            mlflow.start_run(run_id=run_id, nested=True)
            if run_id
            else mlflow.start_run(
                run_name=f"evaluate_{self.entity_type}",
                tags={"entity_type": self.entity_type, "stage": "evaluation"},
            )
        )
        with log_ctx:
            mlflow.log_metrics(metrics)
            mlflow.log_artifact(cm_path,      artifact_path="plots")
            mlflow.log_artifact(roc_path,     artifact_path="plots")
            mlflow.log_artifact(pr_path,      artifact_path="plots")
            mlflow.log_artifact(metrics_path, artifact_path="results")
            mlflow.log_artifact(preds_path,   artifact_path="results")
            mlflow.set_tag("quality_gate", "GO" if self.quality_gate(metrics) else "NO-GO")

        return metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config_path  = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config       = load_config(config_path)
    entity_types = config["data"]["entity_types"]

    # Optional: attach to an existing training run
    # Set MLFLOW_RUN_ID env var if you want evaluate to log into train's run
    run_id = os.environ.get("MLFLOW_RUN_ID", None)

    print("=" * 70)
    print("ENTITY RESOLUTION — MODEL EVALUATION")
    print("=" * 70)
    print(f"Config       : {config_path}")
    print(f"Entity types : {entity_types}")
    print(f"MLflow run   : {run_id or 'new run per entity'}")
    print("=" * 70)

    mlflow.set_tracking_uri(config["mlflow"]["tracking_uri"])
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    summary = {}

    for entity_type in entity_types:
        print(f"\n{'=' * 70}")
        print(f"Evaluating: {entity_type.upper()}")
        print(f"{'=' * 70}")

        evaluator = EntityResolutionEvaluator(config=config, entity_type=entity_type)
        metrics   = evaluator.evaluate(run_id=run_id)

        summary[entity_type] = metrics

    print("\n" + "=" * 70)
    print("EVALUATION COMPLETE — SUMMARY")
    print("=" * 70)
    for et, m in summary.items():
        print(
            f"  {et:15s} | F1={m['test_f1']:.4f} "
            f"| AUC={m['test_auc']:.4f} "
            f"| Precision={m['test_precision']:.4f} "
            f"| Recall={m['test_recall']:.4f}"
        )
    print("=" * 70)


if __name__ == "__main__":
    main()