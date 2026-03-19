"""
Entity Resolution Model Training Script

Fine-tunes DeBERTa-v3-base with LoRA adapters for multi-domain entity resolution.
Tracks experiments with MLflow and validates against fairness criteria.

Config-driven — no CLI arguments. Set config path via env var or default:
    CONFIG_PATH=config/training_config.yaml python scripts/train.py

Entity types are read from config: data.entity_types
"""

import os
import yaml
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import Dict, List, Tuple

import mlflow
import mlflow.transformers
import numpy as np
import pandas as pd
import torch
from datasets import Dataset
from google.cloud import storage
from peft import LoraConfig, TaskType, get_peft_model
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
    roc_auc_score,
)
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
)

# ---------------------------------------------------------------------------
# Config path — override via env var, fallback to default
# ---------------------------------------------------------------------------
DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> Dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def resolve_path(template: str, **kwargs) -> str:
    """Format a path template from config substituting provided kwargs."""
    return template.format(**kwargs)


# ---------------------------------------------------------------------------
# Trainer class
# ---------------------------------------------------------------------------

class EntityResolutionTrainer:
    """Manages training of entity resolution models with LoRA fine-tuning."""

    def __init__(self, config: Dict, entity_type: str):
        """
        Args:
            config:      Loaded YAML config dict.
            entity_type: One of the entity types listed in config data.entity_types.
        """
        self.entity_type = entity_type.lower()
        self.config = config
        self.tokenizer = None  # set during prepare_datasets

        # Reproducibility
        seed = self.config.get("seed", 42)
        torch.manual_seed(seed)
        np.random.seed(seed)

        # Device
        use_cuda = self.config["device"]["use_cuda"]
        cuda_id = self.config["device"]["cuda_device"]
        self.device = (
            torch.device(f"cuda:{cuda_id}")
            if use_cuda and torch.cuda.is_available()
            else torch.device("cpu")
        )

        print(f"[Trainer] entity_type : {self.entity_type}")
        print(f"[Trainer] device      : {self.device}")
        print(f"[Trainer] seed        : {seed}")

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------

    def download_training_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Download train / val / test CSVs from GCS and return as DataFrames."""
        print(f"[Data] Downloading {self.entity_type} data from GCS…")

        bucket_name = self.config["data"]["gcs_bucket"]
        gcs_path    = self.config["data"]["gcs_path"]
        local_dir   = self.config["data"]["local_data_dir"]
        os.makedirs(local_dir, exist_ok=True)

        client = storage.Client()
        bucket = client.bucket(bucket_name)

        splits = {}
        for split in ["train", "val", "test"]:
            blob_path  = f"{gcs_path}/{self.entity_type}/{split}.csv"
            local_path = os.path.join(local_dir, f"{self.entity_type}_{split}.csv")
            try:
                bucket.blob(blob_path).download_to_filename(local_path)
            except Exception as e:
                raise RuntimeError(f"Failed to download {blob_path}: {e}")
            splits[split] = pd.read_csv(local_path)
            print(f"[Data]   {split}: {len(splits[split])} pairs")

        return splits["train"], splits["val"], splits["test"]

    def prepare_datasets(
        self,
        train_df: pd.DataFrame,
        val_df: pd.DataFrame,
        test_df: pd.DataFrame,
    ) -> Tuple[Dataset, Dataset, Dataset]:
        """Tokenize entity-pair DataFrames into HuggingFace Datasets."""
        print("[Data] Tokenizing datasets…")

        cache_dir  = self.config["model"].get("cache_dir")
        max_length = self.config["model"]["max_length"]
        cols       = self.config["data"]["columns"]

        tokenizer = AutoTokenizer.from_pretrained(
            self.config["model"]["base_model"], cache_dir=cache_dir
        )
        self.tokenizer = tokenizer

        def _build_text(row: pd.Series) -> str:
            # FIX: explicit [SEP] separators between fields so the model can
            # distinguish entity-1 fields from entity-2 fields.
            return (
                f"{row[cols['name1']]} [SEP] {row[cols['address1']]} "
                f"[SEP] {row[cols['name2']]} [SEP] {row[cols['address2']]}"
            )

        def _tokenize(df: pd.DataFrame) -> Dataset:
            texts = [_build_text(row) for _, row in df.iterrows()]
            enc   = tokenizer(
                texts,
                truncation=True,
                padding="max_length",
                max_length=max_length,
            )
            # FIX: DeBERTa-v3 does not use token_type_ids — drop them
            # to avoid shape mismatches inside the model.
            dataset_dict = {
                "input_ids":      enc["input_ids"],
                "attention_mask": enc["attention_mask"],
                "labels":         df[cols["label"]].tolist(),
            }
            return Dataset.from_dict(dataset_dict)

        train_ds = _tokenize(train_df)
        val_ds   = _tokenize(val_df)
        test_ds  = _tokenize(test_df)

        print(
            f"[Data] Tokenized — train: {len(train_ds)} | "
            f"val: {len(val_ds)} | test: {len(test_ds)}"
        )
        return train_ds, val_ds, test_ds

    # ------------------------------------------------------------------
    # Model
    # ------------------------------------------------------------------

    def build_model(self):
        """Load DeBERTa-v3-base and attach LoRA adapters from config."""
        print("[Model] Building DeBERTa + LoRA…")

        cache_dir = self.config["model"].get("cache_dir")
        base = AutoModelForSequenceClassification.from_pretrained(
            self.config["model"]["base_model"],
            num_labels=self.config["model"]["num_labels"],
            cache_dir=cache_dir,
        )

        lora_cfg = LoraConfig(
            task_type    = TaskType.SEQ_CLS,
            r            = self.config["lora"]["r"],
            lora_alpha   = self.config["lora"]["lora_alpha"],
            lora_dropout = self.config["lora"]["lora_dropout"],
            target_modules = self.config["lora"]["target_modules"],
            bias         = self.config["lora"]["bias"],
        )

        model = get_peft_model(base, lora_cfg)
        model.print_trainable_parameters()
        return model

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def compute_metrics(self, eval_pred) -> Dict:
        logits, labels = eval_pred
        preds = np.argmax(logits, axis=-1)

        precision, recall, f1, _ = precision_recall_fscore_support(
            labels, preds, average="binary"
        )
        accuracy = accuracy_score(labels, preds)
        probs    = torch.softmax(torch.tensor(logits, dtype=torch.float32), dim=-1)[:, 1].numpy()
        auc      = roc_auc_score(labels, probs)

        return {
            "accuracy":  float(accuracy),
            "precision": float(precision),
            "recall":    float(recall),
            "f1":        float(f1),
            "auc":       float(auc),
        }

    # ------------------------------------------------------------------
    # Training  (expects an active MLflow run opened by main())
    # ------------------------------------------------------------------

    def train(self, train_ds: Dataset, val_ds: Dataset):
        """
        Fine-tune the model.  Logs params and metrics to the *already-active*
        MLflow run — does NOT open its own run.

        Returns the trained model.
        """
        print("[Training] Starting…")

        base_dir  = self.config["output"]["base_dir"]
        model_dir = resolve_path(
            self.config["output"]["model_dir"],
            base_dir=base_dir,
            entity_type=self.entity_type,
        )
        os.makedirs(model_dir, exist_ok=True)

        # Log all config params to the active run
        mlflow.log_params({
            "entity_type":                self.entity_type,
            "base_model":                 self.config["model"]["base_model"],
            "max_length":                 self.config["model"]["max_length"],
            "batch_size":                 self.config["training"]["batch_size"],
            "learning_rate":              self.config["training"]["learning_rate"],
            "num_epochs":                 self.config["training"]["num_epochs"],
            "warmup_steps":               self.config["training"]["warmup_steps"],
            "weight_decay":               self.config["training"]["weight_decay"],
            "gradient_accumulation_steps": self.config["training"]["gradient_accumulation_steps"],
            "lora_r":                     self.config["lora"]["r"],
            "lora_alpha":                 self.config["lora"]["lora_alpha"],
            "lora_dropout":               self.config["lora"]["lora_dropout"],
        })

        for key, value in self.config["mlflow"]["tags"].items():
            mlflow.set_tag(key, value)
        mlflow.set_tag("entity_type", self.entity_type)

        model = self.build_model()
        model.to(self.device)

        training_args = TrainingArguments(
            output_dir          = model_dir,
            num_train_epochs    = self.config["training"]["num_epochs"],
            per_device_train_batch_size = self.config["training"]["batch_size"],
            per_device_eval_batch_size  = self.config["training"]["batch_size"],
            learning_rate       = self.config["training"]["learning_rate"],
            weight_decay        = self.config["training"]["weight_decay"],
            warmup_steps        = self.config["training"]["warmup_steps"],
            gradient_accumulation_steps = self.config["training"]["gradient_accumulation_steps"],
            max_grad_norm       = self.config["training"]["max_grad_norm"],
            adam_beta1          = self.config["training"]["adam_beta1"],
            adam_beta2          = self.config["training"]["adam_beta2"],
            adam_epsilon        = self.config["training"]["adam_epsilon"],
            lr_scheduler_type   = self.config["training"]["lr_scheduler_type"],
            evaluation_strategy = self.config["training"]["evaluation_strategy"],
            save_strategy       = self.config["training"]["save_strategy"],
            load_best_model_at_end  = self.config["training"]["load_best_model_at_end"],
            metric_for_best_model   = self.config["training"]["metric_for_best_model"],
            greater_is_better       = self.config["training"]["greater_is_better"],
            logging_dir         = self.config["training"]["logging_dir"],
            logging_steps       = self.config["training"]["logging_steps"],
            logging_first_step  = self.config["training"]["logging_first_step"],
            save_total_limit    = self.config["training"]["save_total_limit"],
            save_steps          = self.config["training"]["save_steps"],
            fp16                = self.config["device"]["fp16"],
            report_to           = "none",   # MLflow handles logging
        )

        trainer = Trainer(
            model           = model,
            args            = training_args,
            train_dataset   = train_ds,
            eval_dataset    = val_ds,
            compute_metrics = self.compute_metrics,
        )

        print(f"[Training] Training on {len(train_ds)} samples…")
        trainer.train()

        val_results = trainer.evaluate()
        mlflow.log_metrics({f"val_{k}": v for k, v in val_results.items()})

        print("[Training] Validation metrics:")
        for k, v in val_results.items():
            print(f"  {k}: {v:.4f}")

        # Save model locally
        final_model_dir = resolve_path(
            self.config["output"]["final_model_dir"],
            base_dir=base_dir,
            model_dir=model_dir,
            entity_type=self.entity_type,
        )
        trainer.save_model(final_model_dir)
        self.tokenizer.save_pretrained(final_model_dir)
        print(f"[Training] Model saved → {final_model_dir}")

        # Register in MLflow Model Registry
        registered_name = self.config["mlflow"]["registered_model_name"].format(
            entity_type=self.entity_type
        )
        mlflow.transformers.log_model(
            transformers_model={
                "model":     trainer.model,
                "tokenizer": self.tokenizer,
            },
            artifact_path    = self.config["mlflow"]["artifact_path"],
            registered_model_name = registered_name,
        )
        print(f"[Training] Registered as: {registered_name}")

        return trainer.model

    # ------------------------------------------------------------------
    # Evaluation  (expects an active MLflow run opened by main())
    # ------------------------------------------------------------------

    def evaluate_on_test(
        self,
        model,
        test_ds: Dataset,
        test_df: pd.DataFrame,
    ) -> Dict:
        """
        Evaluate on the held-out test set.  Logs to the *already-active*
        MLflow run — does NOT open a nested run.

        Returns dict with keys: metrics, confusion_matrix, predictions_df.
        """
        print("[Evaluation] Running test-set evaluation…")

        eval_args = TrainingArguments(
            output_dir                 = "./tmp_eval",
            per_device_eval_batch_size = self.config["training"]["batch_size"],
            report_to                  = "none",
        )

        trainer = Trainer(
            model           = model,
            args            = eval_args,
            compute_metrics = self.compute_metrics,
        )

        predictions    = trainer.predict(test_ds)
        pred_labels    = np.argmax(predictions.predictions, axis=-1)
        true_labels    = predictions.label_ids
        probs          = torch.softmax(
            torch.tensor(predictions.predictions, dtype=torch.float32), dim=-1
        )[:, 1].numpy()

        # Compute metrics
        precision, recall, f1, _ = precision_recall_fscore_support(
            true_labels, pred_labels, average="binary"
        )
        accuracy = accuracy_score(true_labels, pred_labels)
        auc      = roc_auc_score(true_labels, probs)
        cm       = confusion_matrix(true_labels, pred_labels)

        test_metrics = {
            "test_accuracy":  float(accuracy),
            "test_precision": float(precision),
            "test_recall":    float(recall),
            "test_f1":        float(f1),
            "test_auc":       float(auc),
        }

        mlflow.log_metrics(test_metrics)

        print("[Evaluation] Test metrics:")
        for k, v in test_metrics.items():
            print(f"  {k}: {v:.4f}")
        print(
            f"[Evaluation] Confusion matrix:\n"
            f"  TN={cm[0,0]}  FP={cm[0,1]}\n"
            f"  FN={cm[1,0]}  TP={cm[1,1]}"
        )

        # Save artefacts
        base_dir  = self.config["output"]["base_dir"]
        model_dir = resolve_path(
            self.config["output"]["model_dir"],
            base_dir=base_dir,
            entity_type=self.entity_type,
        )
        plots_dir   = resolve_path(
            self.config["output"]["plots_dir"],
            base_dir=base_dir,
            model_dir=model_dir,
            entity_type=self.entity_type,
        )
        results_dir = resolve_path(
            self.config["output"]["results_dir"],
            base_dir=base_dir,
            model_dir=model_dir,
            entity_type=self.entity_type,
        )
        os.makedirs(plots_dir,   exist_ok=True)
        os.makedirs(results_dir, exist_ok=True)

        # Confusion-matrix plot
        plt.figure(figsize=(6, 5))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                    xticklabels=["no-match", "match"],
                    yticklabels=["no-match", "match"])
        plt.title(f"Confusion Matrix — {self.entity_type}")
        plt.ylabel("True label")
        plt.xlabel("Predicted label")
        plt.tight_layout()
        cm_path = os.path.join(plots_dir, "confusion_matrix.png")
        plt.savefig(cm_path, dpi=150)
        plt.close()
        mlflow.log_artifact(cm_path)

        # Metrics JSON
        metrics_path = os.path.join(results_dir, "test_metrics.json")
        with open(metrics_path, "w") as f:
            json.dump(test_metrics, f, indent=2)
        mlflow.log_artifact(metrics_path)

        # Predictions CSV (used downstream by bias_detection.py)
        test_df_out = test_df.copy()
        test_df_out["predicted_label"] = pred_labels
        test_df_out["predicted_prob"]  = probs
        preds_path = os.path.join(results_dir, "test_predictions.csv")
        test_df_out.to_csv(preds_path, index=False)
        mlflow.log_artifact(preds_path)

        print(f"[Evaluation] Artefacts saved → {results_dir}")

        return {
            "metrics":          test_metrics,
            "confusion_matrix": cm,
            "predictions_df":   test_df_out,
        }


# ---------------------------------------------------------------------------
# Quality gate (checks validation thresholds from config)
# ---------------------------------------------------------------------------

def quality_gate(metrics: Dict, config: Dict) -> bool:
    """Return True (GO) if all validation thresholds are met."""
    thresholds = config["validation"]
    checks = {
        "f1":        (metrics.get("test_f1",        0), thresholds["min_f1"]),
        "precision": (metrics.get("test_precision", 0), thresholds["min_precision"]),
        "recall":    (metrics.get("test_recall",    0), thresholds["min_recall"]),
        "auc":       (metrics.get("test_auc",       0), thresholds["min_auc"]),
    }
    passed = True
    print("[QualityGate] Threshold checks:")
    for metric, (value, threshold) in checks.items():
        ok = value >= threshold
        status = "PASS" if ok else "FAIL"
        print(f"  {metric}: {value:.4f} >= {threshold} → {status}")
        if not ok:
            passed = False
    return passed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config_path = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config      = load_config(config_path)

    entity_types: List[str] = config["data"]["entity_types"]

    print("=" * 70)
    print("ENTITY RESOLUTION — MODEL TRAINING PIPELINE")
    print("=" * 70)
    print(f"Config      : {config_path}")
    print(f"Entity types: {entity_types}")
    print("=" * 70)

    # Set up MLflow (once, outside per-entity runs)
    mlflow.set_tracking_uri(config["mlflow"]["tracking_uri"])
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    results = {}

    for entity_type in entity_types:
        print(f"\n{'=' * 70}")
        print(f"Training entity type: {entity_type.upper()}")
        print(f"{'=' * 70}")

        trainer = EntityResolutionTrainer(config=config, entity_type=entity_type)

        # Download data
        train_df, val_df, test_df = trainer.download_training_data()

        # Tokenize
        train_ds, val_ds, test_ds = trainer.prepare_datasets(train_df, val_df, test_df)

        # FIX: single MLflow run wraps both train() and evaluate_on_test()
        # so mlflow.active_run() is never None inside either method.
        run_name = (
            f"{config['mlflow']['run_name_prefix']}_"
            f"{entity_type}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        with mlflow.start_run(run_name=run_name) as run:
            print(f"[MLflow] Run ID: {run.info.run_id}")

            # Train
            model = trainer.train(train_ds, val_ds)

            # Evaluate on test (same run — no nested start_run)
            test_results = trainer.evaluate_on_test(model, test_ds, test_df)

            # Quality gate
            go = quality_gate(test_results["metrics"], config)
            mlflow.set_tag("quality_gate", "GO" if go else "NO-GO")

        results[entity_type] = {
            "run_id":       run.info.run_id,
            "metrics":      test_results["metrics"],
            "quality_gate": "GO" if go else "NO-GO",
        }

        print(f"\n[Pipeline] {entity_type.upper()} complete")
        print(f"  Quality gate : {'GO ✓' if go else 'NO-GO ✗'}")
        print(f"  Test F1      : {test_results['metrics']['test_f1']:.4f}")
        print(f"  Test AUC     : {test_results['metrics']['test_auc']:.4f}")
        print(f"  MLflow run   : {run.info.run_id}")

    print("\n" + "=" * 70)
    print("TRAINING COMPLETE — SUMMARY")
    print("=" * 70)
    for et, r in results.items():
        print(
            f"  {et:15s} | F1={r['metrics']['test_f1']:.4f} "
            f"| AUC={r['metrics']['test_auc']:.4f} "
            f"| {r['quality_gate']}"
        )
    print("=" * 70)


if __name__ == "__main__":
    main()