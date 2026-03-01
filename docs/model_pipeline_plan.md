# Model Pipeline Strategic Plan

## Entity Resolution: Multi-Domain LoRA Adapter Training

**Status:** Planning
**Data Pipeline:** Complete (55K training pairs ready)
**Target:** Production-ready multi-domain entity matching

---

## 1. Objectives

### Primary Goals
- Train XLM-RoBERTa with domain-specific LoRA adapters
- Target: 90%+ F1 score per entity type
- Multi-lingual capability for international entity matching
- Production-ready inference (<100ms latency)

### Deliverables
- 3 trained LoRA adapters (person, product, publication)
- MLflow experiment tracking
- Model cards with performance metrics
- Inference-ready model artifacts

---

## 2. Architecture Decisions

### Base Model Selection

| Option | Model | Parameters | Pros | Cons |
|--------|-------|------------|------|------|
| **A** | XLM-RoBERTa-large | 560M | Multi-lingual, proven | Larger, slower |
| B | DeBERTa-v3-large | 304M | Better English, smaller | English-only |
| C | XLM-RoBERTa-base | 270M | Faster, multi-lingual | Less capacity |

**Recommendation:** XLM-RoBERTa-large for multi-lingual support (OFAC contains international names)

### LoRA Adapter Configuration

```python
lora_config = {
    "r": 16,                    # Rank (balance capacity vs efficiency)
    "lora_alpha": 32,           # Scaling factor
    "lora_dropout": 0.1,        # Regularization
    "target_modules": [
        "query", "key", "value",  # Attention layers
        "dense"                    # FFN layers
    ],
    "task_type": "SEQ_CLS"      # Sequence classification
}
```

### Training Infrastructure

| Component | Specification |
|-----------|---------------|
| GPU | T4 (16GB) or better |
| Platform | GCP Vertex AI / Local GPU |
| Framework | PyTorch + HuggingFace + PEFT |
| Tracking | MLflow |

---

## 3. Training Data Strategy

### Current Data (Data Pipeline Output)

| Entity Type | Train | Validation | Test | Total |
|-------------|-------|------------|------|-------|
| Person | 17,500 | 3,750 | 3,750 | 25,000 |
| Product | 14,000 | 3,000 | 3,000 | 20,000 |
| Publication | 7,000 | 1,500 | 1,500 | 10,000 |
| **Total** | **38,500** | **8,250** | **8,250** | **55,000** |

### Data Loading

```python
# Load from data pipeline output
from pathlib import Path
import pandas as pd

def load_training_data(entity_type: str):
    base_dir = Path("Data-Pipeline/data/training") / entity_type
    return {
        "train": pd.read_csv(base_dir / "train.csv"),
        "val": pd.read_csv(base_dir / "val.csv"),
        "test": pd.read_csv(base_dir / "test.csv")
    }

# Load all entity types
person_data = load_training_data("person")
product_data = load_training_data("product")
publication_data = load_training_data("publication")
```

### Data Augmentation Plan (Future)

| Week | Dataset | Entity Type | Additional Pairs |
|------|---------|-------------|------------------|
| 2 | Rakuten | Product | +20K |
| 3 | GLEIF LEI | Company | +15K |
| 3 | Wikipedia | Person | +10K |

**Target Final Dataset:**
- Person: 35K pairs (4 sources)
- Product: 34K pairs (3 sources)
- Publication: 17K pairs (2 sources)
- Company: 15K pairs (NEW entity type)

---

## 4. Training Pipeline Components

### 4.1 Custom Dataset Class

```python
import torch
from torch.utils.data import Dataset
from transformers import AutoTokenizer

class EntityPairDataset(Dataset):
    def __init__(self, df, tokenizer, max_length=256):
        self.df = df
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]

        # Combine entity pair as input
        text = f"{row['name1']} [SEP] {row['name2']} [SEP] {row['address1']} [SEP] {row['address2']}"

        encoding = self.tokenizer(
            text,
            max_length=self.max_length,
            padding="max_length",
            truncation=True,
            return_tensors="pt"
        )

        return {
            "input_ids": encoding["input_ids"].squeeze(),
            "attention_mask": encoding["attention_mask"].squeeze(),
            "labels": torch.tensor(row["label"], dtype=torch.long)
        }
```

### 4.2 Model with LoRA

```python
from transformers import AutoModelForSequenceClassification
from peft import get_peft_model, LoraConfig, TaskType

def create_lora_model(base_model_name: str = "xlm-roberta-large"):
    # Load base model
    model = AutoModelForSequenceClassification.from_pretrained(
        base_model_name,
        num_labels=2  # Match / Non-match
    )

    # Configure LoRA
    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        lora_dropout=0.1,
        target_modules=["query", "key", "value", "dense"],
        task_type=TaskType.SEQ_CLS
    )

    # Apply LoRA
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    return model
```

### 4.3 Training Loop

```python
from transformers import Trainer, TrainingArguments

training_args = TrainingArguments(
    output_dir="./results",
    num_train_epochs=5,
    per_device_train_batch_size=32,
    per_device_eval_batch_size=64,
    learning_rate=2e-5,
    weight_decay=0.01,
    evaluation_strategy="steps",
    eval_steps=500,
    save_strategy="steps",
    save_steps=500,
    load_best_model_at_end=True,
    metric_for_best_model="f1",
    logging_dir="./logs",
    logging_steps=100,
    fp16=True,  # Mixed precision
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics,
)

trainer.train()
```

### 4.4 Evaluation Metrics

```python
from sklearn.metrics import precision_recall_fscore_support, accuracy_score

def compute_metrics(eval_pred):
    predictions, labels = eval_pred
    predictions = predictions.argmax(axis=-1)

    precision, recall, f1, _ = precision_recall_fscore_support(
        labels, predictions, average="binary"
    )
    accuracy = accuracy_score(labels, predictions)

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }
```

---

## 5. Experiment Tracking

### MLflow Setup

```python
import mlflow
from mlflow.tracking import MlflowClient

# Start experiment
mlflow.set_experiment("entity-resolution-lora")

with mlflow.start_run(run_name="person-adapter-v1"):
    # Log parameters
    mlflow.log_params({
        "base_model": "xlm-roberta-large",
        "lora_rank": 16,
        "lora_alpha": 32,
        "learning_rate": 2e-5,
        "batch_size": 32,
        "epochs": 5,
        "entity_type": "person",
        "dvc_version": "abc123"  # From data pipeline
    })

    # Train model
    trainer.train()

    # Log metrics
    eval_results = trainer.evaluate()
    mlflow.log_metrics(eval_results)

    # Log model artifact
    mlflow.pytorch.log_model(model, "model")
```

### Tracked Metrics

| Metric | Description |
|--------|-------------|
| train_loss | Training loss per epoch |
| eval_loss | Validation loss |
| eval_f1 | F1 score on validation |
| eval_precision | Precision on validation |
| eval_recall | Recall on validation |
| eval_accuracy | Accuracy on validation |

---

## 6. Timeline (6 Weeks)

### Week 1: Setup + Baseline
- [ ] Environment setup (GPU, libraries)
- [ ] Load training data from data pipeline
- [ ] Train baseline model (no LoRA)
- [ ] Establish baseline metrics
- [ ] Setup MLflow tracking

### Week 2: LoRA Adapter Training
- [ ] Train person adapter
- [ ] Train product adapter
- [ ] Train publication adapter
- [ ] Evaluate on test sets
- [ ] Compare to baseline

### Week 3: Dataset Expansion
- [ ] Integrate Rakuten dataset (products)
- [ ] Integrate GLEIF dataset (companies)
- [ ] Re-train with expanded data
- [ ] Compare performance

### Week 4: Hyperparameter Tuning
- [ ] Grid search or Optuna integration
- [ ] Find optimal LoRA rank (8, 16, 32)
- [ ] Find optimal learning rate
- [ ] Re-train best configurations

### Week 5: Evaluation & Analysis
- [ ] Comprehensive error analysis
- [ ] Benchmark against baselines
- [ ] Document performance
- [ ] Create model cards

### Week 6: Model Registry & Handoff
- [ ] Package final models
- [ ] Create deployment artifacts
- [ ] Prepare for inference pipeline
- [ ] Handoff documentation

---

## 7. Success Criteria

### Minimum (Pass)

| Adapter | F1 Score |
|---------|----------|
| Person | ≥ 85% |
| Product | ≥ 80% |
| Publication | ≥ 75% |

### Target (Excellent)

| Adapter | F1 Score |
|---------|----------|
| Person | ≥ 92% |
| Product | ≥ 88% |
| Publication | ≥ 85% |

### Stretch (Production-Ready)

| Criteria | Target |
|----------|--------|
| All adapters | ≥ 90% F1 |
| Multi-lingual | Works on non-English |
| Inference latency | < 100ms |

---

## 8. Dataset Expansion Roadmap

### Model Pipeline Phase (Weeks 2-3)

| Dataset | Entity Type | Pairs | Priority |
|---------|-------------|-------|----------|
| Rakuten | Product | +20K | High |
| GLEIF LEI | Company | +15K | High |
| Wikipedia | Person | +10K | Medium |

### Deployment Phase (If Time)

| Dataset | Entity Type | Pairs | Priority |
|---------|-------------|-------|----------|
| Synthea | Medical | +10K | Low |
| EU Sanctions | Person | +5K | Low |

### Not Needed for MVP

- ICD-10 (too specialized)
- SNOMED (medical only)
- FDA Drugs (niche)

---

## 9. Risks & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| GPU availability | Medium | High | Use GCP Vertex AI or Colab Pro |
| Training time too long | Medium | Medium | Start with smaller model (base), scale up |
| Overfitting on synthetic | High | High | Add real datasets by Week 3 |
| Multi-lingual poor | Medium | Medium | XLM-RoBERTa + validate on non-ASCII |
| Memory issues | Medium | High | Gradient accumulation, reduce batch size |

---

## 10. Handoff: Data → Model Pipeline

### What Model Team Receives

| Item | Status | Location |
|------|--------|----------|
| Training pairs | ✅ 55K ready | `data/training/` |
| Train/val/test splits | ✅ 70/15/15 | Per entity type folder |
| Zero data leakage | ✅ Verified | Quality gate passed |
| Entity type labels | ✅ Included | `entity_type` column |
| Source tracking | ✅ Included | `source_dataset` column |
| Balanced labels | ✅ 45-55% | Bias detection verified |

### What Model Team Does

```bash
# 1. Run data pipeline locally (if needed)
cd Data-Pipeline
docker compose up -d
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline

# 2. Install model dependencies
pip install torch transformers peft mlflow scikit-learn

# 3. Load training data
python -c "
import pandas as pd
train = pd.read_csv('data/training/person/train.csv')
print(f'Person training: {len(train)} pairs')
"

# 4. Train adapter
python train_adapter.py --entity-type person
```

---

## 11. Directory Structure (Proposed)

```
Entity-Resolution/
├── Data-Pipeline/           # ✅ Complete
│   └── data/training/       # Training data ready
│
├── Model-Pipeline/          # 🚧 To Build
│   ├── src/
│   │   ├── data/
│   │   │   └── dataset.py   # EntityPairDataset
│   │   ├── models/
│   │   │   └── lora.py      # LoRA model config
│   │   ├── training/
│   │   │   └── trainer.py   # Training loop
│   │   └── evaluation/
│   │       └── metrics.py   # Evaluation functions
│   ├── configs/
│   │   ├── person.yaml
│   │   ├── product.yaml
│   │   └── publication.yaml
│   ├── scripts/
│   │   └── train_adapter.py
│   ├── notebooks/
│   │   └── exploration.ipynb
│   └── requirements.txt
│
├── docs/
│   └── model_pipeline_plan.md  # This document
│
└── mlruns/                  # MLflow tracking
```

---

## 12. Quick Start Commands

```bash
# Run data pipeline locally first
cd Data-Pipeline
docker compose up -d
docker exec data-pipeline-airflow-scheduler-1 airflow dags trigger er_data_pipeline

# Wait for completion (~4 minutes)
docker exec data-pipeline-airflow-scheduler-1 airflow tasks states-for-dag-run \
  er_data_pipeline "RUN_ID"

# Verify training data exists
ls -la data/training/person/
ls -la data/training/product/
ls -la data/training/publication/

# Check data counts
wc -l data/training/*/train.csv
```

---

**Document Version:** 1.0
**Created:** March 2026
**Last Updated:** March 2026
