"""
Entity Resolution — Model Serving Container
============================================

FastAPI server exposing /health and /predict for Vertex AI Endpoint.
Loaded by the er_{entity}_deberta Docker image built by push_to_registry.py.

Routes:
    GET  /health   → 200 once model is loaded
    POST /predict  → pairwise entity resolution inference

Input (POST /predict):
    {
        "instances": [
            {"name1": "Robert Smith", "address1": "123 Main St",
             "name2": "Bob Smith",   "address2": "123 Main St"},
            ...
        ]
    }

Output:
    {
        "predictions": [
            {"match": true,  "probability": 0.91},
            ...
        ]
    }

Environment variables:
    CONFIG_PATH              path to training_config.yaml (default: /app/config/training_config.yaml)
    CLASSIFICATION_THRESHOLD override threshold from config (set by register_model_op)
    MODEL_DIR                local path to LoRA weights   (default: /app/model_weights)
    GCS_MODEL_PATH           gs:// URI to download weights from if MODEL_DIR is empty

Startup sequence:
    1. Load config → get threshold, base model name
    2. If MODEL_DIR has weights → load from there
    3. Else download from GCS_MODEL_PATH → load
    4. Mark server as ready → /health returns 200
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import torch
import yaml
from fastapi import FastAPI, HTTPException
from peft import PeftModel
from pydantic import BaseModel
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("serve")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config/training_config.yaml")

with open(CONFIG_PATH) as f:
    CONFIG = yaml.safe_load(f)

# Threshold — env var from register_model_op takes priority, then config
THRESHOLD = float(
    os.environ.get(
        "CLASSIFICATION_THRESHOLD",
        CONFIG["validation"]["classification_threshold"],
    )
)
BASE_MODEL  = CONFIG["model"]["base_model"]
MAX_LENGTH  = CONFIG["model"]["max_length"]
COLS        = CONFIG["data"]["columns"]
MODEL_DIR   = os.environ.get("MODEL_DIR", "/app/model_weights")
GCS_MODEL_PATH = os.environ.get("GCS_MODEL_PATH", "")

# ---------------------------------------------------------------------------
# Global model state
# ---------------------------------------------------------------------------
_model     = None
_tokenizer = None
_device    = None
_ready     = False


# ---------------------------------------------------------------------------
# Helpers — identical to train.py so inference matches training exactly
# ---------------------------------------------------------------------------

def _safe(val) -> str:
    """Replace NaN / empty with [MISSING] — must match train.py exactly."""
    if pd.isna(val) or str(val).strip() == "":
        return "[MISSING]"
    return str(val).strip()


def _build_text_pair(instance: Dict) -> tuple:
    """
    Build (text_a, text_b) pair — must match train.py _build_text_pair exactly.
    Tokenizer inserts real [SEP] between them.
    Produces: [CLS] record_1 name: ... address: ... [SEP] record_2 name: ... address: ... [SEP]
    """
    text_a = (
        f"record_1 "
        f"name: {_safe(instance.get('name1', ''))} "
        f"address: {_safe(instance.get('address1', ''))}"
    )
    text_b = (
        f"record_2 "
        f"name: {_safe(instance.get('name2', ''))} "
        f"address: {_safe(instance.get('address2', ''))}"
    )
    return text_a, text_b


# ---------------------------------------------------------------------------
# Model loading
# ---------------------------------------------------------------------------

def _download_weights_from_gcs(gcs_path: str, local_dir: str):
    """Download LoRA adapter weights from GCS to local_dir."""
    import re
    from pathlib import Path
    from google.cloud import storage

    log.info(f"Downloading weights from {gcs_path} → {local_dir}")
    match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    bucket_name = match.group(1)
    prefix      = match.group(2).rstrip("/") + "/"

    client = storage.Client()
    blobs  = list(client.list_blobs(bucket_name, prefix=prefix))
    if not blobs:
        raise RuntimeError(f"No files found at {gcs_path}")

    for blob in blobs:
        rel   = blob.name[len(prefix):]
        local = Path(local_dir) / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))
        log.info(f"  Downloaded: {rel}")


def _load_model():
    global _model, _tokenizer, _device, _ready

    _device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"Device: {_device}")
    log.info(f"Threshold: {THRESHOLD}")

    # Download weights if MODEL_DIR is empty and GCS path is provided
    if not os.path.exists(os.path.join(MODEL_DIR, "adapter_config.json")):
        if GCS_MODEL_PATH:
            _download_weights_from_gcs(GCS_MODEL_PATH, MODEL_DIR)
        else:
            raise RuntimeError(
                f"No weights found at {MODEL_DIR} and GCS_MODEL_PATH is not set."
            )

    log.info(f"Loading base model: {BASE_MODEL}")
    base = AutoModelForSequenceClassification.from_pretrained(
        BASE_MODEL,
        num_labels=CONFIG["model"]["num_labels"],
        cache_dir=CONFIG["model"].get("cache_dir"),
    )

    log.info(f"Loading LoRA adapter from: {MODEL_DIR}")
    peft_model = PeftModel.from_pretrained(base, MODEL_DIR)

    # Merge LoRA weights into base — single model, faster inference
    _model = peft_model.merge_and_unload()
    _model.to(_device)
    _model.eval()

    _tokenizer = AutoTokenizer.from_pretrained(MODEL_DIR)

    _ready = True
    log.info("Model ready.")


# ---------------------------------------------------------------------------
# Lifespan — load model at startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


app = FastAPI(title="Entity Resolution Serving", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    instances: List[Dict[str, Any]]


class Prediction(BaseModel):
    match: bool
    probability: float


class PredictResponse(BaseModel):
    predictions: List[Prediction]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    if not _ready:
        raise HTTPException(status_code=503, detail="Model not loaded yet")
    return {"status": "ok"}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    if not _ready:
        raise HTTPException(status_code=503, detail="Model not loaded yet")

    if not request.instances:
        raise HTTPException(status_code=400, detail="instances list is empty")

    # Build text pairs — same logic as train.py
    pairs   = [_build_text_pair(inst) for inst in request.instances]
    texts_a = [p[0] for p in pairs]
    texts_b = [p[1] for p in pairs]

    # Tokenize — same call signature as train.py _tokenize()
    enc = _tokenizer(
        texts_a,
        texts_b,
        truncation=True,
        padding="max_length",
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )

    # DeBERTa-v3 does not use token_type_ids
    input_ids      = enc["input_ids"].to(_device)
    attention_mask = enc["attention_mask"].to(_device)

    with torch.no_grad():
        logits = _model(input_ids=input_ids, attention_mask=attention_mask).logits

    probs = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
    labels = (probs >= THRESHOLD).astype(bool)

    return PredictResponse(
        predictions=[
            Prediction(match=bool(labels[i]), probability=float(probs[i]))
            for i in range(len(probs))
        ]
    )


# ---------------------------------------------------------------------------
# Entry point (local testing only — Vertex AI uses uvicorn directly)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)