"""
Entity Resolution — Model Serving Container
============================================

FastAPI server exposing /health, /predict, and /metrics for Vertex AI Endpoint.

Routes:
    GET  /health   → 200 once model is loaded
    POST /predict  → pairwise entity resolution inference
    GET  /metrics  → prediction logging health stats
"""

import logging
import math
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import torch
import yaml
from fastapi import BackgroundTasks, FastAPI, HTTPException
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

THRESHOLD = float(
    os.environ.get(
        "CLASSIFICATION_THRESHOLD",
        CONFIG["validation"]["classification_threshold"],
    )
)
BASE_MODEL = CONFIG["model"]["base_model"]
MAX_LENGTH = CONFIG["model"]["max_length"]
COLS = CONFIG["data"]["columns"]
MODEL_DIR = os.environ.get("MODEL_DIR", "/app/model_weights")
GCS_MODEL_PATH = os.environ.get("GCS_MODEL_PATH", "")

# Logging config
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BQ_DATASET = os.environ.get("BQ_DATASET", "entity_resolution")
BQ_TABLE = os.environ.get("BQ_TABLE", "prediction_logs")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "unknown")
LOG_BATCH_SIZE = int(os.environ.get("LOG_BATCH_SIZE", "50"))
LOG_FLUSH_INTERVAL = int(os.environ.get("LOG_FLUSH_INTERVAL", "30"))
ENABLE_PREDICTION_LOG = os.environ.get("ENABLE_PREDICTION_LOG", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Global model state
# ---------------------------------------------------------------------------
_model = None
_tokenizer = None
_device = None
_ready = False


# ---------------------------------------------------------------------------
# Prediction Logger
# ---------------------------------------------------------------------------
class PredictionLogger:
    """Batched async writes to BigQuery. Never blocks predictions."""

    def __init__(self):
        self._buffer: List[Dict] = []
        self._lock = threading.Lock()
        self._bq_client = None
        self._table_ref = None
        self._flush_thread: Optional[threading.Thread] = None
        self._running = False
        self.total_logged = 0
        self.total_failed = 0
        self.total_flushed = 0
        self.last_flush_time: Optional[str] = None

    def start(self):
        if not ENABLE_PREDICTION_LOG:
            log.info("Prediction logging is DISABLED")
            return
        if not GCP_PROJECT_ID:
            log.warning("GCP_PROJECT_ID not set — prediction logging disabled")
            return
        try:
            from google.cloud import bigquery
            self._bq_client = bigquery.Client(project=GCP_PROJECT_ID)
            self._table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
            self._running = True
            self._flush_thread = threading.Thread(
                target=self._periodic_flush, daemon=True
            )
            self._flush_thread.start()
            log.info(
                f"Prediction logger started -> {self._table_ref} "
                f"(batch={LOG_BATCH_SIZE}, interval={LOG_FLUSH_INTERVAL}s)"
            )
        except Exception as e:
            log.error(f"Failed to initialize prediction logger: {e}")
            self._bq_client = None

    def log_predictions(self, instances, probabilities, matches, latency_ms):
        if not self._bq_client:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for i, inst in enumerate(instances):
            rows.append({
                "prediction_id": str(uuid.uuid4()),
                "timestamp": now,
                "name1": str(inst.get("name1", "")),
                "address1": str(inst.get("address1", "")),
                "name2": str(inst.get("name2", "")),
                "address2": str(inst.get("address2", "")),
                "probability": float(probabilities[i]),
                "match": bool(matches[i]),
                "model_version": MODEL_VERSION,
                "threshold_used": THRESHOLD,
                "latency_ms": latency_ms,
            })
        with self._lock:
            self._buffer.extend(rows)
            self.total_logged += len(rows)
            if len(self._buffer) >= LOG_BATCH_SIZE:
                self._flush()

    def _periodic_flush(self):
        while self._running:
            time.sleep(LOG_FLUSH_INTERVAL)
            with self._lock:
                if self._buffer:
                    self._flush()

    def _flush(self):
        if not self._buffer or not self._bq_client:
            return
        rows_to_write = self._buffer.copy()
        self._buffer.clear()
        try:
            errors = self._bq_client.insert_rows_json(self._table_ref, rows_to_write)
            if errors:
                log.error(f"BigQuery insert errors: {errors}")
                self.total_failed += len(rows_to_write)
            else:
                self.total_flushed += len(rows_to_write)
                self.last_flush_time = datetime.now(timezone.utc).isoformat()
                log.info(f"Flushed {len(rows_to_write)} prediction logs to BigQuery")
        except Exception as e:
            log.error(f"Failed to flush predictions to BigQuery: {e}")
            self.total_failed += len(rows_to_write)

    def stop(self):
        self._running = False
        with self._lock:
            if self._buffer:
                self._flush()
        log.info("Prediction logger stopped")

    def get_stats(self):
        return {
            "enabled": self._bq_client is not None,
            "total_logged": self.total_logged,
            "total_flushed": self.total_flushed,
            "total_failed": self.total_failed,
            "buffer_size": len(self._buffer),
            "table": self._table_ref if self._bq_client else None,
            "last_flush": self.last_flush_time,
        }


prediction_logger = PredictionLogger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe(val) -> str:
    if (
        val is None
        or (isinstance(val, float) and math.isnan(val))
        or str(val).strip() == ""
    ):
        return "[MISSING]"
    return str(val).strip()


def _build_text_pair(instance: Dict) -> tuple:
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
    import re
    from pathlib import Path
    from google.cloud import storage
    log.info(f"Downloading weights from {gcs_path} -> {local_dir}")
    match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    bucket_name = match.group(1)
    prefix = match.group(2).rstrip("/") + "/"
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    if not blobs:
        raise RuntimeError(f"No files found at {gcs_path}")
    for blob in blobs:
        rel = blob.name[len(prefix):]
        local = Path(local_dir) / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local))
        log.info(f"  Downloaded: {rel}")


def _load_model():
    global _model, _tokenizer, _device, _ready
    _device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    log.info(f"Device: {_device}")
    log.info(f"Threshold: {THRESHOLD}")
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
    _model = peft_model.merge_and_unload()
    _model.to(_device)
    _model.eval()
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_DIR)
    _ready = True
    log.info("Model ready.")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    prediction_logger.start()
    yield
    prediction_logger.stop()


app = FastAPI(title="Entity Resolution Serving", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Schemas
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


@app.get("/metrics")
def metrics():
    return {
        "model_version": MODEL_VERSION,
        "threshold": THRESHOLD,
        "device": str(_device),
        "logging": prediction_logger.get_stats(),
    }


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest, background_tasks: BackgroundTasks):
    if not _ready:
        raise HTTPException(status_code=503, detail="Model not loaded yet")
    if not request.instances:
        raise HTTPException(status_code=400, detail="instances list is empty")

    start_time = time.perf_counter()

    pairs = [_build_text_pair(inst) for inst in request.instances]
    texts_a = [p[0] for p in pairs]
    texts_b = [p[1] for p in pairs]

    enc = _tokenizer(
        texts_a, texts_b,
        truncation=True, padding="max_length",
        max_length=MAX_LENGTH, return_tensors="pt",
    )
    input_ids = enc["input_ids"].to(_device)
    attention_mask = enc["attention_mask"].to(_device)

    with torch.no_grad():
        logits = _model(input_ids=input_ids, attention_mask=attention_mask).logits

    probs = torch.softmax(logits, dim=-1)[:, 1].cpu().numpy()
    labels = (probs >= THRESHOLD).astype(bool)

    latency_ms = (time.perf_counter() - start_time) * 1000

    background_tasks.add_task(
        prediction_logger.log_predictions,
        instances=request.instances,
        probabilities=probs.tolist(),
        matches=labels.tolist(),
        latency_ms=latency_ms,
    )

    return PredictResponse(
        predictions=[
            Prediction(match=bool(labels[i]), probability=float(probs[i]))
            for i in range(len(probs))
        ]
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
