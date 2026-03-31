FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Serving dependencies (used by serve.py in the er_{entity}_deberta image)
RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    pydantic

# KFP + Vertex AI SDK (used by pipeline.py to compile and submit)
RUN pip install --no-cache-dir \
    kfp==2.7.0 \
    google-cloud-aiplatform \
    google-cloud-storage

ENV TRANSFORMERS_CACHE=/app/cache
RUN python -c "
from transformers import AutoTokenizer, AutoModelForSequenceClassification
AutoTokenizer.from_pretrained('microsoft/deberta-v3-base', cache_dir='/app/cache')
AutoModelForSequenceClassification.from_pretrained('microsoft/deberta-v3-base', cache_dir='/app/cache', num_labels=2)
"

# Copy pipeline code
COPY scripts/ ./scripts/
COPY config/  ./config/
COPY pipeline.py ./pipeline.py

ENV CONFIG_PATH=config/training_config.yaml
ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/train.py"]