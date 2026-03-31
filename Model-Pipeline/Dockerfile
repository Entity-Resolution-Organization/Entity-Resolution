FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime
WORKDIR /app

# System deps + gcloud CLI (single apt block)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl apt-transport-https ca-certificates gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] \
    https://packages.cloud.google.com/apt cloud-sdk main" | \
    tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-cli && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir \
    fastapi "uvicorn[standard]" pydantic \
    kfp==2.7.0 google-cloud-aiplatform google-cloud-storage

# Pre-bake DeBERTa weights
ENV TRANSFORMERS_CACHE=/app/cache
RUN python -c "
from transformers import AutoTokenizer, AutoModelForSequenceClassification
AutoTokenizer.from_pretrained('microsoft/deberta-v3-base', cache_dir='/app/cache')
AutoModelForSequenceClassification.from_pretrained('microsoft/deberta-v3-base', cache_dir='/app/cache', num_labels=2)
"

# Copy code
COPY scripts/    ./scripts/
COPY config/     ./config/
COPY pipeline.py ./pipeline.py
COPY utils.py    ./utils.py

ENV CONFIG_PATH=config/training_config.yaml
ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/train.py"]