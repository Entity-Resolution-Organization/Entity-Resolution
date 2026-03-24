FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY scripts/ ./scripts/
COPY config/  ./config/
COPY dags/    ./dags/

ENV CONFIG_PATH=config/training_config.yaml
ENV PYTHONUNBUFFERED=1

CMD ["python", "scripts/train.py"]