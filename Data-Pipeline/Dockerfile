# Using Airflow 2.8.1 (stable LTS) - Airflow 3.x has breaking API changes
FROM apache/airflow:2.8.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python dependencies with Airflow constraints
COPY requirements.txt /requirements.txt
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
RUN pip install --no-cache-dir -c "${AIRFLOW_CONSTRAINTS_URL}" -r /requirements.txt