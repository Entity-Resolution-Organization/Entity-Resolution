from google.cloud import storage
import os

def get_mlflow_uri(gcs_bucket: str) -> str:

    env_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if env_uri and env_uri != "http://mlflow:5000":
        print(f"[mlflow_uri] Using env var: {env_uri}")
        return env_uri

    # Read from GCS
    try:
        uri = (
            storage.Client().bucket(gcs_bucket).blob("config/mlflow_uri.txt")
            .download_as_text()
            .strip()
        )
        if not uri:
            raise ValueError("mlflow_uri.txt is empty")
        print(f"[mlflow_uri] Read from GCS: {uri}")
        return uri

    except Exception as e:
        raise RuntimeError(
            f"Could not read MLflow URI from "
            f"gs://{gcs_bucket}/config/mlflow_uri.txt: {e}\n"
            f"Fix: echo 'http://<VM-IP>:5000' | "
            f"gcloud storage cp - gs://{gcs_bucket}/config/mlflow_uri.txt"
        ) from e