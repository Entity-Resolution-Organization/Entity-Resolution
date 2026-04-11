"""
Generate reference dataset profile for drift detection.
Downloads training data from GCS, computes feature statistics,
saves reference data to GCS for the monitoring pipeline.

Run once after training, and again after each successful retraining.

Usage:
    python scripts/generate_reference.py
"""

import json
import os
import pandas as pd
import yaml
from google.cloud import storage

DEFAULT_CONFIG_PATH = "config/training_config.yaml"


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def download_training_data(config: dict, entity_type: str) -> pd.DataFrame:
    bucket_name = config["data"]["gcs_bucket"]
    gcs_path = config["data"]["gcs_path"]
    blob_path = f"{gcs_path}/{entity_type}/train.csv"
    local_path = f"/tmp/train_{entity_type}.csv"

    print(f"[Reference] Downloading gs://{bucket_name}/{blob_path}")
    client = storage.Client()
    client.bucket(bucket_name).blob(blob_path).download_to_filename(local_path)

    df = pd.read_csv(local_path)
    print(f"[Reference] Loaded {len(df)} training pairs")
    return df


def generate_reference(config: dict, entity_type: str):
    cols = config["data"]["columns"]
    bucket_name = config["data"]["gcs_bucket"]

    # Download training data
    df = download_training_data(config, entity_type)

    # Build feature dataframe for drift detection
    df_features = pd.DataFrame()
    df_features["name1"] = df[cols["name1"]].fillna("")
    df_features["name2"] = df[cols["name2"]].fillna("")
    df_features["address1"] = df[cols["address1"]].fillna("")
    df_features["address2"] = df[cols["address2"]].fillna("")
    df_features["name1_length"] = df[cols["name1"]].fillna("").str.len()
    df_features["name2_length"] = df[cols["name2"]].fillna("").str.len()
    df_features["address1_length"] = df[cols["address1"]].fillna("").str.len()
    df_features["address2_length"] = df[cols["address2"]].fillna("").str.len()
    df_features["name1_has_missing"] = df[cols["name1"]].isna().astype(int)
    df_features["address1_has_missing"] = df[cols["address1"]].isna().astype(int)
    df_features["label"] = df[cols["label"]]

    # Save reference CSV
    ref_csv_path = f"/tmp/reference_{entity_type}.csv"
    df_features.to_csv(ref_csv_path, index=False)

    # Compute stats
    stats = {
        "total_records": len(df),
        "positive_rate": float(df[cols["label"]].mean()),
        "name1_null_rate": float(df[cols["name1"]].isna().mean()),
        "name2_null_rate": float(df[cols["name2"]].isna().mean()),
        "address1_null_rate": float(df[cols["address1"]].isna().mean()),
        "address2_null_rate": float(df[cols["address2"]].isna().mean()),
        "name1_avg_length": float(df[cols["name1"]].fillna("").str.len().mean()),
        "name2_avg_length": float(df[cols["name2"]].fillna("").str.len().mean()),
        "address1_avg_length": float(df[cols["address1"]].fillna("").str.len().mean()),
        "address2_avg_length": float(df[cols["address2"]].fillna("").str.len().mean()),
    }
    stats_path = f"/tmp/reference_stats_{entity_type}.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"[Reference] Feature stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    gcs_ref_csv = f"monitoring/reference/{entity_type}/reference_features.csv"
    bucket.blob(gcs_ref_csv).upload_from_filename(ref_csv_path)
    print(f"[Reference] Uploaded -> gs://{bucket_name}/{gcs_ref_csv}")

    gcs_stats = f"monitoring/reference/{entity_type}/reference_stats.json"
    bucket.blob(gcs_stats).upload_from_filename(stats_path)
    print(f"[Reference] Uploaded -> gs://{bucket_name}/{gcs_stats}")

    print(f"\n[Reference] Done for {entity_type}!")


def main():
    config_path = os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    config = load_config(config_path)

    print("=" * 60)
    print("GENERATE REFERENCE DATASET PROFILE")
    print("=" * 60)

    for entity_type in config["data"]["entity_types"]:
        print(f"\nProcessing: {entity_type}")
        print("-" * 40)
        generate_reference(config, entity_type)

    print("\n" + "=" * 60)
    print("REFERENCE GENERATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
