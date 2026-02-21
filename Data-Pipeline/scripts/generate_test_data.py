#!/usr/bin/env python3
"""
Generate test data for DVC tracking and pipeline testing.
This script runs the dataset handlers in local mode to generate synthetic data.

Usage:
    python scripts/generate_test_data.py
    python scripts/generate_test_data.py --dataset pseudopeople
    python scripts/generate_test_data.py --all
"""

import argparse
import logging
import os
import sys

import yaml

# Add scripts to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dataset_factory import get_dataset_handler  # noqa: E402
from preprocessing import preprocess_dataset  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config():
    """Load dataset configuration."""
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "config", "datasets.yaml"
    )
    with open(config_path) as f:
        return yaml.safe_load(f)


def generate_dataset(dataset_name: str, config: dict, output_dir: str):
    """Generate a single dataset and save to disk."""
    logger.info(f"Generating dataset: {dataset_name}")

    dataset_config = config["datasets"][dataset_name]

    # Get handler and generate data
    handler = get_dataset_handler(dataset_name, dataset_config)
    raw_df = handler.download()
    raw_df = handler.normalize_schema(raw_df)

    # Subsample if needed (use smaller size for testing)
    test_size = min(1000, dataset_config.get("base_records", 100))
    if len(raw_df) > test_size:
        raw_df = raw_df.sample(n=test_size, random_state=42)

    # Save raw data
    raw_path = os.path.join(output_dir, "raw", f"{dataset_name}.csv")
    raw_df.to_csv(raw_path, index=False)
    logger.info(f"Saved raw data: {raw_path} ({len(raw_df)} records)")

    return raw_df, dataset_config


def preprocess_and_save(
    raw_df, dataset_config: dict, dataset_name: str, output_dir: str
):
    """Run preprocessing and save outputs."""
    logger.info(f"Preprocessing dataset: {dataset_name}")

    # Use smaller numbers for testing
    test_config = dataset_config.copy()
    test_config["target_records"] = min(500, test_config.get("target_records", 500))
    test_config["pairs"] = {
        "positive": min(100, test_config.get("pairs", {}).get("positive", 100)),
        "negative": min(100, test_config.get("pairs", {}).get("negative", 100)),
    }

    # Run preprocessing
    accounts_df, pairs_df = preprocess_dataset(raw_df, test_config)

    # Save processed data
    accounts_path = os.path.join(
        output_dir, "processed", f"{dataset_name}_accounts.csv"
    )
    pairs_path = os.path.join(output_dir, "processed", f"{dataset_name}_pairs.csv")

    accounts_df.to_csv(accounts_path, index=False)
    pairs_df.to_csv(pairs_path, index=False)

    logger.info(f"Saved accounts: {accounts_path} ({len(accounts_df)} records)")
    logger.info(f"Saved pairs: {pairs_path} ({len(pairs_df)} pairs)")

    # Save metrics
    metrics = {
        "dataset": dataset_name,
        "raw_records": len(raw_df),
        "expanded_records": len(accounts_df),
        "total_pairs": len(pairs_df),
        "positive_pairs": int((pairs_df["label"] == 1).sum()),
        "negative_pairs": int((pairs_df["label"] == 0).sum()),
    }

    return metrics


def main():
    parser = argparse.ArgumentParser(
        description="Generate test data for entity resolution pipeline"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="pseudopeople",
        choices=[
            "pseudopeople",
            "nc_voters",
            "ofac_sdn",
            "wdc_products",
            "amazon_2018",
            "dblp_acm",
        ],
        help="Dataset to generate",
    )
    parser.add_argument("--all", action="store_true", help="Generate all datasets")
    parser.add_argument(
        "--output-dir", type=str, default="data", help="Output directory"
    )

    args = parser.parse_args()

    # Ensure output directories exist
    os.makedirs(os.path.join(args.output_dir, "raw"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "processed"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "metrics"), exist_ok=True)

    config = load_config()

    if args.all:
        datasets = [
            "pseudopeople",
            "nc_voters",
            "ofac_sdn",
            "wdc_products",
            "amazon_2018",
            "dblp_acm",
        ]
    else:
        datasets = [args.dataset]

    all_metrics = []

    for dataset_name in datasets:
        try:
            raw_df, dataset_config = generate_dataset(
                dataset_name, config, args.output_dir
            )
            metrics = preprocess_and_save(
                raw_df, dataset_config, dataset_name, args.output_dir
            )
            all_metrics.append(metrics)
        except Exception as e:
            logger.error(f"Failed to process {dataset_name}: {e}")
            continue

    # Save combined metrics
    import json

    metrics_path = os.path.join(args.output_dir, "metrics", "generation_stats.json")
    with open(metrics_path, "w") as f:
        json.dump(all_metrics, f, indent=2)

    logger.info(f"Saved metrics: {metrics_path}")
    logger.info("Data generation complete!")

    # Print summary
    print("\n" + "=" * 60)
    print("GENERATION SUMMARY")
    print("=" * 60)
    for m in all_metrics:
        print(f"\n{m['dataset']}:")
        print(f"  Raw records: {m['raw_records']}")
        print(f"  Expanded records: {m['expanded_records']}")
        print(
            f"  Training pairs: {m['total_pairs']} (pos: {m['positive_pairs']}, neg: {m['negative_pairs']})"
        )


if __name__ == "__main__":
    main()
