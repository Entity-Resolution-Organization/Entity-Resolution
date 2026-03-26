"""
Schema Validation for Entity Resolution Pipeline.

Validates accounts and pairs dataframes for:
- Required columns present
- Null value thresholds
- ID uniqueness
- Label validity (binary 0/1)
- Value length constraints
- Entity type and source dataset integrity
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd


class SchemaValidator:
    """Validates entity resolution data schema and quality."""

    def __init__(self, output_dir: str = "data/metrics"):
        """
        Initialize the schema validator.

        Args:
            output_dir: Directory to save validation results
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def validate_accounts(self, df: pd.DataFrame) -> Dict:
        """
        Validate accounts dataframe schema and quality.

        Args:
            df: Accounts dataframe

        Returns:
            Validation results dictionary
        """
        results = {
            "suite_name": "accounts_suite",
            "timestamp": datetime.now().isoformat(),
            "success": True,
            "statistics": {"total_rows": len(df), "total_columns": len(df.columns)},
            "expectations": [],
            "failed_expectations": [],
        }

        # Required columns check
        required_cols = ["id", "name", "address"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        exp_result = {
            "expectation": "expect_columns_to_exist",
            "columns": required_cols,
            "success": len(missing_cols) == 0,
            "details": {"missing_columns": missing_cols},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        if missing_cols:
            return results

        # Handle empty dataframe
        if len(df) == 0:
            exp_result = {
                "expectation": "expect_table_row_count_to_be_between",
                "min_value": 1,
                "max_value": None,
                "success": False,
                "details": {"row_count": 0},
            }
            results["expectations"].append(exp_result)
            results["failed_expectations"].append(exp_result)
            results["success"] = False
            return results

        # ID not null check
        id_nulls = df["id"].isnull().sum()
        exp_result = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "id",
            "success": id_nulls == 0,
            "details": {
                "null_count": int(id_nulls),
                "null_pct": round(id_nulls / len(df) * 100, 2),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Name mostly not null (95%)
        name_nulls = df["name"].isnull().sum()
        name_null_pct = name_nulls / len(df)
        exp_result = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "name",
            "mostly": 0.95,
            "success": name_null_pct <= 0.05,
            "details": {
                "null_count": int(name_nulls),
                "null_pct": round(name_null_pct * 100, 2),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Address mostly not null (90%)
        addr_nulls = df["address"].isnull().sum()
        addr_null_pct = addr_nulls / len(df)
        exp_result = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "address",
            "mostly": 0.90,
            "success": addr_null_pct <= 0.10,
            "details": {
                "null_count": int(addr_nulls),
                "null_pct": round(addr_null_pct * 100, 2),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # ID uniqueness check
        id_unique = df["id"].nunique()
        exp_result = {
            "expectation": "expect_column_values_to_be_unique",
            "column": "id",
            "success": id_unique == len(df),
            "details": {
                "unique_count": int(id_unique),
                "total_count": len(df),
                "duplicate_count": len(df) - int(id_unique),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Name length check
        name_lengths = df["name"].dropna().astype(str).str.len()
        valid_lengths = ((name_lengths >= 1) & (name_lengths <= 500)).sum()
        valid_pct = valid_lengths / len(name_lengths) if len(name_lengths) > 0 else 1.0
        exp_result = {
            "expectation": "expect_column_value_lengths_to_be_between",
            "column": "name",
            "min_value": 1,
            "max_value": 500,
            "success": valid_pct >= 0.99,
            "details": {
                "valid_count": int(valid_lengths),
                "total_count": len(name_lengths),
                "valid_pct": round(valid_pct * 100, 2),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Row count check
        exp_result = {
            "expectation": "expect_table_row_count_to_be_between",
            "min_value": 1,
            "max_value": 10000000,
            "success": 1 <= len(df) <= 10000000,
            "details": {"row_count": len(df)},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Entity type validation (if present)
        if "entity_type" in df.columns:
            valid_entity_types = {"PERSON", "UNKNOWN"}
            entity_values = set(df["entity_type"].dropna().unique())
            invalid_types = entity_values - valid_entity_types
            exp_result = {
                "expectation": "expect_column_values_to_be_in_set",
                "column": "entity_type",
                "value_set": list(valid_entity_types),
                "success": len(invalid_types) == 0,
                "details": {
                    "valid_types": list(entity_values & valid_entity_types),
                    "invalid_types": list(invalid_types),
                    "distribution": df["entity_type"].value_counts().to_dict(),
                },
            }
            results["expectations"].append(exp_result)
            if not exp_result["success"]:
                results["failed_expectations"].append(exp_result)
                results["success"] = False

        # Source dataset validation (if present)
        if "source_dataset" in df.columns:
            source_nulls = df["source_dataset"].isnull().sum()
            exp_result = {
                "expectation": "expect_column_values_to_not_be_null",
                "column": "source_dataset",
                "success": source_nulls == 0,
                "details": {
                    "null_count": int(source_nulls),
                    "sources_present": df["source_dataset"].nunique(),
                    "source_distribution": df["source_dataset"]
                    .value_counts()
                    .to_dict(),
                },
            }
            results["expectations"].append(exp_result)
            if not exp_result["success"]:
                results["failed_expectations"].append(exp_result)
                results["success"] = False

        # Statistics
        results["statistics"]["expectations_count"] = len(results["expectations"])
        results["statistics"]["successful_expectations"] = len(
            results["expectations"]
        ) - len(results["failed_expectations"])
        results["statistics"]["failed_expectations"] = len(
            results["failed_expectations"]
        )
        results["statistics"]["success_rate"] = round(
            results["statistics"]["successful_expectations"]
            / results["statistics"]["expectations_count"]
            * 100,
            2,
        )

        if "entity_type" in df.columns:
            results["statistics"]["entity_types"] = df["entity_type"].nunique()
            results["statistics"]["entity_distribution"] = (
                df["entity_type"].value_counts().to_dict()
            )
        if "source_dataset" in df.columns:
            results["statistics"]["source_datasets"] = df["source_dataset"].nunique()

        return results

    def validate_pairs(self, df: pd.DataFrame) -> Dict:
        """
        Validate pairs dataframe schema and quality.

        Args:
            df: Pairs dataframe

        Returns:
            Validation results dictionary
        """
        results = {
            "suite_name": "pairs_suite",
            "timestamp": datetime.now().isoformat(),
            "success": True,
            "statistics": {"total_rows": len(df), "total_columns": len(df.columns)},
            "expectations": [],
            "failed_expectations": [],
        }

        # Required columns check
        required_cols = ["id1", "id2", "label"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        exp_result = {
            "expectation": "expect_columns_to_exist",
            "columns": required_cols,
            "success": len(missing_cols) == 0,
            "details": {"missing_columns": missing_cols},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        if missing_cols:
            return results

        # ID1 not null
        id1_nulls = df["id1"].isnull().sum()
        exp_result = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "id1",
            "success": id1_nulls == 0,
            "details": {"null_count": int(id1_nulls)},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # ID2 not null
        id2_nulls = df["id2"].isnull().sum()
        exp_result = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "id2",
            "success": id2_nulls == 0,
            "details": {"null_count": int(id2_nulls)},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Label not null
        label_nulls = df["label"].isnull().sum()
        exp_result = {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "label",
            "success": label_nulls == 0,
            "details": {"null_count": int(label_nulls)},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Label values in {0, 1}
        valid_labels = df["label"].isin([0, 1]).sum()
        exp_result = {
            "expectation": "expect_column_values_to_be_in_set",
            "column": "label",
            "value_set": [0, 1],
            "success": valid_labels == len(df),
            "details": {
                "valid_count": int(valid_labels),
                "invalid_count": len(df) - int(valid_labels),
                "label_distribution": df["label"].value_counts().to_dict(),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Pair uniqueness
        unique_pairs = df.drop_duplicates(subset=["id1", "id2"]).shape[0]
        exp_result = {
            "expectation": "expect_compound_columns_to_be_unique",
            "columns": ["id1", "id2"],
            "success": unique_pairs == len(df),
            "details": {
                "unique_pairs": int(unique_pairs),
                "total_pairs": len(df),
                "duplicate_pairs": len(df) - int(unique_pairs),
            },
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Row count check
        exp_result = {
            "expectation": "expect_table_row_count_to_be_between",
            "min_value": 1,
            "max_value": 10000000,
            "success": 1 <= len(df) <= 10000000,
            "details": {"row_count": len(df)},
        }
        results["expectations"].append(exp_result)
        if not exp_result["success"]:
            results["failed_expectations"].append(exp_result)
            results["success"] = False

        # Entity type validation (if present)
        if "entity_type" in df.columns:
            valid_entity_types = {"PERSON", "UNKNOWN"}
            entity_values = set(df["entity_type"].dropna().unique())
            invalid_types = entity_values - valid_entity_types
            exp_result = {
                "expectation": "expect_column_values_to_be_in_set",
                "column": "entity_type",
                "value_set": list(valid_entity_types),
                "success": len(invalid_types) == 0,
                "details": {
                    "valid_types": list(entity_values & valid_entity_types),
                    "invalid_types": list(invalid_types),
                    "distribution": df["entity_type"].value_counts().to_dict(),
                },
            }
            results["expectations"].append(exp_result)
            if not exp_result["success"]:
                results["failed_expectations"].append(exp_result)
                results["success"] = False

        # Source dataset validation (if present)
        if "source_dataset" in df.columns:
            source_nulls = df["source_dataset"].isnull().sum()
            exp_result = {
                "expectation": "expect_column_values_to_not_be_null",
                "column": "source_dataset",
                "success": source_nulls == 0,
                "details": {
                    "null_count": int(source_nulls),
                    "sources_present": df["source_dataset"].nunique(),
                    "source_distribution": df["source_dataset"]
                    .value_counts()
                    .to_dict(),
                },
            }
            results["expectations"].append(exp_result)
            if not exp_result["success"]:
                results["failed_expectations"].append(exp_result)
                results["success"] = False

        # Statistics
        results["statistics"]["expectations_count"] = len(results["expectations"])
        results["statistics"]["successful_expectations"] = len(
            results["expectations"]
        ) - len(results["failed_expectations"])
        results["statistics"]["failed_expectations"] = len(
            results["failed_expectations"]
        )
        results["statistics"]["success_rate"] = round(
            results["statistics"]["successful_expectations"]
            / results["statistics"]["expectations_count"]
            * 100,
            2,
        )
        results["statistics"]["positive_pairs"] = int((df["label"] == 1).sum())
        results["statistics"]["negative_pairs"] = int((df["label"] == 0).sum())

        if "entity_type" in df.columns:
            results["statistics"]["entity_types"] = df["entity_type"].nunique()
            results["statistics"]["entity_distribution"] = (
                df["entity_type"].value_counts().to_dict()
            )
        if "source_dataset" in df.columns:
            results["statistics"]["source_datasets"] = df["source_dataset"].nunique()

        return results

    def validate_all(self, accounts_df: pd.DataFrame, pairs_df: pd.DataFrame) -> Dict:
        """
        Validate both accounts and pairs dataframes.

        Args:
            accounts_df: Accounts dataframe
            pairs_df: Pairs dataframe

        Returns:
            Combined validation results
        """
        print("[Schema Validation] Starting validation...")

        accounts_results = self.validate_accounts(accounts_df)
        print(
            f"[Schema Validation] Accounts: "
            f"{accounts_results['statistics']['success_rate']}% success rate"
        )

        pairs_results = self.validate_pairs(pairs_df)
        print(
            f"[Schema Validation] Pairs: "
            f"{pairs_results['statistics']['success_rate']}% success rate"
        )

        combined_results = {
            "timestamp": datetime.now().isoformat(),
            "overall_success": accounts_results["success"] and pairs_results["success"],
            "accounts_validation": accounts_results,
            "pairs_validation": pairs_results,
            "summary": {
                "total_expectations": (
                    accounts_results["statistics"]["expectations_count"]
                    + pairs_results["statistics"]["expectations_count"]
                ),
                "successful_expectations": (
                    accounts_results["statistics"]["successful_expectations"]
                    + pairs_results["statistics"]["successful_expectations"]
                ),
                "failed_expectations": (
                    accounts_results["statistics"]["failed_expectations"]
                    + pairs_results["statistics"]["failed_expectations"]
                ),
            },
        }

        combined_results["summary"]["overall_success_rate"] = round(
            combined_results["summary"]["successful_expectations"]
            / combined_results["summary"]["total_expectations"]
            * 100,
            2,
        )

        return combined_results

    def save_results(
        self, results: Dict, filename: str = "schema_validation_results.json"
    ) -> str:
        """
        Save validation results to JSON file.

        Args:
            results: Validation results dictionary
            filename: Output filename

        Returns:
            Path to saved file
        """
        output_path = self.output_dir / filename
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"[Schema Validation] Results saved to {output_path}")
        return str(output_path)
