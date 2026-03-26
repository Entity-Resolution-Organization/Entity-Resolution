"""
Data Validation Module for Entity Resolution Pipeline.

Provides validation gates for:
1. Per-dataset raw data validation (after load, before transform)
2. Training split validation (after split creation)
3. Quality gate checks (aggregates all validation results)
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, Tuple
import pandas as pd


class DatasetValidator:
    """Validates individual dataset outputs after load."""

    REQUIRED_COLUMNS = {
        "raw": ["id", "name"],
        "pairs": ["id1", "id2", "label"],
    }

    def __init__(self, output_dir: str = "data/metrics"):
        """Initialize validator."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def validate_raw_dataset(
        self,
        dataset_name: str,
        data_path: str,
        entity_type: str,
        min_records: int = 100,
    ) -> Dict:
        """
        Validate raw dataset after load.

        Checks:
        - File exists and is not empty
        - Has minimum required columns
        - Has minimum record count
        - No critical nulls in ID column
        - Name column mostly present

        Args:
            dataset_name: Name of the dataset
            data_path: Path to the raw data CSV
            entity_type: Entity type (PERSON)
            min_records: Minimum required records (default 100)

        Returns:
            Validation results dict with 'success' boolean
        """
        results = {
            "dataset_name": dataset_name,
            "entity_type": entity_type,
            "data_path": data_path,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "success": True,
            "critical_failures": [],
        }

        # Check 1: File exists
        file_exists = os.path.exists(data_path)
        results["checks"].append(
            {
                "check": "file_exists",
                "passed": file_exists,
                "details": {"path": data_path},
            }
        )
        if not file_exists:
            results["success"] = False
            results["critical_failures"].append("File does not exist")
            return results

        # Check 2: File not empty
        file_size = os.path.getsize(data_path)
        file_not_empty = file_size > 0
        results["checks"].append(
            {
                "check": "file_not_empty",
                "passed": file_not_empty,
                "details": {"file_size_bytes": file_size},
            }
        )
        if not file_not_empty:
            results["success"] = False
            results["critical_failures"].append("File is empty")
            return results

        # Check 3: File readable
        try:
            df = pd.read_csv(data_path)
        except Exception as e:
            results["checks"].append(
                {
                    "check": "file_readable",
                    "passed": False,
                    "details": {"error": str(e)},
                }
            )
            results["success"] = False
            results["critical_failures"].append(f"Cannot read file: {e}")
            return results

        results["checks"].append(
            {
                "check": "file_readable",
                "passed": True,
                "details": {"columns": list(df.columns), "rows": len(df)},
            }
        )

        # Check 4: Minimum record count
        record_count = len(df)
        has_min_records = record_count >= min_records
        results["checks"].append(
            {
                "check": "minimum_records",
                "passed": has_min_records,
                "details": {
                    "record_count": record_count,
                    "minimum_required": min_records,
                },
            }
        )
        if not has_min_records:
            results["success"] = False
            results["critical_failures"].append(
                f"Only {record_count} records, need at least {min_records}"
            )

        # Check 5: Required columns exist
        required_cols = self.REQUIRED_COLUMNS["raw"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        has_required_cols = len(missing_cols) == 0
        results["checks"].append(
            {
                "check": "required_columns",
                "passed": has_required_cols,
                "details": {
                    "required": required_cols,
                    "present": [col for col in required_cols if col in df.columns],
                    "missing": missing_cols,
                },
            }
        )
        if not has_required_cols:
            results["success"] = False
            results["critical_failures"].append(
                f"Missing required columns: {missing_cols}"
            )

        # Check 6: ID column not null
        if "id" in df.columns:
            id_nulls = df["id"].isnull().sum()
            id_null_pct = (id_nulls / len(df)) * 100 if len(df) > 0 else 0
            no_critical_nulls = id_null_pct < 5
            results["checks"].append(
                {
                    "check": "id_not_null",
                    "passed": no_critical_nulls,
                    "details": {
                        "null_count": int(id_nulls),
                        "null_percentage": round(id_null_pct, 2),
                        "threshold_percentage": 5,
                    },
                }
            )
            if not no_critical_nulls:
                results["success"] = False
                results["critical_failures"].append(
                    f"ID column has {id_null_pct:.1f}% nulls (>5% threshold)"
                )

        # Check 7: Name column mostly not null
        if "name" in df.columns:
            name_nulls = df["name"].isnull().sum()
            name_null_pct = (name_nulls / len(df)) * 100 if len(df) > 0 else 0
            name_ok = name_null_pct < 20
            results["checks"].append(
                {
                    "check": "name_mostly_present",
                    "passed": name_ok,
                    "details": {
                        "null_count": int(name_nulls),
                        "null_percentage": round(name_null_pct, 2),
                        "threshold_percentage": 20,
                    },
                }
            )
            if not name_ok:
                results["success"] = False
                results["critical_failures"].append(
                    f"Name column has {name_null_pct:.1f}% nulls (>20% threshold)"
                )

        # Summary statistics
        results["statistics"] = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "columns": list(df.columns),
            "null_counts": {col: int(df[col].isnull().sum()) for col in df.columns},
        }

        return results


class TrainingSplitValidator:
    """Validates training split outputs."""

    REQUIRED_SPLIT_FILES = ["train.csv", "val.csv", "test.csv"]
    EXPECTED_RATIOS = {"train": 0.70, "val": 0.15, "test": 0.15}
    RATIO_TOLERANCE = 0.05

    def __init__(self, output_dir: str = "data/metrics"):
        """Initialize validator."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def validate_entity_splits(
        self,
        entity_type: str,
        entity_dir: str,
    ) -> Dict:
        """
        Validate training splits for a single entity type.

        Checks:
        - All split files exist (train, val, test)
        - Split ratios approximately 70/15/15
        - Label distribution balanced in each split
        - No data leakage between splits
        - Required columns present
        - Labels are binary (0 or 1)
        """
        results = {
            "entity_type": entity_type,
            "entity_dir": entity_dir,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "success": True,
            "critical_failures": [],
        }

        # Check 1: All split files exist
        existing_files = []
        missing_files = []
        for filename in self.REQUIRED_SPLIT_FILES:
            filepath = os.path.join(entity_dir, filename)
            if os.path.exists(filepath):
                existing_files.append(filename)
            else:
                missing_files.append(filename)

        results["checks"].append(
            {
                "check": "split_files_exist",
                "passed": len(missing_files) == 0,
                "details": {"existing": existing_files, "missing": missing_files},
            }
        )

        if missing_files:
            results["success"] = False
            results["critical_failures"].append(f"Missing split files: {missing_files}")
            return results

        # Load splits
        splits = {}
        for split_name in ["train", "val", "test"]:
            filepath = os.path.join(entity_dir, f"{split_name}.csv")
            splits[split_name] = pd.read_csv(filepath)

        total_records = sum(len(df) for df in splits.values())

        # Check 2: Split ratios
        for split_name, df in splits.items():
            expected_ratio = self.EXPECTED_RATIOS[split_name]
            actual_ratio = len(df) / total_records if total_records > 0 else 0
            ratio_diff = abs(actual_ratio - expected_ratio)
            passed = ratio_diff <= self.RATIO_TOLERANCE

            results["checks"].append(
                {
                    "check": f"{split_name}_ratio",
                    "passed": passed,
                    "details": {
                        "expected_ratio": expected_ratio,
                        "actual_ratio": round(actual_ratio, 3),
                        "difference": round(ratio_diff, 3),
                        "tolerance": self.RATIO_TOLERANCE,
                        "record_count": len(df),
                    },
                }
            )

        # Check 3: Required columns in each split
        required_cols = ["id1", "id2", "label"]
        for split_name, df in splits.items():
            missing = [col for col in required_cols if col not in df.columns]
            results["checks"].append(
                {
                    "check": f"{split_name}_schema",
                    "passed": len(missing) == 0,
                    "details": {"required": required_cols, "missing": missing},
                }
            )
            if missing:
                results["success"] = False
                results["critical_failures"].append(
                    f"{split_name} missing columns: {missing}"
                )

        # Check 4: Label distribution in each split
        for split_name, df in splits.items():
            if "label" in df.columns:
                pos = int((df["label"] == 1).sum())
                neg = int((df["label"] == 0).sum())
                total = pos + neg
                balance_ratio = min(pos, neg) / max(pos, neg) if total > 0 else 0

                results["checks"].append(
                    {
                        "check": f"{split_name}_label_balance",
                        "passed": balance_ratio >= 0.4,
                        "details": {
                            "positive": pos,
                            "negative": neg,
                            "balance_ratio": round(balance_ratio, 3),
                        },
                    }
                )

        # Check 5: No data leakage between splits
        pair_sets: Dict[str, Set[Tuple[str, str]]] = {}
        for split_name, df in splits.items():
            pairs = set(zip(df["id1"].astype(str), df["id2"].astype(str)))
            pair_sets[split_name] = pairs

        overlaps = []
        splits_list = list(pair_sets.keys())
        for i, split1 in enumerate(splits_list):
            for split2 in splits_list[i + 1 :]:
                overlap = pair_sets[split1] & pair_sets[split2]
                if overlap:
                    overlaps.append(
                        {
                            "splits": [split1, split2],
                            "overlap_count": len(overlap),
                        }
                    )

        no_leakage = len(overlaps) == 0
        results["checks"].append(
            {
                "check": "no_data_leakage",
                "passed": no_leakage,
                "details": {
                    "overlaps": overlaps if overlaps else "None detected",
                    "train_pairs": len(pair_sets.get("train", set())),
                    "val_pairs": len(pair_sets.get("val", set())),
                    "test_pairs": len(pair_sets.get("test", set())),
                },
            }
        )

        if not no_leakage:
            results["success"] = False
            results["critical_failures"].append(f"Data leakage detected: {overlaps}")

        # Check 6: Labels are binary (0 or 1)
        for split_name, df in splits.items():
            if "label" in df.columns:
                unique_labels = set(df["label"].dropna().unique())
                valid_labels = unique_labels <= {0, 1}
                results["checks"].append(
                    {
                        "check": f"{split_name}_binary_labels",
                        "passed": valid_labels,
                        "details": {
                            "unique_labels": list(unique_labels),
                            "expected": [0, 1],
                        },
                    }
                )
                if not valid_labels:
                    results["success"] = False
                    results["critical_failures"].append(
                        f"{split_name} has invalid labels: {unique_labels}"
                    )

        # Summary statistics
        results["statistics"] = {
            "total_pairs": total_records,
            "train_count": len(splits["train"]),
            "val_count": len(splits["val"]),
            "test_count": len(splits["test"]),
            "split_ratios": {
                name: round(len(df) / total_records, 3) if total_records > 0 else 0
                for name, df in splits.items()
            },
        }

        return results


class QualityGate:
    """
    Quality gate that aggregates all validation results.

    Decides go/no-go for DVC versioning and cloud upload based on:
    - Schema validation results
    - Training split validation results
    - Bias detection results
    """

    SCHEMA_MIN_SUCCESS_RATE = 70.0
    TRAINING_MIN_SUCCESS_RATE = 80.0

    def evaluate(
        self,
        schema_results: Dict = None,
        training_results: Dict = None,
        bias_results: Dict = None,
        fail_on_high_bias: bool = False,
    ) -> Dict:
        """
        Evaluate all validation results and make go/no-go decision.

        Args:
            schema_results: Schema validation results
            training_results: Training split validation results
            bias_results: Bias detection results
            fail_on_high_bias: If True, HIGH bias risk fails the gate

        Returns:
            Quality gate decision with details
        """
        decision = {
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "decision": "GO",
            "checks": [],
            "warnings": [],
            "failures": [],
            "summary": {},
        }

        # Check 1: Schema validation
        if schema_results:
            success_rate = schema_results.get("summary", {}).get(
                "overall_success_rate", 0
            )
            schema_passed = success_rate >= self.SCHEMA_MIN_SUCCESS_RATE
            failed_count = schema_results.get("summary", {}).get(
                "failed_expectations", 0
            )

            decision["checks"].append(
                {
                    "name": "schema_validation",
                    "passed": schema_passed,
                    "details": {
                        "success_rate": success_rate,
                        "min_required": self.SCHEMA_MIN_SUCCESS_RATE,
                        "failed_expectations": failed_count,
                    },
                }
            )
            if not schema_passed:
                decision["passed"] = False
                decision["failures"].append(
                    f"Schema validation below threshold: {success_rate}% < {self.SCHEMA_MIN_SUCCESS_RATE}%"
                )
            elif failed_count > 0:
                decision["warnings"].append(
                    f"Schema validation: {failed_count} non-critical expectations failed"
                )

        # Check 2: Training split validation
        if training_results:
            success_rate = training_results.get("summary", {}).get("success_rate", 0)
            training_passed = success_rate >= self.TRAINING_MIN_SUCCESS_RATE
            critical_failures = training_results.get("summary", {}).get(
                "critical_failures", []
            )

            decision["checks"].append(
                {
                    "name": "training_split_validation",
                    "passed": training_passed,
                    "details": {
                        "success_rate": success_rate,
                        "min_required": self.TRAINING_MIN_SUCCESS_RATE,
                        "critical_failures": critical_failures,
                    },
                }
            )

            if critical_failures:
                decision["passed"] = False
                decision["failures"].append(
                    f"Training split critical failures: {critical_failures}"
                )
            elif not training_passed:
                decision["passed"] = False
                decision["failures"].append(
                    f"Training split validation below threshold: {success_rate}% < {self.TRAINING_MIN_SUCCESS_RATE}%"
                )

        # Check 3: Bias detection
        if bias_results:
            bias_risk = bias_results.get("summary", {}).get(
                "overall_bias_risk", "UNKNOWN"
            )
            high_risk_count = bias_results.get("summary", {}).get("high_risk_count", 0)

            if bias_risk == "CRITICAL":
                decision["passed"] = False
                decision["failures"].append("Critical bias risk detected")
            elif bias_risk == "HIGH" and fail_on_high_bias:
                decision["passed"] = False
                decision["failures"].append("High bias risk detected (strict mode)")
            elif bias_risk == "HIGH":
                decision["warnings"].append(
                    f"High bias risk: {high_risk_count} high-risk issues detected"
                )

            decision["checks"].append(
                {
                    "name": "bias_detection",
                    "passed": bias_risk not in ["CRITICAL"],
                    "details": {
                        "overall_risk": bias_risk,
                        "high_risk_count": high_risk_count,
                        "bias_issues": bias_results.get("summary", {}).get(
                            "bias_issues", []
                        ),
                    },
                }
            )

        # Set final decision
        if not decision["passed"]:
            decision["decision"] = "NO-GO"
        elif decision["warnings"]:
            decision["decision"] = "GO-WITH-WARNINGS"

        decision["summary"] = {
            "total_checks": len(decision["checks"]),
            "passed_checks": sum(1 for c in decision["checks"] if c["passed"]),
            "failed_checks": sum(1 for c in decision["checks"] if not c["passed"]),
            "warning_count": len(decision["warnings"]),
            "failure_count": len(decision["failures"]),
        }

        return decision
