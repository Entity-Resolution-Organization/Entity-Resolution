"""
Bias Detection Module for Entity Resolution Data Pipeline.

Detects various biases in PERSON entity resolution datasets:
- Entity type distribution bias
- Language/character set bias
- Geographic bias
- Match label distribution bias
- Data source bias (synthetic vs real)

Used by the DAG's bias_detection task and the quality gate.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BiasDetector:
    """Detect and report biases in entity resolution datasets."""

    def __init__(self, output_dir: str = "data/metrics"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def analyze_entity_type_distribution(
        self, df: pd.DataFrame, type_col: str = "entity_type"
    ) -> Dict[str, Any]:
        """
        Check if entity types are balanced.

        Bias threshold: >30% imbalance between most and least common type.
        """
        if len(df) == 0:
            return {"status": "skipped", "reason": "Empty dataframe"}

        if type_col not in df.columns:
            return {
                "status": "skipped",
                "reason": f"No {type_col} column found",
                "recommendation": "Add entity_type column to track entity categories",
            }

        distribution = df[type_col].value_counts(normalize=True)

        if len(distribution) == 0:
            return {"status": "skipped", "reason": "No entity types found"}

        imbalance = (
            float(distribution.max() - distribution.min())
            if len(distribution) > 1
            else 0.0
        )

        return {
            "distribution": distribution.to_dict(),
            "most_common": str(distribution.idxmax()),
            "least_common": str(distribution.idxmin()),
            "imbalance_ratio": round(imbalance, 4),
            "is_balanced": bool(imbalance < 0.3),
            "has_entity_type_bias": bool(imbalance >= 0.3),
            "severity": (
                "HIGH" if imbalance > 0.5 else "MEDIUM" if imbalance > 0.3 else "LOW"
            ),
            "recommendation": (
                "Oversample minority entity types or add more diverse data sources"
                if imbalance >= 0.3
                else "Distribution is acceptable"
            ),
        }

    def analyze_language_bias(
        self, df: pd.DataFrame, name_col: str = "name"
    ) -> Dict[str, Any]:
        """
        Detect non-ASCII/non-English name underrepresentation.

        Bias threshold: <5% non-ASCII names indicates language bias.
        Real-world data should have diverse names (Chinese, Arabic, Spanish, etc.)
        """
        if name_col not in df.columns:
            return {"status": "skipped", "reason": f"No {name_col} column found"}

        def analyze_name_charset(text):
            """Categorize name by character set."""
            if pd.isna(text):
                return "empty"

            text = str(text)

            has_cjk = bool(
                re.search(r"[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]", text)
            )
            has_arabic = bool(re.search(r"[\u0600-\u06ff]", text))
            has_cyrillic = bool(re.search(r"[\u0400-\u04ff]", text))
            has_latin_extended = bool(re.search(r"[\u00c0-\u024f]", text))

            if has_cjk:
                return "cjk"
            elif has_arabic:
                return "arabic"
            elif has_cyrillic:
                return "cyrillic"
            elif has_latin_extended:
                return "latin_extended"
            elif any(ord(c) > 127 for c in text):
                return "other_non_ascii"
            else:
                return "ascii_only"

        charset_distribution = df[name_col].apply(analyze_name_charset)
        charset_counts = charset_distribution.value_counts()

        total = len(df)
        non_ascii = (
            total - charset_counts.get("ascii_only", 0) - charset_counts.get("empty", 0)
        )
        non_ascii_pct = (non_ascii / total * 100) if total > 0 else 0

        return {
            "total_records": total,
            "charset_distribution": charset_counts.to_dict(),
            "non_ascii_names": int(non_ascii),
            "non_ascii_percentage": round(float(non_ascii_pct), 2),
            "has_language_bias": bool(non_ascii_pct < 5.0),
            "severity": (
                "HIGH"
                if non_ascii_pct < 2
                else "MEDIUM" if non_ascii_pct < 5 else "LOW"
            ),
            "recommendation": (
                "Add datasets with international names (e.g. OFAC international sanctions entries)"
                if non_ascii_pct < 5
                else "Language diversity is acceptable"
            ),
        }

    def analyze_geographic_bias(
        self, df: pd.DataFrame, address_col: str = "address"
    ) -> Dict[str, Any]:
        """
        Check if addresses are too US-centric.

        Bias threshold: >80% US addresses indicates geographic bias.
        """
        if address_col not in df.columns:
            return {"status": "skipped", "reason": f"No {address_col} column found"}

        us_states = [
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DE",
            "FL",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY",
            "DC",
        ]
        us_patterns = ["USA", "United States", "U.S.A", "U.S."]

        def classify_address_region(addr):
            if pd.isna(addr):
                return "unknown"

            addr_upper = str(addr).upper()

            if any(p.upper() in addr_upper for p in us_patterns):
                return "US"

            for state in us_states:
                if re.search(rf"\b{state}\b", addr_upper):
                    return "US"

            intl_patterns = {
                "UK": ["UK", "UNITED KINGDOM", "ENGLAND", "SCOTLAND", "WALES"],
                "Canada": ["CANADA", "ONTARIO", "QUEBEC", "BC", "ALBERTA"],
                "Germany": ["GERMANY", "DEUTSCHLAND"],
                "China": ["CHINA", "BEIJING", "SHANGHAI", "HONG KONG"],
                "Japan": ["JAPAN", "TOKYO", "OSAKA"],
                "Other": ["FRANCE", "SPAIN", "ITALY", "AUSTRALIA", "INDIA", "BRAZIL"],
            }

            for region, patterns in intl_patterns.items():
                if any(p in addr_upper for p in patterns):
                    return region if region != "Other" else "International"

            return "other"

        region_distribution = df[address_col].apply(classify_address_region)
        region_counts = region_distribution.value_counts()

        total_known = (
            len(df) - region_counts.get("unknown", 0) - region_counts.get("other", 0)
        )
        us_count = region_counts.get("US", 0)
        us_pct = (us_count / total_known * 100) if total_known > 0 else 0

        return {
            "total_addresses": len(df),
            "region_distribution": region_counts.to_dict(),
            "us_addresses": int(us_count),
            "us_percentage": round(float(us_pct), 2),
            "has_geographic_bias": bool(us_pct > 80.0),
            "severity": "HIGH" if us_pct > 95 else "MEDIUM" if us_pct > 80 else "LOW",
            "recommendation": (
                "Add international person datasets (e.g. international voter registries, OFAC non-US entries)"
                if us_pct > 80
                else "Geographic diversity is acceptable"
            ),
        }

    def analyze_match_label_distribution(
        self, pairs_df: pd.DataFrame, label_col: str = "label"
    ) -> Dict[str, Any]:
        """
        Check if positive/negative pairs are balanced.

        Bias threshold: >15% deviation from 50/50 indicates label imbalance.
        """
        if label_col not in pairs_df.columns:
            return {"status": "skipped", "reason": f"No {label_col} column found"}

        total = len(pairs_df)
        if total == 0:
            return {"status": "skipped", "reason": "Empty pairs dataframe"}

        positive_count = int((pairs_df[label_col] == 1).sum())
        negative_count = int((pairs_df[label_col] == 0).sum())

        positive_pct = positive_count / total * 100
        negative_pct = negative_count / total * 100

        imbalance = abs(positive_pct - 50)

        return {
            "total_pairs": total,
            "positive_pairs": positive_count,
            "negative_pairs": negative_count,
            "positive_percentage": round(float(positive_pct), 2),
            "negative_percentage": round(float(negative_pct), 2),
            "imbalance_from_balanced": round(float(imbalance), 2),
            "is_balanced": bool(imbalance < 15),
            "has_label_bias": imbalance >= 15,
            "severity": (
                "HIGH" if imbalance > 30 else "MEDIUM" if imbalance > 15 else "LOW"
            ),
            "recommendation": (
                f"Adjust pair sampling ratio (current: {positive_pct:.0f}/{negative_pct:.0f}, target: 50/50)"
                if imbalance >= 15
                else "Label distribution is acceptable"
            ),
        }

    def analyze_data_source_bias(
        self, df: pd.DataFrame, id_col: str = "id"
    ) -> Dict[str, Any]:
        """
        Check if data is too synthetic vs real.

        Bias threshold: >70% synthetic data indicates source bias.
        """
        if id_col not in df.columns:
            return {"status": "skipped", "reason": f"No {id_col} column found"}

        synthetic_markers = ["_var", "_syn", "fake", "test", "synthetic", "generated"]

        def is_synthetic(record_id):
            if pd.isna(record_id):
                return False
            id_str = str(record_id).lower()
            return any(marker in id_str for marker in synthetic_markers)

        synthetic_mask = df[id_col].apply(is_synthetic)
        synthetic_count = int(synthetic_mask.sum())
        total = len(df)

        synthetic_pct = (synthetic_count / total * 100) if total > 0 else 0

        return {
            "total_records": total,
            "synthetic_records": synthetic_count,
            "real_records": total - synthetic_count,
            "synthetic_percentage": round(float(synthetic_pct), 2),
            "has_source_bias": synthetic_pct > 70,
            "severity": (
                "HIGH"
                if synthetic_pct > 90
                else "MEDIUM" if synthetic_pct > 70 else "LOW"
            ),
            "recommendation": (
                "Incorporate more real-world person datasets to reduce synthetic data dominance"
                if synthetic_pct > 70
                else "Source balance is acceptable"
            ),
        }

    def generate_bias_report(
        self, accounts_df: pd.DataFrame, pairs_df: Optional[pd.DataFrame] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive bias analysis report.

        Args:
            accounts_df: DataFrame with entity records
            pairs_df: Optional DataFrame with entity pairs

        Returns:
            Complete bias report dictionary
        """
        logger.info(f"[Bias Detection] Analyzing {len(accounts_df)} account records")

        report = {
            "timestamp": datetime.now().isoformat(),
            "total_accounts": len(accounts_df),
            "total_pairs": len(pairs_df) if pairs_df is not None else 0,
            "analyses": {},
        }

        report["analyses"]["entity_type_bias"] = self.analyze_entity_type_distribution(
            accounts_df
        )
        report["analyses"]["language_bias"] = self.analyze_language_bias(accounts_df)
        report["analyses"]["geographic_bias"] = self.analyze_geographic_bias(
            accounts_df
        )
        report["analyses"]["data_source_bias"] = self.analyze_data_source_bias(
            accounts_df
        )

        if pairs_df is not None and len(pairs_df) > 0:
            logger.info(f"[Bias Detection] Analyzing {len(pairs_df)} entity pairs")
            report["analyses"]["match_label_bias"] = (
                self.analyze_match_label_distribution(pairs_df)
            )

        # Calculate overall bias risk
        high_risk = 0
        medium_risk = 0
        bias_issues = []

        for name, analysis in report["analyses"].items():
            if isinstance(analysis, dict):
                severity = analysis.get("severity", "LOW")
                if severity == "HIGH":
                    high_risk += 1
                    bias_issues.append(name)
                elif severity == "MEDIUM":
                    medium_risk += 1
                    bias_issues.append(name)

        report["summary"] = {
            "high_risk_count": high_risk,
            "medium_risk_count": medium_risk,
            "total_issues": high_risk + medium_risk,
            "bias_issues": bias_issues,
            "overall_bias_risk": (
                "HIGH"
                if high_risk >= 2
                else "MEDIUM" if high_risk >= 1 or medium_risk >= 2 else "LOW"
            ),
        }

        # Save report
        output_path = self.output_dir / "bias_report.json"
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"[Bias Detection] Report saved to {output_path}")
        logger.info(
            f"[Bias Detection] Overall risk: {report['summary']['overall_bias_risk']}"
        )

        return report
