"""
Dataset Factory for Entity Resolution Pipeline.

Provides dataset handlers for downloading, generating, and normalizing
PERSON entity data from multiple sources.

Each handler implements:
- download(): Fetch real data (production) or generate synthetic (local)
- normalize_schema(): Map source columns to standard schema (id, name, address, dob)
- subsample(): Reduce to target size if source data exceeds target_records
"""

import io
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatasetHandler(ABC):
    """Base class for dataset handlers."""

    def __init__(self, config: Dict):
        self.config = config
        self.name = config["name"]
        self.is_production = os.getenv("ENVIRONMENT", "local") == "production"

    @abstractmethod
    def download(self) -> pd.DataFrame:
        """Download raw dataset."""
        pass

    @abstractmethod
    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map dataset columns to standard schema: id, name, address, dob."""
        pass

    def subsample(self, df: pd.DataFrame) -> pd.DataFrame:
        """Subsample to target record count if source data exceeds it."""
        target = self.config["target_records"]
        if len(df) > target:
            logger.info(f"Subsampling from {len(df)} to {target} records")
            return df.sample(n=target, random_state=42)
        return df

    def download_with_retry(
        self, url: str, retries: int = 3, timeout: int = 60
    ) -> requests.Response:
        """Download URL with retry logic and exponential backoff."""
        for attempt in range(retries):
            try:
                logger.info(f"Downloading from {url} (attempt {attempt + 1}/{retries})")
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    logger.error(f"Failed to download after {retries} attempts: {e}")
                    raise
                wait_time = 2**attempt
                logger.warning(f"Download failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)


class PseudopeopleHandler(DatasetHandler):
    """
    Handler for Pseudopeople synthetic population data.

    Production: Downloads pre-generated data from GCS bucket.
    Local: Generates synthetic person records for testing.
    """

    def download(self) -> pd.DataFrame:
        """Download or generate Pseudopeople data."""
        if self.is_production:
            return self._download_production()
        return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download pre-generated data from GCS bucket."""
        try:
            from google.cloud import storage

            bucket_name = os.getenv("GCS_BUCKET", "")
            if not bucket_name:
                logger.warning(
                    "[Pseudopeople] GCS_BUCKET not set, falling back to test data"
                )
                return self._generate_test_data()

            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob("raw/pseudopeople.csv")

            logger.info(
                f"[Pseudopeople] Downloading from gs://{bucket_name}/raw/pseudopeople.csv"
            )

            temp_path = "/tmp/pseudopeople.csv"
            blob.download_to_filename(temp_path)
            df = pd.read_csv(temp_path)

            logger.info(f"[Pseudopeople] Downloaded {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"[Pseudopeople] Production download failed: {e}")
            logger.warning("[Pseudopeople] Falling back to test data generation")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic person data for local testing."""
        base_count = self.config["base_records"]
        logger.info(f"[Pseudopeople] Generating {base_count} test records")

        first_names = [
            "James",
            "Mary",
            "John",
            "Patricia",
            "Robert",
            "Jennifer",
            "Michael",
            "Linda",
            "William",
            "Barbara",
            "David",
            "Elizabeth",
            "Richard",
            "Susan",
            "Joseph",
            "Jessica",
        ]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson",
            "Thomas",
        ]
        streets = ["Main", "Oak", "Maple", "Cedar", "Elm", "Washington", "Park", "Pine"]

        data = {
            "person_id": range(1, base_count + 1),
            "first_name": [random.choice(first_names) for _ in range(base_count)],
            "last_name": [random.choice(last_names) for _ in range(base_count)],
            "street_address": [
                f"{random.randint(1, 9999)} {random.choice(streets)} St"
                for _ in range(base_count)
            ],
            "date_of_birth": [
                f"{random.randint(1950, 2000)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
                for _ in range(base_count)
            ],
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema: id, name, address, dob."""
        df_normalized = df.copy()

        # Combine first + last name if separate columns exist
        if "first_name" in df.columns and "last_name" in df.columns:
            df_normalized["name"] = df["first_name"] + " " + df["last_name"]
        elif "name" not in df.columns:
            raise ValueError(
                "[Pseudopeople] Cannot construct 'name' column from available data"
            )

        df_normalized = df_normalized.rename(
            columns={
                "person_id": "id",
                "street_address": "address",
                "date_of_birth": "dob",
            }
        )

        return df_normalized[["id", "name", "address", "dob"]]


class NCVotersHandler(DatasetHandler):
    """
    Handler for NC Voters registry data.

    Production: Downloads from Leipzig benchmark dataset.
    Local: Generates synthetic NC voter records for testing.
    """

    # Direct download URL for the Leipzig NC Voters benchmark CSV
    DOWNLOAD_URL = (
        "https://pages.cs.wisc.edu/~anhai/data/784_data/ncvoters-20140619.csv"
    )

    def download(self) -> pd.DataFrame:
        """Download NC Voters dataset."""
        if self.is_production:
            return self._download_production()
        return self._generate_test_data()

    def _download_production(self) -> pd.DataFrame:
        """Download from benchmark dataset source."""
        try:
            # Use direct CSV URL instead of the project landing page
            url = self.DOWNLOAD_URL
            logger.info(f"[NC Voters] Downloading from {url}")

            response = self.download_with_retry(url, retries=3, timeout=120)
            df = pd.read_csv(io.StringIO(response.text))

            logger.info(f"[NC Voters] Downloaded {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"[NC Voters] Production download failed: {e}")
            logger.warning("[NC Voters] Falling back to test data")
            return self._generate_test_data()

    def _generate_test_data(self) -> pd.DataFrame:
        """Generate synthetic NC voter data for local testing."""
        base_count = self.config["base_records"]
        logger.info(f"[NC Voters] Generating {base_count} test records")

        first_names = [
            "Michael",
            "Sarah",
            "David",
            "Emily",
            "Christopher",
            "Jessica",
            "Matthew",
            "Ashley",
        ]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Miller",
            "Davis",
            "Wilson",
        ]
        cities = [
            "Raleigh",
            "Charlotte",
            "Greensboro",
            "Durham",
            "Winston-Salem",
            "Fayetteville",
        ]
        streets = ["Main", "Oak", "Elm", "Cedar", "Maple", "Pine", "Washington", "Park"]

        data = {
            "voter_id": range(1, base_count + 1),
            "full_name": [
                f"{random.choice(first_names)} {random.choice(last_names)}"
                for _ in range(base_count)
            ],
            "street_address": [
                f"{random.randint(100, 9999)} {random.choice(streets)} St"
                for _ in range(base_count)
            ],
            "city": [random.choice(cities) for _ in range(base_count)],
            "state": ["NC"] * base_count,
            "zip": [f"{random.randint(27000, 28999)}" for _ in range(base_count)],
            "birth_year": [random.randint(1940, 2005) for _ in range(base_count)],
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map to standard schema: id, name, address, dob."""
        df_normalized = df.copy()

        # Combine address fields
        df_normalized["address"] = (
            df["street_address"]
            + ", "
            + df["city"]
            + ", "
            + df["state"]
            + " "
            + df["zip"].astype(str)
        )

        df_normalized = df_normalized.rename(
            columns={
                "voter_id": "id",
                "full_name": "name",
                "birth_year": "dob",
            }
        )

        return df_normalized[["id", "name", "address", "dob"]]


class OFACHandler(DatasetHandler):
    """
    Handler for OFAC SDN sanctions list.

    Downloads real sanctions data from US Treasury in both
    production and local modes (publicly accessible, no auth needed).
    Falls back to synthetic data only on download failure.
    """

    OFAC_COLUMNS = [
        "ent_num",
        "sdn_name",
        "sdn_type",
        "program",
        "title",
        "call_sign",
        "vess_type",
        "tonnage",
        "grt",
        "vess_flag",
        "vess_owner",
        "remarks",
    ]

    def download(self) -> pd.DataFrame:
        """Download OFAC SDN list from Treasury.gov."""
        try:
            url = self.config["source_url"]
            logger.info(f"[OFAC SDN] Downloading from {url}")

            response = self.download_with_retry(url, retries=3, timeout=60)

            df = pd.read_csv(
                io.StringIO(response.text),
                encoding="utf-8",
                header=None,
                names=self.OFAC_COLUMNS,
                on_bad_lines="skip",
                quotechar='"',
            )

            # Clean placeholder values
            df = df.replace("-0-", "")
            df = df.replace("-0- ", "")
            logger.info(f"[OFAC SDN] Downloaded {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"[OFAC SDN] Download failed: {e}")
            logger.warning("[OFAC SDN] Falling back to synthetic data generation")
            return self._generate_synthetic_data()

    def _generate_synthetic_data(self) -> pd.DataFrame:
        """Generate synthetic OFAC-like sanctioned entity data."""
        base_count = self.config["base_records"]
        logger.info(f"[OFAC SDN] Generating {base_count} synthetic records")

        first_names = [
            "Ivan",
            "Vladimir",
            "Sergei",
            "Dmitri",
            "Alexei",
            "Mikhail",
            "Nikolai",
            "Ahmed",
            "Hassan",
            "Ali",
            "Omar",
            "Mohammed",
            "Abdul",
            "Khalid",
            "Kim",
            "Park",
            "Lee",
            "Chen",
            "Wang",
            "Zhang",
        ]
        last_names = [
            "Petrov",
            "Ivanov",
            "Sidorov",
            "Kuznetsov",
            "Volkov",
            "Sokolov",
            "Khan",
            "Al-Rashid",
            "Al-Mahmoud",
            "Al-Hussein",
        ]
        countries = [
            "Russia",
            "Iran",
            "Syria",
            "North Korea",
            "Venezuela",
            "Belarus",
            "Cuba",
            "Myanmar",
            "Zimbabwe",
            "Sudan",
        ]
        programs = ["SDGT", "IRAN", "SYRIA", "DPRK", "VENEZUELA", "CUBA", "RUSSIA"]

        data = {
            "ent_num": range(1, base_count + 1),
            "sdn_name": [
                f"{random.choice(first_names)} {random.choice(last_names)}"
                for _ in range(base_count)
            ],
            "sdn_type": ["individual"] * base_count,
            "program": [random.choice(programs) for _ in range(base_count)],
            "title": [""] * base_count,
            "call_sign": [""] * base_count,
            "vess_type": [""] * base_count,
            "tonnage": [""] * base_count,
            "grt": [""] * base_count,
            "vess_flag": [""] * base_count,
            "vess_owner": [""] * base_count,
            "remarks": [
                f"Nationality: {random.choice(countries)}" for _ in range(base_count)
            ],
        }

        return pd.DataFrame(data)

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map OFAC SDN to standard schema: id, name, address.

        Note: OFAC data lacks structured addresses. The 'address' field is
        populated from program and title fields for consistency with the
        standard schema. DOB is not available in the SDN CSV format.
        """
        df_normalized = pd.DataFrame()

        df_normalized["id"] = df["ent_num"].astype(str)
        df_normalized["name"] = df["sdn_name"].fillna("").astype(str)

        # Use program + title as address context (OFAC lacks structured addresses)
        df_normalized["address"] = (
            df["program"].fillna("") + " " + df["title"].fillna("")
        ).str.strip()

        return df_normalized[["id", "name", "address"]]


def get_dataset_handler(dataset_name: str, config: Dict) -> DatasetHandler:
    """Factory function to get appropriate dataset handler."""
    handlers = {
        "pseudopeople": PseudopeopleHandler,
        "nc_voters": NCVotersHandler,
        "ofac_sdn": OFACHandler,
    }

    if dataset_name not in handlers:
        raise ValueError(
            f"Unknown dataset: {dataset_name}. Available: {list(handlers.keys())}"
        )

    return handlers[dataset_name](config)
