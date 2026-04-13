"""
Demo Dataset Generator for Entity Resolution Project
=====================================================

Generates ~500 realistic PERSON records spread across three business sources
(CRM, Billing, Fraud DB) with deliberate overlaps that test the full ER pipeline:

  - Same person appearing in multiple sources with name/address variations
  - Shared emails across different name spellings
  - Legitimate business records whose company names overlap with OFAC SDN entities
  - Actual OFAC-style sanctioned individuals mixed into the data

Output schema matches the Data Pipeline's normalized format:
  id, name, address, dob, email, company, source

Usage:
  python generate_demo_data.py
  python generate_demo_data.py --output data/raw/demo
  python generate_demo_data.py --records 500 --seed 42
"""

import argparse
import csv
import json
import os
import random
from datetime import date, timedelta
from pathlib import Path

# =============================================================================
# SEED DATA — names, addresses, companies, OFAC entities
# =============================================================================

# Pool of first names (mix of common US + international)
FIRST_NAMES_M = [
    "James",
    "Robert",
    "Michael",
    "William",
    "David",
    "Richard",
    "Joseph",
    "Thomas",
    "Christopher",
    "Daniel",
    "Matthew",
    "Anthony",
    "Andrew",
    "Mark",
    "Steven",
    "Carlos",
    "Miguel",
    "Ahmed",
    "Hassan",
    "Dmitri",
    "Sergei",
    "Yusuf",
    "Omar",
    "Chen",
    "Wei",
    "Raj",
    "Amit",
    "Kwame",
    "Emeka",
]
FIRST_NAMES_F = [
    "Mary",
    "Patricia",
    "Jennifer",
    "Linda",
    "Elizabeth",
    "Barbara",
    "Susan",
    "Jessica",
    "Sarah",
    "Karen",
    "Emily",
    "Amanda",
    "Stephanie",
    "Nicole",
    "Maria",
    "Fatima",
    "Aisha",
    "Priya",
    "Mei",
    "Yuki",
    "Olga",
    "Svetlana",
    "Ana",
    "Gabriela",
    "Nadia",
    "Ingrid",
]
LAST_NAMES = [
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
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Green",
    "Baker",
    "Adams",
    "Nelson",
    "Carter",
    "Mitchell",
    "Roberts",
    "Turner",
    "Phillips",
    "Campbell",
    "Parker",
    "Evans",
    "Edwards",
    "Collins",
    "Stewart",
    "Morris",
    "Reed",
    "Cook",
    "Morgan",
    "Bell",
    "Murphy",
    "Bailey",
    "Rivera",
    "Cooper",
    "Cox",
    "Howard",
    "Ward",
    "Torres",
    "Peterson",
    "Gray",
]

STREETS = [
    "Main St",
    "Oak Ave",
    "Elm Rd",
    "Pine Dr",
    "Maple Ln",
    "Cedar Ct",
    "Washington Blvd",
    "Park Ave",
    "Broadway",
    "Lincoln Way",
    "Highland Dr",
    "Sunset Blvd",
    "River Rd",
    "Lake Shore Dr",
    "Market St",
    "Church St",
    "Spring St",
    "Franklin Ave",
    "Adams St",
    "Jefferson Blvd",
]
CITIES_STATES_ZIPS = [
    ("Boston", "MA", "02101"),
    ("New York", "NY", "10001"),
    ("Los Angeles", "CA", "90001"),
    ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"),
    ("Austin", "TX", "73301"),
    ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80201"),
    ("Miami", "FL", "33101"),
    ("Atlanta", "GA", "30301"),
    ("Portland", "OR", "97201"),
    ("Raleigh", "NC", "27601"),
    ("Charlotte", "NC", "28201"),
    ("Durham", "NC", "27701"),
    ("San Francisco", "CA", "94101"),
    ("Nashville", "TN", "37201"),
    ("Minneapolis", "MN", "55401"),
    ("Detroit", "MI", "48201"),
    ("Philadelphia", "PA", "19101"),
]

# Companies — includes names that will overlap with OFAC entities
LEGITIMATE_COMPANIES = [
    "Acme Corp",
    "TechNova Inc",
    "GlobalTrade Solutions",
    "Summit Financial",
    "Pinnacle Health Systems",
    "Bright Horizons LLC",
    "Metro Logistics",
    "DataStream Analytics",
    "Coastal Engineering",
    "Prairie Wind Energy",
    "Meridian Insurance",
    "Apex Manufacturing",
    "BlueRock Capital",
    "Silverline Consulting",
    "Horizon Pharma",
    "Atlas Freight",
    "Vanguard Properties",
    "Ironclad Security",
    "Clearwater Systems",
    "Northstar Advisory",
]

# Companies whose names intentionally resemble or overlap with sanctioned entities
# This tests whether the model can distinguish legitimate businesses from sanctions hits
OFAC_ADJACENT_COMPANIES = [
    "Petrov Trading LLC",  # resembles sanctioned Russian names
    "Al-Rashid Import Export",  # resembles sanctioned Middle Eastern entities
    "Volkov Energy Partners",  # resembles sanctioned Russian oligarch companies
    "Khalid International Group",  # resembles sanctioned names
    "Eastern Star Shipping Co",  # resembles sanctioned shipping companies
]

EMAIL_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "protonmail.com",
    "icloud.com",
    "aol.com",
    "mail.com",
]

SOURCES = ["CRM", "Billing", "Fraud_DB"]

# ---------------------------------------------------------------------------
# OFAC-style sanctioned individuals (synthetic — not real people)
# These records simulate what the SDN list looks like after normalization
# ---------------------------------------------------------------------------
OFAC_SDN_RECORDS = [
    {
        "name": "Sergei Volkov",
        "address": "SDGT RUSSIA",
        "dob": "1968-03-15",
        "company": "Volkov Energy Partners",
        "program": "RUSSIA-EO14024",
    },
    {
        "name": "Ahmed Al-Rashid",
        "address": "SDGT IRAN",
        "dob": "1975-07-22",
        "company": "Al-Rashid Import Export",
        "program": "IRAN-TRA",
    },
    {
        "name": "Dmitri Kuznetsov",
        "address": "SDGT RUSSIA",
        "dob": "1972-11-08",
        "company": "",
        "program": "RUSSIA-EO14024",
    },
    {
        "name": "Hassan Al-Mahmoud",
        "address": "SDGT SYRIA",
        "dob": "1980-01-30",
        "company": "",
        "program": "SYRIA",
    },
    {
        "name": "Kim Yong-chul",
        "address": "DPRK",
        "dob": "1965-09-12",
        "company": "",
        "program": "DPRK3",
    },
    {
        "name": "Khalid Ibrahim",
        "address": "SDGT IRAN",
        "dob": "1978-04-05",
        "company": "Khalid International Group",
        "program": "SDGT",
    },
    {
        "name": "Vladimir Petrov",
        "address": "SDGT RUSSIA",
        "dob": "1970-06-18",
        "company": "Petrov Trading LLC",
        "program": "RUSSIA-EO14024",
    },
    {
        "name": "Omar Farooq",
        "address": "SDGT",
        "dob": "1982-12-01",
        "company": "",
        "program": "SDGT",
    },
    {
        "name": "Alexei Sidorov",
        "address": "SDGT RUSSIA",
        "dob": "1974-08-25",
        "company": "",
        "program": "RUSSIA-EO14024",
    },
    {
        "name": "Abdul Rahman",
        "address": "SDGT VENEZUELA",
        "dob": "1985-02-14",
        "company": "",
        "program": "VENEZUELA",
    },
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def random_dob(rng: random.Random, min_year=1950, max_year=2000) -> str:
    """Generate a random date of birth as YYYY-MM-DD."""
    start = date(min_year, 1, 1)
    end = date(max_year, 12, 31)
    delta = (end - start).days
    d = start + timedelta(days=rng.randint(0, delta))
    return d.strftime("%Y-%m-%d")


def random_address(rng: random.Random) -> str:
    """Generate a random US street address."""
    num = rng.randint(1, 9999)
    street = rng.choice(STREETS)
    city, state, zipcode = rng.choice(CITIES_STATES_ZIPS)
    return f"{num} {street}, {city}, {state} {zipcode}"


def make_email(first: str, last: str, rng: random.Random) -> str:
    """Generate a plausible email address."""
    domain = rng.choice(EMAIL_DOMAINS)
    style = rng.choice(["dot", "initial", "full", "num"])
    if style == "dot":
        return f"{first.lower()}.{last.lower()}@{domain}"
    elif style == "initial":
        return f"{first[0].lower()}{last.lower()}@{domain}"
    elif style == "full":
        return f"{first.lower()}{last.lower()}@{domain}"
    else:
        return f"{first.lower()}.{last.lower()}{rng.randint(1, 99)}@{domain}"


def corrupt_name(name: str, rng: random.Random) -> str:
    """Apply a light corruption to a name for cross-source variation."""
    strategy = rng.choice(
        [
            "nickname",
            "typo",
            "initial",
            "swap",
            "case",
        ]
    )
    parts = name.split()
    if len(parts) < 2:
        return name

    first, last = parts[0], parts[-1]

    if strategy == "nickname":
        nicknames = {
            "Robert": "Bob",
            "William": "Bill",
            "James": "Jim",
            "Michael": "Mike",
            "Richard": "Rick",
            "Joseph": "Joe",
            "Thomas": "Tom",
            "Christopher": "Chris",
            "Daniel": "Dan",
            "Matthew": "Matt",
            "Elizabeth": "Liz",
            "Jennifer": "Jen",
            "Patricia": "Pat",
            "Jessica": "Jess",
            "Barbara": "Barb",
            "Margaret": "Maggie",
            "Susan": "Sue",
            "Katherine": "Kate",
        }
        if first in nicknames:
            return f"{nicknames[first]} {last}"
        return name

    elif strategy == "typo":
        if len(last) > 3:
            pos = rng.randint(1, len(last) - 2)
            chars = list(last)
            chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
            return f"{first} {''.join(chars)}"
        return name

    elif strategy == "initial":
        return f"{first[0]}. {last}"

    elif strategy == "swap":
        return f"{last}, {first}"

    elif strategy == "case":
        return f"{first.upper()} {last.upper()}"

    return name


def corrupt_address(address: str, rng: random.Random) -> str:
    """Apply a light corruption to an address for cross-source variation."""
    strategy = rng.choice(["abbreviate", "drop_zip", "reorder", "typo"])

    if strategy == "abbreviate":
        replacements = {
            "Street": "St",
            "Avenue": "Ave",
            "Road": "Rd",
            "Drive": "Dr",
            "Lane": "Ln",
            "Court": "Ct",
            "Boulevard": "Blvd",
        }
        for full, abbrev in replacements.items():
            if full in address:
                return address.replace(full, abbrev)
        # Try reverse — expand abbreviations
        for full, abbrev in replacements.items():
            if f" {abbrev}," in address or f" {abbrev} " in address:
                return address.replace(abbrev, full)
        return address

    elif strategy == "drop_zip":
        # Remove ZIP code
        parts = address.rsplit(" ", 1)
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0]
        return address

    elif strategy == "reorder":
        parts = address.split(", ", 1)
        if len(parts) == 2:
            return f"{parts[1]}, {parts[0]}"
        return address

    elif strategy == "typo":
        # Swap two adjacent digits in house number
        i = 0
        while i < len(address) and address[i].isdigit():
            i += 1
        if i >= 2:
            chars = list(address)
            chars[0], chars[1] = chars[1], chars[0]
            return "".join(chars)
        return address

    return address


# =============================================================================
# MAIN GENERATOR
# =============================================================================


def generate_demo_dataset(
    n_records: int = 500,
    seed: int = 42,
) -> list:
    """
    Generate a demo dataset of ~n_records PERSON records.

    Composition:
      - ~60% unique individuals across CRM, Billing, Fraud DB
      - ~25% cross-source duplicates (same person, different formatting)
      - ~5%  shared-email overlaps (different names sharing an email)
      - ~5%  OFAC-adjacent company employees (legitimate records at flagged companies)
      - ~5%  OFAC SDN sanctioned individuals

    Returns:
        List of record dicts with keys: id, name, address, dob, email, company, source
    """
    rng = random.Random(seed)
    records = []
    record_id = 1

    # Track emails for cross-linking
    all_emails = {}  # email → list of record IDs that share it
    identity_pool = []  # base identities for creating duplicates

    # -----------------------------------------------------------------
    # Phase 1: Generate unique base identities (~180 people)
    # -----------------------------------------------------------------
    n_base = int(n_records * 0.36)
    for _ in range(n_base):
        first = rng.choice(FIRST_NAMES_M + FIRST_NAMES_F)
        last = rng.choice(LAST_NAMES)
        name = f"{first} {last}"
        address = random_address(rng)
        dob = random_dob(rng)
        email = make_email(first, last, rng)
        company = rng.choice(LEGITIMATE_COMPANIES)
        source = rng.choice(SOURCES)

        rec = {
            "id": f"demo_{record_id:04d}",
            "name": name,
            "address": address,
            "dob": dob,
            "email": email,
            "company": company,
            "source": source,
        }
        records.append(rec)
        identity_pool.append(rec.copy())
        all_emails.setdefault(email, []).append(rec["id"])
        record_id += 1

    # -----------------------------------------------------------------
    # Phase 2: Cross-source duplicates (~125 records)
    # Same person appearing in a different source with name/address corruption
    # -----------------------------------------------------------------
    n_dupes = int(n_records * 0.25)
    for _ in range(n_dupes):
        base = rng.choice(identity_pool)

        # Pick a different source than the original
        other_sources = [s for s in SOURCES if s != base["source"]]
        new_source = rng.choice(other_sources)

        # Apply corruption to name and/or address
        new_name = corrupt_name(base["name"], rng)
        new_address = (
            corrupt_address(base["address"], rng)
            if rng.random() < 0.6
            else base["address"]
        )

        # Sometimes use the same email (strong match signal), sometimes generate new
        if rng.random() < 0.4:
            new_email = base["email"]
        else:
            parts = base["name"].split()
            new_email = make_email(parts[0], parts[-1], rng)

        # Sometimes slight DOB variation (typo in year)
        new_dob = base["dob"]
        if rng.random() < 0.1:
            year = int(new_dob[:4])
            new_dob = f"{year + rng.choice([-1, 1])}{new_dob[4:]}"

        rec = {
            "id": f"demo_{record_id:04d}",
            "name": new_name,
            "address": new_address,
            "dob": new_dob,
            "email": new_email,
            "company": base["company"],
            "source": new_source,
        }
        records.append(rec)
        all_emails.setdefault(new_email, []).append(rec["id"])
        record_id += 1

    # -----------------------------------------------------------------
    # Phase 3: Shared-email records (~25 records)
    # Different people who share an email (e.g., couples, department aliases)
    # These should be HARD NEGATIVES — same email but different identity
    # -----------------------------------------------------------------
    n_shared_email = int(n_records * 0.05)
    shared_emails_used = set()

    for _ in range(n_shared_email):
        # Pick an existing email to reuse
        donor = rng.choice(records[: len(identity_pool)])
        shared_email = donor["email"]

        # Create a genuinely different person with that email
        first = rng.choice(FIRST_NAMES_M + FIRST_NAMES_F)
        last = rng.choice(LAST_NAMES)

        # Make sure name is actually different
        while f"{first} {last}" == donor["name"]:
            first = rng.choice(FIRST_NAMES_M + FIRST_NAMES_F)
            last = rng.choice(LAST_NAMES)

        rec = {
            "id": f"demo_{record_id:04d}",
            "name": f"{first} {last}",
            "address": donor["address"] if rng.random() < 0.3 else random_address(rng),
            "dob": random_dob(rng),
            "email": shared_email,
            "company": rng.choice(LEGITIMATE_COMPANIES),
            "source": rng.choice(SOURCES),
        }
        records.append(rec)
        all_emails.setdefault(shared_email, []).append(rec["id"])
        record_id += 1

    # -----------------------------------------------------------------
    # Phase 4: OFAC-adjacent company employees (~25 records)
    # Legitimate people who work at companies with sanctioned-sounding names
    # Tests: model should NOT flag these as sanctions matches
    # -----------------------------------------------------------------
    n_ofac_adjacent = int(n_records * 0.05)
    for _ in range(n_ofac_adjacent):
        first = rng.choice(FIRST_NAMES_M + FIRST_NAMES_F)
        last = rng.choice(LAST_NAMES)
        company = rng.choice(OFAC_ADJACENT_COMPANIES)

        rec = {
            "id": f"demo_{record_id:04d}",
            "name": f"{first} {last}",
            "address": random_address(rng),
            "dob": random_dob(rng),
            "email": make_email(first, last, rng),
            "company": company,
            "source": rng.choice(SOURCES),
        }
        records.append(rec)
        record_id += 1

    # -----------------------------------------------------------------
    # Phase 5: OFAC SDN sanctioned individuals (~10 base + variants)
    # Real-format sanctions records plus name/transliteration variants
    # appearing in business systems (the scary scenario)
    # -----------------------------------------------------------------
    for sdn in OFAC_SDN_RECORDS:
        # Base SDN record (source = OFAC)
        rec = {
            "id": f"demo_{record_id:04d}",
            "name": sdn["name"],
            "address": sdn["address"],
            "dob": sdn["dob"],
            "email": "",
            "company": sdn["company"],
            "source": "OFAC",
        }
        records.append(rec)
        record_id += 1

        # Some SDN individuals also appear in business sources
        # with slightly different name formatting — this is what
        # the model needs to catch
        if rng.random() < 0.6:
            variant_name = corrupt_name(sdn["name"], rng)
            variant_source = rng.choice(["CRM", "Billing", "Fraud_DB"])
            parts = sdn["name"].split()
            first = parts[0]
            last = parts[-1] if len(parts) > 1 else parts[0]

            rec = {
                "id": f"demo_{record_id:04d}",
                "name": variant_name,
                "address": random_address(rng),
                "dob": sdn["dob"],
                "email": make_email(first, last, rng),
                "company": sdn.get("company", ""),
                "source": variant_source,
            }
            records.append(rec)
            record_id += 1

    # -----------------------------------------------------------------
    # Phase 6: Fill remaining slots with unique clean records
    # -----------------------------------------------------------------
    while len(records) < n_records:
        first = rng.choice(FIRST_NAMES_M + FIRST_NAMES_F)
        last = rng.choice(LAST_NAMES)

        rec = {
            "id": f"demo_{record_id:04d}",
            "name": f"{first} {last}",
            "address": random_address(rng),
            "dob": random_dob(rng),
            "email": make_email(first, last, rng),
            "company": rng.choice(LEGITIMATE_COMPANIES + OFAC_ADJACENT_COMPANIES),
            "source": rng.choice(SOURCES),
        }
        records.append(rec)
        record_id += 1

    # Shuffle to mix all record types
    rng.shuffle(records)

    return records


def compute_stats(records: list) -> dict:
    """Compute summary statistics for the generated dataset."""
    sources = {}
    companies = {}
    emails = {}
    ofac_count = 0

    for r in records:
        sources[r["source"]] = sources.get(r["source"], 0) + 1
        if r["company"]:
            companies[r["company"]] = companies.get(r["company"], 0) + 1
        emails.setdefault(r["email"], []).append(r["id"])
        if r["source"] == "OFAC":
            ofac_count += 1

    shared_emails = {e: ids for e, ids in emails.items() if len(ids) > 1 and e}
    ofac_adjacent = sum(
        1
        for r in records
        if r["company"] in OFAC_ADJACENT_COMPANIES and r["source"] != "OFAC"
    )

    return {
        "total_records": len(records),
        "source_distribution": dict(sorted(sources.items())),
        "unique_emails": len([e for e in emails if e]),
        "shared_email_groups": len(shared_emails),
        "shared_email_records": sum(len(ids) for ids in shared_emails.values()),
        "ofac_sdn_records": ofac_count,
        "ofac_adjacent_company_records": ofac_adjacent,
        "unique_companies": len(companies),
        "top_companies": dict(sorted(companies.items(), key=lambda x: -x[1])[:10]),
    }


# =============================================================================
# OUTPUT
# =============================================================================


def save_dataset(records: list, output_dir: str) -> dict:
    """
    Save the demo dataset as CSV + metadata JSON.

    Files created:
      {output_dir}/demo_data.csv     — full dataset
      {output_dir}/metadata.json     — generation stats
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Save CSV
    csv_path = output_path / "demo_data.csv"
    fieldnames = ["id", "name", "address", "dob", "email", "company", "source"]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"[Demo Data] Saved {len(records)} records → {csv_path}")

    # Save metadata
    stats = compute_stats(records)
    stats["output_path"] = str(csv_path)
    stats["columns"] = fieldnames

    meta_path = output_path / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"[Demo Data] Metadata → {meta_path}")
    print(f"\n[Demo Data] Summary:")
    print(f"  Total records:              {stats['total_records']}")
    print(f"  Source distribution:         {stats['source_distribution']}")
    print(f"  OFAC SDN records:           {stats['ofac_sdn_records']}")
    print(f"  OFAC-adjacent company recs: {stats['ofac_adjacent_company_records']}")
    print(f"  Shared email groups:        {stats['shared_email_groups']}")
    print(f"  Unique companies:           {stats['unique_companies']}")

    return stats


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate demo dataset for Entity Resolution pipeline",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="Demo-Dataset/generated_datasets",
        help="Output directory (default: Demo-Dataset/generated_datasets)",
    )
    parser.add_argument(
        "--records",
        "-n",
        type=int,
        default=500,
        help="Target number of records (default: 500)",
    )
    parser.add_argument(
        "--seed",
        "-s",
        type=int,
        default=42,
        help="Random seed (default: 42)",
    )
    args = parser.parse_args()

    print(f"[Demo Data] Generating {args.records} records (seed={args.seed})...")
    records = generate_demo_dataset(n_records=args.records, seed=args.seed)
    save_dataset(records, args.output)
