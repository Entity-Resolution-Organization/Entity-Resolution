"""
Entity Resolution — Synthetic Demo Data Generator

Generates ~30 records representing 5 real people with 3-6 variants each,
designed to exercise every demo scenario:

    Person 1 (Clean Merge): Michael Greene — consistent DOB + email variants
    Person 2 (Veto Demo):   Sarah Chen — one variant has wrong DOB
    Person 3 (Fraud Ring):  James Wilson — shares email with Person 4
    Person 4 (2-Hop):       Maria Garcia — shares email with Person 3,
                             connected to flagged Person 5 via company
    Person 5 (Flagged):     Viktor Petrov — known bad actor

Usage:
    python scripts/demo_data.py                     # print to stdout
    python scripts/demo_data.py --upload             # upload to GCS to-process/
    python scripts/demo_data.py --output demo.csv    # write to local file
"""

import argparse

import pandas as pd


def generate_demo_records() -> pd.DataFrame:
    """Generate synthetic demo records with intentional patterns."""

    records = [
        # =====================================================================
        # Person 1: Michael Greene — Clean merge (3 sources, consistent fields)
        # Demonstrates: basic transitive clustering, name/address variation
        # =====================================================================
        {
            "id": "P1-CRM-001",
            "name": "Michael Greene",
            "address": "22 Oak Street, Boston MA 02108",
            "dob": "1985-03-15",
            "email": "mgreene85@gmail.com",
            "phone": "617-555-0142",
            "company": "Vertex Analytics",
            "source": "CRM",
        },
        {
            "id": "P1-BILL-001",
            "name": "Mike J Green",
            "address": "22 Oak St, Boston MA 02108",
            "dob": "1985-03-15",
            "email": "mike.greene@vertexanalytics.com",
            "phone": "6175550142",
            "company": "Vertex Analytics Inc",
            "source": "Billing",
        },
        {
            "id": "P1-FRAUD-001",
            "name": "M Greene",
            "address": "22 Oak Street Boston 02108",
            "dob": "1985-03-15",
            "email": "mgreene85@gmail.com",
            "phone": "",
            "company": "",
            "source": "Fraud DB",
        },
        {
            "id": "P1-VOTER-001",
            "name": "Michael J Greene",
            "address": "22 Oak St Apt 4B, Boston MA 02108",
            "dob": "1985-03-15",
            "email": "",
            "phone": "617-555-0142",
            "company": "",
            "source": "Voter Registry",
        },

        # =====================================================================
        # Person 2: Sarah Chen — Veto demo (DOB mismatch on one variant)
        # Demonstrates: veto_on_mismatch rule splitting a high-score edge
        # Variant 2c has wrong DOB (1990 vs 1988) — should be vetoed
        # =====================================================================
        {
            "id": "P2-CRM-001",
            "name": "Sarah Chen",
            "address": "456 Market Street, San Francisco CA 94105",
            "dob": "1988-11-22",
            "email": "sarah.chen@outlook.com",
            "phone": "415-555-0198",
            "company": "DataBridge Solutions",
            "source": "CRM",
        },
        {
            "id": "P2-BILL-001",
            "name": "S. Chen",
            "address": "456 Market St SF CA 94105",
            "dob": "1988-11-22",
            "email": "sarah.chen@outlook.com",
            "phone": "4155550198",
            "company": "DataBridge",
            "source": "Billing",
        },
        {
            "id": "P2-FRAUD-001",
            "name": "Sarah L Chen",
            "address": "456 Market Street, San Francisco 94105",
            "dob": "1990-06-10",  # WRONG DOB — should trigger veto
            "email": "sarah.chen@outlook.com",
            "phone": "415-555-0198",
            "company": "DataBridge Solutions",
            "source": "Fraud DB",
        },
        {
            "id": "P2-HR-001",
            "name": "Sarah Chen",
            "address": "456 Market St, San Francisco CA 94105",
            "dob": "1988-11-22",
            "email": "schen@databridge.io",
            "phone": "",
            "company": "DataBridge Solutions",
            "source": "HR System",
        },

        # =====================================================================
        # Person 3: James Wilson — Fraud ring (shares email with Person 4)
        # Demonstrates: shared_field_score anomaly detection
        # Same email (jwilson.trades@proton.me) appears on both Person 3 and 4
        # =====================================================================
        {
            "id": "P3-CRM-001",
            "name": "James Wilson",
            "address": "789 Pine Avenue, Chicago IL 60601",
            "dob": "1975-07-04",
            "email": "jwilson.trades@proton.me",
            "phone": "312-555-0167",
            "company": "Midwest Trading Co",
            "source": "CRM",
        },
        {
            "id": "P3-BILL-001",
            "name": "Jim Wilson",
            "address": "789 Pine Ave, Chicago IL 60601",
            "dob": "1975-07-04",
            "email": "james.wilson@midwest-trading.com",
            "phone": "312-555-0167",
            "company": "Midwest Trading",
            "source": "Billing",
        },
        {
            "id": "P3-TAX-001",
            "name": "James R Wilson",
            "address": "789 Pine Avenue Chicago 60601",
            "dob": "1975-07-04",
            "email": "",
            "phone": "3125550167",
            "company": "Midwest Trading Co",
            "source": "Tax Records",
        },
        {
            "id": "P3-BANK-001",
            "name": "J Wilson",
            "address": "789 Pine Ave, Chicago IL 60601",
            "dob": "",
            "email": "jwilson.trades@proton.me",
            "phone": "",
            "company": "",
            "source": "Bank Records",
        },

        # =====================================================================
        # Person 4: Maria Garcia — 2-hop to flagged entity + fraud ring
        # Demonstrates: bad_neighbour_score (2-hop) + shared email with Person 3
        # Connected to Person 5 via shared company "Eurasian Import Export"
        # Uses the SAME email as Person 3 (fraud ring signal)
        # =====================================================================
        {
            "id": "P4-CRM-001",
            "name": "Maria Garcia",
            "address": "321 Elm Drive, Miami FL 33101",
            "dob": "1982-09-18",
            "email": "jwilson.trades@proton.me",  # SAME as Person 3 — fraud ring
            "phone": "305-555-0134",
            "company": "Eurasian Import Export",
            "source": "CRM",
        },
        {
            "id": "P4-BILL-001",
            "name": "M Garcia",
            "address": "321 Elm Dr, Miami FL 33101",
            "dob": "1982-09-18",
            "email": "maria.g@eiexport.com",
            "phone": "305-555-0134",
            "company": "Eurasian Import Export LLC",
            "source": "Billing",
        },
        {
            "id": "P4-BANK-001",
            "name": "Maria L Garcia",
            "address": "321 Elm Drive Miami 33101",
            "dob": "1982-09-18",
            "email": "maria.g@eiexport.com",
            "phone": "3055550134",
            "company": "EI Export",
            "source": "Bank Records",
        },
        {
            "id": "P4-TRADE-001",
            "name": "Maria Garcia",
            "address": "321 Elm Dr, Miami FL 33101",
            "dob": "",
            "email": "",
            "phone": "305-555-0134",
            "company": "Eurasian Import Export",
            "source": "Trade License",
        },

        # =====================================================================
        # Person 5: Viktor Petrov — Flagged entity (known bad actor)
        # Demonstrates: flagged cluster, bad_neighbour propagation
        # Connected to Person 4 via shared company "Eurasian Import Export"
        # =====================================================================
        {
            "id": "P5-OFAC-001",
            "name": "Viktor Petrov",
            "address": "15 Nevsky Prospect, Saint Petersburg 190000",
            "dob": "1970-01-30",
            "email": "v.petrov@eurasian-ie.ru",
            "phone": "+7-812-555-0199",
            "company": "Eurasian Import Export",
            "source": "OFAC SDN List",
        },
        {
            "id": "P5-BANK-001",
            "name": "V Petrov",
            "address": "15 Nevsky Pr, St Petersburg 190000",
            "dob": "1970-01-30",
            "email": "vpetrov@mail.ru",
            "phone": "78125550199",
            "company": "Eurasian Import Export LLC",
            "source": "Bank Records",
        },
        {
            "id": "P5-TRADE-001",
            "name": "Viktor A Petrov",
            "address": "15 Nevsky Prospect Saint Petersburg",
            "dob": "1970-01-30",
            "email": "v.petrov@eurasian-ie.ru",
            "phone": "",
            "company": "EI Export",
            "source": "Trade License",
        },

        # =====================================================================
        # Extra singletons — records with no match (realistic noise)
        # =====================================================================
        {
            "id": "NOISE-001",
            "name": "Emily Watson",
            "address": "100 Broadway, New York NY 10005",
            "dob": "1995-04-12",
            "email": "ewatson@nyu.edu",
            "phone": "212-555-0177",
            "company": "",
            "source": "CRM",
        },
        {
            "id": "NOISE-002",
            "name": "Robert Kim",
            "address": "2200 Pennsylvania Ave, Washington DC 20037",
            "dob": "1968-12-01",
            "email": "",
            "phone": "202-555-0188",
            "company": "Kim & Associates",
            "source": "Billing",
        },
        {
            "id": "NOISE-003",
            "name": "Aisha Patel",
            "address": "55 Temple Street, New Haven CT 06510",
            "dob": "1991-08-25",
            "email": "aisha.patel@yale.edu",
            "phone": "",
            "company": "Yale University",
            "source": "HR System",
        },
        {
            "id": "NOISE-004",
            "name": "Carlos Mendoza",
            "address": "3400 Montrose Blvd, Houston TX 77006",
            "dob": "",
            "email": "cmendoza@rice.edu",
            "phone": "713-555-0156",
            "company": "Rice University",
            "source": "CRM",
        },
        {
            "id": "NOISE-005",
            "name": "Lisa Thompson",
            "address": "900 S Michigan Ave, Chicago IL 60605",
            "dob": "1983-02-14",
            "email": "lthompson@deloitte.com",
            "phone": "312-555-0145",
            "company": "Deloitte",
            "source": "HR System",
        },
        {
            "id": "NOISE-006",
            "name": "David Chang",
            "address": "1600 Amphitheatre Pkwy, Mountain View CA 94043",
            "dob": "1990-06-30",
            "email": "dchang@google.com",
            "phone": "650-555-0112",
            "company": "Google",
            "source": "CRM",
        },
    ]

    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(description="Generate demo data for entity resolution")
    parser.add_argument("--upload", action="store_true", help="Upload to GCS to-process/")
    parser.add_argument("--output", type=str, default="", help="Write to local file")
    parser.add_argument("--bucket", type=str,
                        default="entity-resolution-bucket-1",
                        help="GCS bucket name")
    args = parser.parse_args()

    df = generate_demo_records()
    print(f"Generated {len(df)} demo records ({df['source'].nunique()} sources)")
    print(f"Persons: {len([c for c in df['id'].str[:2].unique() if c.startswith('P')])} real + "
          f"{len(df[df['id'].str.startswith('NOISE')])} singletons")

    if args.output:
        df.to_csv(args.output, index=False)
        print(f"Written to {args.output}")

    elif args.upload:
        from google.cloud import storage
        gcs_path = f"to-process/demo_records.csv"
        client = storage.Client()
        client.bucket(args.bucket).blob(gcs_path).upload_from_string(
            df.to_csv(index=False), content_type="text/csv"
        )
        print(f"Uploaded to gs://{args.bucket}/{gcs_path}")

    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
