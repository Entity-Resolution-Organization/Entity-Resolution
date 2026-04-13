"""
DeBERTa Model Test Data Generator
===================================

Generates ~1500 labelled record pairs designed to stress-test the DeBERTa
entity resolution model across every corruption category used in the
Data Pipeline, plus adversarial edge cases that might break inference.

Each row is a PAIR: (name1, address1, email1, company1) vs (name2, address2, email2, company2)
with a label (1 = match, 0 = non-match) and a test_category tag explaining what's being tested.

Test categories:
  POSITIVE PAIRS (label=1):
    - nickname                Formal ↔ short name (Robert → Bob)
    - transliteration         Cross-script romanization (Mohamed → Muhammad)
    - ocr_error               Scanned-document artifacts (Robert → R0bert)
    - typo                    Keyboard typos (Smith → Smtih)
    - middle_name_variation   Add/drop/expand middle name or initial
    - name_swap               Last, First ordering
    - punctuation_variant     Hyphen/apostrophe handling (O'Brien → OBrien)
    - address_abbreviation    Street ↔ St, Avenue ↔ Ave
    - address_unit_format     Apt ↔ # ↔ Unit ↔ Suite
    - address_directional     North ↔ N, Southeast ↔ SE
    - address_component_drop  Missing ZIP, missing city
    - address_digit_swap      Transposed house numbers (123 → 213)
    - address_reorder         City before street
    - email_variation         Same person, different email
    - company_variation       Same person, company name abbreviated
    - multi_field_corruption  2-3 fields corrupted at once
    - all_caps                Entire record in uppercase
    - all_lowercase           Entire record in lowercase
    - extra_whitespace        Double spaces, leading/trailing spaces
    - unicode_accents         Accented characters (José, Müller)

  NEGATIVE PAIRS (label=0):
    - same_name_diff_person   Same name, completely different address/DOB
    - same_address_diff_name  Same address, different people
    - shared_email_diff_name  Same email, different identity
    - phonetic_near_miss      Similar-sounding names (Garcia ↔ Garsia)
    - family_member           Same last name + address, different first name
    - ofac_adjacent           Legitimate employee vs SDN record at same company
    - partial_overlap         One matching field, rest different

  EDGE CASES (label=1 or 0, tagged):
    - empty_fields            Missing name, address, or email
    - single_word_name        Mononyms (Madonna, Cher)
    - very_long_name          50+ character names
    - numeric_in_name         Names with numbers (John Smith III, 3rd)
    - special_characters      Slashes, semicolons, quotes in fields
    - null_values             Explicit "null", "N/A", "None" strings
    - html_injection          <script> tags and HTML entities
    - sql_injection           SQL injection strings in name fields
    - mixed_language          Chinese/Arabic/Cyrillic mixed with Latin
    - date_format_variation   DOB in different formats

Output: Demo-Dataset/generated_datasets/april_test_records.csv

Usage:
    python generate_test_pairs.py
"""

import argparse
import csv
import json
import os
import random
from datetime import date, timedelta
from pathlib import Path


# =============================================================================
# SEED DATA
# =============================================================================

FIRST_NAMES = [
    "Robert", "William", "James", "Michael", "Richard", "Thomas", "Charles",
    "Joseph", "Christopher", "Daniel", "Matthew", "Anthony", "David", "Andrew",
    "Jonathan", "Nicholas", "Alexander", "Benjamin", "Samuel", "Timothy",
    "Elizabeth", "Jennifer", "Patricia", "Jessica", "Barbara", "Susan",
    "Margaret", "Katherine", "Rebecca", "Stephanie", "Victoria", "Samantha",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson",
    "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee",
    "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson", "Walker",
    "Green", "Baker", "Adams", "Nelson", "Carter", "Mitchell", "Roberts",
    "Turner", "Phillips", "Campbell", "Parker", "Evans", "Stewart",
    "O'Brien", "MacDonald", "Van Der Berg", "De La Cruz", "Al-Hassan",
]

NICKNAME_MAP = {
    "Robert": ["Bob", "Rob", "Bobby", "Robbie", "Bert"],
    "William": ["Will", "Bill", "Billy", "Liam", "Willy"],
    "James": ["Jim", "Jimmy", "Jamie"],
    "Michael": ["Mike", "Mikey", "Mick"],
    "Richard": ["Rick", "Rich", "Dick", "Ricky"],
    "Thomas": ["Tom", "Tommy", "Thom"],
    "Charles": ["Charlie", "Chuck"],
    "Joseph": ["Joe", "Joey"],
    "Christopher": ["Chris", "Topher"],
    "Daniel": ["Dan", "Danny"],
    "Matthew": ["Matt", "Matty"],
    "Anthony": ["Tony"],
    "David": ["Dave", "Davey"],
    "Andrew": ["Andy", "Drew"],
    "Jonathan": ["Jon", "Johnny"],
    "Nicholas": ["Nick", "Nicky"],
    "Alexander": ["Alex", "Xander"],
    "Benjamin": ["Ben", "Benny"],
    "Samuel": ["Sam", "Sammy"],
    "Timothy": ["Tim", "Timmy"],
    "Elizabeth": ["Liz", "Beth", "Lizzie", "Betty"],
    "Jennifer": ["Jen", "Jenny"],
    "Patricia": ["Pat", "Tricia", "Patty"],
    "Jessica": ["Jess", "Jessie"],
    "Barbara": ["Barb", "Babs"],
    "Susan": ["Sue", "Susie"],
    "Margaret": ["Maggie", "Meg", "Peggy"],
    "Katherine": ["Kate", "Kathy", "Katie"],
    "Rebecca": ["Becky", "Becca"],
    "Stephanie": ["Steph"],
    "Victoria": ["Vicky", "Tori"],
    "Samantha": ["Sam", "Sammy"],
}

TRANSLITERATION_MAP = {
    "Mohamed": ["Muhammad", "Mohammed", "Mohamad", "Mohammad"],
    "Ahmed": ["Ahmad", "Ahmet", "Achmed"],
    "Hassan": ["Hasan", "Hasen"],
    "Hussein": ["Husein", "Husain", "Hussain", "Hossein"],
    "Sergei": ["Sergey", "Serge"],
    "Dmitri": ["Dmitry", "Dimitri"],
    "Alexei": ["Alexey", "Aleksei"],
    "Mikhail": ["Mikail", "Michael"],
    "Vladimir": ["Vladimer", "Wladimir"],
    "Yusuf": ["Yousef", "Youssef", "Joseph"],
    "John": ["Johann", "Juan", "Giovanni", "Ivan", "Jean"],
    "Peter": ["Pyotr", "Pedro", "Pierre"],
    "George": ["Georgi", "Jorge", "Giorgio"],
}

OCR_MAP = {
    "O": "0", "0": "O", "l": "1", "1": "l", "I": "l",
    "m": "rn", "B": "8", "S": "5", "G": "6", "Z": "2",
    "g": "q", "u": "v",
}

STREETS = [
    "Main Street", "Oak Avenue", "Elm Road", "Pine Drive", "Maple Lane",
    "Cedar Court", "Washington Boulevard", "Park Avenue", "Broadway",
    "North Highland Drive", "South Sunset Boulevard", "East River Road",
    "West Lake Shore Drive", "Market Street", "Church Street",
    "Northeast Spring Street", "Southeast Franklin Avenue",
]

CITIES = [
    ("Boston", "MA", "02101"), ("New York", "NY", "10001"),
    ("Los Angeles", "CA", "90001"), ("Chicago", "IL", "60601"),
    ("Houston", "TX", "77001"), ("Austin", "TX", "73301"),
    ("Seattle", "WA", "98101"), ("Denver", "CO", "80201"),
    ("Miami", "FL", "33101"), ("Atlanta", "GA", "30301"),
    ("Raleigh", "NC", "27601"), ("San Francisco", "CA", "94101"),
]

COMPANIES = [
    "Acme Corp", "TechNova Inc.", "GlobalTrade Solutions LLC",
    "Summit Financial Group", "Pinnacle Health Systems",
    "Bright Horizons LLC", "Metro Logistics International",
    "DataStream Analytics Corp", "Coastal Engineering & Design",
    "Petrov Trading LLC", "Al-Rashid Import Export",
    "Volkov Energy Partners", "Khalid International Group",
]

EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "protonmail.com", "icloud.com", "company.com", "work.org",
]

SOURCES = ["CRM", "Billing", "Fraud_DB"]


# =============================================================================
# HELPERS
# =============================================================================

def rand_dob(rng):
    start = date(1950, 1, 1)
    d = start + timedelta(days=rng.randint(0, 18000))
    return d.strftime("%Y-%m-%d")

def rand_addr(rng):
    num = rng.randint(1, 9999)
    st = rng.choice(STREETS)
    city, state, z = rng.choice(CITIES)
    return f"{num} {st}, {city}, {state} {z}"

def rand_email(first, last, rng):
    dom = rng.choice(EMAIL_DOMAINS)
    style = rng.choice(["dot", "init", "full", "num"])
    f, l = first.lower().replace("'", ""), last.lower().replace("'", "").replace(" ", "")
    if style == "dot":    return f"{f}.{l}@{dom}"
    elif style == "init": return f"{f[0]}{l}@{dom}"
    elif style == "full": return f"{f}{l}@{dom}"
    else:                 return f"{f}.{l}{rng.randint(1,99)}@{dom}"

def rand_company(rng):
    return rng.choice(COMPANIES)

def rand_source(rng):
    return rng.choice(SOURCES)

def make_identity(rng, pid):
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    name = f"{first} {last}"
    addr = rand_addr(rng)
    dob = rand_dob(rng)
    email = rand_email(first, last, rng)
    company = rand_company(rng)
    source = rand_source(rng)
    return {
        "id": f"test_{pid:04d}", "name": name, "address": addr,
        "dob": dob, "email": email, "company": company, "source": source,
    }


# =============================================================================
# PAIR GENERATORS — one function per test category
# =============================================================================

def gen_nickname_pairs(rng, n, pid):
    """Formal name ↔ nickname. Label=1."""
    pairs = []
    for _ in range(n):
        first = rng.choice(list(NICKNAME_MAP.keys()))
        last = rng.choice(LAST_NAMES)
        nick = rng.choice(NICKNAME_MAP[first])
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        email = rand_email(first, last, rng)
        co = rand_company(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", first + " " + last, addr, dob, email, co, rand_source(rng),
            f"test_{pid+1:04d}", nick + " " + last, addr, dob, email, co, rand_source(rng),
            1, "nickname",
        ))
        pid += 2
    return pairs, pid

def gen_transliteration_pairs(rng, n, pid):
    """Cross-script name variants. Label=1."""
    pairs = []
    for _ in range(n):
        canon = rng.choice(list(TRANSLITERATION_MAP.keys()))
        variant = rng.choice(TRANSLITERATION_MAP[canon])
        last = rng.choice(LAST_NAMES[:20])
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        email = rand_email(canon, last, rng)
        co = rand_company(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", f"{canon} {last}", addr, dob, email, co, rand_source(rng),
            f"test_{pid+1:04d}", f"{variant} {last}", addr, dob,
            rand_email(variant, last, rng), co, rand_source(rng),
            1, "transliteration",
        ))
        pid += 2
    return pairs, pid

def gen_ocr_pairs(rng, n, pid):
    """OCR scanning artifacts. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        name2 = list(ident["name"])
        applied = False
        for i, ch in enumerate(name2):
            if ch in OCR_MAP and rng.random() < 0.4:
                name2[i] = OCR_MAP[ch]
                applied = True
        if not applied and len(name2) > 3:
            pos = rng.randint(1, len(name2)-2)
            if name2[pos].isalpha():
                name2[pos] = OCR_MAP.get(name2[pos], name2[pos])
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", "".join(name2), ident["address"], ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "ocr_error",
        ))
        pid += 2
    return pairs, pid

def gen_typo_pairs(rng, n, pid):
    """Keyboard typos — delete, substitute, transpose. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        name = ident["name"]
        parts = name.split()
        target = rng.choice(parts)
        if len(target) >= 3:
            op = rng.choice(["delete", "substitute", "transpose"])
            pos = rng.randint(1, len(target)-2)
            chars = list(target)
            if op == "delete":
                chars.pop(pos)
            elif op == "substitute":
                chars[pos] = chr(rng.randint(97, 122))
            else:
                chars[pos], chars[pos-1] = chars[pos-1], chars[pos]
            corrupted_name = name.replace(target, "".join(chars), 1)
        else:
            corrupted_name = name
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", corrupted_name, ident["address"], ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "typo",
        ))
        pid += 2
    return pairs, pid

def gen_middle_name_pairs(rng, n, pid):
    """Add/drop/initialize middle name. Label=1."""
    pairs = []
    middles = ["James", "Marie", "Lee", "Ann", "Michael", "Lynn", "Ray", "Grace"]
    for _ in range(n):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        middle = rng.choice(middles)
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        email = rand_email(first, last, rng)
        co = rand_company(rng)
        variant_type = rng.choice(["add_middle", "initial_only", "drop_middle"])
        if variant_type == "add_middle":
            name1, name2 = f"{first} {last}", f"{first} {middle} {last}"
        elif variant_type == "initial_only":
            name1, name2 = f"{first} {middle} {last}", f"{first} {middle[0]}. {last}"
        else:
            name1, name2 = f"{first} {middle} {last}", f"{first} {last}"
        pairs.append(_pair(
            f"test_{pid:04d}", name1, addr, dob, email, co, rand_source(rng),
            f"test_{pid+1:04d}", name2, addr, dob, email, co, rand_source(rng),
            1, "middle_name_variation",
        ))
        pid += 2
    return pairs, pid

def gen_name_swap_pairs(rng, n, pid):
    """Last, First ordering. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        parts = ident["name"].split()
        swapped = f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) >= 2 else ident["name"]
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", swapped, ident["address"], ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "name_swap",
        ))
        pid += 2
    return pairs, pid

def gen_punctuation_pairs(rng, n, pid):
    """Hyphen/apostrophe variations. Label=1."""
    pairs = []
    punct_names = [
        ("Mary-Jane Watson", ["Mary Jane Watson", "MaryJane Watson", "Maryjane Watson"]),
        ("O'Brien", ["OBrien", "O Brien", "Obrien"]),
        ("O'Connor", ["OConnor", "O Connor", "Oconnor"]),
        ("Anne-Marie Smith", ["Anne Marie Smith", "AnneMarie Smith"]),
        ("Jean-Pierre Dupont", ["Jean Pierre Dupont", "JeanPierre Dupont"]),
        ("MacDonald", ["Macdonald", "McDonald", "Mac Donald"]),
        ("Van Der Berg", ["van der Berg", "Vanderberg", "Van der Berg"]),
        ("De La Cruz", ["Delacruz", "de la Cruz", "DeLaCruz"]),
    ]
    for _ in range(n):
        original, variants = rng.choice(punct_names)
        last = rng.choice(LAST_NAMES[:10]) if " " not in original.split()[-1] else ""
        n1 = f"{original} {last}".strip() if last else original
        n2 = f"{rng.choice(variants)} {last}".strip() if last else rng.choice(variants)
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        f1 = original.split()[0].replace("'", "").replace("-", "")
        email = rand_email(f1, last or original.split()[-1], rng)
        co = rand_company(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", n1, addr, dob, email, co, rand_source(rng),
            f"test_{pid+1:04d}", n2, addr, dob, email, co, rand_source(rng),
            1, "punctuation_variant",
        ))
        pid += 2
    return pairs, pid

def gen_address_abbreviation_pairs(rng, n, pid):
    """Street ↔ St, Avenue ↔ Ave. Label=1."""
    abbrevs = {
        "Street": "St", "Avenue": "Ave", "Road": "Rd", "Drive": "Dr",
        "Lane": "Ln", "Court": "Ct", "Boulevard": "Blvd",
    }
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        addr2 = ident["address"]
        for full, short in abbrevs.items():
            if full in addr2:
                addr2 = addr2.replace(full, short, 1)
                break
            elif short in addr2.split():
                addr2 = addr2.replace(f" {short},", f" {full},", 1).replace(f" {short} ", f" {full} ", 1)
                break
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "address_abbreviation",
        ))
        pid += 2
    return pairs, pid

def gen_address_unit_pairs(rng, n, pid):
    """Apt ↔ # ↔ Unit ↔ Suite. Label=1."""
    units = [
        ("Apt 4B", ["#4B", "Unit 4B", "Suite 4B", "Apartment 4B"]),
        ("Suite 200", ["Ste 200", "#200", "Unit 200", "Ste. 200"]),
        ("Unit 12", ["#12", "Apt 12", "Suite 12"]),
        ("#7", ["Apt 7", "Unit 7", "Suite 7"]),
    ]
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        orig_unit, variants = rng.choice(units)
        addr1 = ident["address"].split(",")[0] + f", {orig_unit}" + "," + ",".join(ident["address"].split(",")[1:])
        addr2 = ident["address"].split(",")[0] + f", {rng.choice(variants)}" + "," + ",".join(ident["address"].split(",")[1:])
        pairs.append(_pair(
            ident["id"], ident["name"], addr1, ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "address_unit_format",
        ))
        pid += 2
    return pairs, pid

def gen_address_directional_pairs(rng, n, pid):
    """North ↔ N, Southeast ↔ SE. Label=1."""
    directionals = {
        "North": "N", "South": "S", "East": "E", "West": "W",
        "Northeast": "NE", "Southeast": "SE", "Northwest": "NW", "Southwest": "SW",
    }
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        addr2 = ident["address"]
        for full, short in directionals.items():
            if full in addr2:
                addr2 = addr2.replace(full, short, 1)
                break
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "address_directional",
        ))
        pid += 2
    return pairs, pid

def gen_address_drop_pairs(rng, n, pid):
    """Missing ZIP or city. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        drop = rng.choice(["zip", "city_state"])
        if drop == "zip":
            addr2 = " ".join(ident["address"].rsplit(" ", 1)[:-1])
        else:
            parts = ident["address"].split(",")
            addr2 = parts[0] if parts else ident["address"]
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "address_component_drop",
        ))
        pid += 2
    return pairs, pid

def gen_address_digit_swap_pairs(rng, n, pid):
    """Transposed house numbers. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        addr = ident["address"]
        digits = [(i, c) for i, c in enumerate(addr) if c.isdigit()]
        addr2 = addr
        if len(digits) >= 2:
            chars = list(addr)
            i1, i2 = digits[0][0], digits[1][0]
            chars[i1], chars[i2] = chars[i2], chars[i1]
            addr2 = "".join(chars)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "address_digit_swap",
        ))
        pid += 2
    return pairs, pid

def gen_address_reorder_pairs(rng, n, pid):
    """City before street. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        parts = ident["address"].split(", ", 1)
        addr2 = f"{parts[1]}, {parts[0]}" if len(parts) == 2 else ident["address"]
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "address_reorder",
        ))
        pid += 2
    return pairs, pid

def gen_email_variation_pairs(rng, n, pid):
    """Same person, different email format. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        parts = ident["name"].split()
        email2 = rand_email(parts[0], parts[-1], rng)
        while email2 == ident["email"]:
            email2 = rand_email(parts[0], parts[-1], rng)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], ident["address"], ident["dob"],
            email2, ident["company"], rand_source(rng),
            1, "email_variation",
        ))
        pid += 2
    return pairs, pid

def gen_company_variation_pairs(rng, n, pid):
    """Same person, company name abbreviated. Label=1."""
    co_variants = {
        "TechNova Inc.": ["TechNova", "Technova Inc", "TECHNOVA INC."],
        "GlobalTrade Solutions LLC": ["GlobalTrade Solutions", "Global Trade Solutions", "GlobalTrade"],
        "Coastal Engineering & Design": ["Coastal Engineering", "Coastal Eng & Design", "Coastal Eng."],
        "Metro Logistics International": ["Metro Logistics", "Metro Logistics Intl", "Metro Logistics Int'l"],
    }
    pairs = []
    for _ in range(n):
        co_orig = rng.choice(list(co_variants.keys()))
        co_var = rng.choice(co_variants[co_orig])
        ident = make_identity(rng, pid)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], co_orig, ident["source"],
            f"test_{pid+1:04d}", ident["name"], ident["address"], ident["dob"],
            ident["email"], co_var, rand_source(rng),
            1, "company_variation",
        ))
        pid += 2
    return pairs, pid

def gen_multi_corruption_pairs(rng, n, pid):
    """2-3 fields corrupted at once. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        name2 = ident["name"]
        addr2 = ident["address"]
        email2 = ident["email"]
        # Corrupt name
        parts = name2.split()
        if parts[0] in NICKNAME_MAP:
            name2 = rng.choice(NICKNAME_MAP[parts[0]]) + " " + " ".join(parts[1:])
        # Corrupt address
        for full, short in {"Street": "St", "Avenue": "Ave", "Road": "Rd"}.items():
            if full in addr2:
                addr2 = addr2.replace(full, short)
                break
        # Corrupt email
        email2 = rand_email(parts[0], parts[-1], rng)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", name2, addr2, ident["dob"],
            email2, ident["company"], rand_source(rng),
            1, "multi_field_corruption",
        ))
        pid += 2
    return pairs, pid

def gen_case_pairs(rng, n, pid):
    """ALL CAPS and all lowercase. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        case = rng.choice(["upper", "lower"])
        if case == "upper":
            name2 = ident["name"].upper()
            addr2 = ident["address"].upper()
            cat = "all_caps"
        else:
            name2 = ident["name"].lower()
            addr2 = ident["address"].lower()
            cat = "all_lowercase"
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", name2, addr2, ident["dob"],
            ident["email"].lower(), ident["company"], rand_source(rng),
            1, cat,
        ))
        pid += 2
    return pairs, pid

def gen_whitespace_pairs(rng, n, pid):
    """Extra spaces, tabs, leading/trailing. Label=1."""
    pairs = []
    for _ in range(n):
        ident = make_identity(rng, pid)
        name2 = "  " + ident["name"].replace(" ", "  ") + "  "
        addr2 = " " + ident["address"].replace(",", " , ") + " "
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", name2, addr2, ident["dob"],
            ident["email"], ident["company"], rand_source(rng),
            1, "extra_whitespace",
        ))
        pid += 2
    return pairs, pid

def gen_unicode_pairs(rng, n, pid):
    """Accented and international characters. Label=1."""
    unicode_names = [
        ("Jose Garcia", "José García"), ("Bjorn Mueller", "Björn Müller"),
        ("Rene Dupont", "René Dupont"), ("Francois Leblanc", "François Leblanc"),
        ("Nuria Lopez", "Núria López"), ("Zoe Martin", "Zoë Martin"),
        ("Helene Fischer", "Hélène Fischer"), ("Andre Silva", "André Silva"),
    ]
    pairs = []
    for _ in range(n):
        ascii_name, uni_name = rng.choice(unicode_names)
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        first = ascii_name.split()[0]
        last = ascii_name.split()[-1]
        email = rand_email(first, last, rng)
        co = rand_company(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", ascii_name, addr, dob, email, co, rand_source(rng),
            f"test_{pid+1:04d}", uni_name, addr, dob, email, co, rand_source(rng),
            1, "unicode_accents",
        ))
        pid += 2
    return pairs, pid

# --- NEGATIVE PAIRS ---

def gen_same_name_diff_person(rng, n, pid):
    """Same name, completely different address/DOB/email. Label=0."""
    pairs = []
    for _ in range(n):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        name = f"{first} {last}"
        pairs.append(_pair(
            f"test_{pid:04d}", name, rand_addr(rng), rand_dob(rng),
            rand_email(first, last, rng), rand_company(rng), rand_source(rng),
            f"test_{pid+1:04d}", name, rand_addr(rng), rand_dob(rng),
            rand_email(first, last, rng), rand_company(rng), rand_source(rng),
            0, "same_name_diff_person",
        ))
        pid += 2
    return pairs, pid

def gen_same_address_diff_name(rng, n, pid):
    """Same address, different people (roommates). Label=0."""
    pairs = []
    for _ in range(n):
        addr = rand_addr(rng)
        f1, f2 = rng.sample(FIRST_NAMES, 2)
        l1, l2 = rng.sample(LAST_NAMES, 2)
        pairs.append(_pair(
            f"test_{pid:04d}", f"{f1} {l1}", addr, rand_dob(rng),
            rand_email(f1, l1, rng), rand_company(rng), rand_source(rng),
            f"test_{pid+1:04d}", f"{f2} {l2}", addr, rand_dob(rng),
            rand_email(f2, l2, rng), rand_company(rng), rand_source(rng),
            0, "same_address_diff_name",
        ))
        pid += 2
    return pairs, pid

def gen_shared_email_diff_name(rng, n, pid):
    """Same email, different identity. Label=0."""
    pairs = []
    for _ in range(n):
        f1, f2 = rng.sample(FIRST_NAMES, 2)
        l1, l2 = rng.sample(LAST_NAMES, 2)
        shared = rand_email(f1, l1, rng)
        pairs.append(_pair(
            f"test_{pid:04d}", f"{f1} {l1}", rand_addr(rng), rand_dob(rng),
            shared, rand_company(rng), rand_source(rng),
            f"test_{pid+1:04d}", f"{f2} {l2}", rand_addr(rng), rand_dob(rng),
            shared, rand_company(rng), rand_source(rng),
            0, "shared_email_diff_name",
        ))
        pid += 2
    return pairs, pid

def gen_phonetic_near_miss(rng, n, pid):
    """Similar-sounding names. Label=0."""
    phonetic_pairs = [
        ("Garcia", "Garsia"), ("Smith", "Smyth"), ("Johnson", "Jonson"),
        ("Williams", "Willams"), ("Brown", "Braun"), ("Thompson", "Thomson"),
        ("Clark", "Clarke"), ("Lewis", "Louis"), ("Green", "Greene"),
        ("Stewart", "Stuart"), ("Gray", "Grey"), ("Reed", "Reid"),
        ("Allen", "Allan"), ("Wright", "Right"), ("Phillips", "Philips"),
    ]
    pairs = []
    for _ in range(n):
        last1, last2 = rng.choice(phonetic_pairs)
        first = rng.choice(FIRST_NAMES)
        pairs.append(_pair(
            f"test_{pid:04d}", f"{first} {last1}", rand_addr(rng), rand_dob(rng),
            rand_email(first, last1, rng), rand_company(rng), rand_source(rng),
            f"test_{pid+1:04d}", f"{first} {last2}", rand_addr(rng), rand_dob(rng),
            rand_email(first, last2, rng), rand_company(rng), rand_source(rng),
            0, "phonetic_near_miss",
        ))
        pid += 2
    return pairs, pid

def gen_family_member(rng, n, pid):
    """Same last name + address, different first name (spouse/child). Label=0."""
    pairs = []
    for _ in range(n):
        last = rng.choice(LAST_NAMES)
        f1, f2 = rng.sample(FIRST_NAMES, 2)
        addr = rand_addr(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", f"{f1} {last}", addr, rand_dob(rng),
            rand_email(f1, last, rng), rand_company(rng), rand_source(rng),
            f"test_{pid+1:04d}", f"{f2} {last}", addr, rand_dob(rng),
            rand_email(f2, last, rng), rand_company(rng), rand_source(rng),
            0, "family_member",
        ))
        pid += 2
    return pairs, pid

def gen_ofac_adjacent(rng, n, pid):
    """Legitimate employee vs SDN record at same company. Label=0."""
    ofac_names = [
        ("Sergei Volkov", "Volkov Energy Partners"),
        ("Ahmed Al-Rashid", "Al-Rashid Import Export"),
        ("Vladimir Petrov", "Petrov Trading LLC"),
        ("Khalid Ibrahim", "Khalid International Group"),
    ]
    pairs = []
    for _ in range(n):
        sdn_name, company = rng.choice(ofac_names)
        legit_first = rng.choice(FIRST_NAMES)
        legit_last = rng.choice(LAST_NAMES)
        pairs.append(_pair(
            f"test_{pid:04d}", f"{legit_first} {legit_last}", rand_addr(rng),
            rand_dob(rng), rand_email(legit_first, legit_last, rng), company, rand_source(rng),
            f"test_{pid+1:04d}", sdn_name, "SDGT RUSSIA", rand_dob(rng),
            "", company, "OFAC",
            0, "ofac_adjacent",
        ))
        pid += 2
    return pairs, pid

def gen_partial_overlap(rng, n, pid):
    """One field matches, rest different. Label=0."""
    pairs = []
    for _ in range(n):
        i1 = make_identity(rng, pid)
        i2 = make_identity(rng, pid + 1)
        overlap = rng.choice(["name", "address", "email"])
        if overlap == "name":
            i2["name"] = i1["name"]
        elif overlap == "address":
            i2["address"] = i1["address"]
        else:
            i2["email"] = i1["email"]
        pairs.append(_pair(
            i1["id"], i1["name"], i1["address"], i1["dob"],
            i1["email"], i1["company"], i1["source"],
            i2["id"], i2["name"], i2["address"], i2["dob"],
            i2["email"], i2["company"], i2["source"],
            0, "partial_overlap",
        ))
        pid += 2
    return pairs, pid

# --- EDGE CASES ---

def gen_edge_cases(rng, pid):
    """Adversarial and boundary inputs. Mixed labels."""
    pairs = []

    # Empty fields — label=1 (same person, missing data)
    for _ in range(10):
        ident = make_identity(rng, pid)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], "", ident["dob"],
            "", ident["company"], rand_source(rng),
            1, "empty_fields",
        ))
        pid += 2

    # Single-word names (mononyms)
    for mono in ["Madonna", "Cher", "Prince", "Bono", "Seal"]:
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", mono, addr, dob, "", "", rand_source(rng),
            f"test_{pid+1:04d}", mono.lower(), addr, dob, "", "", rand_source(rng),
            1, "single_word_name",
        ))
        pid += 2

    # Very long names
    for _ in range(5):
        long_name = "Muhammad Abdul Rahman Ibn Khalid Al-Saud Al-Rashid Bin Faisal"
        short_name = "Muhammad Al-Saud"
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", long_name, addr, dob, "", "", rand_source(rng),
            f"test_{pid+1:04d}", short_name, addr, dob, "", "", rand_source(rng),
            1, "very_long_name",
        ))
        pid += 2

    # Numeric in name (Jr, III, 3rd)
    suffixed = [
        ("Robert Smith Jr", "Robert Smith Jr."),
        ("Robert Smith III", "Robert Smith 3rd"),
        ("James Brown Sr.", "James Brown Senior"),
        ("Charles Davis II", "Charles Davis 2nd"),
    ]
    for n1, n2 in suffixed:
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", n1, addr, dob, rand_email("Robert", "Smith", rng), "", rand_source(rng),
            f"test_{pid+1:04d}", n2, addr, dob, rand_email("Robert", "Smith", rng), "", rand_source(rng),
            1, "numeric_in_name",
        ))
        pid += 2

    # Special characters in fields
    specials = [
        'John "Johnny" Smith', "John; DROP TABLE users;--",
        "John Smith / Jane Smith", "John Smith & Associates",
        "John <b>Smith</b>", 'John Smith "CEO"',
    ]
    for sp in specials:
        addr = rand_addr(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", "John Smith", addr, rand_dob(rng),
            "john@test.com", "Acme Corp", "CRM",
            f"test_{pid+1:04d}", sp, addr, rand_dob(rng),
            "john@test.com", "Acme Corp", "CRM",
            1, "special_characters",
        ))
        pid += 2

    # Null-like string values
    nulls = ["null", "N/A", "None", "n/a", "NULL", "-", "NA", "unknown", "UNKNOWN", ""]
    for nv in nulls:
        ident = make_identity(rng, pid)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], ident["dob"],
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], nv, nv,
            nv, nv, rand_source(rng),
            1, "null_values",
        ))
        pid += 2

    # HTML / script injection
    injections = [
        "<script>alert('xss')</script>",
        "<img src=x onerror=alert(1)>",
        "Robert&amp;Smith",
        "Robert&#39;s Record",
    ]
    for inj in injections:
        addr = rand_addr(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", "Robert Smith", addr, rand_dob(rng),
            "robert@test.com", "Acme Corp", "CRM",
            f"test_{pid+1:04d}", inj, addr, rand_dob(rng),
            "robert@test.com", "Acme Corp", "CRM",
            0, "html_injection",
        ))
        pid += 2

    # SQL injection
    sql_attacks = [
        "Robert'; DROP TABLE users;--",
        "1' OR '1'='1",
        "Robert Smith' UNION SELECT * FROM passwords--",
    ]
    for sq in sql_attacks:
        addr = rand_addr(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", "Robert Smith", addr, rand_dob(rng),
            "robert@test.com", "Acme Corp", "CRM",
            f"test_{pid+1:04d}", sq, addr, rand_dob(rng),
            "robert@test.com", "Acme Corp", "CRM",
            0, "sql_injection",
        ))
        pid += 2

    # Mixed-language names
    mixed = [
        ("Wei Zhang", "张伟"), ("Ali Hassan", "علي حسن"),
        ("Nikolai Petrov", "Николай Петров"),
    ]
    for latin, native in mixed:
        addr = rand_addr(rng)
        dob = rand_dob(rng)
        pairs.append(_pair(
            f"test_{pid:04d}", latin, addr, dob, "", "", rand_source(rng),
            f"test_{pid+1:04d}", native, addr, dob, "", "", rand_source(rng),
            1, "mixed_language",
        ))
        pid += 2

    # DOB format variations (same date, different format)
    dob_variants = [
        ("1980-01-15", "01/15/1980"), ("1980-01-15", "15-01-1980"),
        ("1980-01-15", "Jan 15, 1980"), ("1980-01-15", "1980/01/15"),
        ("1980-01-15", "15 January 1980"),
    ]
    for d1, d2 in dob_variants:
        ident = make_identity(rng, pid)
        pairs.append(_pair(
            ident["id"], ident["name"], ident["address"], d1,
            ident["email"], ident["company"], ident["source"],
            f"test_{pid+1:04d}", ident["name"], ident["address"], d2,
            ident["email"], ident["company"], rand_source(rng),
            1, "date_format_variation",
        ))
        pid += 2

    return pairs, pid


# =============================================================================
# PAIR BUILDER
# =============================================================================

COLUMNS = [
    "id1", "name1", "address1", "dob1", "email1", "company1", "source1",
    "id2", "name2", "address2", "dob2", "email2", "company2", "source2",
    "label", "test_category",
]

def _pair(id1, n1, a1, d1, e1, c1, s1, id2, n2, a2, d2, e2, c2, s2, label, cat):
    return {
        "id1": id1, "name1": n1, "address1": a1, "dob1": d1,
        "email1": e1, "company1": c1, "source1": s1,
        "id2": id2, "name2": n2, "address2": a2, "dob2": d2,
        "email2": e2, "company2": c2, "source2": s2,
        "label": label, "test_category": cat,
    }


# =============================================================================
# MAIN
# =============================================================================

def generate_test_pairs(n_target=1500, seed=42):
    rng = random.Random(seed)
    pid = 1
    all_pairs = []

    # Positive pair categories (~55% of total)
    generators_pos = [
        (gen_nickname_pairs,             100),
        (gen_transliteration_pairs,      80),
        (gen_ocr_pairs,                  80),
        (gen_typo_pairs,                 80),
        (gen_middle_name_pairs,          60),
        (gen_name_swap_pairs,            50),
        (gen_punctuation_pairs,          50),
        (gen_address_abbreviation_pairs, 60),
        (gen_address_unit_pairs,         40),
        (gen_address_directional_pairs,  40),
        (gen_address_drop_pairs,         40),
        (gen_address_digit_swap_pairs,   40),
        (gen_address_reorder_pairs,      30),
        (gen_email_variation_pairs,      50),
        (gen_company_variation_pairs,    40),
        (gen_multi_corruption_pairs,     60),
        (gen_case_pairs,                 50),
        (gen_whitespace_pairs,           30),
        (gen_unicode_pairs,              30),
    ]

    for gen_fn, count in generators_pos:
        pairs, pid = gen_fn(rng, count, pid)
        all_pairs.extend(pairs)

    # Negative pair categories (~35% of total)
    generators_neg = [
        (gen_same_name_diff_person,   80),
        (gen_same_address_diff_name,  60),
        (gen_shared_email_diff_name,  60),
        (gen_phonetic_near_miss,      60),
        (gen_family_member,           60),
        (gen_ofac_adjacent,           40),
        (gen_partial_overlap,         50),
    ]

    for gen_fn, count in generators_neg:
        pairs, pid = gen_fn(rng, count, pid)
        all_pairs.extend(pairs)

    # Edge cases (~10% of total)
    edge_pairs, pid = gen_edge_cases(rng, pid)
    all_pairs.extend(edge_pairs)

    rng.shuffle(all_pairs)
    return all_pairs


def save_test_pairs(pairs, output_dir):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_path = output_path / "april_test_records.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(pairs)

    # Stats
    from collections import Counter
    cats = Counter(p["test_category"] for p in pairs)
    labels = Counter(p["label"] for p in pairs)

    stats = {
        "total_pairs": len(pairs),
        "positive_pairs": labels.get(1, 0),
        "negative_pairs": labels.get(0, 0),
        "test_categories": dict(sorted(cats.items(), key=lambda x: -x[1])),
        "output_path": str(csv_path),
    }

    meta_path = output_path / "april_test_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"[Test Data] Saved {len(pairs)} pairs → {csv_path}")
    print(f"[Test Data] Metadata → {meta_path}")
    print(f"\n[Test Data] Summary:")
    print(f"  Total pairs:    {stats['total_pairs']}")
    print(f"  Positive (1):   {stats['positive_pairs']}")
    print(f"  Negative (0):   {stats['negative_pairs']}")
    print(f"\n  Categories:")
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        lbl = "+" if all(p["label"] == 1 for p in pairs if p["test_category"] == cat) else \
              "-" if all(p["label"] == 0 for p in pairs if p["test_category"] == cat) else "±"
        print(f"    {lbl} {cat:30s} {count:4d}")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate DeBERTa model test pairs for Entity Resolution",
    )
    parser.add_argument(
        "--output", "-o",
        default="Demo-Dataset/generated_datasets",
        help="Output directory (default: Demo-Dataset/generated_datasets)",
    )
    parser.add_argument(
        "--seed", "-s", type=int, default=42,
        help="Random seed (default: 42)",
    )
    args = parser.parse_args()

    print(f"[Test Data] Generating test pairs (seed={args.seed})...")
    pairs = generate_test_pairs(seed=args.seed)
    save_test_pairs(pairs, args.output)
