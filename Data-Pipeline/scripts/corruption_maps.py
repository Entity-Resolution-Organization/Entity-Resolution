"""
Corruption Maps for Entity Resolution Data Pipeline.

Centralized dictionaries for all name and address corruption strategies.
Used by preprocessing.py to generate realistic training pair variants.

Categories:
- Nicknames (formal name → common nicknames)
- OCR errors (character substitutions that mimic scanning artifacts)
- Transliterations (name variants across romanization systems)
- Middle name rules (drop, expand, initialize)
- Punctuation variants (hyphen, apostrophe handling)
- Address abbreviations (street type normalization)
- Address unit formats (apartment/unit/suite variants)
- Address directionals (North/South/East/West abbreviations)
"""

# =============================================================================
# NAME CORRUPTION MAPS
# =============================================================================

NICKNAME_MAP = {
    # Male names
    "Robert": ["Rob", "Bobby", "Robbie", "Bob", "Bert"],
    "William": ["Will", "Liam", "Bill", "Billy", "Willy"],
    "James": ["Jim", "Jimmy", "Jamie", "Jas"],
    "Michael": ["Mike", "Mikey", "Mick"],
    "Richard": ["Rick", "Richie", "Rich", "Dick", "Ricky"],
    "Thomas": ["Tom", "Tommy", "Thom"],
    "Charles": ["Charlie", "Chuck", "Chas"],
    "Joseph": ["Joe", "Joey", "Jos"],
    "Christopher": ["Chris", "Topher", "Kit"],
    "Daniel": ["Dan", "Danny"],
    "Matthew": ["Matt", "Matty"],
    "Anthony": ["Tony", "Ant"],
    "David": ["Dave", "Davey"],
    "Andrew": ["Andy", "Drew"],
    "Jonathan": ["Jon", "Johnny", "Nathan"],
    "Nicholas": ["Nick", "Nicky"],
    "Alexander": ["Alex", "Xander", "Lex"],
    "Benjamin": ["Ben", "Benny", "Benji"],
    "Samuel": ["Sam", "Sammy"],
    "Timothy": ["Tim", "Timmy"],
    "Edward": ["Ed", "Eddie", "Ted", "Teddy"],
    "Lawrence": ["Larry", "Laurie"],
    "Francis": ["Frank", "Fran"],
    "Frederick": ["Fred", "Freddy", "Fritz"],
    "Gregory": ["Greg", "Gregg"],
    "Raymond": ["Ray"],
    "Patrick": ["Pat", "Paddy"],
    "Kenneth": ["Ken", "Kenny"],
    "Theodore": ["Ted", "Teddy", "Theo"],
    "Stephen": ["Steve", "Stevie"],
    "Phillip": ["Phil"],
    "Gerald": ["Jerry", "Gerry"],
    "Donald": ["Don", "Donny"],
    "Ronald": ["Ron", "Ronnie"],
    "Harold": ["Harry", "Hal"],
    "Walter": ["Walt", "Wally"],
    "Albert": ["Al", "Bert", "Bertie"],
    # Female names
    "Elizabeth": ["Liz", "Beth", "Lizzie", "Eliza", "Betty", "Bess"],
    "Jennifer": ["Jen", "Jenny", "Jenn"],
    "Jessica": ["Jess", "Jessie"],
    "Margaret": ["Maggie", "Meg", "Peggy", "Marge"],
    "Katherine": ["Kate", "Kathy", "Katie", "Kit", "Kitty"],
    "Christine": ["Chris", "Christy", "Tina"],
    "Rebecca": ["Becky", "Becca"],
    "Patricia": ["Pat", "Tricia", "Patty"],
    "Barbara": ["Barb", "Babs"],
    "Susan": ["Sue", "Susie", "Suzy"],
    "Dorothy": ["Dot", "Dotty", "Dottie"],
    "Deborah": ["Deb", "Debbie"],
    "Michelle": ["Shelly", "Mitch"],
    "Kimberly": ["Kim", "Kimmy"],
    "Stephanie": ["Steph", "Stevie"],
    "Victoria": ["Vicky", "Tori", "Vic"],
    "Alexandra": ["Alex", "Alexa", "Lexi"],
    "Samantha": ["Sam", "Sammy"],
    "Abigail": ["Abby", "Gail"],
    "Emily": ["Em", "Emmy"],
    "Amanda": ["Mandy", "Manda"],
    "Catherine": ["Cathy", "Cat", "Kate"],
    "Jacqueline": ["Jackie", "Jacky"],
    "Carolyn": ["Carol", "Carrie", "Lyn"],
    "Josephine": ["Jo", "Josie"],
    "Theresa": ["Terry", "Tess", "Tessa"],
    "Virginia": ["Ginny", "Ginger"],
}

# OCR error map: character → common OCR misreads
# Bidirectional — apply in either direction for realistic errors
OCR_ERROR_MAP = {
    "O": ["0"],  # capital O → zero
    "0": ["O"],  # zero → capital O
    "o": ["0"],  # lowercase o → zero
    "l": ["1", "I"],  # lowercase L → one or capital I
    "1": ["l", "I"],  # one → lowercase L or capital I
    "I": ["l", "1"],  # capital I → lowercase L or one
    "m": ["rn", "nn"],  # m → rn or nn
    "rn": ["m"],  # rn → m
    "w": ["vv"],  # w → double v
    "vv": ["w"],  # double v → w
    "cl": ["d"],  # cl → d
    "d": ["cl"],  # d → cl
    "B": ["8"],  # B → 8
    "8": ["B"],  # 8 → B
    "S": ["5"],  # S → 5
    "5": ["S"],  # 5 → S
    "G": ["6"],  # G → 6
    "6": ["G"],  # 6 → G
    "Z": ["2"],  # Z → 2
    "2": ["Z"],  # 2 → Z
    "g": ["q"],  # g → q
    "q": ["g"],  # q → g
    "h": ["b"],  # h → b (in poor scans)
    "u": ["v"],  # u → v
    "v": ["u"],  # v → u
}

# Transliteration variants: canonical form → known romanization alternatives
# Critical for OFAC/sanctions matching and international names
TRANSLITERATION_MAP = {
    # Arabic names
    "Mohamed": ["Muhammad", "Mohammed", "Mohamad", "Mohammad", "Muhammed", "Mohamud"],
    "Ahmed": ["Ahmad", "Ahmet", "Achmed"],
    "Hassan": ["Hasan", "Hasen", "Hussan"],
    "Hussein": ["Husein", "Husain", "Hussain", "Hossein"],
    "Ali": ["Aly", "Alee"],
    "Omar": ["Umar", "Omer"],
    "Yusuf": ["Yousef", "Youssef", "Yosef", "Josef", "Joseph"],
    "Ibrahim": ["Ibraheem", "Ebrahim", "Abraham"],
    "Khalid": ["Khaled", "Kalid"],
    "Abdul": ["Abdel", "Abdal", "Abd"],
    "Tariq": ["Tarek", "Tarik"],
    "Jamal": ["Gamal", "Jamaal"],
    "Rashid": ["Rachid", "Rasheed"],
    # Russian/Cyrillic names
    "Sergei": ["Sergey", "Serge"],
    "Dmitri": ["Dmitry", "Dimitri", "Dimitry"],
    "Alexei": ["Alexey", "Aleksei", "Aleksey"],
    "Mikhail": ["Mikail", "Michael"],
    "Nikolai": ["Nikolay", "Nicolai"],
    "Vladimir": ["Vladimer", "Wladimir"],
    "Yevgeny": ["Evgeni", "Evgeny", "Eugene"],
    "Aleksandr": ["Alexander", "Alexandr", "Oleksandr"],
    "Andrei": ["Andrey", "Andre", "Andrew"],
    "Pavel": ["Paul"],
    # East Asian names
    "Park": ["Pak", "Bak"],
    "Kim": ["Gim"],
    "Lee": ["Li", "Yi", "Rhee"],
    "Chen": ["Chan", "Chin"],
    "Wang": ["Wong", "Huang"],
    "Zhang": ["Chang", "Cheung"],
    "Liu": ["Lau", "Lieu"],
    # Common cross-language
    "John": ["Johann", "Juan", "Giovanni", "Ivan", "Jan", "Jean"],
    "Peter": ["Pyotr", "Pedro", "Pierre", "Pietro"],
    "George": ["Georgi", "Jorge", "Giorgio", "Yuri"],
    "Paul": ["Pablo", "Paolo", "Pavel"],
    "James": ["Jaime", "Jacques", "Giacomo"],
}

# Middle name handling: common middle initials for expansion
COMMON_MIDDLE_NAMES = {
    "A": ["Alan", "Ann", "Arthur", "Alice"],
    "B": ["Brian", "Beth", "Bruce"],
    "C": ["Charles", "Carol", "Claire"],
    "D": ["David", "Diane", "Daniel"],
    "E": ["Edward", "Ellen", "Elizabeth"],
    "F": ["Francis", "Faye"],
    "G": ["George", "Grace", "Gloria"],
    "H": ["Henry", "Helen", "Howard"],
    "J": ["James", "Jane", "John", "Joseph"],
    "K": ["Keith", "Karen", "Kenneth"],
    "L": ["Lee", "Lynn", "Louis"],
    "M": ["Marie", "Michael", "Mary", "Mark"],
    "N": ["Neil", "Nancy", "Norman"],
    "P": ["Paul", "Patricia", "Peter"],
    "R": ["Robert", "Rose", "Raymond"],
    "S": ["Scott", "Susan", "Samuel"],
    "T": ["Thomas", "Teresa"],
    "W": ["William", "Wayne"],
}

# Punctuation variant rules
PUNCTUATION_VARIANTS = {
    "hyphenated_to_space": True,  # Mary-Jane → Mary Jane
    "hyphenated_to_joined": True,  # Mary-Jane → Maryjane
    "apostrophe_removed": True,  # O'Brien → OBrien
    "apostrophe_to_space": True,  # O'Brien → O Brien
    "period_removed": True,  # St. → St
    "comma_removed": True,  # Smith, Jr → Smith Jr
}


# =============================================================================
# ADDRESS CORRUPTION MAPS
# =============================================================================

STREET_ABBREVIATIONS = {
    "Street": ["St", "St.", "Str"],
    "Avenue": ["Ave", "Ave.", "Av"],
    "Boulevard": ["Blvd", "Blvd.", "Boul"],
    "Road": ["Rd", "Rd."],
    "Drive": ["Dr", "Dr.", "Drv"],
    "Lane": ["Ln", "Ln."],
    "Court": ["Ct", "Ct."],
    "Place": ["Pl", "Pl."],
    "Circle": ["Cir", "Cir."],
    "Highway": ["Hwy", "Hwy."],
    "Parkway": ["Pkwy", "Pky"],
    "Square": ["Sq", "Sq."],
    "Terrace": ["Ter", "Ter."],
    "Trail": ["Trl", "Tr"],
    "Way": ["Wy"],
}

# Unit/apartment format variants
UNIT_FORMATS = {
    "Apartment": ["Apt", "Apt.", "#", "Unit", "Suite", "Ste"],
    "Apt": ["Apartment", "#", "Unit", "Suite"],
    "Unit": ["Apt", "#", "Suite", "Ste"],
    "Suite": ["Ste", "Ste.", "#", "Unit"],
    "#": ["Apt", "Unit", "Suite"],
}

# Directional abbreviations
DIRECTIONAL_MAP = {
    "North": ["N", "N."],
    "South": ["S", "S."],
    "East": ["E", "E."],
    "West": ["W", "W."],
    "Northeast": ["NE", "N.E."],
    "Northwest": ["NW", "N.W."],
    "Southeast": ["SE", "S.E."],
    "Southwest": ["SW", "S.W."],
    # Reverse: abbreviation → full word
    "N": ["North"],
    "S": ["South"],
    "E": ["East"],
    "W": ["West"],
}

# Common address component patterns for dropping/adding
ADDRESS_COMPONENTS = {
    "zip_pattern": r"\b\d{5}(-\d{4})?\b",  # 5-digit or 9-digit ZIP
    "state_abbreviations": [
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
    ],
}
