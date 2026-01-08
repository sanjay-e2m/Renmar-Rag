import random
from pathlib import Path
from datetime import datetime
import pandas as pd

# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = Path("rag_excel_postgres/Data/Input")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

NUM_REPORTS = 10
ROWS_PER_REPORT = 100

DOMAIN = "https://www.stewartmovingandstorage.com"

KEYWORDS = [
    "moving a grandfather clock",
    "long distance moving services",
    "professional piano movers",
    "packing and moving services",
    "commercial moving company",
    "office relocation services",
    "local movers near me",
    "storage services near me",
    "how to move antique furniture",
    "secure storage solutions",
    "cross country movers",
    "fragile item moving service",
    "residential moving services",
    "international moving company",
    "climate controlled storage",
    "moving truck rental",
    "furniture moving services",
    "apartment moving help",
    "corporate relocation services",
    "warehouse storage solutions",
    "household goods moving",
    "specialty item movers",
    "full service movers",
    "self storage units",
    "moving and packing supplies",
    "best moving companies",
    "affordable moving services",
    "white glove moving",
    "moving boxes and supplies",
    "portable storage containers",
    "local moving quotes",
    "long distance moving costs",
    "packing services near me",
    "moving labor only",
    "storage facilities near me",
    "office furniture moving",
    "antique movers",
    "art moving services",
    "vehicle shipping services",
    "pet relocation services",
    "senior moving services",
    "military moving companies",
    "student moving services",
    "same day moving",
    "last minute movers",
    "weekend moving services",
    "evening moving services",
    "eco friendly movers",
    "moving insurance coverage",
    "storage unit sizes",
    "moving checklist",
    "how to pack fragile items",
    "moving day tips",
    "moving cost calculator",
    "storage unit prices",
    "climate controlled storage units",
    "drive up storage units",
    "24 hour storage access",
    "storage unit security",
    "moving quotes online",
    "free moving estimates",
    "licensed movers",
    "insured moving companies",
    "dismantle and reassemble furniture",
    "custom crating services",
    "temporary storage solutions",
    "warehouse storage near me",
    "document storage services",
    "moving blankets rental",
    "heavy lifting services",
    "appliance moving",
    "piano tuning after move",
    "electronics packing",
    "wine cellar moving",
    "pool table movers",
    "hot tub movers",
    "grandfather clock repair",
    "moving stress relief",
    "change of address services",
    "utilities transfer help",
    "postal forwarding services",
    "moving company reviews",
    "top rated movers",
    "moving scams to avoid",
    "how to choose movers",
    "moving contract review",
    "dispute resolution movers",
    "moving company licensing",
    "better business bureau movers",
    "angie's list movers",
    "homeadvisor movers",
    "thumbtack moving services",
    "taskrabbit moving help",
    "u-haul alternatives",
    "budget truck rental",
    "penske truck rental",
    "hertz truck rental",
    "ryder truck rental",
    "moving truck size guide",
]

# File naming options
FILE_NAMES = ["abc", "efg", "fgh", "klm", "xyz", "def", "ghi", "jkl", "mno", "pqr", "stu", "vwx", "yza", "bcd", "cde"]
MONTHS = ["january", "february", "march", "april", "may", "june", 
          "july", "august", "september", "october", "november", "december"]
YEARS = [2023, 2024, 2025, 2026]

SEARCH_INTENTS = ["Informational", "Commercial", "Transactional", None]
LOCATIONS = [
    "National",
    "California, USA",
    "New York, USA",
    "Texas, USA",
    "Florida, USA",
    "Illinois, USA",
    "Pennsylvania, USA",
    "Ohio, USA",
    "Georgia, USA",
    "North Carolina, USA",
    "Michigan, USA",
    "New Jersey, USA",
    "Virginia, USA",
    "Washington, USA",
    "Arizona, USA",
    "Massachusetts, USA",
    "Tennessee, USA",
    "Indiana, USA",
    "Missouri, USA",
    "Maryland, USA",
    "Wisconsin, USA",
    "Colorado, USA",
    "Minnesota, USA",
    "South Carolina, USA",
    "Alabama, USA",
    "Louisiana, USA",
    "Kentucky, USA",
    "Oregon, USA",
    "Oklahoma, USA",
    "Connecticut, USA",
    "Utah, USA",
    "Iowa, USA",
    "Nevada, USA",
    "Arkansas, USA",
    "Mississippi, USA",
    "Kansas, USA",
    "New Mexico, USA",
    "Nebraska, USA",
    "West Virginia, USA",
    "Idaho, USA",
    "Hawaii, USA",
    "New Hampshire, USA",
    "Maine, USA",
    "Montana, USA",
    "Rhode Island, USA",
    "Delaware, USA",
    "South Dakota, USA",
    "North Dakota, USA",
    "Alaska, USA",
    "Vermont, USA",
    "Wyoming, USA",
    "District of Columbia, USA",
]
URL_PATHS = [
    "/how-to-move-a-grandfather-clock",
    "/long-distance-moving-services",
    "/piano-moving-services",
    "/packing-and-moving",
    "/commercial-moving",
    "/office-relocation",
    "/local-movers",
    "/storage-solutions",
    "/move-antique-furniture",
]

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def generate_row(keyword):
    initial_rank = random.randint(5, 50)
    current_rank = max(1, initial_rank - random.randint(-3, 20))
    change = initial_rank - current_rank

    # Randomly assign Difficulty - can be None (30% chance)
    difficulty = None if random.random() < 0.3 else random.randint(10, 70)
    
    # Randomly assign Search Intent - can be None (25% chance)
    search_intent = None if random.random() < 0.25 else random.choice([x for x in SEARCH_INTENTS if x is not None])

    return {
        "Keyword": keyword,
        "Initial ranking_month_year": initial_rank,
        "Current Rank_month_year": current_rank,
        "Change +/-": f"{'↑' if change > 0 else '↓'} {abs(change)}",
        "Search Volume": random.choice([250, 500, 750, 1000, 1500, 3000]),
        "Map Ranking (GBP)": random.choice([10, 20, 30, 40, 50]),
        "Location(state,country)": random.choice(LOCATIONS),
        "URL": DOMAIN + random.choice(URL_PATHS),
        "Difficulty": difficulty,
        "Search Intent": search_intent,
    }

# ============================================================
# GENERATE REPORTS
# ============================================================

for report_index in range(1, NUM_REPORTS + 1):
    rows = []

    # Select unique keywords for this report (no duplicates within a sheet)
    # Use random.sample to ensure all keywords are unique
    num_keywords_needed = min(ROWS_PER_REPORT, len(KEYWORDS))
    selected_keywords = random.sample(KEYWORDS, k=num_keywords_needed)
    
    # Generate rows with unique keywords only
    for keyword in selected_keywords:
        rows.append(generate_row(keyword))
    
    # Shuffle the rows for randomness (order within the sheet)
    random.shuffle(rows)

    df = pd.DataFrame(rows)

    # Preserve column order exactly as your master sheet
    df = df[
        [
            "Keyword",
            "Initial ranking_month_year",
            "Current Rank_month_year",
            "Change +/-",
            "Search Volume",
            "Map Ranking (GBP)",
            "Location(state,country)",
            "URL",
            "Difficulty",
            "Search Intent",
        ]
    ]

    # Generate file name in format: name_month_year.xlsx
    file_name = random.choice(FILE_NAMES)
    month = random.choice(MONTHS)
    year = random.choice(YEARS)
    file_path = OUTPUT_DIR / f"{file_name}_{month}_{year}.xlsx"
    
    df.to_excel(file_path, index=False)

    print(f"Created: {file_path}")

print("\nAll dummy keyword reports generated successfully.")