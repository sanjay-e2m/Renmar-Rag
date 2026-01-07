import random
from pathlib import Path
from datetime import datetime
import pandas as pd

# ============================================================
# CONFIG
# ============================================================

OUTPUT_DIR = Path("dummy_reports")
OUTPUT_DIR.mkdir(exist_ok=True)

NUM_REPORTS = 5
ROWS_PER_REPORT = 15

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
]

SEARCH_INTENTS = ["Informational", "Commercial", "Transactional"]
LOCATIONS = ["National"]
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

    return {
        "Keyword": keyword,
        "Initial ranking_month_year": initial_rank,
        "Current Rank_month_year": current_rank,
        "Change +/-": f"{'↑' if change > 0 else '↓'} {abs(change)}",
        "Search Volume": random.choice([250, 500, 750, 1000, 1500, 3000]),
        "Map Ranking (GBP)": random.choice([10, 20, 30, 40, 50]),
        "Location(state,country)": random.choice(LOCATIONS),
        "URL": DOMAIN + random.choice(URL_PATHS),
        "Difficulty": random.randint(10, 70),
        "Search Intent": random.choice(SEARCH_INTENTS),
    }

# ============================================================
# GENERATE REPORTS
# ============================================================

for report_index in range(1, NUM_REPORTS + 1):
    rows = []

    selected_keywords = random.sample(KEYWORDS, k=min(ROWS_PER_REPORT, len(KEYWORDS)))

    for kw in selected_keywords:
        rows.append(generate_row(kw))

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

    file_path = OUTPUT_DIR / f"keyword_report_{report_index:02d}.xlsx"
    df.to_excel(file_path, index=False)

    print(f"Created: {file_path}")

print("\nAll dummy keyword reports generated successfully.")