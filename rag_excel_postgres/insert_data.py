from pathlib import Path
import pandas as pd
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

# -------------------------
# Load env & DB config
# -------------------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# -------------------------
# Load Excel File
# -------------------------
# Get the script's directory (rag_excel_postgres/)
script_dir = Path(__file__).parent.resolve()
file_path = script_dir / "Data" / "Input" / "irby_march_2025.xlsx"

if not file_path.exists():
    raise FileNotFoundError(f"Excel file not found: {file_path}")

df = pd.read_excel(file_path, header=1)
print(f"Loaded {len(df)} rows from {file_path}")

# -------------------------
# Parse file name
# -------------------------
file_stem = file_path.stem.lower()  # irby_march_2025
parts = file_stem.split("_")

client_name = parts[0]             # irby
month_name = parts[1].capitalize() # March
year = int(parts[2])               # 2025

month_map = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}
month_id = month_map[month_name]

# -------------------------
# Expected Columns
# -------------------------
expected_cols = [
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

missing = [c for c in expected_cols if c not in df.columns]
if missing:
    raise ValueError(f"Missing expected columns: {missing}")

# -------------------------
# Insert Logic
# -------------------------
conn = psycopg2.connect(**DB_CONFIG)

with conn:
    with conn.cursor() as cur:

        # -------------------------
        # 1. UPSERT month
        # -------------------------
        cur.execute(
            """
            INSERT INTO months (month_name, year, month_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (month_name, year)
            DO UPDATE SET month_id = EXCLUDED.month_id
            RETURNING month_pk;
            """,
            (month_name, year, month_id),
        )
        month_pk = cur.fetchone()[0]

        # -------------------------
        # 2. INSERT file record
        # -------------------------
        cur.execute(
            """
            INSERT INTO files (client_name, file_name, month_pk)
            VALUES (%s, %s, %s)
            RETURNING file_id;
            """,
            (client_name, file_path.name, month_pk),
        )
        file_id = cur.fetchone()[0]

        print(f"Created file_id={file_id} for {file_path.name}")

        # -------------------------
        # 3. Prepare keyword rows
        # -------------------------
        records = []
        for _, row in df[expected_cols].iterrows():
            clean = row.where(pd.notna(row), None)
            records.append(
                (
                    file_id,
                    clean["Keyword"],
                    clean["Initial ranking_month_year"],
                    clean["Current Rank_month_year"],
                    clean["Change +/-"],
                    clean["Search Volume"],
                    clean["Map Ranking (GBP)"],
                    clean["Location(state,country)"],
                    clean["URL"],
                    clean["Difficulty"],
                    clean["Search Intent"],
                )
            )

        # -------------------------
        # 4. Insert keyword data
        # -------------------------
        insert_query = """
        INSERT INTO "Mastersheet-Keyword_report" (
            file_id,
            keyword,
            initial_ranking,
            current_ranking,
            change,
            search_volume,
            map_ranking_gbp,
            location,
            url,
            difficulty,
            search_intent
        ) VALUES %s
        """

        execute_values(cur, insert_query, records)

        print(f"Inserted {len(records)} keyword rows for file_id={file_id}")

conn.close()
print("✅ Data insertion completed successfully")
