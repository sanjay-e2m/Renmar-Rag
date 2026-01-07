# from pathlib import Path
# import pandas as pd
# import os
# import psycopg2
# from psycopg2.extras import execute_values
# from dotenv import load_dotenv
# from datetime import datetime

# # -------------------------
# # Load env & DB config
# # -------------------------
# load_dotenv()

# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "port": int(os.getenv("DB_PORT")),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# # -------------------------
# # Load Excel File
# # -------------------------
# # Get the script's directory (rag_excel_postgres/)
# script_dir = Path(__file__).parent.resolve()
# file_path = script_dir / "Data" / "Input" / "irby_march_2025.xlsx"

# if not file_path.exists():
#     raise FileNotFoundError(f"Excel file not found: {file_path}")

# df = pd.read_excel(file_path, header=1)
# print(f"Loaded {len(df)} rows from {file_path}")

# # -------------------------
# # Parse file name
# # -------------------------
# file_stem = file_path.stem.lower()  # irby_march_2025
# parts = file_stem.split("_")

# client_name = parts[0]             # irby
# month_name = parts[1].capitalize() # March
# year = int(parts[2])               # 2025

# month_map = {
#     "January": 1, "February": 2, "March": 3, "April": 4,
#     "May": 5, "June": 6, "July": 7, "August": 8,
#     "September": 9, "October": 10, "November": 11, "December": 12
# }
# month_id = month_map[month_name]

# # -------------------------
# # Expected Columns
# # -------------------------
# expected_cols = [
#     "Keyword",
#     "Initial ranking_month_year",
#     "Current Rank_month_year",
#     "Change +/-",
#     "Search Volume",
#     "Map Ranking (GBP)",
#     "Location(state,country)",
#     "URL",
#     "Difficulty",
#     "Search Intent",
# ]

# missing = [c for c in expected_cols if c not in df.columns]
# if missing:
#     raise ValueError(f"Missing expected columns: {missing}")

# # -------------------------
# # Insert Logic
# # -------------------------
# conn = psycopg2.connect(**DB_CONFIG)

# with conn:
#     with conn.cursor() as cur:

#         # -------------------------
#         # 1. UPSERT month
#         # -------------------------
#         cur.execute(
#             """
#             INSERT INTO months (month_name, year, month_id)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (month_name, year)
#             DO UPDATE SET month_id = EXCLUDED.month_id
#             RETURNING month_pk;
#             """,
#             (month_name, year, month_id),
#         )
#         month_pk = cur.fetchone()[0]

#         # -------------------------
#         # 2. INSERT file record
#         # -------------------------
#         cur.execute(
#             """
#             INSERT INTO files (client_name, file_name, month_pk)
#             VALUES (%s, %s, %s)
#             RETURNING file_id;
#             """,
#             (client_name, file_path.name, month_pk),
#         )
#         file_id = cur.fetchone()[0]

#         print(f"Created file_id={file_id} for {file_path.name}")

#         # -------------------------
#         # 3. Prepare keyword rows
#         # -------------------------
#         records = []
#         for _, row in df[expected_cols].iterrows():
#             clean = row.where(pd.notna(row), None)
#             records.append(
#                 (
#                     file_id,
#                     clean["Keyword"],
#                     clean["Initial ranking_month_year"],
#                     clean["Current Rank_month_year"],
#                     clean["Change +/-"],
#                     clean["Search Volume"],
#                     clean["Map Ranking (GBP)"],
#                     clean["Location(state,country)"],
#                     clean["URL"],
#                     clean["Difficulty"],
#                     clean["Search Intent"],
#                 )
#             )

#         # -------------------------
#         # 4. Insert keyword data
#         # -------------------------
#         insert_query = """
#         INSERT INTO "Mastersheet-Keyword_report" (
#             file_id,
#             keyword,
#             initial_ranking,
#             current_ranking,
#             change,
#             search_volume,
#             map_ranking_gbp,
#             location,
#             url,
#             difficulty,
#             search_intent
#         ) VALUES %s
#         """

#         execute_values(cur, insert_query, records)

#         print(f"Inserted {len(records)} keyword rows for file_id={file_id}")

# conn.close()
# print("✅ Data insertion completed successfully")








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
# Folder Path - CSV Files from Preprocessing Folder
# -------------------------
script_dir = Path(__file__).parent.resolve()
input_folder = script_dir / "Data" / "preprocessing"

if not input_folder.exists():
    raise FileNotFoundError(f"Input folder not found: {input_folder}")

csv_files = list(input_folder.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(f"No CSV files found in: {input_folder}")

print(f"Found {len(csv_files)} CSV file(s) in preprocessing folder")

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

month_map = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}

# -------------------------
# DB Connection (UNCHANGED)
# -------------------------
conn = psycopg2.connect(**DB_CONFIG)

with conn:
    with conn.cursor() as cur:

        # -------------------------
        # Iterate CSV Files (PROCESS ONE BY ONE)
        # -------------------------
        for file_path in csv_files:

            print(f"\n{'='*80}")
            print(f"📄 Processing CSV file: {file_path.name}")
            print(f"{'='*80}")

            # -------------------------
            # Load CSV File
            # -------------------------
            df = pd.read_csv(file_path)
            print(f"Loaded {len(df)} rows from {file_path.name}")

            # Check for required columns
            missing = [c for c in expected_cols if c not in df.columns]
            if missing:
                print(f"⚠ Warning: Missing expected columns in {file_path.name}: {missing}")
                print(f"   Available columns: {list(df.columns)}")
                print(f"   Skipping this file...")
                continue

            # -------------------------
            # Parse file name from CSV filename
            # CSV filename format: clientname_monthname_year.csv (e.g., efg_december_2025.csv)
            # -------------------------
            file_stem = file_path.stem.lower()  # efg_december_2025
            parts = file_stem.split("_")

            if len(parts) < 3:
                print(f"⚠ Warning: Invalid filename format: {file_path.name}")
                print(f"   Expected format: clientname_monthname_year.csv")
                print(f"   Skipping this file...")
                continue

            client_name = parts[0]             # efg
            month_name = parts[1].capitalize() # December
            year = int(parts[2])               # 2025
            
            if month_name not in month_map:
                print(f"⚠ Warning: Invalid month name '{month_name}' in {file_path.name}")
                print(f"   Skipping this file...")
                continue
                
            month_id = month_map[month_name]

            # -------------------------
            # 1. UPSERT month (UNCHANGED)
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

            print(f"✓ Created file_id={file_id} for {file_path.name}")

            # -------------------------
            # 3. Prepare keyword rows
            # Handle "None" string values from CSV preprocessing
            # -------------------------
            records = []
            for _, row in df[expected_cols].iterrows():
                # Convert "None" strings to actual None, and handle NaN values
                clean = {}
                for col in expected_cols:
                    value = row[col]
                    # Convert "None" string to None
                    if isinstance(value, str) and value.strip().lower() == "none":
                        clean[col] = None
                    # Handle NaN values
                    elif pd.isna(value):
                        clean[col] = None
                    else:
                        clean[col] = value
                
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
            # 4. Insert keyword data (UNCHANGED)
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

            print(f"✓ Inserted {len(records)} keyword rows for file_id={file_id}")
            print(f"✅ Successfully processed {file_path.name}")

conn.close()
print(f"\n{'='*80}")
print(f"✅ All CSV files processed successfully!")
print(f"   Total files processed: {len(csv_files)}")
print(f"{'='*80}")

