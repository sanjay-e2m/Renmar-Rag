import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -------------------------
# PostgreSQL Configuration
# -------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# -------------------------
# Create Tables SQL
# -------------------------

CREATE_MONTHS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS months (
    month_pk SERIAL PRIMARY KEY,
    month_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    month_id INTEGER NOT NULL CHECK (month_id BETWEEN 1 AND 12),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (month_name, year)
);
"""

CREATE_FILES_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_name TEXT NOT NULL,
    file_name TEXT NOT NULL,
    month_pk INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_month
        FOREIGN KEY (month_pk)
        REFERENCES months(month_pk)
        ON DELETE CASCADE
);
"""

CREATE_KEYWORD_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS "Mastersheet-Keyword_report" (
    id SERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL,

    keyword TEXT NOT NULL,
    initial_ranking INTEGER,
    current_ranking INTEGER,
    change INTEGER,
    search_volume INTEGER,
    map_ranking_gbp INTEGER,
    location TEXT,
    url TEXT,

    difficulty INTEGER,
    search_intent TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_file
        FOREIGN KEY (file_id)
        REFERENCES files(file_id)
        ON DELETE CASCADE
);
"""

# -------------------------
# Create Tables Function
# -------------------------
def create_tables():
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        print("Creating 'months' table...")
        cursor.execute(CREATE_MONTHS_TABLE_QUERY)

        print("Creating 'files' table...")
        cursor.execute(CREATE_FILES_TABLE_QUERY)

        print("Creating 'Mastersheet-Keyword_report' table...")
        cursor.execute(CREATE_KEYWORD_TABLE_QUERY)

        connection.commit()
        print("✅ All tables created successfully")

    except Exception as e:
        print("❌ Error:", e)
        if connection:
            connection.rollback()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    create_tables()
