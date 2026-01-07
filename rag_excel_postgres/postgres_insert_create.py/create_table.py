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
# Create Master Table SQL
# -------------------------
# Single master table optimized for SQL-RAG
# Denormalized structure: client_name, year, month as filterable columns
# All keyword report metrics stored as columns for easy querying
# No joins needed - perfect for LLM-generated SQL queries

CREATE_MASTER_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS reports_master (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Filterable dimensions (for easy SQL-RAG queries)
    client_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    month TEXT NOT NULL,
    month_id INTEGER CHECK (month_id BETWEEN 1 AND 12),
    
    -- Keyword report metrics (all columns from CSV)
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
    
    -- Metadata for tracking and deduplication
    source_file TEXT NOT NULL,
    row_hash TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for common query patterns
    CONSTRAINT unique_keyword_per_client_month UNIQUE (client_name, year, month, keyword, source_file)
);

-- Create indexes for fast filtering (critical for SQL-RAG performance)
CREATE INDEX IF NOT EXISTS idx_reports_master_client_year_month 
    ON reports_master(client_name, year, month);

CREATE INDEX IF NOT EXISTS idx_reports_master_client 
    ON reports_master(client_name);

CREATE INDEX IF NOT EXISTS idx_reports_master_year_month 
    ON reports_master(year, month);

CREATE INDEX IF NOT EXISTS idx_reports_master_keyword 
    ON reports_master(keyword);

CREATE INDEX IF NOT EXISTS idx_reports_master_source_file 
    ON reports_master(source_file);
"""

# -------------------------
# Create Tables Function
# -------------------------
def create_tables():
    """
    Create the master reports table optimized for SQL-RAG.
    This function only creates the new master table without affecting existing tables.
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        print("Creating 'reports_master' table...")
        cursor.execute(CREATE_MASTER_TABLE_QUERY)

        connection.commit()
        print("✅ Master table 'reports_master' created successfully")
        print("\n📊 Table Structure:")
        print("   - Filterable columns: client_name, year, month")
        print("   - Keyword metrics: keyword, initial_ranking, current_ranking, change, etc.")
        print("   - Metadata: source_file, row_hash, ingested_at")
        print("\n🔍 Indexes created for fast queries:")
        print("   - client_name, year, month")
        print("   - client_name")
        print("   - year, month")
        print("   - keyword")
        print("   - source_file")
        print("\n💡 Perfect for SQL-RAG queries like:")
        print("   SELECT client_name, SUM(search_volume)")
        print("   FROM reports_master")
        print("   WHERE year = 2025 AND month = 'December'")
        print("   GROUP BY client_name;")
        print("\nℹ️  Note: Old tables (months, files, Mastersheet-Keyword_report) remain unchanged.")

    except Exception as e:
        print("❌ Error:", e)
        if connection:
            connection.rollback()
        raise

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
