"""
Migration script to add intent and strategy columns to existing knowledgebase_query table.
This script adds:
- intent: TEXT column for query intent classification
- strategy: JSONB column for strategy metadata
- Indexes for efficient querying
"""

import os
import psycopg2
from dotenv import load_dotenv
import json

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
# Migration SQL Queries
# -------------------------

ADD_INTENT_COLUMN_QUERY = """
ALTER TABLE knowledgebase_query 
ADD COLUMN IF NOT EXISTS intent TEXT 
CHECK (intent IN ('lookup', 'aggregation', 'time_based', 'multi_table_join', 'comparative'));
"""

ADD_STRATEGY_COLUMN_QUERY = """
ALTER TABLE knowledgebase_query 
ADD COLUMN IF NOT EXISTS strategy JSONB;
"""

CREATE_INTENT_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS knowledgebase_query_intent_idx 
ON knowledgebase_query (intent);
"""

CREATE_STRATEGY_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS knowledgebase_query_strategy_idx 
ON knowledgebase_query USING GIN (strategy);
"""

# -------------------------
# Migration Functions
# -------------------------

def check_column_exists(cursor, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_name = %s 
            AND column_name = %s
        );
    """, (table_name, column_name))
    return cursor.fetchone()[0]

def migrate_add_intent_strategy():
    """
    Add intent and strategy columns to existing knowledgebase_query table.
    """
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        print("🔄 Starting migration: Adding intent and strategy columns...")
        print("="*60)
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'knowledgebase_query'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("❌ Error: knowledgebase_query table does not exist!")
            print("   Please run create_knowledge_base.py first to create the table.")
            return False
        
        # Check if columns already exist
        intent_exists = check_column_exists(cursor, "knowledgebase_query", "intent")
        strategy_exists = check_column_exists(cursor, "knowledgebase_query", "strategy")
        
        if intent_exists and strategy_exists:
            print("✅ Intent and strategy columns already exist. Migration not needed.")
            return True
        
        # Add intent column
        if not intent_exists:
            print("\n📝 Adding 'intent' column...")
            cursor.execute(ADD_INTENT_COLUMN_QUERY)
            print("   ✅ 'intent' column added successfully")
        else:
            print("\n⚠️  'intent' column already exists, skipping...")
        
        # Add strategy column
        if not strategy_exists:
            print("\n📝 Adding 'strategy' column...")
            cursor.execute(ADD_STRATEGY_COLUMN_QUERY)
            print("   ✅ 'strategy' column added successfully")
        else:
            print("\n⚠️  'strategy' column already exists, skipping...")
        
        # Create indexes
        print("\n📊 Creating indexes...")
        print("   - Creating intent index...")
        cursor.execute(CREATE_INTENT_INDEX_QUERY)
        print("      ✅ Intent index created")
        
        print("   - Creating strategy GIN index...")
        cursor.execute(CREATE_STRATEGY_INDEX_QUERY)
        print("      ✅ Strategy index created")
        
        # Commit changes
        connection.commit()
        
        print("\n" + "="*60)
        print("✅ Migration completed successfully!")
        print("="*60)
        
        # Verify migration
        print("\n📋 Verifying migration...")
        verify_migration(cursor)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during migration: {e}")
        if connection:
            connection.rollback()
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def verify_migration(cursor):
    """Verify that the migration was successful."""
    try:
        # Check columns
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'knowledgebase_query'
            AND column_name IN ('intent', 'strategy')
            ORDER BY column_name;
        """)
        columns = cursor.fetchall()
        
        if columns:
            print("\n✅ Migration verification:")
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"   • {col_name}: {data_type} ({nullable})")
        else:
            print("⚠️  Warning: Could not verify columns")
        
        # Check indexes
        cursor.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'knowledgebase_query'
            AND indexname IN ('knowledgebase_query_intent_idx', 'knowledgebase_query_strategy_idx')
            ORDER BY indexname;
        """)
        indexes = cursor.fetchall()
        
        if indexes:
            print("\n✅ Indexes created:")
            for (index_name,) in indexes:
                print(f"   • {index_name}")
        
        # Count records with intent/strategy
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(intent) as with_intent,
                COUNT(strategy) as with_strategy
            FROM knowledgebase_query;
        """)
        counts = cursor.fetchone()
        total, with_intent, with_strategy = counts
        
        print(f"\n📊 Current data status:")
        print(f"   • Total records: {total}")
        print(f"   • Records with intent: {with_intent}")
        print(f"   • Records with strategy: {with_strategy}")
        
        if total > 0 and (with_intent == 0 or with_strategy == 0):
            print(f"\n⚠️  Note: Existing records don't have intent/strategy values yet.")
            print(f"   You may want to update existing records using the query classifier.")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not verify migration: {e}")


def update_existing_records():
    """
    Optional: Update existing records with intent and strategy using query classifier.
    This is a separate function that can be called if you want to backfill existing data.
    """
    print("\n" + "="*60)
    print("🔄 Updating existing records with intent and strategy...")
    print("="*60)
    print("\n⚠️  This function requires the query classifier to be available.")
    print("   To update existing records, use the insert script with --update-existing flag")
    print("   or manually update records using the query classifier.\n")


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Migrate knowledge base table to add intent and strategy columns"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify migration status, don't perform migration"
    )
    
    args = parser.parse_args()
    
    print("🧠 Knowledge Base Migration: Add Intent & Strategy Columns\n")
    
    if args.verify_only:
        # Just verify, don't migrate
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            verify_migration(cursor)
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        # Perform migration
        success = migrate_add_intent_strategy()
        
        if success:
            print("\n💡 Next steps:")
            print("   1. Update insert_knowledge_base.py to include intent and strategy")
            print("   2. Re-run data insertion to populate intent and strategy fields")
            print("   3. Update RAG pipeline to use intent for filtering/searching")
