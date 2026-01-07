import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -------------------------
# Embedding Configuration
# -------------------------
EMBEDDING_DIMENSION = os.getenv("EMBEDDING_DIMENSION", "768")

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
# Enable pgvector Extension
# -------------------------
ENABLE_PGVECTOR_EXTENSION_QUERY = """
CREATE EXTENSION IF NOT EXISTS vector;
"""

# -------------------------
# Create Knowledge Base Table SQL
# -------------------------

CREATE_KNOWLEDGE_BASE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS knowledgebase_query (
    id BIGSERIAL PRIMARY KEY,
    question_text TEXT NOT NULL,
    sql_query TEXT NOT NULL,
    complexity TEXT CHECK (complexity IN ('easy', 'medium', 'hard') OR complexity ~ '^[0-9]+(\.[0-9]+)?$'),
    embedding VECTOR({EMBEDDING_DIMENSION}),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -------------------------
# Create Indexes for Efficient Querying
# -------------------------

CREATE_EMBEDDING_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS knowledgebase_query_embedding_idx 
ON knowledgebase_query 
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
"""

CREATE_COMPLEXITY_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS knowledgebase_query_complexity_idx 
ON knowledgebase_query (complexity);
"""

# -------------------------
# Create Knowledge Base Function
# -------------------------
def create_knowledge_base():
    """
    Create the knowledge base table for storing RAG examples with pgvector support.
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Enable pgvector extension
        print("Enabling pgvector extension...")
        cursor.execute(ENABLE_PGVECTOR_EXTENSION_QUERY)

        # Create knowledge base table
        print("Creating 'knowledgebase_query' table...")
        cursor.execute(CREATE_KNOWLEDGE_BASE_TABLE_QUERY)

        # Create indexes for efficient querying
        print("Creating indexes...")
        print("  - Embedding vector index (for similarity search)...")
        cursor.execute(CREATE_EMBEDDING_INDEX_QUERY)
        
        print("  - Complexity index...")
        cursor.execute(CREATE_COMPLEXITY_INDEX_QUERY)

        connection.commit()
        print("✅ Knowledge base table and indexes created successfully")
        print("\n📋 Table Structure:")
        print("   • id: BIGSERIAL PRIMARY KEY")
        print("   • question_text: TEXT NOT NULL")
        print("   • sql_query: TEXT NOT NULL")
        print("   • complexity: TEXT (easy/medium/hard or numeric)")
        print(f"   • embedding: VECTOR({EMBEDDING_DIMENSION})")
        print("   • created_at: TIMESTAMP")
        print("   • updated_at: TIMESTAMP")

    except Exception as e:
        print("❌ Error:", e)
        if connection:
            connection.rollback()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def verify_knowledge_base():
    """
    Verify that the knowledge base table was created correctly.
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'knowledgebase_query'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            # Get column information
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'knowledgebase_query'
                ORDER BY ordinal_position;
            """)
            columns = cursor.fetchall()

            print("\n✅ Knowledge base table exists!")
            print("\n📊 Table Columns:")
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"   • {col_name}: {data_type} ({nullable})")

            # Check if pgvector extension is enabled
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_extension 
                    WHERE extname = 'vector'
                );
            """)
            extension_exists = cursor.fetchone()[0]

            if extension_exists:
                print("\n✅ pgvector extension is enabled")
            else:
                print("\n⚠️  pgvector extension not found (may need to be installed)")

        else:
            print("❌ Knowledge base table does not exist")

    except Exception as e:
        print(f"❌ Error verifying knowledge base: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    print("🧠 Creating Knowledge Base for Text-to-SQL RAG System\n")
    create_knowledge_base()
    
    print("\n" + "="*60)
    print("Verifying knowledge base creation...")
    print("="*60)
    verify_knowledge_base()

