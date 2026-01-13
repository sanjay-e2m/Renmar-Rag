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
# Create Conversation History Table SQL
# -------------------------
# Table to store conversation history with session tracking
# Each turn (user query + system response) is stored with a session identifier
# New sessions are created when user refreshes or starts a new chat

CREATE_CONVERSATION_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS conversation_history (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Session identifier (new session for each new chat/refresh)
    session_id TEXT NOT NULL,
    
    -- User query and system response
    user_query TEXT NOT NULL,
    system_response TEXT NOT NULL,
    
    -- Timestamp for tracking conversation flow
    created_at TIMESTAMP DEFAULT now()
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_conversation_history_session_id 
    ON conversation_history(session_id);

CREATE INDEX IF NOT EXISTS idx_conversation_history_created_at 
    ON conversation_history(created_at);

CREATE INDEX IF NOT EXISTS idx_conversation_history_session_created 
    ON conversation_history(session_id, created_at);
"""

# -------------------------
# Create Tables Function
# -------------------------
def create_conversation_table():
    """
    Create the conversation_history table for storing chat sessions.
    Each conversation turn is stored with a session_id to group related messages.
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        print("Creating 'conversation_history' table...")
        cursor.execute(CREATE_CONVERSATION_TABLE_QUERY)

        connection.commit()
        print("✅ Conversation history table 'conversation_history' created successfully")
        print("\n📊 Table Structure:")
        print("   - id: SERIAL PRIMARY KEY")
        print("   - session_id: TEXT (for grouping conversation turns)")
        print("   - user_query: TEXT (user's question/input)")
        print("   - system_response: TEXT (system's response)")
        print("   - created_at: TIMESTAMP (default: now())")
        print("\n🔍 Indexes created for fast queries:")
        print("   - session_id (for retrieving all messages in a session)")
        print("   - created_at (for time-based queries)")
        print("   - session_id, created_at (for ordered session retrieval)")
        print("\n💡 Usage:")
        print("   - Assign a new session_id when user starts a new chat/refresh")
        print("   - Store each turn (user_query + system_response) with the same session_id")
        print("   - Query by session_id to retrieve full conversation history")

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
    create_conversation_table()
