"""
Insert synthetic data into the knowledge base table.
Generates synthetic question-SQL pairs, creates embeddings, and inserts into knowledgebase_query table.
"""

import os
import psycopg2
from dotenv import load_dotenv
from typing import List, Dict
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from generate_synthetic_data import generate_synthetic_data
from generate_embeddings import get_embeddings

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


def insert_knowledge_base_data(data_list: List[Dict[str, str]], embeddings: List[List[float]]):
    """
    Insert data into the knowledgebase_query table.
    
    Args:
        data_list: List of dictionaries with 'question_text', 'sql_query', 'complexity'
        embeddings: List of embedding vectors
    """
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Prepare insert query
        insert_query = """
            INSERT INTO knowledgebase_query (question_text, sql_query, complexity, embedding)
            VALUES (%s, %s, %s, %s::vector)
            RETURNING id;
        """
        
        inserted_count = 0
        
        print(f"\n📥 Inserting {len(data_list)} records into knowledgebase_query...")
        
        for i, (data, embedding) in enumerate(zip(data_list, embeddings), 1):
            try:
                # Format embedding as string for pgvector: '[1,2,3,...]'
                embedding_str = '[' + ','.join(map(str, embedding)) + ']'
                
                cursor.execute(
                    insert_query,
                    (
                        data['question_text'],
                        data['sql_query'],
                        data['complexity'],
                        embedding_str
                    )
                )
                inserted_count += 1
                
                if i % 5 == 0:
                    print(f"   Inserted {i}/{len(data_list)} records...")
                    
            except Exception as e:
                print(f"   ❌ Error inserting record {i}: {e}")
                continue
        
        connection.commit()
        print(f"\n✅ Successfully inserted {inserted_count}/{len(data_list)} records")
        
        return inserted_count
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if connection:
            connection.rollback()
        return 0
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def verify_inserted_data():
    """
    Verify the inserted data in the knowledge base.
    """
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Count total records
        cursor.execute("SELECT COUNT(*) FROM knowledgebase_query;")
        total_count = cursor.fetchone()[0]
        
        # Count by complexity
        cursor.execute("""
            SELECT complexity, COUNT(*) 
            FROM knowledgebase_query 
            GROUP BY complexity 
            ORDER BY complexity;
        """)
        complexity_counts = cursor.fetchall()
        
        print("\n" + "="*60)
        print("📊 Knowledge Base Statistics")
        print("="*60)
        print(f"Total records: {total_count}")
        print("\nRecords by complexity:")
        for complexity, count in complexity_counts:
            print(f"   • {complexity}: {count}")
        
        # Show sample records
        cursor.execute("""
            SELECT id, question_text, complexity 
            FROM knowledgebase_query 
            ORDER BY id 
            LIMIT 5;
        """)
        samples = cursor.fetchall()
        
        print("\n📋 Sample Records:")
        for record_id, question, complexity in samples:
            print(f"   [{record_id}] ({complexity}) {question[:60]}...")
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def clear_knowledge_base():
    """
    Clear all data from the knowledge base table (for testing purposes).
    """
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        cursor.execute("DELETE FROM knowledgebase_query;")
        connection.commit()
        
        print("✅ Knowledge base cleared")
        
    except Exception as e:
        print(f"❌ Error clearing knowledge base: {e}")
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
    import argparse
    
    parser = argparse.ArgumentParser(description="Insert synthetic data into knowledge base")
    parser.add_argument(
        "--num-examples",
        type=int,
        default=20,
        help="Number of synthetic examples to generate (default: 20)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing data before inserting"
    )
    
    args = parser.parse_args()
    
    print("🧠 Knowledge Base Data Insertion\n")
    print("="*60)
    
    # Clear if requested
    if args.clear:
        print("🗑️  Clearing existing data...")
        clear_knowledge_base()
    
    # Step 1: Generate synthetic data
    print("\n📝 Step 1: Generating synthetic data...")
    synthetic_data = generate_synthetic_data(num_examples=args.num_examples)
    print(f"✅ Generated {len(synthetic_data)} examples")
    
    # Step 2: Generate embeddings
    print("\n🔮 Step 2: Generating embeddings...")
    embeddings_model = get_embeddings()
    embeddings = embeddings_model.generate_embeddings_batch(synthetic_data)
    print(f"✅ Generated {len(embeddings)} embeddings (dimension: {len(embeddings[0])})")
    
    # Step 3: Insert into database
    print("\n💾 Step 3: Inserting into database...")
    inserted_count = insert_knowledge_base_data(synthetic_data, embeddings)
    
    # Step 4: Verify
    if inserted_count > 0:
        verify_inserted_data()
    
    print("\n" + "="*60)
    print("✅ Process completed!")

