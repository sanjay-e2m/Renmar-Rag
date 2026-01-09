"""
Insert synthetic data into the knowledge base table.
Generates synthetic question-SQL pairs, creates embeddings, and inserts into knowledgebase_query table.
"""

import os
import psycopg2
import json
from dotenv import load_dotenv
from typing import List, Dict, Optional
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Add llm directory to path for embeddings and query classifier
llm_dir = Path(__file__).parent.parent / "llm"
sys.path.insert(0, str(llm_dir))

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


def insert_knowledge_base_data(data_list: List[Dict[str, str]], embeddings: List[List[float]], 
                                intents: Optional[List[str]] = None, 
                                strategies: Optional[List[Dict]] = None):
    """
    Insert data into the knowledgebase_query table.
    
    Args:
        data_list: List of dictionaries with 'question_text', 'sql_query', 'complexity'
        embeddings: List of embedding vectors
        intents: Optional list of intent classifications (lookup, aggregation, etc.)
        strategies: Optional list of strategy metadata dictionaries
    """
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Prepare insert query - check if intent/strategy columns exist
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'knowledgebase_query' 
            AND column_name IN ('intent', 'strategy');
        """)
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        has_intent = 'intent' in existing_columns
        has_strategy = 'strategy' in existing_columns
        
        # Build insert query based on available columns
        if has_intent and has_strategy:
            insert_query = """
                INSERT INTO knowledgebase_query (question_text, sql_query, complexity, intent, strategy, embedding)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::vector)
                RETURNING id;
            """
        elif has_intent:
            insert_query = """
                INSERT INTO knowledgebase_query (question_text, sql_query, complexity, intent, embedding)
                VALUES (%s, %s, %s, %s, %s::vector)
                RETURNING id;
            """
        elif has_strategy:
            insert_query = """
                INSERT INTO knowledgebase_query (question_text, sql_query, complexity, strategy, embedding)
                VALUES (%s, %s, %s, %s::jsonb, %s::vector)
                RETURNING id;
            """
        else:
            insert_query = """
                INSERT INTO knowledgebase_query (question_text, sql_query, complexity, embedding)
                VALUES (%s, %s, %s, %s::vector)
                RETURNING id;
            """
        
        inserted_count = 0
        
        print(f"\n📥 Inserting {len(data_list)} records into knowledgebase_query...")
        if has_intent:
            print(f"   ✓ Intent column available")
        if has_strategy:
            print(f"   ✓ Strategy column available")
        
        for i, (data, embedding) in enumerate(zip(data_list, embeddings), 1):
            try:
                # Format embedding as string for pgvector: '[1,2,3,...]'
                embedding_str = '[' + ','.join(map(str, embedding)) + ']'
                
                # Prepare values based on available columns
                # Use intent/strategy from data dictionary if available, otherwise use separate lists
                if has_intent and has_strategy:
                    intent = data.get('intent') or (intents[i-1] if intents and i <= len(intents) else None)
                    strategy = data.get('strategy') or (strategies[i-1] if strategies and i <= len(strategies) else None)
                    strategy_json = json.dumps(strategy) if strategy else None
                    
                    cursor.execute(
                        insert_query,
                        (
                            data['question_text'],
                            data['sql_query'],
                            data['complexity'],
                            intent,
                            strategy_json,
                            embedding_str
                        )
                    )
                elif has_intent:
                    intent = data.get('intent') or (intents[i-1] if intents and i <= len(intents) else None)
                    cursor.execute(
                        insert_query,
                        (
                            data['question_text'],
                            data['sql_query'],
                            data['complexity'],
                            intent,
                            embedding_str
                        )
                    )
                elif has_strategy:
                    strategy = data.get('strategy') or (strategies[i-1] if strategies and i <= len(strategies) else None)
                    strategy_json = json.dumps(strategy) if strategy else None
                    cursor.execute(
                        insert_query,
                        (
                            data['question_text'],
                            data['sql_query'],
                            data['complexity'],
                            strategy_json,
                            embedding_str
                        )
                    )
                else:
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
        
        # Count by intent (if column exists)
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'knowledgebase_query' 
            AND column_name = 'intent';
        """)
        has_intent = cursor.fetchone() is not None
        
        intent_counts = []
        if has_intent:
            cursor.execute("""
                SELECT intent, COUNT(*) 
                FROM knowledgebase_query 
                WHERE intent IS NOT NULL
                GROUP BY intent 
                ORDER BY intent;
            """)
            intent_counts = cursor.fetchall()
        
        print("\n" + "="*60)
        print("📊 Knowledge Base Statistics")
        print("="*60)
        print(f"Total records: {total_count}")
        print("\nRecords by complexity:")
        for complexity, count in complexity_counts:
            print(f"   • {complexity}: {count}")
        
        if has_intent and intent_counts:
            print("\nRecords by intent:")
            for intent, count in intent_counts:
                print(f"   • {intent}: {count}")
        
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
        "--no-clear",
        action="store_true",
        help="Skip clearing existing data before inserting (default: clears existing data)"
    )
    
    args = parser.parse_args()
    
    print("🧠 Knowledge Base Data Insertion\n")
    print("="*60)
    
    # Clear existing data by default (unless --no-clear is specified)
    if not args.no_clear:
        print("🗑️  Clearing existing data...")
        clear_knowledge_base()
    else:
        print("⚠️  Skipping data clearing (--no-clear flag specified)")
    
    # Step 1: Generate synthetic data
    print("\n📝 Step 1: Generating synthetic data...")
    synthetic_data = generate_synthetic_data(num_examples=args.num_examples)
    print(f"✅ Generated {len(synthetic_data)} examples")
    
    # Step 2: Extract intent and strategy from synthetic data (if already included)
    print("\n🤖 Step 2: Extracting intent and strategy...")
    intents = []
    strategies = []
    
    # Check if intent and strategy are already in the data
    has_intent_in_data = any('intent' in data for data in synthetic_data)
    has_strategy_in_data = any('strategy' in data for data in synthetic_data)
    
    if has_intent_in_data and has_strategy_in_data:
        print("   ✓ Intent and strategy already included in synthetic data")
        for data in synthetic_data:
            intents.append(data.get('intent'))
            strategies.append(data.get('strategy'))
        print(f"   ✅ Extracted intent and strategy from {len(synthetic_data)} examples")
    elif has_intent_in_data:
        print("   ✓ Intent already included in synthetic data")
        for data in synthetic_data:
            intents.append(data.get('intent'))
        strategies = None
        print(f"   ✅ Extracted intent from {len(synthetic_data)} examples")
    else:
        # Try to classify using query classifier if not in data
        print("   ⚠️  Intent/strategy not found in synthetic data, trying query classifier...")
        try:
            # Try to import query classifier
            query_classifier_path = llm_dir / "query_classifier.py"
            if query_classifier_path.exists():
                import importlib.util
                spec = importlib.util.spec_from_file_location("query_classifier", query_classifier_path)
                query_classifier_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(query_classifier_module)
                QueryClassifier = query_classifier_module.QueryClassifier
                
                classifier = QueryClassifier()
                
                for i, data in enumerate(synthetic_data, 1):
                    question = data['question_text']
                    try:
                        result = classifier.process_query(question)
                        intents.append(result.get('intent'))
                        strategies.append(result.get('strategy'))
                        if i % 5 == 0:
                            print(f"   Classified {i}/{len(synthetic_data)} questions...")
                    except Exception as e:
                        print(f"   ⚠️  Warning: Could not classify question {i}: {e}")
                        intents.append(None)
                        strategies.append(None)
                
                print(f"   ✅ Classified {len([x for x in intents if x])} questions with intent")
            else:
                print("   ⚠️  Query classifier not found, skipping intent/strategy classification")
                intents = None
                strategies = None
        except Exception as e:
            print(f"   ⚠️  Warning: Could not use query classifier: {e}")
            print("   Continuing without intent/strategy classification...")
            intents = None
            strategies = None
    
    # Step 3: Generate embeddings
    print("\n🔮 Step 3: Generating embeddings...")
    embeddings_model = get_embeddings()
    embeddings = embeddings_model.generate_embeddings_batch(synthetic_data)
    print(f"✅ Generated {len(embeddings)} embeddings (dimension: {len(embeddings[0])})")
    
    # Step 4: Insert into database
    print("\n💾 Step 4: Inserting into database...")
    inserted_count = insert_knowledge_base_data(synthetic_data, embeddings, intents, strategies)
    
    # Step 4: Verify
    if inserted_count > 0:
        verify_inserted_data()
    
    print("\n" + "="*60)
    print("✅ Process completed!")

