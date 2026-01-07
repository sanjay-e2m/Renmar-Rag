"""
RAG Pipeline for Text-to-SQL
Takes user query, classifies it, searches knowledge base, and returns relevant examples.
"""

import os
import sys
import importlib.util
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv
import psycopg2

# Add parent directories to path for imports
current_dir = Path(__file__).parent
rag_excel_dir = current_dir.parent
postgres_dir = rag_excel_dir / "postgres_create_insert"
sys.path.insert(0, str(postgres_dir))
sys.path.insert(0, str(rag_excel_dir))

# Import query_classifier dynamically
query_classifier_path = current_dir / "query_classifier.py"
spec = importlib.util.spec_from_file_location("query_classifier", query_classifier_path)
query_classifier_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_classifier_module)
QueryClassifier = query_classifier_module.QueryClassifier

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

# Default parameters
DEFAULT_TOP_K = 5
DEFAULT_SIMILARITY_THRESHOLD = 0.7


class RAGPipeline:
    """
    RAG Pipeline that retrieves relevant SQL examples from knowledge base
    based on user query similarity and complexity matching.
    """
    
    def __init__(self, top_k: int = DEFAULT_TOP_K, similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD):
        """
        Initialize the RAG Pipeline.
        
        Args:
            top_k: Number of top results to return
            similarity_threshold: Minimum similarity score (0-1) for results
        """
        # Initialize query classifier
        self.classifier = QueryClassifier()
        self.top_k = top_k
        self.similarity_threshold = similarity_threshold
        
        print(f"✅ RAG Pipeline initialized (top_k={top_k}, threshold={similarity_threshold})")
    
    def search_knowledge_base(
        self, 
        embedding: List[float], 
        complexity: str, 
        top_k: Optional[int] = None
    ) -> List[Dict[str, any]]:
        """
        Search knowledge base using vector similarity and complexity filter.
        
        Args:
            embedding: Query embedding vector
            complexity: Complexity level to filter by
            top_k: Number of results to return (defaults to self.top_k)
            
        Returns:
            List of dictionaries with question_text and sql_query
        """
        if top_k is None:
            top_k = self.top_k
        
        connection = None
        cursor = None
        
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            # Format embedding as string for pgvector
            embedding_str = '[' + ','.join(map(str, embedding)) + ']'
            
            # Query using cosine similarity (1 - cosine_distance = similarity)
            # Filter by complexity and order by similarity
            query = """
                SELECT 
                    id,
                    question_text,
                    sql_query,
                    complexity,
                    1 - (embedding <=> %s::vector) as similarity
                FROM knowledgebase_query
                WHERE complexity = %s
                  AND embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT %s;
            """
            
            cursor.execute(query, (embedding_str, complexity, embedding_str, top_k))
            results = cursor.fetchall()
            
            # Format results
            formatted_results = []
            for row in results:
                kb_id, question_text, sql_query, comp, similarity = row
                
                # Filter by similarity threshold
                if similarity >= self.similarity_threshold:
                    formatted_results.append({
                        "id": kb_id,
                        "question_text": question_text,
                        "sql_query": sql_query,
                        "complexity": comp,
                        "similarity": round(similarity, 4)
                    })
            
            return formatted_results
            
        except Exception as e:
            print(f"❌ Error searching knowledge base: {e}")
            return []
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def process_query(self, user_question: str, top_k: Optional[int] = None) -> Dict[str, any]:
        """
        Complete pipeline: classify query, search knowledge base, return results.
        
        Args:
            user_question: User's natural language question
            top_k: Number of results to return (defaults to self.top_k)
            
        Returns:
            Dictionary with:
            - user_question: Original question
            - complexity: Classified complexity
            - embedding_dimension: Dimension of embedding
            - results: List of retrieved examples (question_text, sql_query, similarity)
            - total_results: Number of results found
        """
        if not user_question or not user_question.strip():
            raise ValueError("Question cannot be empty")
        
        print(f"\n{'='*60}")
        print(f"🔍 Processing Query: {user_question}")
        print("="*60)
        
        # Step 1: Classify query and generate embedding
        print("\n📊 Step 1: Classifying query and generating embedding...")
        classification_result = self.classifier.process_query(user_question)
        
        embedding = classification_result['embedding']
        complexity = classification_result['complexity']
        
        print(f"   ✅ Complexity: {complexity}")
        print(f"   ✅ Embedding dimension: {classification_result['embedding_dimension']}")
        
        # Step 2: Search knowledge base
        print(f"\n🔎 Step 2: Searching knowledge base (complexity={complexity}, top_k={top_k or self.top_k})...")
        results = self.search_knowledge_base(embedding, complexity, top_k)
        
        print(f"   ✅ Found {len(results)} relevant examples")
        
        # Step 3: Format and return results
        pipeline_result = {
            "user_question": user_question,
            "complexity": complexity,
            "embedding_dimension": classification_result['embedding_dimension'],
            "results": results,
            "total_results": len(results)
        }
        
        return pipeline_result
    
    def process_query_with_fallback(
        self, 
        user_question: str, 
        top_k: Optional[int] = None,
        allow_complexity_fallback: bool = True
    ) -> Dict[str, any]:
        """
        Process query with fallback to other complexity levels if no results found.
        
        Args:
            user_question: User's natural language question
            top_k: Number of results to return
            allow_complexity_fallback: If True, try other complexity levels if no results
            
        Returns:
            Dictionary with results (same format as process_query)
        """
        # First try with exact complexity match
        result = self.process_query(user_question, top_k)
        
        # If no results and fallback is enabled, try other complexity levels
        if result['total_results'] == 0 and allow_complexity_fallback:
            print(f"\n⚠️  No results found for complexity '{result['complexity']}'. Trying fallback...")
            
            classification_result = self.classifier.process_query(user_question)
            embedding = classification_result['embedding']
            original_complexity = classification_result['complexity']
            
            # Try other complexity levels
            complexity_levels = ["easy", "medium", "hard"]
            complexity_levels.remove(original_complexity)
            
            all_results = []
            for comp in complexity_levels:
                print(f"   Trying complexity: {comp}...")
                fallback_results = self.search_knowledge_base(embedding, comp, top_k)
                all_results.extend(fallback_results)
                
                if fallback_results:
                    print(f"   ✅ Found {len(fallback_results)} results with complexity '{comp}'")
            
            # Sort by similarity and take top_k
            all_results.sort(key=lambda x: x['similarity'], reverse=True)
            all_results = all_results[:top_k or self.top_k]
            
            result['results'] = all_results
            result['total_results'] = len(all_results)
            result['fallback_used'] = True
            result['original_complexity'] = original_complexity
        
        return result


# -------------------------
# Main (for testing)
# -------------------------
if __name__ == "__main__":
    print("="*60)
    print("RAG Pipeline for Text-to-SQL")
    print("="*60)
    
    # Initialize pipeline
    pipeline = RAGPipeline(top_k=5, similarity_threshold=0.6)
    
    # Test queries
    test_questions = [
        # "What are all the keywords in the database?",
        # "Show keywords for ABC in March 2025 with their search volumes.",
        "What is the average search volume for keywords of client EFG across all months?"
    ]
    
    print("\n📋 Testing RAG Pipeline:\n")
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n{'='*60}")
        print(f"Test {i}: {question}")
        print("="*60)
        
        try:
            result = pipeline.process_query_with_fallback(question, top_k=5)
            
            print(f"\n✅ Pipeline Results:")
            print(f"   User Question: {result['user_question']}")
            print(f"   Complexity: {result['complexity']}")
            print(f"   Total Results: {result['total_results']}")
            
            if result['results']:
                print(f"\n📚 Retrieved Examples:")
                for j, example in enumerate(result['results'], 1):
                    print(f"\n   Example {j} (similarity: {example['similarity']}):")
                    print(f"   Question: {example['question_text']}")
                    print(f"   SQL: {example['sql_query'][:80]}...")
            else:
                print("\n   ⚠️  No relevant examples found in knowledge base")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

