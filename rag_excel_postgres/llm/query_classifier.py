"""
Query Classifier and Embedding Generator
Takes user question, generates embedding, and classifies query complexity using LLM.
Also predicts SQL query generation in the backend.
"""

import os
import sys
import importlib.util
from pathlib import Path
from typing import Dict, Optional, Tuple
from dotenv import load_dotenv
from groq import Groq

# Add parent directories to path for imports
current_dir = Path(__file__).parent
rag_excel_dir = current_dir.parent  # rag_excel_postgres directory
postgres_dir = rag_excel_dir / "postgres_create_insert"
sys.path.insert(0, str(postgres_dir))
sys.path.insert(0, str(rag_excel_dir))

# Import generate_embeddings module dynamically
generate_embeddings_path = postgres_dir / "generate_embeddings.py"
spec = importlib.util.spec_from_file_location("generate_embeddings", generate_embeddings_path)
generate_embeddings_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_embeddings_module)
get_embeddings = generate_embeddings_module.get_embeddings

# Load environment variables
load_dotenv()

# -------------------------
# Configuration
# -------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# Database schema information for SQL generation context
SCHEMA_CONTEXT = """
Database Schema:
- months (month_pk, month_name, year, month_id)
- files (file_id, client_name, file_name, month_pk)
- Mastersheet-Keyword_report (id, file_id, keyword, initial_ranking, current_ranking, change, search_volume, map_ranking_gbp, location, url, difficulty, search_intent)
"""


class QueryClassifier:
    """
    Classifies user queries and generates embeddings.
    Uses LLM to determine complexity and predict SQL query generation.
    """
    
    def __init__(self):
        """Initialize the query classifier with Groq LLM and embedding model."""
        if not GROQ_API_KEY:
            raise EnvironmentError("GROQ_API_KEY is missing. Set it in your .env file.")
        
        # Configure Groq
        self.client = Groq(api_key=GROQ_API_KEY)
        self.model = GROQ_MODEL
        
        # Initialize embedding model
        self.embeddings_model = get_embeddings()
        
        print(f"✅ QueryClassifier initialized with {GROQ_MODEL}")
    
    def _build_complexity_classification_prompt(self, question: str) -> str:
        """
        Build prompt for complexity classification.
        
        Args:
            question: User's natural language question
            
        Returns:
            Formatted prompt string
        """
        prompt = f"""You are a SQL query complexity classifier. Analyze the following natural language question and classify its complexity for generating a SQL query.

Database Schema:
{SCHEMA_CONTEXT}

Complexity Levels:
- "easy": Simple SELECT queries from a single table, no JOINs, basic WHERE clauses
- "medium": Queries with JOINs between 2-3 tables, WHERE clauses with conditions, basic aggregations
- "hard": Complex queries with multiple JOINs (3+ tables), aggregations (COUNT, SUM, AVG, GROUP BY), subqueries, or complex WHERE conditions

User Question: {question}

Based on the question above, classify the complexity as one of: easy, medium, or hard.

Respond with ONLY the complexity level (one word: easy, medium, or hard). Do not include any explanation or additional text."""
        
        return prompt
    
    def _build_sql_prediction_prompt(self, question: str) -> str:
        """
        Build prompt for SQL query prediction (internal use, not shown to user).
        
        Args:
            question: User's natural language question
            
        Returns:
            Formatted prompt string
        """
        prompt = f"""You are a SQL query generator. Based on the following natural language question, generate the corresponding SQL query.

Database Schema:
{SCHEMA_CONTEXT}

Important Rules:
- Use proper table names: "Mastersheet-Keyword_report" (with quotes and hyphens)
- Use proper JOIN syntax: JOIN files f ON k.file_id = f.file_id
- Use proper column references with table aliases
- Include proper WHERE clauses based on the question
- Use appropriate aggregations if needed (COUNT, SUM, AVG, etc.)
- End queries with semicolon

User Question: {question}

Generate the SQL query that would answer this question. Return ONLY the SQL query, no explanations."""
        
        return prompt
    
    def classify_complexity(self, question: str) -> str:
        """
        Classify the complexity of a user question using LLM.
        
        Args:
            question: User's natural language question
            
        Returns:
            Complexity level: "easy", "medium", or "hard"
        """
        try:
            prompt = self._build_complexity_classification_prompt(question)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a SQL query complexity classifier. Respond with only one word: easy, medium, or hard."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0
            )
            
            # Extract complexity from response
            complexity = response.choices[0].message.content.strip().lower()
            
            # Validate and normalize
            if complexity not in ["easy", "medium", "hard"]:
                # Try to extract from response if it contains the word
                if "easy" in complexity:
                    complexity = "easy"
                elif "medium" in complexity:
                    complexity = "medium"
                elif "hard" in complexity:
                    complexity = "hard"
                else:
                    # Default to medium if unclear
                    complexity = "medium"
                    print(f"⚠️  Could not determine complexity, defaulting to 'medium'")
            
            return complexity
            
        except Exception as e:
            print(f"❌ Error classifying complexity: {e}")
            # Default to medium on error
            return "medium"
    
    def predict_sql_query(self, question: str) -> str:
        """
        Predict SQL query for the question (internal use, not shown to user).
        
        Args:
            question: User's natural language question
            
        Returns:
            Predicted SQL query string
        """
        try:
            prompt = self._build_sql_prediction_prompt(question)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a SQL query generator. Return ONLY the SQL query, no explanations or markdown."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0
            )
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up SQL query (remove markdown code blocks if present)
            if sql_query.startswith("```sql"):
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            elif sql_query.startswith("```"):
                sql_query = sql_query.replace("```", "").strip()
            
            # Remove any leading/trailing backticks
            sql_query = sql_query.strip('`').strip()
            
            # Remove any prefixes like "Query:", "SQL:", etc.
            for prefix in ['Query:', 'SQL:', 'Answer:', 'Output:', 'Result:']:
                if sql_query.startswith(prefix):
                    sql_query = sql_query[len(prefix):].strip()
            
            return sql_query
            
        except Exception as e:
            print(f"❌ Error predicting SQL query: {e}")
            return ""
    
    def process_query(self, question: str) -> Dict[str, any]:
        """
        Process a user question: generate embedding and classify complexity.
        Also predicts SQL query in the background (not returned).
        
        Args:
            question: User's natural language question
            
        Returns:
            Dictionary with:
            - question_text: Original question
            - embedding: Generated embedding vector
            - complexity: Classified complexity level
            - embedding_dimension: Dimension of the embedding
        """
        if not question or not question.strip():
            raise ValueError("Question cannot be empty")
        
        print(f"\n🔍 Processing query: {question[:60]}...")
        
        # Step 1: Predict SQL query (internal, not shown)
        print("   📝 Predicting SQL query (internal)...")
        predicted_sql = self.predict_sql_query(question)
        
        # Step 2: Generate embedding (using question + predicted SQL)
        print("   🔮 Generating embedding...")
        if predicted_sql:
            embedding = self.embeddings_model.generate_embedding(question, predicted_sql)
        else:
            # Fallback: use question only if SQL prediction fails
            embedding = self.embeddings_model.generate_embedding(question, "")
        
        # Step 3: Classify complexity (parallel operation)
        print("   🎯 Classifying complexity...")
        complexity = self.classify_complexity(question)
        
        result = {
            "question_text": question,
            "embedding": embedding,
            "complexity": complexity,
            "embedding_dimension": len(embedding)
        }
        
        print(f"   ✅ Complexity: {complexity}, Embedding dimension: {len(embedding)}")
        
        return result
    
    def process_query_batch(self, questions: list) -> list:
        """
        Process multiple questions in batch.
        
        Args:
            questions: List of user questions
            
        Returns:
            List of result dictionaries
        """
        results = []
        for question in questions:
            try:
                result = self.process_query(question)
                results.append(result)
            except Exception as e:
                print(f"❌ Error processing question '{question[:50]}...': {e}")
                continue
        
        return results


# -------------------------
# Main (for testing)
# -------------------------
if __name__ == "__main__":
    print("="*60)
    print("Query Classifier and Embedding Generator")
    print("="*60)
    
    # Initialize classifier
    classifier = QueryClassifier()
    
    # Test queries
    test_questions = [
        "What are all the keywords in the database?",
        "Show keywords for ABC in March 2025 with their search volumes.",
        "What is the average search volume for keywords of client EFG across all months?"
    ]
    
    print("\n📋 Testing with sample questions:\n")
    
    for i, question in enumerate(test_questions, 1):
        print(f"\n{'='*60}")
        print(f"Test {i}: {question}")
        print("="*60)
        
        try:
            result = classifier.process_query(question)
            
            print(f"\n✅ Results:")
            print(f"   Question: {result['question_text']}")
            print(f"   Complexity: {result['complexity']}")
            print(f"   Embedding dimension: {result['embedding_dimension']}")
            print(f"   Embedding (first 5 values): {result['embedding'][:5]}")
            
        except Exception as e:
            print(f"❌ Error: {e}")

