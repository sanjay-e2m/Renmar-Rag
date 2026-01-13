"""
SQL Query Generator using LLM
Generates SQL queries from natural language questions for PostgreSQL reports_master table
"""

import os
import sys
import re
import time
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from groq import Groq

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import local modules
from rag_excel_postgres.llm.schema import get_database_schema
from rag_excel_postgres.postgres_insert_create.conversation_manager import ConversationManager

# Load environment variables
load_dotenv()

# Get configuration from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not GROQ_API_KEY:
    raise Exception("❌ Missing GROQ_API_KEY in environment variables (set GROQ_API_KEY in .env file)")


class QueryGenerator:
    """
    Generates SQL queries from natural language questions using LLM.
    Optimized for reports_master table schema.
    """
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None
    ):
        """
        Initialize QueryGenerator.
        
        Parameters:
        -----------
        groq_api_key : Optional[str], optional
            Groq API key. If None, uses environment variable.
        groq_model : Optional[str], optional
            Groq model name. If None, uses environment variable.
        """
        api_key = groq_api_key or GROQ_API_KEY
        model = groq_model or GROQ_MODEL
        
        if not api_key:
            raise Exception("❌ Missing GROQ_API_KEY")
        
        try:
            self.client = Groq(api_key=api_key)
            self.model = model
            print(f"✓ Groq model configured: {self.model}")
        except Exception as e:
            print(f"⚠ Warning: Could not configure Groq model: {e}")
            self.client = None
        
        # Initialize conversation manager
        try:
            self.conversation_manager = ConversationManager()
        except Exception as e:
            print(f"⚠ Warning: Could not initialize conversation manager: {e}")
            self.conversation_manager = None
    
    def get_schema_context(self) -> str:
        """
        Get the database schema context for reports_master table.
        Uses the comprehensive schema file for detailed descriptions.
        
        Returns:
        --------
        str
            Formatted schema description with detailed column information
        """
        return get_database_schema()
    
    def get_query_examples(self) -> str:
        """
        Get concise SQL query examples with natural language questions.
        Optimized for token efficiency.
        
        Returns:
        --------
        str
            Formatted examples
        """
        examples = """

SQL QUERY EXAMPLES WITH NATURAL LANGUAGE QUESTIONS:

═══════════════════════════════════════════════════════════════════════════════
1. TOP N / HIGHEST / LARGEST VALUES
═══════════════════════════════════════════════════════════════════════════════

Question: "Show me top 5 keywords with highest search volume for client efg in December 2025"
SQL: SELECT keyword, search_volume, current_ranking, location
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December'
     ORDER BY search_volume DESC
     LIMIT 5;

Question: "What are the top 10 keywords by search volume for abc in March 2025?"
SQL: SELECT keyword, search_volume, change, search_intent
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March'
     ORDER BY search_volume DESC
     LIMIT 10;

═══════════════════════════════════════════════════════════════════════════════
2. BOTTOM N / LOWEST / SMALLEST VALUES
═══════════════════════════════════════════════════════════════════════════════

Question: "Show me bottom 5 keywords with lowest search volume for xyz in May 2024"
SQL: SELECT keyword, search_volume, current_ranking
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May'
     ORDER BY search_volume ASC
     LIMIT 5;


═══════════════════════════════════════════════════════════════════════════════
3. FILTERING WITH CONDITIONS
═══════════════════════════════════════════════════════════════════════════════

Question: "Show all keywords with search volume greater than 1000 for efg in December 2025"
SQL: SELECT keyword, search_volume, current_ranking, change
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December' 
       AND search_volume > 1000
     ORDER BY search_volume DESC;

Question: "Keywords where ranking improved (change is positive) for abc in March 2025"
SQL: SELECT keyword, initial_ranking, current_ranking, change, search_volume
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March' 
       AND change > 0
     ORDER BY change DESC;


═══════════════════════════════════════════════════════════════════════════════
4. COUNTING AND AGGREGATIONS
═══════════════════════════════════════════════════════════════════════════════

Question: "How many keywords are tracked for efg in December 2025?"
SQL: SELECT COUNT(*) as total_keywords
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December';

Question: "What is the total search volume for abc in March 2025?"
SQL: SELECT SUM(search_volume) as total_search_volume
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March';

Question: "What is the average search volume for xyz in May 2024?"
SQL: SELECT AVG(search_volume) as avg_search_volume
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May';


═══════════════════════════════════════════════════════════════════════════════
5. COMPARISONS AND RANKINGS
═══════════════════════════════════════════════════════════════════════════════

Question: "Which keyword has the highest search volume for efg in December 2025?"
SQL: SELECT keyword, search_volume, current_ranking, change
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December'
     ORDER BY search_volume DESC
     LIMIT 1;

Question: "Show keywords ranked in top 10 (current_ranking <= 10) for abc in March 2025"
SQL: SELECT keyword, current_ranking, search_volume, change
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March' 
       AND current_ranking <= 10
     ORDER BY current_ranking ASC;

═══════════════════════════════════════════════════════════════════════════════
6. GROUP BY AND AGGREGATIONS
═══════════════════════════════════════════════════════════════════════════════

Question: "Show total search volume by client for year 2025"
SQL: SELECT client_name, SUM(search_volume) as total_search_volume, COUNT(*) as keyword_count
     FROM reports_master
     WHERE year = 2025
     GROUP BY client_name
     ORDER BY total_search_volume DESC;

Question: "Average search volume by month for client efg in 2025"
SQL: SELECT month, AVG(search_volume) as avg_search_volume, COUNT(*) as keyword_count
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025
     GROUP BY month, month_id
     ORDER BY month_id;


═══════════════════════════════════════════════════════════════════════════════
7. MULTIPLE CONDITIONS (AND/OR)
═══════════════════════════════════════════════════════════════════════════════

Question: "Keywords with search volume > 500 AND ranking < 20 for efg in December 2025"
SQL: SELECT keyword, search_volume, current_ranking, change
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December' 
       AND search_volume > 500 AND current_ranking < 20
     ORDER BY search_volume DESC;

Question: "Keywords with search intent 'Commercial' OR 'Transactional' for abc in March 2025"
SQL: SELECT keyword, search_intent, search_volume, current_ranking
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March' 
       AND (search_intent = 'Commercial' OR search_intent = 'Transactional')
     ORDER BY search_volume DESC;

Question: "Keywords with search volume between 100 and 1000 for xyz in May 2024"
SQL: SELECT keyword, search_volume, current_ranking
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May' 
       AND search_volume BETWEEN 100 AND 1000
     ORDER BY search_volume DESC;


═══════════════════════════════════════════════════════════════════════════════
8. MONTH-TO-MONTH COMPARISONS
═══════════════════════════════════════════════════════════════════════════════

Question: "Compare total search volume for efg between December 2025 and November 2025"
SQL: SELECT month, SUM(search_volume) as total_volume, COUNT(*) as keyword_count
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 
       AND month IN ('November', 'December')
     GROUP BY month, month_id
     ORDER BY month_id;

Question: "Show keywords that exist in both March and April 2025 for abc"
SQL: SELECT keyword, 
       MAX(CASE WHEN month = 'March' THEN search_volume END) as march_volume,
       MAX(CASE WHEN month = 'April' THEN search_volume END) as april_volume
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 
       AND month IN ('March', 'April')
     GROUP BY keyword
     HAVING COUNT(DISTINCT month) = 2;

═══════════════════════════════════════════════════════════════════════════════
9. YEAR-OVER-YEAR COMPARISONS
═══════════════════════════════════════════════════════════════════════════════

Question: "Compare search volume for xyz in March 2024 vs March 2025"
SQL: SELECT year, SUM(search_volume) as total_volume, COUNT(*) as keyword_count
     FROM reports_master
     WHERE client_name = 'xyz' AND month = 'March' 
       AND year IN (2024, 2025)
     GROUP BY year
     ORDER BY year;

═══════════════════════════════════════════════════════════════════════════════
10. SEARCHING KEYWORDS (LIKE/ILIKE)
═══════════════════════════════════════════════════════════════════════════════

Question: "Find keywords containing 'moving' for efg in December 2025"
SQL: SELECT keyword, search_volume, current_ranking, url
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December' 
       AND keyword ILIKE '%moving%'
     ORDER BY search_volume DESC;

Question: "Keywords starting with 'dental' for abc in March 2025"
SQL: SELECT keyword, search_volume, search_intent
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March' 
       AND keyword ILIKE 'dental%'
     ORDER BY search_volume DESC;

═══════════════════════════════════════════════════════════════════════════════
11. NULL HANDLING
═══════════════════════════════════════════════════════════════════════════════

Question: "Show keywords where difficulty is not set (NULL) for efg in December 2025"
SQL: SELECT keyword, search_volume, current_ranking, search_intent
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December' 
       AND difficulty IS NULL
     ORDER BY search_volume DESC;

Question: "Keywords with search intent set (not NULL) for abc in March 2025"
SQL: SELECT keyword, search_intent, search_volume
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March' 
       AND search_intent IS NOT NULL
     ORDER BY search_volume DESC;

"""
        return examples
    
    def generate_sql_query(
        self,
        user_question: str,
        session_id: Optional[str] = None,
        max_retries: int = 3
    ) -> Optional[str]:
        """
        Generate a SQL query from a natural language question using Groq.
        
        Parameters:
        -----------
        user_question : str
            The natural language question about the data
        session_id : Optional[str], optional
            Session identifier for conversation history. If provided, includes 
            top 5 recent conversations in context. Default is None.
        max_retries : int, optional
            Maximum number of retry attempts for API calls
        
        Returns:
        --------
        Optional[str]
            The SQL query as a string, or None if generation fails
        """
        if self.client is None:
            return None
        
        # Get comprehensive schema from schema file
        schema_context = self.get_schema_context()
        
        # Get query examples
        query_examples = self.get_query_examples()
        
        # Get conversation history if session_id provided (limit to 3 for token efficiency)
        conversation_history_context = ""
        if session_id and self.conversation_manager:
            try:
                conversations = self.conversation_manager.get_recent_conversations(
                    session_id=session_id,
                    limit=3  # Reduced from 5 to 3 for token efficiency
                )
                conversation_history_context = self.conversation_manager.format_conversation_history(
                    conversations
                )
            except Exception as e:
                print(f"⚠ Warning: Could not fetch conversation history: {e}")
                conversation_history_context = ""
        
        # Get unique client list from database for context
        client_list_context = ""
        if self.conversation_manager:
            try:
                clients = self.conversation_manager.get_unique_clients()
                client_list_context = self.conversation_manager.format_client_list(clients)
            except Exception as e:
                print(f"⚠ Warning: Could not fetch client list: {e}")
                client_list_context = ""
        
        # Use schema as-is (already optimized)
        schema_truncated = schema_context
        
        prompt = f"""Generate SQL for reports_master table.

SCHEMA:
{schema_truncated}

{client_list_context}

{query_examples}

{conversation_history_context}

RULES:
- Table: reports_master (lowercase)
- Columns: lowercase_with_underscores (client_name, search_volume, current_ranking)
- Filter by client_name, year, month when specified
- Month names: 'December' (capitalized, full name)
- Client names: lowercase ('efg', not 'EFG')
- Year: integer (2025, not '2025')
- Top N: ORDER BY column DESC LIMIT N
- Aggregations: COUNT(*), SUM(), AVG(), MAX(), MIN()
- Text search: ILIKE '%pattern%'
- change > 0 = improved, change < 0 = declined
- Output ONLY SQL query, no explanations

QUESTION: {user_question}

SQL:"""

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system", 
                            "content": """You are an expert PostgreSQL SQL query generator for the reports_master table. 
You output ONLY executable SQL queries, nothing else. 
No explanations, no markdown, no code blocks - just the raw SQL query.
Always use lowercase table and column names with underscores.
Always use proper filtering by client_name, year, and month when specified."""
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0  # Lower temperature for more deterministic output
                )
                sql_query = response.choices[0].message.content.strip()
                
                # Clean up the response - remove markdown code blocks if present
                if sql_query.startswith("```sql"):
                    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                elif sql_query.startswith("```"):
                    sql_query = sql_query.replace("```", "").strip()
                
                # Remove any leading/trailing backticks
                sql_query = sql_query.strip('`').strip()
                
                # Remove any "Query:" or similar prefixes
                for prefix in ['Query:', 'SQL:', 'Answer:', 'Output:', 'Result:', 'SELECT']:
                    if sql_query.startswith(prefix) and prefix != 'SELECT':
                        sql_query = sql_query[len(prefix):].strip()
                
                # Ensure it starts with SELECT
                if not sql_query.upper().startswith('SELECT'):
                    # Try to find SELECT in the query
                    select_idx = sql_query.upper().find('SELECT')
                    if select_idx != -1:
                        sql_query = sql_query[select_idx:].strip()
                
                # Remove trailing semicolon if present
                sql_query = sql_query.rstrip(';').strip()
                
                return sql_query
                
            except Exception as e:
                error_str = str(e)
                
                # Check for quota errors
                if "429" in error_str or "quota" in error_str.lower() or "rate limit" in error_str.lower():
                    if attempt < max_retries - 1:
                        wait_time = 60 * (attempt + 1)
                        print(f"⚠ API quota exceeded. Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                        time.sleep(wait_time)
                        continue
                    else:
                        return None
                else:
                    print(f"⚠ Error generating query: {error_str}")
                    return None
        
        return None


# Example usage
if __name__ == "__main__":
    # Initialize generator
    generator = QueryGenerator()
    
    # Example: Generate SQL query without session (no conversation history)
    question = "Show me top 5 keywords with highestt search volume for efg in December 2025"
    query = generator.generate_sql_query(question)
    print(f"Question: {question}")
    print(f"Generated SQL: {query}")
    
    # Example: Generate SQL query with session (includes conversation history)
    session_id = "test_session_123"
    question_with_context = "What about for client abc?"
    query_with_history = generator.generate_sql_query(question_with_context, session_id=session_id)
    print(f"\nQuestion (with context): {question_with_context}")
    print(f"Generated SQL: {query_with_history}")
