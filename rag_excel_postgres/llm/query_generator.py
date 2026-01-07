"""
SQL Query Generator using LLM
Generates SQL queries from natural language questions for PostgreSQL reports_master table
"""

import os
import re
import time
from pathlib import Path
from typing import Optional, Union
from dotenv import load_dotenv
from groq import Groq

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
    
    def get_schema_context(self) -> str:
        """
        Get the database schema context for reports_master table.
        
        Returns:
        --------
        str
            Formatted schema description
        """
        schema = """
DATABASE SCHEMA: reports_master

TABLE STRUCTURE:
----------------
Filterable Dimensions (for WHERE clauses):
  - client_name (TEXT NOT NULL): Client name (e.g., 'efg', 'abc', 'xyz')
  - year (INTEGER NOT NULL): Year (e.g., 2024, 2025)
  - month (TEXT NOT NULL): Month name (e.g., 'January', 'February', 'March', 'December')
  - month_id (INTEGER): Month number 1-12 (1=January, 12=December)

Keyword Report Metrics:
  - keyword (TEXT NOT NULL): The keyword/phrase being tracked
  - initial_ranking (INTEGER): Initial ranking position
  - current_ranking (INTEGER): Current ranking position
  - change (INTEGER): Change in ranking (positive = improved, negative = declined)
  - search_volume (INTEGER): Search volume for the keyword
  - map_ranking_gbp (INTEGER): Map pack ranking (GBP)
  - location (TEXT): Location/region (e.g., 'National')
  - url (TEXT): URL associated with the keyword
  - difficulty (INTEGER): Keyword difficulty score
  - search_intent (TEXT): Search intent type (e.g., 'Informational', 'Commercial', 'Transactional')

Metadata:
  - source_file (TEXT NOT NULL): Original CSV filename
  - row_hash (TEXT): Hash for deduplication
  - ingested_at (TIMESTAMP): When the row was inserted

INDEXES (for fast queries):
  - idx_reports_master_client_year_month (client_name, year, month)
  - idx_reports_master_client (client_name)
  - idx_reports_master_year_month (year, month)
  - idx_reports_master_keyword (keyword)
  - idx_reports_master_source_file (source_file)

UNIQUE CONSTRAINT:
  - (client_name, year, month, keyword, source_file) - prevents duplicates
"""
        return schema
    
    def get_query_examples(self) -> str:
        """
        Get comprehensive SQL query examples with natural language questions.
        
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

Question: "Show keywords with highest search volume across all clients in 2025"
SQL: SELECT client_name, keyword, search_volume
     FROM reports_master
     WHERE year = 2025
     ORDER BY search_volume DESC
     LIMIT 20;

═══════════════════════════════════════════════════════════════════════════════
2. BOTTOM N / LOWEST / SMALLEST VALUES
═══════════════════════════════════════════════════════════════════════════════

Question: "Show me bottom 5 keywords with lowest search volume for xyz in May 2024"
SQL: SELECT keyword, search_volume, current_ranking
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May'
     ORDER BY search_volume ASC
     LIMIT 5;

Question: "Keywords with lowest ranking (highest rank number) for klm in March 2024"
SQL: SELECT keyword, current_ranking, search_volume
     FROM reports_master
     WHERE client_name = 'klm' AND year = 2024 AND month = 'March'
     ORDER BY current_ranking DESC
     LIMIT 10;

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

Question: "Keywords with difficulty less than 50 for xyz in May 2024"
SQL: SELECT keyword, difficulty, search_volume, search_intent
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May' 
       AND difficulty < 50
     ORDER BY difficulty ASC;

Question: "Show keywords with search intent 'Commercial' for efg in December 2025"
SQL: SELECT keyword, search_volume, current_ranking, url
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December' 
       AND search_intent = 'Commercial'
     ORDER BY search_volume DESC;

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

Question: "What is the maximum search volume for klm in March 2024?"
SQL: SELECT MAX(search_volume) as max_search_volume
     FROM reports_master
     WHERE client_name = 'klm' AND year = 2024 AND month = 'March';

Question: "How many keywords improved (positive change) for efg in December 2025?"
SQL: SELECT COUNT(*) as improved_keywords
     FROM reports_master
     WHERE client_name = 'efg' AND year = 2025 AND month = 'December' 
       AND change > 0;

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

Question: "Keywords that dropped in ranking (negative change) for xyz in May 2024"
SQL: SELECT keyword, initial_ranking, current_ranking, change, search_volume
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May' 
       AND change < 0
     ORDER BY change ASC;

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

Question: "Count keywords by search intent for abc in March 2025"
SQL: SELECT search_intent, COUNT(*) as keyword_count, AVG(search_volume) as avg_volume
     FROM reports_master
     WHERE client_name = 'abc' AND year = 2025 AND month = 'March'
     GROUP BY search_intent
     ORDER BY keyword_count DESC;

Question: "Total search volume by location for xyz in May 2024"
SQL: SELECT location, SUM(search_volume) as total_volume, COUNT(*) as keyword_count
     FROM reports_master
     WHERE client_name = 'xyz' AND year = 2024 AND month = 'May'
     GROUP BY location
     ORDER BY total_volume DESC;

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
        max_retries: int = 3
    ) -> Optional[str]:
        """
        Generate a SQL query from a natural language question using Groq.
        
        Parameters:
        -----------
        user_question : str
            The natural language question about the data
        max_retries : int, optional
            Maximum number of retry attempts for API calls
        
        Returns:
        --------
        Optional[str]
            The SQL query as a string, or None if generation fails
        """
        if self.client is None:
            return None
        
        schema_context = self.get_schema_context()
        query_examples = self.get_query_examples()
        
        prompt = f"""You are an expert PostgreSQL SQL query generator. Generate precise SQL queries to answer questions about the reports_master table.

{schema_context}

{query_examples}

CRITICAL RULES AND REGULATIONS:
═══════════════════════════════════════════════════════════════════════════════

1. TABLE NAME: Always use 'reports_master' (lowercase, with underscore)

2. COLUMN NAMES: Use exact column names as shown in schema:
   - Use lowercase with underscores: client_name, search_volume, current_ranking
   - Month names are stored as TEXT: 'January', 'February', 'March', etc. (capitalized)
   - Client names are lowercase: 'efg', 'abc', 'xyz', 'klm'

3. FILTERING BEST PRACTICES:
   - ALWAYS filter by client_name, year, and month when specified in question
   - Use exact month names: 'December' (not 'Dec' or '12')
   - Use exact client names: 'efg' (not 'EFG' or 'Efg')
   - Use integers for year: 2025 (not '2025' as string)

4. WHERE CLAUSE PATTERNS:
   - client_name = 'clientname' (always lowercase, single quotes)
   - year = 2025 (integer, no quotes)
   - month = 'December' (text, capitalized, single quotes)
   - Use AND to combine multiple conditions

5. SORTING:
   - For "top/highest/largest": ORDER BY column DESC
   - For "bottom/lowest/smallest": ORDER BY column ASC
   - For rankings: lower number = better rank, so ORDER BY current_ranking ASC for best ranks

6. LIMIT:
   - Always use LIMIT for "top N" or "bottom N" questions
   - Default to LIMIT 10 if number not specified

7. AGGREGATIONS:
   - COUNT(*): Count all rows
   - SUM(column): Sum numeric values
   - AVG(column): Average numeric values
   - MAX(column): Maximum value
   - MIN(column): Minimum value
   - Use GROUP BY when aggregating by categories

8. NULL HANDLING:
   - Use IS NULL for missing values
   - Use IS NOT NULL for non-missing values
   - NULL values are excluded from aggregations automatically

9. TEXT SEARCHING:
   - Use ILIKE for case-insensitive pattern matching
   - Use % for wildcards: ILIKE '%keyword%'
   - Use single quotes for text literals

10. COMPARISONS:
    - Use >, <, >=, <= for numeric comparisons
    - Use = for exact matches
    - Use BETWEEN for ranges: column BETWEEN value1 AND value2
    - Use IN for multiple values: column IN ('value1', 'value2')

11. CHANGE COLUMN INTERPRETATION:
    - change > 0: Ranking improved (moved up)
    - change < 0: Ranking declined (moved down)
    - change = 0: No change

12. OUTPUT FORMAT:
    - Output ONLY the SQL query
    - NO explanations, NO comments, NO markdown code blocks
    - NO semicolon at the end (optional, but preferred without)
    - Start directly with SELECT

13. COLUMN SELECTION:
    - Include relevant columns in SELECT clause
    - For aggregations, use aliases: SUM(search_volume) AS total_volume
    - Include context columns (keyword, client_name, month) when useful

14. PERFORMANCE:
    - Use indexed columns in WHERE clauses: client_name, year, month
    - Avoid functions on indexed columns in WHERE clauses
    - Use LIMIT to restrict result sets

15. QUESTION ANALYSIS:
    - Extract client name, year, month from question
    - Identify what metric to query (search_volume, ranking, change, etc.)
    - Determine if aggregation needed (COUNT, SUM, AVG, etc.)
    - Check if comparison needed (top N, bottom N, filtering)

USER QUESTION: {user_question}

Generate the SQL query following all rules above. Output ONLY the SQL query, nothing else:"""

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
    
    # Example: Generate SQL query
    question = "Show me top 5 keywords with highest search volume for client efg in December 2025"
    query = generator.generate_sql_query(question)
    print(f"Question: {question}")
    print(f"Generated SQL: {query}")
