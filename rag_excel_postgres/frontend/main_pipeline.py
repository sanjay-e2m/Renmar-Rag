"""
Main Pipeline for SQL-RAG System
Integrates query execution and LLM-based answer formatting for user-friendly responses
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from groq import Groq
import pandas as pd

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
_current_file = Path(__file__).resolve()
_parent_dir = _current_file.parent.parent
if str(_parent_dir) not in sys.path:
    sys.path.insert(0, str(_parent_dir))

# Import QueryPipeline - handle multiple import paths
try:
    from rag_excel_postgres.llm.query_pipeline import QueryPipeline
except ImportError:
    try:
        # Try direct import using importlib to avoid __init__.py issues
        import importlib.util
        from pathlib import Path
        
        current_file = Path(__file__).resolve()
        query_pipeline_path = current_file.parent.parent / "llm" / "query_pipeline.py"
        
        spec = importlib.util.spec_from_file_location("query_pipeline", query_pipeline_path)
        query_pipeline_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(query_pipeline_module)
        QueryPipeline = query_pipeline_module.QueryPipeline
    except Exception as e:
        # Last resort: add parent to path and import
        import sys
        from pathlib import Path
        parent_dir = Path(__file__).parent.parent
        if str(parent_dir) not in sys.path:
            sys.path.insert(0, str(parent_dir))
        # Import the module directly
        import importlib
        query_pipeline = importlib.import_module('llm.query_pipeline')
        QueryPipeline = query_pipeline.QueryPipeline

# Get configuration from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not GROQ_API_KEY:
    raise Exception("❌ Missing GROQ_API_KEY in environment variables")


class MainPipeline:
    """
    Main pipeline that executes SQL queries and formats answers in a user-friendly manner.
    """
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize MainPipeline.
        
        Parameters:
        -----------
        groq_api_key : Optional[str], optional
            Groq API key. If None, uses environment variable.
        groq_model : Optional[str], optional
            Groq model name. If None, uses environment variable.
        db_config : Optional[Dict[str, Any]], optional
            Database configuration dictionary. If None, uses environment variables.
        """
        api_key = groq_api_key or GROQ_API_KEY
        model = groq_model or GROQ_MODEL
        
        # Initialize query pipeline
        self.query_pipeline = QueryPipeline(
            groq_api_key=api_key,
            groq_model=model,
            db_config=db_config
        )
        
        # Initialize Groq client for answer formatting
        try:
            self.client = Groq(api_key=api_key)
            self.model = model
            print(f"✓ Main Pipeline initialized with model: {self.model}")
        except Exception as e:
            print(f"⚠ Warning: Could not configure Groq client: {e}")
            self.client = None
    
    def format_data_for_context(self, result: Any) -> str:
        """
        Format query result data for LLM context.
        
        Parameters:
        -----------
        result : Any
            Query result (DataFrame, list, or scalar)
        
        Returns:
        --------
        str
            Formatted string representation of the data
        """
        if isinstance(result, pd.DataFrame):
            if len(result) == 0:
                return "No data found (empty result set)"
            
            # Convert DataFrame to formatted string
            # Limit to first 50 rows for context
            df_display = result.head(50)
            
            # Create a readable format
            formatted_lines = []
            formatted_lines.append(f"Data Summary: {len(result)} rows × {len(result.columns)} columns")
            formatted_lines.append("\nColumn Names:")
            formatted_lines.append(", ".join(result.columns.tolist()))
            formatted_lines.append("\nData:")
            
            # Format as table
            formatted_lines.append(df_display.to_string(index=False))
            
            if len(result) > 50:
                formatted_lines.append(f"\n... (showing first 50 of {len(result)} rows)")
            
            return "\n".join(formatted_lines)
        
        elif isinstance(result, list):
            if len(result) == 0:
                return "No data found (empty result set)"
            
            df = pd.DataFrame(result)
            return self.format_data_for_context(df)
        
        elif isinstance(result, dict):
            # Handle dictionary results (like aggregations)
            formatted_lines = []
            for key, value in result.items():
                formatted_lines.append(f"{key}: {value}")
            return "\n".join(formatted_lines)
        
        else:
            # Scalar value
            return f"Result: {result}"
    
    def format_answer(
        self,
        user_question: str,
        sql_query: str,
        query_result: Any,
        max_retries: int = 3
    ) -> str:
        """
        Format the answer in a user-friendly manner using LLM.
        
        Parameters:
        -----------
        user_question : str
            Original user question
        sql_query : str
            Generated SQL query
        query_result : Any
            Query result data
        max_retries : int, optional
            Maximum number of retry attempts. Default is 3.
        
        Returns:
        --------
        str
            Formatted user-friendly answer
        """
        if self.client is None:
            # Fallback: return basic formatted result
            return self.format_data_for_context(query_result)
        
        # Format data for context
        data_context = self.format_data_for_context(query_result)
        
        # Determine result type
        if isinstance(query_result, pd.DataFrame):
            result_type = "table"
            row_count = len(query_result)
            column_count = len(query_result.columns)
        elif isinstance(query_result, list):
            result_type = "list"
            row_count = len(query_result)
            column_count = len(query_result[0]) if query_result else 0
        elif isinstance(query_result, dict):
            result_type = "aggregation"
            row_count = 1
            column_count = len(query_result)
        else:
            result_type = "scalar"
            row_count = 1
            column_count = 1
        
        prompt = f"""You are a helpful data analyst assistant. Your task is to format database query results into a clear, user-friendly answer.

USER QUESTION: {user_question}

SQL QUERY EXECUTED: {sql_query}

QUERY RESULT DATA:
{data_context}

RESULT TYPE: {result_type}
ROW COUNT: {row_count}
COLUMN COUNT: {column_count}

INSTRUCTIONS:
1. Provide a clear, conversational answer that directly addresses the user's question
2. If the result is empty, politely explain that no data was found matching the criteria
3. For tables/lists with multiple rows:
   - Summarize key findings
   - Highlight important numbers or trends
   - Present data in a readable format
   - Use bullet points or numbered lists when appropriate
4. For aggregations (counts, sums, averages):
   - State the result clearly
   - Provide context if helpful
   - Use natural language (e.g., "There are 25 keywords" instead of just "25")
5. For single values:
   - Answer directly
   - Provide context if relevant
6. Use professional but friendly tone
7. Don't repeat the SQL query unless specifically asked
8. Focus on insights and key information
9. Format numbers nicely (use commas for large numbers)
10. If showing rankings or comparisons, highlight the top/bottom items

FORMAT YOUR ANSWER:"""

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a helpful data analyst assistant. You format database query results into clear, 
user-friendly answers. You communicate in a professional but friendly tone. You focus on providing insights 
and answering the user's question directly."""
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1  # Lower temperature for more consistent formatting
                )
                
                formatted_answer = response.choices[0].message.content.strip()
                return formatted_answer
                
            except Exception as e:
                error_str = str(e)
                
                # Check for quota errors
                if "429" in error_str or "quota" in error_str.lower() or "rate limit" in error_str.lower():
                    if attempt < max_retries - 1:
                        import time
                        wait_time = 60 * (attempt + 1)
                        print(f"⚠ API quota exceeded. Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                        time.sleep(wait_time)
                        continue
                    else:
                        # Fallback to basic formatting
                        return self.format_data_for_context(query_result)
                else:
                    print(f"⚠ Error formatting answer: {error_str}")
                    # Fallback to basic formatting
                    return self.format_data_for_context(query_result)
        
        # Final fallback
        return self.format_data_for_context(query_result)
    
    def process_user_question(
        self,
        user_question: str,
        format_answer: bool = True,
        show_sql: bool = False
    ) -> Dict[str, Any]:
        """
        Complete workflow: Process user question, execute query, and format answer.
        
        Parameters:
        -----------
        user_question : str
            Natural language question
        format_answer : bool, optional
            Whether to format answer using LLM. Default is True.
        show_sql : bool, optional
            Whether to include SQL query in response. Default is False.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary containing:
            - 'success': Boolean indicating success
            - 'question': User question
            - 'sql_query': Generated SQL query
            - 'raw_result': Raw query result
            - 'formatted_answer': Formatted user-friendly answer
            - 'error': Error message if failed
        """
        print("\n" + "=" * 80)
        print("MAIN PIPELINE - Processing User Question")
        print("=" * 80)
        print(f"Question: {user_question}")
        print("=" * 80)
        
        try:
            # Step 1: Execute query using query pipeline
            query_result = self.query_pipeline.process_query(user_question)
            
            if not query_result['success']:
                return {
                    'success': False,
                    'question': user_question,
                    'sql_query': query_result.get('query'),
                    'raw_result': None,
                    'formatted_answer': f"I encountered an error while processing your question: {query_result.get('error', 'Unknown error')}",
                    'error': query_result.get('error')
                }
            
            sql_query = query_result['query']
            raw_result = query_result['result']
            
            # Step 2: Format answer using LLM
            if format_answer:
                print("\n" + "-" * 80)
                print("Formatting answer...")
                print("-" * 80)
                formatted_answer = self.format_answer(
                    user_question=user_question,
                    sql_query=sql_query,
                    query_result=raw_result
                )
            else:
                formatted_answer = self.format_data_for_context(raw_result)
            
            # Prepare response
            response = {
                'success': True,
                'question': user_question,
                'sql_query': sql_query if show_sql else None,
                'raw_result': raw_result,
                'formatted_answer': formatted_answer,
                'error': None
            }
            
            return response
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Error in main pipeline: {error_msg}")
            return {
                'success': False,
                'question': user_question,
                'sql_query': None,
                'raw_result': None,
                'formatted_answer': f"I encountered an error while processing your question: {error_msg}",
                'error': error_msg
            }
    
    def display_result(self, result: Dict[str, Any]) -> None:
        """
        Display the result in a formatted manner.
        
        Parameters:
        -----------
        result : Dict[str, Any]
            Result dictionary from process_user_question()
        """
        print("\n" + "=" * 80)
        print("RESULT")
        print("=" * 80)
        
        if result['success']:
            print("\n📊 Formatted Answer:")
            print("-" * 80)
            print(result['formatted_answer'])
            
            if result.get('sql_query') and result['sql_query']:
                print("\n" + "-" * 80)
                print("🔍 SQL Query Used:")
                print("-" * 80)
                print(result['sql_query'])
        else:
            print(f"\n❌ Error: {result.get('error', 'Unknown error')}")
            if result.get('formatted_answer'):
                print(f"\n{result['formatted_answer']}")
        
        print("\n" + "=" * 80)


# Example usage
if __name__ == "__main__":
    # Initialize main pipeline
    pipeline = MainPipeline()
    
    # Test connection
    print("Testing database connection...")
    if not pipeline.query_pipeline.test_connection():
        print("Please check your database configuration in .env file")
        sys.exit(1)
    
    # Example questions
    example_questions = [
        "Show me top 5 keywords with highest search volume for client efg in December 2025"
        # "How many keywords are tracked for abc in March 2025?",
        # "What is the total search volume for xyz in May 2024?",
    ]
    
    # Process first example question
    if example_questions:
        question = example_questions[0]
        result = pipeline.process_user_question(
            user_question=question,
            format_answer=True,
            show_sql=True  # Set to False to hide SQL query
        )
        
        # Display result
        pipeline.display_result(result)

