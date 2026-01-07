"""
Complete SQL Query Pipeline
Integrates QueryGenerator and QueryExecutor for end-to-end SQL query processing on PostgreSQL
"""

from pathlib import Path
from typing import Dict, Any, Optional, Union
import sys
import os

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

try:
    from rag_excel_postgres.llm.query_generator import QueryGenerator
    from rag_excel_postgres.llm.query_executor import QueryExecutor
except ImportError:
    try:
        # Handle relative import if running as script
        from llm.query_generator import QueryGenerator
        from llm.query_executor import QueryExecutor
    except ImportError:
        # Fallback: import from same directory
        from query_generator import QueryGenerator
        from query_executor import QueryExecutor


class QueryPipeline:
    """
    Complete pipeline that generates SQL queries from natural language
    and executes them on PostgreSQL database.
    """
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize QueryPipeline.
        
        Parameters:
        -----------
        groq_api_key : Optional[str], optional
            Groq API key. If None, uses environment variable.
        groq_model : Optional[str], optional
            Groq model name. If None, uses environment variable.
        db_config : Optional[Dict[str, Any]], optional
            Database configuration dictionary. If None, uses environment variables.
        """
        # Initialize components
        self.query_generator = QueryGenerator(
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
        
        self.query_executor = QueryExecutor(
            query_generator=self.query_generator,
            db_config=db_config
        )
    
    def process_query(
        self,
        user_question: str,
        return_dataframe: bool = True
    ) -> Dict[str, Any]:
        """
        Complete workflow: Generate SQL query from natural language and execute it.
        
        Parameters:
        -----------
        user_question : str
            Natural language question
        return_dataframe : bool, optional
            Whether to return results as pandas DataFrame. Default is True.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary containing:
            - 'success': Boolean indicating success
            - 'result': Query result (DataFrame, list, or scalar)
            - 'query': Generated SQL query
            - 'error': Error message if failed
        """
        print("\n" + "=" * 80)
        print("SQL QUERY PIPELINE")
        print("=" * 80)
        print(f"Question: {user_question}")
        print("=" * 80)
        
        # Execute query from question
        result_dict = self.query_executor.execute_from_question(
            user_question=user_question,
            return_dataframe=return_dataframe
        )
        
        return result_dict
    
    def format_results(self, pipeline_result: Dict[str, Any]) -> str:
        """
        Format pipeline results for display.
        
        Parameters:
        -----------
        pipeline_result : Dict[str, Any]
            Result dictionary from process_query()
        
        Returns:
        --------
        str
            Formatted string representation of results
        """
        output = []
        
        if pipeline_result['success']:
            output.append("\n" + "=" * 80)
            output.append("✓ Query executed successfully!")
            output.append("=" * 80)
            output.append(f"\nSQL Query:\n{pipeline_result['query']}\n")
            output.append("-" * 80)
            output.append("\nResults:")
            output.append("-" * 80)
            output.append(self.query_executor.format_result(pipeline_result['result']))
        else:
            output.append("\n" + "=" * 80)
            output.append("❌ Query execution failed")
            output.append("=" * 80)
            output.append(f"\nError: {pipeline_result.get('error', 'Unknown error')}")
            if pipeline_result.get('query'):
                output.append(f"\nGenerated Query: {pipeline_result['query']}")
        
        return "\n".join(output)
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get information about the database and reports_master table.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary with database information
        """
        return self.query_executor.get_table_info()
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
        --------
        bool
            True if connection successful, False otherwise
        """
        try:
            conn = self.query_executor.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            print("✓ Database connection successful")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {str(e)}")
            return False


# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = QueryPipeline()
    
    # Test connection
    print("Testing database connection...")
    if not pipeline.test_connection():
        print("Please check your database configuration in .env file")
        sys.exit(1)
    
    # Get database info
    print("\nGetting database information...")
    db_info = pipeline.get_database_info()
    if 'error' not in db_info:
        print(f"Total rows in reports_master: {db_info['row_count']}")
        print(f"Available clients: {', '.join(db_info['clients'])}")
        print(f"Available years: {', '.join(map(str, db_info['years']))}")
        print(f"Available months: {', '.join(db_info['months'])}")
    
    # Process a query
    print("\n" + "=" * 80)
    question = "Show me top 5 keywords with highest search volume for client efg in December 2025"
    result = pipeline.process_query(question)
    
    # Display results
    print(pipeline.format_results(result))
