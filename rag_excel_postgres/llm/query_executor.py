"""
SQL Query Executor
Executes SQL queries on PostgreSQL database and returns results
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, Union, Dict, Any, List
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

try:
    from rag_excel_postgres.llm.query_generator import QueryGenerator
except ImportError:
    try:
        # Handle relative import if running as script
        from llm.query_generator import QueryGenerator
    except ImportError:
        # Fallback: import from same directory
        from query_generator import QueryGenerator


class QueryExecutor:
    """
    Executes SQL queries on PostgreSQL database and returns results.
    Integrates with QueryGenerator to generate and execute queries.
    """
    
    def __init__(
        self,
        query_generator: Optional[QueryGenerator] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize QueryExecutor.
        
        Parameters:
        -----------
        query_generator : Optional[QueryGenerator], optional
            QueryGenerator instance. If None, creates a new one.
        db_config : Optional[Dict[str, Any]], optional
            Database configuration dictionary. If None, uses environment variables.
        """
        self.query_generator = query_generator or QueryGenerator()
        self.db_config = db_config or DB_CONFIG
    
    def get_connection(self):
        """
        Get a database connection.
        
        Returns:
        --------
        psycopg2.connection
            Database connection
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    def execute_sql_query(
        self,
        sql_query: str,
        return_dataframe: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]], Any]:
        """
        Execute a SQL query on the database.
        
        Parameters:
        -----------
        sql_query : str
            The SQL query to execute
        return_dataframe : bool, optional
            Whether to return results as pandas DataFrame. Default is True.
            If False, returns list of dictionaries.
        
        Returns:
        --------
        Union[pd.DataFrame, List[Dict[str, Any]], Any]
            Query result as DataFrame, list of dicts, or scalar value
        
        Raises:
        -------
        Exception
            If query execution fails
        """
        conn = None
        cursor = None
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Execute query
            cursor.execute(sql_query)
            
            # Check if query returns rows (SELECT) or is a modification query
            if cursor.description:
                # SELECT query - fetch results
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                results = [dict(row) for row in rows]
                
                if return_dataframe and len(results) > 0:
                    # Convert to pandas DataFrame
                    df = pd.DataFrame(results)
                    return df
                elif return_dataframe and len(results) == 0:
                    # Empty result set - return empty DataFrame
                    return pd.DataFrame()
                else:
                    return results
            else:
                # Non-SELECT query (INSERT, UPDATE, DELETE, etc.)
                conn.commit()
                affected_rows = cursor.rowcount
                return {"affected_rows": affected_rows}
                
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            raise
            
        except Exception as e:
            if conn:
                conn.rollback()
            raise
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def execute_from_question(
        self,
        user_question: str,
        return_dataframe: bool = True
    ) -> Dict[str, Any]:
        """
        Complete workflow: Generate SQL query from question and execute it.
        
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
        try:
            # Generate SQL query
            sql_query = self.query_generator.generate_sql_query(user_question)
            
            if sql_query is None:
                return {
                    'success': False,
                    'error': 'Failed to generate SQL query',
                    'result': None,
                    'query': None
                }
            
            # Execute query
            result = self.execute_sql_query(sql_query, return_dataframe=return_dataframe)
            
            return {
                'success': True,
                'result': result,
                'query': sql_query,
                'error': None
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'result': None,
                'query': None
            }
    
    def format_result(self, result: Any) -> str:
        """
        Format query result for display.
        
        Parameters:
        -----------
        result : Any
            Query result (DataFrame, list of dicts, or scalar)
        
        Returns:
        --------
        str
            Formatted string representation of the result
        """
        if isinstance(result, pd.DataFrame):
            if len(result) == 0:
                return "No results found (empty result set)"
            return f"DataFrame ({result.shape[0]} rows × {result.shape[1]} columns)\n{result.to_string(index=False)}"
        elif isinstance(result, list):
            if len(result) == 0:
                return "No results found (empty result set)"
            df = pd.DataFrame(result)
            return f"Results ({len(result)} rows)\n{df.to_string(index=False)}"
        elif isinstance(result, dict):
            return f"Result: {result}"
        else:
            return f"Result: {result}"
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the reports_master table.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary with table information
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM reports_master")
            row_count = cursor.fetchone()[0]
            
            # Get column information
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'reports_master'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            # Get unique clients
            cursor.execute("SELECT DISTINCT client_name FROM reports_master ORDER BY client_name")
            clients = [row[0] for row in cursor.fetchall()]
            
            # Get unique years
            cursor.execute("SELECT DISTINCT year FROM reports_master ORDER BY year")
            years = [row[0] for row in cursor.fetchall()]
            
            # Get unique months
            cursor.execute("SELECT DISTINCT month FROM reports_master ORDER BY month_id")
            months = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return {
                'row_count': row_count,
                'columns': columns,
                'clients': clients,
                'years': years,
                'months': months
            }
            
        except Exception as e:
            return {'error': str(e)}


# Example usage
if __name__ == "__main__":
    # Initialize executor
    executor = QueryExecutor()
    
    # Example: Execute query from user question
    result_dict = executor.execute_from_question(
        user_question="Show me top 5 keywords with highest search volume for client efg in December 2025"
    )
    
    if result_dict['success']:
        print("\n" + "=" * 80)
        print("RESULT")
        print("=" * 80)
        print(executor.format_result(result_dict['result']))
    else:
        print(f"\nError: {result_dict['error']}")
