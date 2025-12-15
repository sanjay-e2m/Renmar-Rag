"""
Query Executor
Executes pandas queries on CSV files and returns results
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Union, Dict, Any

try:
    from src.llm.query_generator import QueryGenerator
except ImportError:
    # Handle relative import if running as script
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.llm.query_generator import QueryGenerator


class QueryExecutor:
    """
    Executes pandas queries on CSV files and returns dataframes.
    Integrates with QueryGenerator to generate and execute queries.
    """
    
    def __init__(
        self,
        query_generator: QueryGenerator | None = None
    ):
        """
        Initialize QueryExecutor.
        
        Parameters:
        -----------
        query_generator : QueryGenerator | None, optional
            QueryGenerator instance. If None, creates a new one.
        """
        self.query_generator = query_generator or QueryGenerator()
    
    def load_csv_file(self, csv_file_path: str | Path) -> pd.DataFrame:
        """
        Load CSV file into a pandas DataFrame.
        
        Parameters:
        -----------
        csv_file_path : str | Path
            Path to the CSV file
        
        Returns:
        --------
        pd.DataFrame
            Loaded dataframe
        """
        csv_path = Path(csv_file_path)
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        print(f"Loading CSV file: {csv_path.name}...")
        df = pd.read_csv(csv_path)
        print(f"✓ CSV file loaded ({df.shape[0]} rows × {df.shape[1]} columns)")
        
        return df
    
    def preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess dataframe to handle common data issues like string numbers with commas.
        Returns a copy of the dataframe with cleaned numeric columns.
        
        Parameters:
        -----------
        df : pd.DataFrame
            Input dataframe
        
        Returns:
        --------
        pd.DataFrame
            Preprocessed dataframe
        """
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Check if column might be numeric but stored as string with commas
            if df_clean[col].dtype == 'object':
                # Try to detect if it's a numeric column with commas
                sample = df_clean[col].dropna().head(10)
                if len(sample) > 0:
                    # Check if values look like comma-separated numbers
                    try:
                        test_vals = sample.astype(str).str.replace(',', '').str.strip()
                        # Check if they're numeric (excluding "None")
                        numeric_count = test_vals[test_vals != 'None'].str.match(r'^-?\d+\.?\d*$').sum()
                        if numeric_count >= len(sample[sample.astype(str) != 'None']) * 0.8:  # 80% are numeric
                            df_clean[col] = pd.to_numeric(
                                df_clean[col].astype(str).str.replace(',', '').str.strip().replace('None', ''),
                                errors='coerce'
                            )
                            # Fill NaN back with "None" if needed
                            df_clean[col] = df_clean[col].fillna("None")
                            print(f"  ℹ Converted '{col}' from string to numeric")
                    except Exception:
                        pass
        
        return df_clean
    
    def execute_query(
        self,
        query: str,
        df: pd.DataFrame,
        preprocess: bool = True
    ) -> Union[pd.DataFrame, pd.Series, np.ndarray, Any]:
        """
        Execute a pandas query on a dataframe.
        
        Parameters:
        -----------
        query : str
            The pandas query string to execute
        df : pd.DataFrame
            The dataframe to query
        preprocess : bool, optional
            Whether to preprocess the dataframe before execution. Default is True.
        
        Returns:
        --------
        Union[pd.DataFrame, pd.Series, np.ndarray, Any]
            Query result (can be DataFrame, Series, array, or scalar)
        
        Raises:
        -------
        Exception
            If query execution fails
        """
        # Preprocess if needed
        if preprocess:
            print("Preprocessing dataframe...")
            df_to_use = self.preprocess_dataframe(df)
        else:
            df_to_use = df
        
        print(f"Executing query: {query}")
        
        # Create a local namespace for eval with the preprocessed dataframe
        local_namespace = {'df': df_to_use, 'pd': pd, 'np': np}
        
        try:
            # Execute the query
            result = eval(query, {"__builtins__": {}}, local_namespace)
            print("✓ Query executed successfully")
            return result
            
        except KeyError as e:
            error_msg = str(e)
            print(f"❌ Column name error: {error_msg}")
            
            # Try to suggest the correct column name
            import re
            match = re.search(r"'([^']+)'", error_msg)
            if match:
                wrong_col = match.group(1)
                print(f"\nThe column '{wrong_col}' was not found.")
                print("\nSimilar columns that exist:")
                for col in df.columns:
                    if wrong_col.lower() in col.lower() or col.lower() in wrong_col.lower():
                        print(f"  - {repr(col)}")
                    elif ' '.join(wrong_col.split()).lower() in ' '.join(col.split()).lower():
                        print(f"  - {repr(col)} (possible match)")
            
            raise
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Error executing query: {error_msg}")
            
            # Provide helpful debugging info
            print("\n📋 Available columns in the dataframe:")
            for i, col in enumerate(df.columns, 1):
                dtype = df[col].dtype
                print(f"  {i}. {repr(col)} (dtype: {dtype})")
            
            raise
    
    def execute_from_query_string(
        self,
        user_question: str,
        csv_file_path: str | Path,
        txt_file_path: str | Path,
        preprocess: bool = True
    ) -> Dict[str, Any]:
        """
        Complete workflow: Load files, generate query, and execute it.
        
        Parameters:
        -----------
        user_question : str
            Natural language question
        csv_file_path : str | Path
            Path to CSV file
        txt_file_path : str | Path
            Path to TXT context file
        preprocess : bool, optional
            Whether to preprocess dataframe. Default is True.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary containing:
            - 'result': Query result (DataFrame, Series, array, or scalar)
            - 'query': Generated pandas query
            - 'dataframe': Loaded dataframe
            - 'success': Boolean indicating success
            - 'error': Error message if failed
        """
        try:
            # Load CSV file
            df = self.load_csv_file(csv_file_path)
            
            # Load context file
            context_content = self.query_generator.load_context_file(txt_file_path)
            
            # Generate pandas query
            print("\nGenerating pandas query...")
            query = self.query_generator.generate_pandas_query(
                user_question=user_question,
                context_content=context_content,
                df=df
            )
            
            if query is None:
                return {
                    'success': False,
                    'error': 'Failed to generate query',
                    'result': None,
                    'query': None,
                    'dataframe': df
                }
            
            print(f"\nGenerated Query:\n  {query}")
            
            # Execute query
            print("\n" + "-" * 80)
            print("Executing query...")
            print("-" * 80)
            
            result = self.execute_query(query, df, preprocess=preprocess)
            
            return {
                'success': True,
                'result': result,
                'query': query,
                'dataframe': df,
                'error': None
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'result': None,
                'query': None,
                'dataframe': None
            }
    
    def format_result(self, result: Any) -> str:
        """
        Format query result for display.
        
        Parameters:
        -----------
        result : Any
            Query result (DataFrame, Series, array, or scalar)
        
        Returns:
        --------
        str
            Formatted string representation of the result
        """
        if isinstance(result, pd.DataFrame):
            return f"DataFrame ({result.shape[0]} rows × {result.shape[1]} columns)\n{result.to_string()}"
        elif isinstance(result, pd.Series):
            return f"Series ({len(result)} values)\n{result.to_string()}"
        elif isinstance(result, np.ndarray):
            return f"Array ({len(result)} items)\n{result}"
        else:
            return f"Result: {result}"


# Example usage
if __name__ == "__main__":
    # Initialize executor
    executor = QueryExecutor()
    
    # Example: Execute query from user question
    # result_dict = executor.execute_from_query_string(
    #     user_question="top 5 highest search volume",
    #     csv_file_path="path/to/data.csv",
    #     txt_file_path="path/to/context.txt"
    # )
    # 
    # if result_dict['success']:
    #     print("\nResult:")
    #     print(executor.format_result(result_dict['result']))
    # else:
    #     print(f"Error: {result_dict['error']}")
    pass
