"""
Complete Query Pipeline
Integrates FileRouter, QueryGenerator, and QueryExecutor for end-to-end query processing
"""

from pathlib import Path
from typing import Dict, Any, Optional
import sys
import os

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.routing.file_router import FileRouter
from src.llm.query_generator import QueryGenerator
from src.llm.query_executor import QueryExecutor


class QueryPipeline:
    """
    Complete pipeline that routes queries to files, generates pandas queries,
    and executes them on CSV files.
    """
    
    def __init__(
        self,
        output_dir: str | Path | None = None,
        preprocessed_dir: str | Path | None = None,
        groq_api_key: str | None = None,
        groq_model: str | None = None
    ):
        """
        Initialize QueryPipeline.
        
        Parameters:
        -----------
        output_dir : str | Path | None, optional
            Directory containing .txt files. If None, uses default path.
        preprocessed_dir : str | Path | None, optional
            Directory containing .csv files. If None, uses default path.
        groq_api_key : str | None, optional
            Groq API key. If None, uses environment variable.
        groq_model : str | None, optional
            Groq model name. If None, uses environment variable.
        """
        # Initialize components
        self.file_router = FileRouter(
            output_dir=output_dir,
            preprocessed_dir=preprocessed_dir,
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
        
        self.query_generator = QueryGenerator(
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
        
        self.query_executor = QueryExecutor(
            query_generator=self.query_generator
        )
    
    def process_query(
        self,
        user_question: str,
        preprocess: bool = True,
        return_txt: bool = True,
        return_csv: bool = True
    ) -> Dict[str, Any]:
        """
        Complete workflow: Route query to files, generate pandas query, and execute it.
        
        Parameters:
        -----------
        user_question : str
            Natural language question
        preprocess : bool, optional
            Whether to preprocess dataframe. Default is True.
        return_txt : bool, optional
            Whether to return txt files. Default is True.
        return_csv : bool, optional
            Whether to return csv files. Default is True.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary containing:
            - 'success': Boolean indicating success
            - 'results': List of query results (one per matched CSV file)
            - 'matched_files': Dictionary with matched txt and csv files
            - 'queries': List of generated queries
            - 'errors': List of error messages
        """
        print("\n" + "=" * 80)
        print("QUERY PIPELINE")
        print("=" * 80)
        
        # Step 1: Route query to files
        matched_files = self.file_router.route(
            user_query=user_question,
            return_txt=return_txt,
            return_csv=return_csv
        )
        
        txt_files = matched_files.get("txt_files", [])
        csv_files = matched_files.get("csv_files", [])
        
        if not csv_files:
            return {
                'success': False,
                'results': [],
                'matched_files': matched_files,
                'queries': [],
                'errors': ['No CSV files matched the query']
            }
        
        # Step 2: Process each CSV file with its corresponding TXT file
        results = []
        queries = []
        errors = []
        
        for csv_file in csv_files:
            csv_path = Path(csv_file)
            csv_stem = csv_path.stem
            
            # Find corresponding TXT file
            txt_file = None
            for txt_path in txt_files:
                if Path(txt_path).stem == csv_stem:
                    txt_file = txt_path
                    break
            
            if not txt_file:
                errors.append(f"No corresponding TXT file found for {csv_path.name}")
                continue
            
            print(f"\n{'='*80}")
            print(f"Processing: {csv_path.name}")
            print(f"{'='*80}")
            
            # Execute query
            result_dict = self.query_executor.execute_from_query_string(
                user_question=user_question,
                csv_file_path=csv_file,
                txt_file_path=txt_file,
                preprocess=preprocess
            )
            
            if result_dict['success']:
                results.append({
                    'csv_file': csv_file,
                    'txt_file': txt_file,
                    'result': result_dict['result'],
                    'query': result_dict['query']
                })
                queries.append(result_dict['query'])
            else:
                errors.append(f"Error processing {csv_path.name}: {result_dict['error']}")
        
        # Determine overall success
        success = len(results) > 0
        
        return {
            'success': success,
            'results': results,
            'matched_files': matched_files,
            'queries': queries,
            'errors': errors
        }
    
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
            output.append("\n✓ Query executed successfully!\n")
            
            for i, result_info in enumerate(pipeline_result['results'], 1):
                output.append(f"\n{'='*80}")
                output.append(f"Result {i}: {Path(result_info['csv_file']).name}")
                output.append(f"{'='*80}")
                output.append(f"\nQuery: {result_info['query']}\n")
                output.append(self.query_executor.format_result(result_info['result']))
        else:
            output.append("\n❌ Query execution failed\n")
            for error in pipeline_result['errors']:
                output.append(f"  - {error}")
        
        return "\n".join(output)


# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = QueryPipeline()
    
    # Process a query
    result = pipeline.process_query("In irby jan report Show me top 5 highest search volume")
    
    # Display results
    print(pipeline.format_results(result))
