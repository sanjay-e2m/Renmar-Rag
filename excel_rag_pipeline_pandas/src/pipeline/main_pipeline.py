"""
Main Pipeline
Orchestrates the complete query pipeline and formats results using LLM
"""

import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from groq import Groq
import sys

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.llm.query_pipeline import QueryPipeline

# Load environment variables
load_dotenv()

# Get configuration from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not GROQ_API_KEY:
    raise Exception("❌ Missing GROQ_API_KEY in environment variables")


class ResponseFormatter:
    """
    Formats query results using LLM to create user-friendly answers.
    """
    
    def __init__(
        self,
        groq_api_key: str | None = None,
        groq_model: str | None = None
    ):
        """
        Initialize ResponseFormatter.
        
        Parameters:
        -----------
        groq_api_key : str | None, optional
            Groq API key. If None, uses environment variable.
        groq_model : str | None, optional
            Groq model name. If None, uses environment variable.
        """
        api_key = groq_api_key or GROQ_API_KEY
        model = groq_model or GROQ_MODEL
        
        if not api_key:
            raise Exception("❌ Missing GROQ_API_KEY")
        
        try:
            self.client = Groq(api_key=api_key)
            self.model = model
        except Exception as e:
            print(f"⚠ Warning: Could not configure Groq model: {e}")
            self.client = None
    
    def dataframe_to_string(self, result: Any) -> str:
        """
        Convert query result to string representation.
        
        Parameters:
        -----------
        result : Any
            Query result (DataFrame, Series, array, or scalar)
        
        Returns:
        --------
        str
            String representation of the result
        """
        if isinstance(result, pd.DataFrame):
            if len(result) == 0:
                return "Empty DataFrame (no rows)"
            # Limit to first 50 rows for context
            display_df = result.head(50) if len(result) > 50 else result
            return f"DataFrame with {len(result)} rows × {len(result.columns)} columns:\n{display_df.to_string()}"
        elif isinstance(result, pd.Series):
            if len(result) == 0:
                return "Empty Series"
            # Limit to first 50 values
            display_series = result.head(50) if len(result) > 50 else result
            return f"Series with {len(result)} values:\n{display_series.to_string()}"
        elif isinstance(result, np.ndarray):
            return f"Array with {len(result)} items:\n{result}"
        else:
            return f"Result: {result}"
    
    def format_result(
        self,
        user_question: str,
        result: Any,
        query: str | None = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Format query result using LLM to create a user-friendly answer.
        
        Parameters:
        -----------
        user_question : str
            Original user question
        result : Any
            Query result (DataFrame, Series, array, or scalar)
        query : str | None, optional
            The pandas query that was executed
        max_retries : int, optional
            Maximum number of retry attempts for API calls
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary containing:
            - 'formatted_answer': User-friendly formatted answer
            - 'summary': Brief summary
            - 'insights': List of key insights
            - 'raw_result': Original result
        """
        if self.client is None:
            # For list-type questions, format directly
            if isinstance(result, pd.Series) and len(result) > 0 and 'list' in user_question.lower():
                answer = "Here is the complete list:\n"
                for i, val in enumerate(result.tolist(), 1):
                    answer += f"{i}. {val}\n"
                return {
                    'formatted_answer': answer.strip(),
                    'insights': [],
                    'raw_result': result
                }
            
            return {
                'formatted_answer': self.dataframe_to_string(result),
                'insights': [],
                'raw_result': result
            }
        
        # Convert result to string for LLM context
        result_str = self.dataframe_to_string(result)
        
        # Determine result type and metadata
        if isinstance(result, pd.DataFrame):
            result_type = "dataframe"
            row_count = len(result)
            col_count = len(result.columns)
            columns = list(result.columns)
        elif isinstance(result, pd.Series):
            result_type = "series"
            row_count = len(result)
            col_count = 1
            columns = [result.name] if result.name else ["Value"]
        elif isinstance(result, np.ndarray):
            result_type = "array"
            row_count = len(result)
            col_count = 1
            columns = ["Value"]
        else:
            result_type = "scalar"
            row_count = 1
            col_count = 1
            columns = ["Result"]
        
        # Extract actual data values for list-type questions
        actual_values = None
        if isinstance(result, pd.Series):
            actual_values = result.tolist()
        elif isinstance(result, pd.DataFrame):
            # If single column, extract values
            if len(result.columns) == 1:
                actual_values = result.iloc[:, 0].tolist()
            else:
                # Convert to list of dicts for multi-column
                actual_values = result.to_dict('records')
        elif isinstance(result, np.ndarray):
            actual_values = result.tolist()
        
        # Create prompt for LLM
        prompt = f"""You are a data analysis assistant. Format the query result into a direct, specific answer to the user's question.

USER'S ORIGINAL QUESTION:
"{user_question}"

EXECUTED QUERY:
{query if query else "N/A"}

QUERY RESULT:
{result_str}

RESULT METADATA:
- Type: {result_type}
- Rows: {row_count}
- Columns: {col_count}
- Column Names: {columns}

CRITICAL INSTRUCTIONS:
1. Answer the user's question DIRECTLY and SPECIFICALLY
2. If the question asks for a "list" of items, provide the ACTUAL COMPLETE LIST from the data
3. If the question asks for specific values, show ALL the values, not a summary
4. Be precise - include actual data values from the result
5. If asking for URLs, keywords, names, etc., list them all
6. Do NOT summarize or truncate - show the complete answer
7. Format lists clearly (one item per line or numbered list)
8. Use natural language but be specific

EXAMPLES:
- Question: "give me list of URLs" → Answer: List all URLs from the data
- Question: "which keyword has highest search volume" → Answer: The specific keyword name
- Question: "top 5 keywords" → Answer: List all 5 keywords with their values

Generate a JSON response with this structure:
{{
    "formatted_answer": "Direct, specific answer with all requested data. If question asks for a list, provide the complete list.",
    "insights": [
        "Key insight 1 with specific values",
        "Key insight 2 with specific values"
    ]
}}

Rules:
- formatted_answer MUST directly answer the question with specific data
- If question asks for a list, include ALL items from the result
- Be specific - use actual values from the data
- Do NOT summarize or say "and X more" - show everything
- Output ONLY valid JSON, no markdown, no code blocks

JSON Response:"""

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a data analysis assistant. You output ONLY valid JSON responses for formatting data results. No markdown, no code blocks, just pure JSON."
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3  # Slightly higher for more natural language
                )
                
                formatted_response = response.choices[0].message.content.strip()
                
                # Clean up response - remove markdown if present
                if formatted_response.startswith("```json"):
                    formatted_response = formatted_response.replace("```json", "").replace("```", "").strip()
                elif formatted_response.startswith("```"):
                    formatted_response = formatted_response.replace("```", "").strip()
                
                # Parse JSON response
                formatted_data = json.loads(formatted_response)
                
                # Ensure formatted_answer contains actual data for list-type questions
                if isinstance(result, pd.Series) and len(result) > 0:
                    # If result is a Series and question asks for a list, append actual values
                    if 'list' in user_question.lower() or 'url' in user_question.lower() or 'keyword' in user_question.lower():
                        values_list = result.tolist()
                        if len(values_list) <= 50:  # If reasonable size, add to answer
                            formatted_answer = formatted_data.get('formatted_answer', '')
                            # Append actual list if not already included
                            if str(values_list[0]) not in formatted_answer:
                                formatted_answer += f"\n\nComplete list:\n"
                                for i, val in enumerate(values_list, 1):
                                    formatted_answer += f"{i}. {val}\n"
                                formatted_data['formatted_answer'] = formatted_answer.strip()
                
                elif isinstance(result, pd.DataFrame) and len(result) > 0:
                    # If single column DataFrame and question asks for a list
                    if len(result.columns) == 1 and ('list' in user_question.lower() or 'url' in user_question.lower()):
                        values_list = result.iloc[:, 0].tolist()
                        if len(values_list) <= 50:
                            formatted_answer = formatted_data.get('formatted_answer', '')
                            if str(values_list[0]) not in formatted_answer:
                                formatted_answer += f"\n\nComplete list:\n"
                                for i, val in enumerate(values_list, 1):
                                    formatted_answer += f"{i}. {val}\n"
                                formatted_data['formatted_answer'] = formatted_answer.strip()
                
                # Remove summary if present (we don't want it)
                if 'summary' in formatted_data:
                    del formatted_data['summary']
                
                # Add raw result reference
                formatted_data['raw_result'] = result
                
                return formatted_data
                
            except json.JSONDecodeError as e:
                print(f"⚠ JSON parsing error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    continue
                else:
                    # Return basic formatted response if JSON parsing fails
                    # For list-type questions, try to format directly
                    if isinstance(result, pd.Series) and len(result) > 0:
                        if 'list' in user_question.lower():
                            answer = "Here is the complete list:\n"
                            for i, val in enumerate(result.tolist(), 1):
                                answer += f"{i}. {val}\n"
                            return {
                                'formatted_answer': answer.strip(),
                                'insights': [],
                                'raw_result': result
                            }
                    
                    return {
                        'formatted_answer': result_str,
                        'insights': [],
                        'raw_result': result
                    }
                    
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "quota" in error_str.lower() or "rate limit" in error_str.lower():
                    if attempt < max_retries - 1:
                        import time
                        wait_time = 60 * (attempt + 1)
                        print(f"⚠ API quota exceeded. Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                        time.sleep(wait_time)
                        continue
                
                print(f"⚠ Error formatting result: {error_str}")
                # Try to format directly for list-type questions
                if isinstance(result, pd.Series) and len(result) > 0 and 'list' in user_question.lower():
                    answer = "Here is the complete list:\n"
                    for i, val in enumerate(result.tolist(), 1):
                        answer += f"{i}. {val}\n"
                    return {
                        'formatted_answer': answer.strip(),
                        'insights': [],
                        'raw_result': result
                    }
                
                return {
                    'formatted_answer': result_str,
                    'insights': [],
                    'raw_result': result
                }
        
        return {
            'formatted_answer': result_str,
            'insights': [],
            'raw_result': result
        }


class MainPipeline:
    """
    Main pipeline that orchestrates query execution and result formatting.
    """
    
    def __init__(
        self,
        output_dir: str | Path | None = None,
        preprocessed_dir: str | Path | None = None,
        groq_api_key: str | None = None,
        groq_model: str | None = None
    ):
        """
        Initialize MainPipeline.
        
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
        # Initialize query pipeline
        self.query_pipeline = QueryPipeline(
            output_dir=output_dir,
            preprocessed_dir=preprocessed_dir,
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
        
        # Initialize response formatter
        self.formatter = ResponseFormatter(
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
    
    def process_and_format(
        self,
        user_question: str,
        preprocess: bool = True,
        return_txt: bool = True,
        return_csv: bool = True
    ) -> Dict[str, Any]:
        """
        Process query and format results using LLM.
        
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
            - 'formatted_results': List of formatted results
            - 'raw_results': Original query pipeline results
            - 'user_question': Original question
        """
        print("\n" + "=" * 80)
        print("MAIN PIPELINE - Query Processing & Formatting")
        print("=" * 80)
        
        # Step 1: Execute query
        pipeline_result = self.query_pipeline.process_query(
            user_question=user_question,
            preprocess=preprocess,
            return_txt=return_txt,
            return_csv=return_csv
        )
        
        if not pipeline_result['success']:
            return {
                'success': False,
                'formatted_results': [],
                'raw_results': pipeline_result,
                'user_question': user_question,
                'error': 'Query execution failed'
            }
        
        # Step 2: Format each result using LLM
        formatted_results = []
        
        print("\n" + "=" * 80)
        print("Formatting Results with LLM")
        print("=" * 80)
        
        for i, result_info in enumerate(pipeline_result['results'], 1):
            print(f"\nFormatting result {i}/{len(pipeline_result['results'])}...")
            
            formatted_result = self.formatter.format_result(
                user_question=user_question,
                result=result_info['result'],
                query=result_info.get('query')
            )
            
            # Add metadata
            formatted_result['csv_file'] = result_info['csv_file']
            formatted_result['txt_file'] = result_info['txt_file']
            formatted_result['query'] = result_info.get('query')
            
            formatted_results.append(formatted_result)
        
        return {
            'success': True,
            'formatted_results': formatted_results,
            'raw_results': pipeline_result,
            'user_question': user_question
        }
    
    def display_formatted_results(self, result: Dict[str, Any]) -> None:
        """
        Display formatted results in a user-friendly way.
        
        Parameters:
        -----------
        result : Dict[str, Any]
            Result dictionary from process_and_format()
        """
        if not result['success']:
            print("\n❌ Query processing failed")
            if 'error' in result:
                print(f"Error: {result['error']}")
            return
        
        print("\n" + "=" * 80)
        print("FORMATTED RESULTS")
        print("=" * 80)
        
        for i, formatted_result in enumerate(result['formatted_results'], 1):
            print(f"\n{'='*80}")
            print(f"Result {i}: {Path(formatted_result['csv_file']).name}")
            print(f"{'='*80}")
            
            print(f"\n💡 Answer:")
            # Print answer with proper formatting (preserve newlines)
            answer = formatted_result['formatted_answer']
            print(answer)
            
            if formatted_result.get('insights'):
                print(f"\n🔍 Key Insights:")
                for j, insight in enumerate(formatted_result['insights'], 1):
                    print(f"  {j}. {insight}")
            
            if formatted_result.get('query'):
                print(f"\n🔧 Executed Query:")
                print(f"  {formatted_result['query']}")


# Example usage
if __name__ == "__main__":
    # Initialize main pipeline
    main_pipeline = MainPipeline()
    
    # Process and format a query
    result = main_pipeline.process_and_format(
        "for Stewart Moving give me list of url having the search intent 'Informational'?"
    )
    
    # Display formatted results
    main_pipeline.display_formatted_results(result)
