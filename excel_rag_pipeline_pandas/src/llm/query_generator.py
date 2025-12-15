"""
Query Generator using LLM
Generates pandas queries from natural language using context from TXT files
"""

import os
import re
import time
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from groq import Groq
import pandas as pd

# Load environment variables
load_dotenv()

# Get configuration from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not GROQ_API_KEY:
    raise Exception("❌ Missing GROQ_API_KEY in environment variables (set GROQ_API_KEY in .env file)")


class QueryGenerator:
    """
    Generates pandas queries from natural language questions using LLM.
    Uses context from TXT files to understand the data structure.
    """
    
    def __init__(
        self,
        groq_api_key: str | None = None,
        groq_model: str | None = None
    ):
        """
        Initialize QueryGenerator.
        
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
            print(f"✓ Groq model configured: {self.model}")
        except Exception as e:
            print(f"⚠ Warning: Could not configure Groq model: {e}")
            self.client = None
    
    def load_context_file(self, txt_file_path: str | Path, max_chars: int = 3000) -> str:
        """
        Load context from TXT file, limiting to first max_chars characters.
        
        Parameters:
        -----------
        txt_file_path : str | Path
            Path to the TXT context file
        max_chars : int, optional
            Maximum number of characters to load. Default is 3000.
        
        Returns:
        --------
        str
            Content of the context file (truncated to max_chars)
        """
        txt_path = Path(txt_file_path)
        
        if not txt_path.exists():
            raise FileNotFoundError(f"Context file not found: {txt_path}")
        
        print(f"Reading context file: {txt_path.name}...")
        with open(txt_path, 'r', encoding='utf-8') as f:
            context_content = f.read()
        
        original_length = len(context_content)
        
        # Truncate to max_chars if needed
        if len(context_content) > max_chars:
            context_content = context_content[:max_chars]
            print(f"✓ Context file loaded ({original_length} characters, truncated to {len(context_content)} characters)")
        else:
            print(f"✓ Context file loaded ({len(context_content)} characters)")
        
        return context_content
    
    def get_sample_data_info(self, df: pd.DataFrame, max_samples: int = 3) -> str:
        """
        Get sample data information for each column to help the model understand data types.
        
        Parameters:
        -----------
        df : pd.DataFrame
            The dataframe to analyze
        max_samples : int, optional
            Maximum number of samples per column. Default is 3.
        
        Returns:
        --------
        str
            Formatted string with column information
        """
        info_lines = []
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].dropna()
            if len(non_null) > 0:
                samples = non_null.head(max_samples).tolist()
                samples_str = ", ".join([repr(s) for s in samples])
                info_lines.append(f"  - {repr(col)}: dtype={dtype}, samples=[{samples_str}]")
            else:
                info_lines.append(f"  - {repr(col)}: dtype={dtype}, samples=[]")
        return "\n".join(info_lines)
    
    def validate_and_fix_column_names(self, query: str, df: pd.DataFrame) -> str:
        """
        Validate column names in the query and fix common issues like spacing and newlines.
        
        Parameters:
        -----------
        query : str
            The pandas query string
        df : pd.DataFrame
            The dataframe with actual column names
        
        Returns:
        --------
        str
            Fixed query with corrected column names
        """
        # Get actual column names from dataframe
        actual_columns = list(df.columns)
        
        # Create a mapping: normalize variations to actual column names
        def normalize_col_name(col_name):
            """Normalize column name for matching (handles spacing, newlines, etc.)"""
            # Replace newlines with space, normalize whitespace
            normalized = ' '.join(col_name.replace('\n', ' ').split())
            return normalized.lower()
        
        # Build mapping from normalized names to actual column names
        column_mapping = {}
        for col in actual_columns:
            normalized = normalize_col_name(col)
            column_mapping[normalized] = col
        
        # Find all quoted strings in the query that might be column names
        pattern = r"['\"]([^'\"]+)['\"]"
        
        def replace_column(match):
            quoted_col = match.group(0)  # Full match including quotes
            col_name = match.group(1)     # Just the column name
            
            # If it's already a valid column name, return as is
            if col_name in actual_columns:
                return quoted_col
            
            # Try to find matching column
            normalized = normalize_col_name(col_name)
            if normalized in column_mapping:
                correct_col = column_mapping[normalized]
                # Use repr to properly escape the column name
                return repr(correct_col)
            
            return quoted_col  # Return original if no match found
        
        # Replace all potential column references
        fixed_query = re.sub(pattern, replace_column, query)
        
        return fixed_query
    
    def generate_pandas_query(
        self,
        user_question: str,
        context_content: str,
        df: pd.DataFrame,
        dataframe_name: str = "df",
        max_retries: int = 3
    ) -> Optional[str]:
        """
        Generate a pandas query from a natural language question using Groq.
        
        Parameters:
        -----------
        user_question : str
            The natural language question about the data
        context_content : str
            The context file content describing the dataframe columns
        df : pd.DataFrame
            The dataframe to query (used for column info)
        dataframe_name : str, optional
            The name of the dataframe variable (default: "df")
        max_retries : int, optional
            Maximum number of retry attempts for API calls
        
        Returns:
        --------
        str | None
            The pandas query as a string, or None if generation fails
        """
        if self.client is None:
            return None
        
        # Get column names from the dataframe with exact representation
        column_names_list = list(df.columns)
        column_names_repr = [repr(col) for col in column_names_list]
        
        # Create a mapping of column descriptions to exact names
        column_mapping = "\n".join([f"  - Column {i+1}: {repr(col)}" for i, col in enumerate(column_names_list)])
        
        column_info = f"\n\nEXACT COLUMN NAMES (use these EXACTLY as shown, including any special characters like \\n):\n{column_mapping}"
        column_dtypes = f"\n\nColumn data types:\n{df.dtypes.to_string()}"
        
        # Get sample data to help understand the data
        sample_info = f"\n\nSAMPLE DATA (showing first few values per column):\n{self.get_sample_data_info(df)}"
        
        # Add actual dataframe preview (first 5 rows) for better context
        df_preview = f"\n\nDATAFRAME PREVIEW (first 5 rows):\n{df.head().to_string()}"
        
        # Add dataframe shape info
        df_shape_info = f"\n\nDATAFRAME SHAPE: {df.shape[0]} rows × {df.shape[1]} columns"
        
        # Add unique values for key columns to help understand data
        unique_info = "\n\nUNIQUE VALUES IN KEY COLUMNS:\n"
        for col in df.columns[:5]:  # First 5 columns
            try:
                unique_vals = df[col].dropna().unique()[:10]  # First 10 unique values
                unique_info += f"  - {repr(col)}: {list(unique_vals)}\n"
            except:
                pass
        
        # Comprehensive examples for different query patterns
        examples = """
QUERY PATTERN EXAMPLES:

1. TOP N / HIGHEST / LARGEST (sorting descending):
   Question: "top 3 highest search volume"
   Query: df.nlargest(3, 'Search Volume')
   
   Question: "top 5 keywords by search volume"  
   Query: df.nlargest(5, 'Search Volume')[['Keywords', 'Search Volume']]
   
   Question: "highest ranked keywords"
   Query: df.nsmallest(10, 'Rank')  # Note: for ranks, smaller is better!

2. BOTTOM N / LOWEST / SMALLEST (sorting ascending):
   Question: "bottom 3 lowest search volume"
   Query: df.nsmallest(3, 'Search Volume')
   
   Question: "keywords with lowest rank"
   Query: df.nlargest(5, 'Rank')  # Note: for ranks, higher number = worse rank

3. SORTING:
   Question: "sort by search volume descending"
   Query: df.sort_values('Search Volume', ascending=False)
   
   Question: "sort by rank ascending"
   Query: df.sort_values('Rank', ascending=True)

4. FILTERING WITH CONDITIONS:
   Question: "keywords with search volume greater than 1000"
   Query: df[df['Search Volume'] > 1000]
   
   Question: "keywords where rank is less than 10"
   Query: df[df['Rank'] < 10]

5. COUNTING:
   Question: "how many keywords have search volume above 500"
   Query: len(df[df['Search Volume'] > 500])
   
   Question: "total number of keywords"
   Query: len(df)

6. AGGREGATIONS:
   Question: "average search volume"
   Query: df['Search Volume'].mean()
   
   Question: "sum of all search volumes"
   Query: df['Search Volume'].sum()
   
   Question: "max search volume"
   Query: df['Search Volume'].max()

7. SELECTING SPECIFIC COLUMNS:
   Question: "show keywords and search volume for top 5"
   Query: df.nlargest(5, 'Search Volume')[['Keywords', 'Search Volume']]

8. MULTIPLE CONDITIONS:
   Question: "keywords with search volume > 100 and rank < 20"
   Query: df[(df['Search Volume'] > 100) & (df['Rank'] < 20)]

9. UNIQUE VALUES:
   Question: "unique locations"
   Query: df['Location'].unique()
   
   Question: "how many unique keywords"
   Query: df['Keywords'].nunique()

10. GROUP BY:
    Question: "average search volume by location"
    Query: df.groupby('Location')['Search Volume'].mean()

11. "WHICH" QUESTIONS (finding the item with highest/lowest value):
    Question: "which keyword has the highest search volume"
    Query: df.loc[df['Search Volume'].idxmax(), 'Keywords']
    
    Question: "which keyword has the highest search volume"
    Query: df.nlargest(1, 'Search Volume')[['Keywords', 'Search Volume']]
    
    Question: "in irby jan which keyword has the highest search volume"
    Query: df.nlargest(1, 'Search Volume')[['Keywords', 'Search Volume']]
    Note: "in irby jan" refers to the file/report name, NOT a filter condition. The entire dataframe is from that report.

12. FINDING SPECIFIC VALUES:
    Question: "what is the highest search volume"
    Query: df['Search Volume'].max()
    
    Question: "show me the keyword with the highest search volume"
    Query: df.loc[df['Search Volume'].idxmax()]
"""
        
        prompt = f"""You are a pandas query generator expert. Generate precise pandas queries to answer questions about dataframes.

CONTEXT ABOUT THE DATAFRAME (from TXT file):
{context_content[:3000]}

DATAFRAME STRUCTURE:
{column_info}
{column_dtypes}
{df_shape_info}
{unique_info}
{sample_info}
{df_preview}

{examples}

CRITICAL RULES:
1. The dataframe variable is '{dataframe_name}'
2. Output ONLY the pandas query - NO explanations, NO comments, NO markdown, NO code blocks
3. Use column names EXACTLY as shown in EXACT COLUMN NAMES (including \\n if present)
4. For "top N highest/largest" questions: use df.nlargest(N, 'column_name')
5. For "bottom N lowest/smallest" questions: use df.nsmallest(N, 'column_name')
6. For "top N by rank" where lower rank is better: use df.nsmallest(N, 'rank_column')
7. NEVER use == for "highest/lowest/top/bottom" questions - use nlargest/nsmallest or sort_values
8. For numeric columns, ensure comparisons use numbers not strings (e.g., > 1000, not > '1000')
9. If the column has comma-separated numbers stored as strings, you may need to convert first
10. When returning top/bottom results, include relevant columns for context
11. For "which [item] has the highest/lowest [value]" questions: use df.nlargest(1, 'column') or df.loc[df['column'].idxmax()]
12. IMPORTANT: If the question mentions a file/report name (e.g., "in irby jan", "in stewart moving report"), this is just context about which file the data comes from. DO NOT filter by file name - the entire dataframe already contains data from that file.

IMPORTANT: Analyze the question carefully:
- "top" / "highest" / "largest" / "most" → use nlargest() or sort_values(ascending=False)
- "bottom" / "lowest" / "smallest" / "least" → use nsmallest() or sort_values(ascending=True)
- "which [item] has the highest [value]" → use df.nlargest(1, 'value_column')[['item_column', 'value_column']] or df.loc[df['value_column'].idxmax()]
- "greater than" / "more than" / "above" → use > operator
- "less than" / "below" / "under" → use < operator
- "in [file name]" → IGNORE this part, it's just context about the data source

QUESTION ANALYSIS:
- Question: "{user_question}"
- If question contains "in [file/report name]", treat it as context only, not a filter
- Focus on the actual question part after any file name mentions

USER QUESTION: {user_question}

PANDAS QUERY:"""

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system", 
                            "content": """You are an expert pandas query generator. You output ONLY executable pandas code, nothing else. No explanations, no markdown, no code blocks - just the raw pandas query.

CRITICAL: If the user question mentions a file/report name (e.g., "in irby jan", "in stewart moving report"), this is ONLY context about which file contains the data. The entire dataframe already contains data from that file. DO NOT add filters for file names - ignore those parts of the question and focus on the actual data question."""
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0  # Lower temperature for more deterministic output
                )
                pandas_query = response.choices[0].message.content.strip()
                
                # Clean up the response - remove markdown code blocks if present
                if pandas_query.startswith("```python"):
                    pandas_query = pandas_query.replace("```python", "").replace("```", "").strip()
                elif pandas_query.startswith("```"):
                    pandas_query = pandas_query.replace("```", "").strip()
                
                # Remove any leading/trailing backticks
                pandas_query = pandas_query.strip('`').strip()
                
                # Remove any "Query:" or similar prefixes
                for prefix in ['Query:', 'Answer:', 'Output:', 'Result:']:
                    if pandas_query.startswith(prefix):
                        pandas_query = pandas_query[len(prefix):].strip()
                
                # Validate and fix column names in the query
                pandas_query = self.validate_and_fix_column_names(pandas_query, df)
                
                # Post-process: Remove common mistakes
                # Remove filters that try to match file/report names in Location or other columns
                # This is a heuristic - if query seems to filter by file name, simplify it
                if "irby" in user_question.lower() or "stewart" in user_question.lower():
                    # Check if query has unnecessary location filters
                    import re
                    # If query filters by location matching file name, it's likely wrong
                    # But we'll let the validation handle column name issues
                    pass
                
                return pandas_query
                
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
    
    # Example: Load context and generate query
    # context = generator.load_context_file("path/to/context.txt")
    # df = pd.read_csv("path/to/data.csv")
    # query = generator.generate_pandas_query("top 5 highest values", context, df)
    # print(f"Generated query: {query}")
    pass
