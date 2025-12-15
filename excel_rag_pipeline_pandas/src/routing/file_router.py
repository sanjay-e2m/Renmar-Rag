"""
File Router using LLM
Uses Groq LLM to match user queries to appropriate files from output and preprocessed folders
"""

import os
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher
from dotenv import load_dotenv
from groq import Groq

# Load environment variables
load_dotenv()

# Get configuration from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not GROQ_API_KEY:
    raise Exception("❌ Missing GROQ_API_KEY in environment variables (set GROQ_API_KEY in .env file)")


class FileRouter:
    """
    Routes user queries to appropriate files using LLM.
    Matches queries to files in both output (.txt) and preprocessed (.csv) folders.
    """
    
    def __init__(
        self,
        output_dir: str | Path | None = None,
        preprocessed_dir: str | Path | None = None,
        groq_api_key: str | None = None,
        groq_model: str | None = None
    ):
        """
        Initialize FileRouter.
        
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
        # Set up directories
        script_dir = Path(__file__).parent.resolve()
        base_dir = script_dir.parent.parent
        
        self.output_dir = Path(output_dir) if output_dir else base_dir / "data" / "output"
        self.preprocessed_dir = Path(preprocessed_dir) if preprocessed_dir else base_dir / "data" / "preprocessed"
        
        # Initialize Groq client
        api_key = groq_api_key or GROQ_API_KEY
        model = groq_model or GROQ_MODEL
        
        if not api_key:
            raise Exception("❌ Missing GROQ_API_KEY")
        
        self.client = Groq(api_key=api_key)
        self.model = model
        
        # Cache for file lists
        self._txt_files_cache: List[Dict[str, str]] = []
        self._csv_files_cache: List[Dict[str, str]] = []
    
    def _fuzzy_match(self, query_word: str, filename: str, threshold: float = 0.6) -> bool:
        """
        Check if a query word fuzzy matches a filename.
        
        Parameters:
        -----------
        query_word : str
            Word from user query
        filename : str
            Filename to match against
        threshold : float
            Similarity threshold (0.0 to 1.0). Default 0.6.
        
        Returns:
        --------
        bool
            True if similarity is above threshold
        """
        query_lower = query_word.lower()
        filename_lower = filename.lower()
        
        # Direct substring match
        if query_lower in filename_lower or filename_lower in query_lower:
            return True
        
        # Extract meaningful words from filename
        stop_words = {'jan', 'report', 'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for', 'storage', 'moving', 'copy'}
        filename_words = [w for w in filename_lower.replace('_', ' ').replace('-', ' ').replace('&', ' ').split() 
                         if w not in stop_words and len(w) > 2]
        
        # Check similarity with each meaningful word in filename
        for word in filename_words:
            similarity = SequenceMatcher(None, query_lower, word).ratio()
            if similarity >= threshold:
                return True
        
        return False
    
    def _extract_company_names_from_query(self, query: str, available_names: List[str]) -> List[str]:
        """
        Extract potential company/file names from query using fuzzy matching.
        
        Parameters:
        -----------
        query : str
            User query
        available_names : List[str]
            List of available file names
        
        Returns:
        --------
        List[str]
            List of matched file names
        """
        query_lower = query.lower()
        matched_names = []
        
        # Extract words from query (skip common words)
        stop_words = {'show', 'me', 'data', 'about', 'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for', 
                     'what', 'is', 'in', 'give', 'compare', 'vs', 'versus', 'with', 'top', 'highest', 'search', 'volume',
                     'information', 'report', 'query', 'tell', 'get', 'find'}
        query_words = [w for w in query_lower.split() if w not in stop_words and len(w) > 2]
        
        # For each query word, find best matching file name
        for word in query_words:
            best_match = None
            best_similarity = 0.0
            
            for name in available_names:
                if self._fuzzy_match(word, name, threshold=0.5):
                    # Calculate overall similarity
                    similarity = SequenceMatcher(None, word, name.lower()).ratio()
                    if similarity > best_similarity:
                        best_similarity = similarity
                        best_match = name
            
            if best_match and best_match not in matched_names and best_similarity >= 0.5:
                matched_names.append(best_match)
        
        return matched_names
    
    def get_all_files(self) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """
        Retrieve all files from output and preprocessed directories.
        
        Returns:
        --------
        Tuple[List[Dict], List[Dict]]
            Tuple of (txt_files, csv_files) where each file is a dict with 'name' and 'path' keys
        """
        txt_files = []
        csv_files = []
        
        # Get .txt files from output directory
        if self.output_dir.exists():
            for txt_file in self.output_dir.glob("*.txt"):
                txt_files.append({
                    "name": txt_file.stem,  # filename without extension
                    "path": str(txt_file.resolve())
                })
        
        # Get .csv files from preprocessed directory
        if self.preprocessed_dir.exists():
            for csv_file in self.preprocessed_dir.glob("*.csv"):
                csv_files.append({
                    "name": csv_file.stem,  # filename without extension
                    "path": str(csv_file.resolve())
                })
        
        # Cache the results
        self._txt_files_cache = txt_files
        self._csv_files_cache = csv_files
        
        return txt_files, csv_files
    
    def match_files_with_llm(
        self,
        user_query: str,
        txt_files: List[Dict[str, str]] | None = None,
        csv_files: List[Dict[str, str]] | None = None
    ) -> Dict[str, List[str]]:
        """
        Use LLM to match user query to appropriate files by filename only.
        Supports single or multiple file names (up to 3) for comparison queries.
        Returns file paths based on matched filenames.
        
        Parameters:
        -----------
        user_query : str
            User's natural language query (can contain single or multiple file names for comparison)
        txt_files : List[Dict] | None, optional
            List of txt files. If None, retrieves from directory.
        csv_files : List[Dict] | None, optional
            List of csv files. If None, retrieves from directory.
        
        Returns:
        --------
        Dict[str, List[str]]
            Dictionary with 'txt_files' and 'csv_files' keys containing matched file paths,
            or empty lists if no match found
        """
        # Get files if not provided
        if txt_files is None or csv_files is None:
            txt_files, csv_files = self.get_all_files()
        
        if not txt_files and not csv_files:
            return {"txt_files": [], "csv_files": []}
        
        # Extract just the filenames (without extension) for LLM
        txt_filenames = [f["name"] for f in txt_files]
        csv_filenames = [f["name"] for f in csv_files]
        
        # Create prompt that handles single and multiple file names (up to 3)
        prompt = f"""You are an intelligent file routing assistant. Your task is to understand natural language queries and match them to specific file names from the available files list.

Available TXT file names:
{chr(10).join([f"  - {name}" for name in txt_filenames]) if txt_filenames else "  (none)"}

Available CSV file names:
{chr(10).join([f"  - {name}" for name in csv_filenames]) if csv_filenames else "  (none)"}

USER QUERY: "{user_query}"

CRITICAL UNDERSTANDING RULES:

1. DISTINGUISH SINGLE vs MULTIPLE FILE QUERIES:
   - SINGLE FILE: Query mentions ONE specific name/company (e.g., "Show me data about X", "What's in X report", "Give me X data")
     → Return ONLY that one file (both CSV and TXT)
   - MULTIPLE FILES: Query explicitly compares/mentions multiple names (e.g., "compare X and Y", "X vs Y", "X and Y data", "for X and Y")
     → Return files for ALL mentioned names (up to 3 maximum)

2. NATURAL LANGUAGE UNDERSTANDING:
   - Understand context: "Show me data about Irby" = SINGLE file (only Irby)
   - Understand comparisons: "Compare Irby and Stewart" = MULTIPLE files (both)
   - Understand lists: "for Irby and Stewart give me..." = MULTIPLE files (both)
   - Be precise: If query mentions only ONE name, return ONLY that file

3. FUZZY MATCHING & TYPO TOLERANCE:
   - Match even with typos: "irrby" should match "Irby", "stewart" should match "Stewart Moving"
   - Partial matches work: "Irby" matches "Irby_Jan_ report"
   - Extract meaningful words from file names (ignore: "Jan", "report", "the", "a", etc.)
   - Match company/client names flexibly

4. EXTRACTION RULES:
   - Extract company/client names from the query
   - Match extracted names to available file names (with typo tolerance)
   - For single file queries: Return ONLY the best matching file
   - For comparison queries: Return ALL mentioned files (up to 3)

5. VALIDATION:
   - If query is generic (no specific company/client name mentioned): Return empty arrays
   - If query mentions a name but it doesn't match any available file: Return empty arrays
   - If single file query but multiple files matched: Return ONLY the best/closest match

6. MAXIMUM LIMIT:
   - Maximum 3 file names can be matched in a single query

Return ONLY a JSON response with this exact structure:
{{
    "txt_files": ["filename1", "filename2", "filename3"],
    "csv_files": ["filename1", "filename2", "filename3"]
}}

IMPORTANT EXAMPLES:
- Query: "Show me data about Irby" → SINGLE file query → {{"txt_files": ["Irby_Jan_ report"], "csv_files": ["Irby_Jan_ report"]}}
- Query: "What's in Irby report?" → SINGLE file query → {{"txt_files": ["Irby_Jan_ report"], "csv_files": ["Irby_Jan_ report"]}}
- Query: "Compare Irby and Stewart" → MULTIPLE files → {{"txt_files": ["Irby_Jan_ report", "Stewart Moving &Storage_Jan_ report"], "csv_files": ["Irby_Jan_ report", "Stewart Moving &Storage_Jan_ report"]}}
- Query: "for Irby and Stewart give me data" → MULTIPLE files → {{"txt_files": ["Irby_Jan_ report", "Stewart Moving &Storage_Jan_ report"], "csv_files": ["Irby_Jan_ report", "Stewart Moving &Storage_Jan_ report"]}}
- Query: "Show me top 5 highest" → NO specific file name → {{"txt_files": [], "csv_files": []}}
- Query: "irrby data" → TYPO but should match "Irby" → {{"txt_files": ["Irby_Jan_ report"], "csv_files": ["Irby_Jan_ report"]}}

CRITICAL: 
- For SINGLE file queries, return ONLY ONE file (the best match)
- For MULTIPLE file queries, return ALL mentioned files
- Use fuzzy matching to handle typos
- Return BOTH txt_files and csv_files for EACH matched name

If the query does NOT contain any specific file/company name that matches the available file names, return:
{{
    "txt_files": [],
    "csv_files": []
}}

JSON Response:"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a file routing assistant. You output ONLY valid JSON with file names. No markdown, no explanations, no paths - just file names in JSON format."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0  # Low temperature for deterministic output
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Clean up response - remove markdown if present
            if result_text.startswith("```json"):
                result_text = result_text.replace("```json", "").replace("```", "").strip()
            elif result_text.startswith("```"):
                result_text = result_text.replace("```", "").strip()
            
            # Parse JSON response
            result = json.loads(result_text)
            
            # Create mapping from names to paths
            txt_name_to_path = {f["name"]: f["path"] for f in txt_files}
            csv_name_to_path = {f["name"]: f["path"] for f in csv_files}
            
            # Map matched filenames to actual paths
            matched_txt_paths = []
            matched_csv_paths = []
            
            # Get matched filenames from LLM response
            matched_txt_names = result.get("txt_files", [])
            matched_csv_names = result.get("csv_files", [])
            
            # Combine all matched names and get unique list
            all_matched_names = matched_txt_names + matched_csv_names
            unique_matched_names = list(dict.fromkeys(all_matched_names))  # Preserve order, remove duplicates
            
            # VALIDATION: Use fuzzy matching to validate and filter matches
            query_lower = user_query.lower()
            
            # Check if query is a comparison query (mentions multiple names explicitly)
            comparison_keywords = ['compare', 'vs', 'versus', 'both', 'all']
            is_comparison_query = any(keyword in query_lower for keyword in comparison_keywords) or \
                                 (query_lower.count(' and ') > 0 and any(word in query_lower for word in ['give', 'show', 'get', 'data', 'report']))
            
            # Extract potential company names from query using fuzzy matching
            all_available_names = txt_filenames + csv_filenames
            fuzzy_matched_names = self._extract_company_names_from_query(user_query, all_available_names)
            
            # Validate LLM matches using fuzzy matching
            validated_names = []
            for matched_name in unique_matched_names:
                # Check if this matched name has any similarity to query words
                is_valid = False
                query_words = [w for w in query_lower.split() if len(w) > 2]
                
                for word in query_words:
                    if self._fuzzy_match(word, matched_name, threshold=0.5):
                        is_valid = True
                        break
                
                # Also check if fuzzy matching found this name
                if matched_name in fuzzy_matched_names:
                    is_valid = True
                
                if is_valid:
                    validated_names.append(matched_name)
            
            # If no validated matches, try using fuzzy matching results
            if not validated_names and fuzzy_matched_names:
                validated_names = fuzzy_matched_names[:3]  # Limit to 3
            
            # For single file queries, return only the best match
            if not is_comparison_query and len(validated_names) > 1:
                # Find the best match (highest similarity)
                best_match = None
                best_score = 0.0
                query_words = [w for w in query_lower.split() if len(w) > 2]
                
                for name in validated_names:
                    name_lower = name.lower()
                    # Calculate similarity score
                    score = 0.0
                    for word in query_words:
                        if word in name_lower:
                            score += 1.0
                        else:
                            # Check fuzzy similarity
                            for name_word in name_lower.split():
                                similarity = SequenceMatcher(None, word, name_word).ratio()
                                score += similarity
                    
                    if score > best_score:
                        best_score = score
                        best_match = name
                
                if best_match:
                    validated_names = [best_match]
                    print(f"ℹ Single file query detected. Returning best match: {best_match}")
            
            # Limit to maximum 3 unique file names
            if len(validated_names) > 3:
                print(f"⚠ Warning: {len(validated_names)} file names matched, but maximum is 3. Limiting to first 3 matches.")
                validated_names = validated_names[:3]
            
            # Filter matched names to only include validated names
            matched_txt_names = [name for name in matched_txt_names if name in validated_names]
            matched_csv_names = [name for name in matched_csv_names if name in validated_names]
            
            # If no valid matches found, return empty
            if not validated_names:
                print(f"⚠ No valid file matches found for query. Returning empty results.")
                return {
                    "txt_files": [],
                    "csv_files": []
                }
            
            # Map TXT filenames to paths
            for filename in matched_txt_names:
                # Remove any extensions if present
                filename_clean = Path(filename).stem
                if filename_clean in txt_name_to_path:
                    matched_txt_paths.append(txt_name_to_path[filename_clean])
                elif filename in txt_name_to_path:
                    matched_txt_paths.append(txt_name_to_path[filename])
            
            # Map CSV filenames to paths
            for filename in matched_csv_names:
                # Remove any extensions if present
                filename_clean = Path(filename).stem
                if filename_clean in csv_name_to_path:
                    matched_csv_paths.append(csv_name_to_path[filename_clean])
                elif filename in csv_name_to_path:
                    matched_csv_paths.append(csv_name_to_path[filename])
            
            # Return paths (empty if no matches)
            return {
                "txt_files": matched_txt_paths,
                "csv_files": matched_csv_paths
            }
            
        except json.JSONDecodeError as e:
            print(f"⚠ Error parsing LLM response as JSON: {e}")
            print(f"Response was: {result_text}")
            return {"txt_files": [], "csv_files": []}
        except Exception as e:
            print(f"⚠ Error matching files with LLM: {e}")
            return {"txt_files": [], "csv_files": []}
    
    def route(
        self,
        user_query: str,
        return_txt: bool = True,
        return_csv: bool = True
    ) -> Dict[str, List[str]]:
        """
        Main routing function that matches user query to files.
        Supports single file queries or comparison queries with up to 3 file names.
        
        Parameters:
        -----------
        user_query : str
            User's natural language query. Can contain:
            - Single file name: "Show me data about Irby"
            - Multiple file names for comparison: "Compare Irby and Stewart", "Irby vs Perfect Smiles"
            - Up to 3 file names maximum: "Compare xyz, abc, and def"
        return_txt : bool, optional
            Whether to return txt files. Default is True.
        return_csv : bool, optional
            Whether to return csv files. Default is True.
        
        Returns:
        --------
        Dict[str, List[str]]
            Dictionary with 'txt_files' and/or 'csv_files' keys containing matched file paths.
            For comparison queries, returns files for all mentioned names (up to 3).
        """
        print(f"\n🔍 Routing query: '{user_query}'")
        print("=" * 80)
        
        # Get all files
        txt_files, csv_files = self.get_all_files()
        
        print(f"📁 Found {len(txt_files)} TXT file(s) and {len(csv_files)} CSV file(s)")
        
        if not txt_files and not csv_files:
            print("⚠ No files found in output or preprocessed directories")
            return {"txt_files": [], "csv_files": []}
        
        # Match files using LLM
        matched_files = self.match_files_with_llm(user_query, txt_files, csv_files)
        
        # Filter based on return preferences
        result = {}
        if return_txt:
            result["txt_files"] = matched_files.get("txt_files", [])
        if return_csv:
            result["csv_files"] = matched_files.get("csv_files", [])
        
        # Print results
        print("\n📋 Matched Files:")
        
        if result.get("txt_files"):
            print(f"\n  TXT files ({len(result['txt_files'])}):")
            for txt_file in result["txt_files"]:
                print(f"    - {Path(txt_file).name}")
        else:
            print("\n  TXT files: ❌ Not found")
        
        if result.get("csv_files"):
            print(f"\n  CSV files ({len(result['csv_files'])}):")
            for csv_file in result["csv_files"]:
                print(f"    - {Path(csv_file).name}")
        else:
            print("\n  CSV files: ❌ Not found")
        
        if not result.get("txt_files") and not result.get("csv_files"):
            print("\n  ⚠ No files matched the query. Please mention a specific file or company name.")
        
        print("=" * 80)
        
        return result


# Example usage
if __name__ == "__main__":
    # Initialize router
    router = FileRouter()
    
    # Example queries
    test_queries = [
        "Show me data about Irby",
        "What's in the Sttewqwart Moving report?",
        "for Irby and Stewart Moving give me the search volumn for last 3 months",
        "Compare Irrrby vs Perfect Smiles vs Reyco"
    ]
    
    for query in test_queries:
        result = router.route(query)
        print(f"\nResult: {result}\n")
