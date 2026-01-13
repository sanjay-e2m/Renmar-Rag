"""
Query Formatter
Cleans and formats user queries before SQL generation
Handles spelling corrections, formatting, and input sanitization
"""

import os
import sys
import re
from pathlib import Path
from typing import Dict, Any, Optional
from groq import Groq
from dotenv import load_dotenv

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from rag_excel_postgres.postgres_insert_create.conversation_manager import ConversationManager

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


class QueryFormatter:
    """Formats and cleans user queries before SQL generation"""
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None
    ):
        """
        Initialize QueryFormatter.
        
        Parameters:
        -----------
        groq_api_key : Optional[str]
            Groq API key. If None, uses environment variable.
        groq_model : Optional[str]
            Groq model name. If None, uses environment variable.
        """
        api_key = groq_api_key or GROQ_API_KEY
        model = groq_model or GROQ_MODEL
        
        if api_key:
            try:
                self.client = Groq(api_key=api_key)
                self.model = model
                self.use_llm = True
            except Exception:
                self.client = None
                self.use_llm = False
        else:
            self.client = None
            self.use_llm = False
        
        # Initialize ConversationManager to get client list
        try:
            self.conversation_manager = ConversationManager()
        except Exception as e:
            print(f"⚠ Warning: Could not initialize conversation manager: {e}")
            self.conversation_manager = None
    
    def basic_clean(self, query: str) -> str:
        """
        Basic cleaning of user query.
        
        Parameters:
        -----------
        query : str
            Raw user query
            
        Returns:
        --------
        str
            Cleaned query
        """
        # Remove extra whitespace
        query = re.sub(r'\s+', ' ', query.strip())
        
        # Fix common punctuation issues
        query = re.sub(r'\s+([,.!?;:])', r'\1', query)  # Remove space before punctuation
        query = re.sub(r'([,.!?;:])\s*([,.!?;:])', r'\1', query)  # Remove duplicate punctuation
        
        # Fix common typos in SQL-related terms
        replacements = {
            'top n': 'top',
            'topn': 'top',
            'top-': 'top',
            'client name': 'client',
            'clientname': 'client',
            'search volume': 'search volume',
            'searchvolume': 'search volume',
            'current ranking': 'current ranking',
            'currentranking': 'current ranking',
        }
        
        for old, new in replacements.items():
            query = re.sub(rf'\b{re.escape(old)}\b', new, query, flags=re.IGNORECASE)
        
        return query
    
    def format_with_llm(self, query: str) -> str:
        """
        Format query using LLM to fix spelling and grammar.
        
        Parameters:
        -----------
        query : str
            User query
            
        Returns:
        --------
        str
            Formatted query
        """
        if not self.use_llm or not self.client:
            return self.basic_clean(query)
        
        try:
            # Get client list from database for reference
            client_list_context = ""
            if self.conversation_manager:
                try:
                    clients = self.conversation_manager.get_unique_clients()
                    if clients:
                        client_list_context = f"\n\nAVAILABLE CLIENT NAMES IN DATABASE:\n"
                        client_list_context += ", ".join([f"'{c}'" for c in clients])
                        client_list_context += "\n\nIMPORTANT FOR CLIENT NAME CORRECTION:\n"
                        client_list_context += "- If user mentions a client name that is misspelled or similar to a valid client name, correct it to the EXACT client name from the list above\n"
                        client_list_context += "- PRESERVE THE EXACT CASE as shown in the list above (if database has 'EFG', use 'EFG'; if 'efg', use 'efg')\n"
                        client_list_context += "- Do NOT change the case - use the exact case as it appears in the database list\n"
                        client_list_context += "- Example: If database has 'efg' and user types 'efggg' or 'EFG', correct to 'efg' (preserving database case)\n"
                        client_list_context += "- Example: If database has 'EFG' and user types 'efggg' or 'efg', correct to 'EFG' (preserving database case)\n"
                        client_list_context += "- If the user's client name doesn't match any in the list, keep it as-is\n"
                except Exception as e:
                    print(f"⚠ Warning: Could not fetch client list: {e}")
            
            prompt = f"""You are a query formatting assistant. Your task is to clean and format user queries about a database.

Fix the following issues:
1. Correct spelling mistakes
2. Fix grammar errors
3. Standardize terminology (e.g., "client name" -> "client", "search volume" -> "search volume")
4. Remove unnecessary words while keeping the meaning
5. Ensure proper capitalization for months (full month names like 'December')
6. Fix common typos in database-related terms
7. CORRECT CLIENT NAMES: Use the client list below to correct misspelled client names to the EXACT valid client name (preserving the exact case from database - if database has 'EFG' use 'EFG', if 'efg' use 'efg')

IMPORTANT:
- Keep the original meaning and intent
- Preserve months, years, and numbers exactly as they appear
- CORRECT client names using the reference list below - if user types a misspelled client name, match it to the closest valid client name from the list
- PRESERVE EXACT CASE: Use the exact case of client names as shown in the database list (do NOT force lowercase or uppercase)
- Only fix obvious spelling/grammar mistakes
- Don't add information that wasn't in the original query
- Return ONLY the formatted query, no explanations

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

{client_list_context}

Original Query: {query}

Formatted Query:"""

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a query formatting assistant. You clean and format user queries while preserving their original meaning and intent."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1  # Low temperature for consistent formatting
            )
            
            formatted = response.choices[0].message.content.strip()
            
            # Remove any quotes if LLM added them
            formatted = formatted.strip('"').strip("'")
            
            # Ensure it's not empty
            if not formatted:
                return self.basic_clean(query)
            
            return formatted
        
        except Exception as e:
            # Fallback to basic cleaning on error
            print(f"⚠ Warning: LLM formatting failed: {e}. Using basic cleaning.")
            return self.basic_clean(query)
    
    def format_query(
        self,
        query: str,
        use_llm: bool = True
    ) -> Dict[str, Any]:
        """
        Format user query with spelling correction and cleaning.
        
        Parameters:
        -----------
        query : str
            Raw user query
        use_llm : bool, optional
            Whether to use LLM for formatting. Default is True.
            
        Returns:
        --------
        Dict[str, Any]
            Dictionary with:
            - 'formatted_query': Formatted query string
            - 'original_query': Original query string
            - 'changes_made': List of changes made
        """
        original_query = query.strip()
        
        if not original_query:
            return {
                'formatted_query': '',
                'original_query': '',
                'changes_made': []
            }
        
        # Basic cleaning always applied
        basic_cleaned = self.basic_clean(original_query)
        
        # LLM formatting if enabled
        if use_llm and self.use_llm:
            formatted_query = self.format_with_llm(basic_cleaned)
        else:
            formatted_query = basic_cleaned
        
        # Detect changes
        changes_made = []
        if formatted_query.lower() != original_query.lower():
            changes_made.append("Query formatted and cleaned")
        
        return {
            'formatted_query': formatted_query,
            'original_query': original_query,
            'changes_made': changes_made
        }
    
    def reformat_query(
        self,
        original_query: str,
        previous_formatted_query: str,
        error_context: str,
        use_llm: bool = True
    ) -> Dict[str, Any]:
        """
        Reformat query with detailed error context for better correction.
        Used when initial formatting and SQL generation failed.
        
        Parameters:
        -----------
        original_query : str
            Original user query before any formatting
        previous_formatted_query : str
            Previously formatted query that failed
        error_context : str
            Error message or context about why the previous attempt failed
        use_llm : bool, optional
            Whether to use LLM for reformatting. Default is True.
            
        Returns:
        --------
        Dict[str, Any]
            Dictionary with:
            - 'formatted_query': Reformatted query string
            - 'original_query': Original query string
            - 'previous_formatted_query': Previous formatted query
            - 'changes_made': List of changes made
        """
        if not self.use_llm or not self.client or not use_llm:
            # Fallback to basic formatting
            return self.format_query(original_query, use_llm=False)
        
        try:
            # Get client list from database for reference
            client_list_context = ""
            if self.conversation_manager:
                try:
                    clients = self.conversation_manager.get_unique_clients()
                    if clients:
                        client_list_context = f"\n\nAVAILABLE CLIENT NAMES IN DATABASE:\n"
                        client_list_context += ", ".join([f"'{c}'" for c in clients])
                        client_list_context += "\n\nIMPORTANT FOR CLIENT NAME CORRECTION:\n"
                        client_list_context += "- If user mentions a client name that is misspelled or similar to a valid client name, correct it to the EXACT client name from the list above\n"
                        client_list_context += "- PRESERVE THE EXACT CASE as shown in the list above (if database has 'EFG', use 'EFG'; if 'efg', use 'efg')\n"
                        client_list_context += "- Do NOT change the case - use the exact case as it appears in the database list\n"
                except Exception as e:
                    print(f"⚠ Warning: Could not fetch client list: {e}")
            
            prompt = f"""You are an advanced query reformatting assistant. A previous formatting attempt failed, and you need to reformat the query more carefully.

CRITICAL CONTEXT:
- Original Query: {original_query}
- Previous Formatted Query: {previous_formatted_query}
- Error/Issue: {error_context}

Your task is to carefully analyze BOTH the original query and the previous formatted query, identify what went wrong, and create a better formatted query.

Fix the following issues with EXTRA CARE:
1. Correct spelling mistakes (pay special attention to client names, keywords, months)
2. Fix grammar errors
3. Standardize terminology (e.g., "client name" -> "client", "search volume" -> "search volume")
4. Remove unnecessary words while keeping the meaning
5. Ensure proper capitalization for months (full month names like 'December')
6. Fix common typos in database-related terms
7. CORRECT CLIENT NAMES: Use the client list below to correct misspelled client names to the EXACT valid client name (preserving the exact case from database - if database has 'EFG' use 'EFG', if 'efg' use 'efg')

IMPORTANT INSTRUCTIONS:
- Think step by step about what might have gone wrong in the previous formatting
- Compare the original query with the previous formatted query
- Focus on fixing the specific issues mentioned in the error context
- Keep the original meaning and intent
- Preserve months, years, and numbers exactly as they appear
- CORRECT client names using the reference list below - match misspelled names to the closest valid client name
- PRESERVE EXACT CASE: Use the exact case of client names as shown in the database list (do NOT force lowercase or uppercase)
- Only fix obvious spelling/grammar mistakes
- Don't add information that wasn't in the original query
- Return ONLY the reformatted query, no explanations

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

{client_list_context}

Think carefully and provide a better formatted query:

Original Query: {original_query}
Previous Formatted Query: {previous_formatted_query}
Error Context: {error_context}

Reformatted Query:"""

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an advanced query reformatting assistant. You carefully analyze failed formatting attempts and create better formatted queries with detailed attention to spelling, grammar, and database-specific terminology."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1  # Low temperature for consistent formatting
            )
            
            reformatted = response.choices[0].message.content.strip()
            
            # Remove any quotes if LLM added them
            reformatted = reformatted.strip('"').strip("'")
            
            # Ensure it's not empty
            if not reformatted:
                return {
                    'formatted_query': self.basic_clean(original_query),
                    'original_query': original_query,
                    'previous_formatted_query': previous_formatted_query,
                    'changes_made': ["Reformatting failed, using basic cleaning"]
                }
            
            return {
                'formatted_query': reformatted,
                'original_query': original_query,
                'previous_formatted_query': previous_formatted_query,
                'changes_made': ["Query reformatted with error context"]
            }
        
        except Exception as e:
            # Fallback to basic formatting
            print(f"⚠ Warning: LLM reformatting failed: {e}. Using basic formatting.")
            return {
                'formatted_query': self.basic_clean(original_query),
                'original_query': original_query,
                'previous_formatted_query': previous_formatted_query,
                'changes_made': ["Reformatting failed, using basic cleaning"]
            }


# Example usage
if __name__ == "__main__":
    # Initialize formatter
    formatter = QueryFormatter()
    
    # Example: Format query with LLM
    question = "Show me top 5 keywords with highesttt searchd volume for  efggg in December 2025"
    result = formatter.format_query(question, use_llm=True)
    
    print(f"Original Query: {result['original_query']}")
    print(f"Formatted Query: {result['formatted_query']}")
    print(f"Changes Made: {result['changes_made']}")
    
    # Example: Format query without LLM (basic cleaning only)
    print("\n" + "=" * 80)
    question_basic = "Show me top 5 keywords with highest search volume for client efg in December 2025"
    result_basic = formatter.format_query(question_basic, use_llm=False)
    
    print(f"Original Query: {result_basic['original_query']}")
    print(f"Formatted Query (Basic): {result_basic['formatted_query']}")
    print(f"Changes Made: {result_basic['changes_made']}")
