"""
Query Classifier and Embedding Generator
Enhanced with Intent & Complexity Agent for intelligent query classification.
Takes user question, generates embedding, and classifies query intent/complexity using LLM.
Also predicts SQL query generation in the backend.

The Intent & Complexity Agent performs multi-step reasoning to determine:
- Query Intent (Lookup, Aggregation, Time-based, Multi-table JOIN, Comparative)
- Complexity Level (easy, medium, hard)
- Strategy Metadata (schema parts to load, join limits, reasoning tools required)
"""

import os
import sys
import importlib.util
import json
import re
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from dotenv import load_dotenv
from groq import Groq

# Add parent directories to path for imports
current_dir = Path(__file__).parent
rag_excel_dir = current_dir.parent  # rag_excel_postgres directory
postgres_dir = rag_excel_dir / "postgres_create_insert"
sys.path.insert(0, str(postgres_dir))
sys.path.insert(0, str(rag_excel_dir))

# Import generate_embeddings module dynamically
# generate_embeddings.py is in the llm directory (same as this file)
generate_embeddings_path = current_dir / "generate_embeddings.py"
spec = importlib.util.spec_from_file_location("generate_embeddings", generate_embeddings_path)
generate_embeddings_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_embeddings_module)
get_embeddings = generate_embeddings_module.get_embeddings

# Load environment variables
load_dotenv()

# -------------------------
# Configuration
# -------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
 
# Database schema information for SQL generation context
SCHEMA_CONTEXT = """
Database Schema:
- months (month_pk, month_name, year, month_id)
- files (file_id, client_name, file_name, month_pk)
- Mastersheet-Keyword_report (id, file_id, keyword, initial_ranking, current_ranking, change, search_volume, map_ranking_gbp, location, url, difficulty, search_intent)
"""

# Intent categories with descriptions
INTENT_CATEGORIES = {
    "lookup": {
        "description": "Simple SELECT queries from a single table with basic WHERE clauses",
        "keywords": ["show", "list", "find", "get", "what are", "display"],
        "schema_parts": ["single_table"],
        "max_joins": 0,
        "requires_reasoning": False
    },
    "aggregation": {
        "description": "Queries with aggregations like SUM, COUNT, AVG, GROUP BY",
        "keywords": ["sum", "count", "average", "total", "aggregate", "group by", "calculate"],
        "schema_parts": ["aggregation_columns", "group_by_columns"],
        "max_joins": 2,
        "requires_reasoning": False
    },
    "time_based": {
        "description": "Queries with time filters like month/year filters, date ranges",
        "keywords": ["month", "year", "date", "time", "period", "range", "between"],
        "schema_parts": ["months", "files", "time_columns"],
        "max_joins": 2,
        "requires_reasoning": False
    },
    "multi_table_join": {
        "description": "Queries requiring JOINs across multiple tables (3+ tables)",
        "keywords": ["join", "across", "multiple", "related", "combine"],
        "schema_parts": ["all_tables", "relationships"],
        "max_joins": 10,
        "requires_reasoning": True
    },
    "comparative": {
        "description": "Comparative queries like Month-over-Month (MoM), Year-over-Year (YoY), trends",
        "keywords": ["compare", "vs", "versus", "change", "growth", "trend", "mom", "yoy", "difference"],
        "schema_parts": ["months", "time_series_columns", "aggregation_columns"],
        "max_joins": 3,
        "requires_reasoning": True
    }
}


class IntentComplexityAgent:
    """
    Agentic Intent & Complexity Classifier
    Uses multi-step reasoning to classify query intent and complexity.
    Determines strategy metadata for SQL generation (schema parts, join limits, reasoning tools).
    """
    
    def __init__(self, client: Groq, model: str):
        """
        Initialize the Intent & Complexity Agent.
        
        Args:
            client: Groq client instance
            model: Model name to use
        """
        self.client = client
        self.model = model
    
    def _build_intent_classification_prompt(self, question: str) -> str:
        """
        Build prompt for intent classification using agentic reasoning.
        
        Args:
            question: User's natural language question
            
        Returns:
            Formatted prompt string
        """
        intent_descriptions = "\n".join([
            f"- {intent.upper()}: {info['description']}"
            for intent, info in INTENT_CATEGORIES.items()
        ])
        
        prompt = f"""You are an Intent Classification Agent. Analyze the following natural language question and classify its intent using multi-step reasoning.

Database Schema:
{SCHEMA_CONTEXT}

Intent Categories:
{intent_descriptions}

Step 1: Analyze the question for keywords and patterns
Step 2: Identify which intent category best matches
Step 3: Consider if multiple intents apply (select the primary one)

User Question: {question}

Think through each step and respond with ONLY the intent category name (one word: lookup, aggregation, time_based, multi_table_join, or comparative).
Do not include explanations or additional text."""
        
        return prompt
    
    def _build_complexity_classification_prompt(self, question: str, intent: str) -> str:
        """
        Build prompt for complexity classification based on intent.
        
        Args:
            question: User's natural language question
            intent: Classified intent category
            
        Returns:
            Formatted prompt string
        """
        intent_info = INTENT_CATEGORIES.get(intent, INTENT_CATEGORIES["lookup"])
        
        prompt = f"""You are a Complexity Classification Agent. Analyze the following question and classify its complexity based on its intent.

Database Schema:
{SCHEMA_CONTEXT}

Intent: {intent.upper()}
Intent Description: {intent_info['description']}

Complexity Levels:
- "easy": Simple queries from a single table, basic WHERE clauses, no JOINs
- "medium": Queries with 1-2 JOINs, WHERE clauses with conditions, basic aggregations, simple time filters
- "hard": Complex queries with 3+ JOINs, complex aggregations, subqueries, comparative logic (MoM, YoY), or complex WHERE conditions

Step 1: Consider the intent category ({intent})
Step 2: Analyze the question structure and required operations
Step 3: Determine the complexity level based on SQL operations needed

User Question: {question}

Based on the intent and question structure, classify the complexity as one of: easy, medium, or hard.
Respond with ONLY the complexity level (one word: easy, medium, or hard)."""
        
        return prompt
    
    def _build_strategy_metadata_prompt(self, question: str, intent: str, complexity: str) -> str:
        """
        Build prompt to generate strategy metadata.
        
        Args:
            question: User's natural language question
            intent: Classified intent category
            complexity: Classified complexity level
            
        Returns:
            Formatted prompt string
        """
        intent_info = INTENT_CATEGORIES.get(intent, INTENT_CATEGORIES["lookup"])
        
        prompt = f"""You are a Strategy Metadata Generator. Based on the classified intent and complexity, generate strategy metadata.

Database Schema:
{SCHEMA_CONTEXT}

Intent: {intent.upper()}
Complexity: {complexity.upper()}
User Question: {question}

Generate a JSON object with the following structure:
{{
    "schema_parts": ["list", "of", "required", "schema", "parts"],
    "max_joins": <number>,
    "requires_reasoning": <true/false>,
    "reasoning_tools": ["list", "of", "required", "tools", "if", "any"]
}}

Schema parts can include:
- "Mastersheet-Keyword_report" (or "keyword_table")
- "files" (or "files_table")
- "months" (or "months_table")
- "aggregation_columns" (for COUNT, SUM, AVG)
- "time_columns" (for date/time filtering)
- "relationships" (for JOIN logic)

Reasoning tools can include:
- "comparative_analysis" (for MoM, YoY, trends)
- "multi_table_join" (for complex JOINs)
- "aggregation_planning" (for complex aggregations)
- "time_series_analysis" (for time-based queries)

Return ONLY the JSON object, no explanations."""
        
        return prompt
    
    def classify_intent(self, question: str) -> str:
        """
        Classify the intent of a user question using agentic reasoning.
        
        Args:
            question: User's natural language question
            
        Returns:
            Intent category: "lookup", "aggregation", "time_based", "multi_table_join", or "comparative"
        """
        try:
            prompt = self._build_intent_classification_prompt(question)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an Intent Classification Agent. Use step-by-step reasoning to classify query intent. Respond with only one word: lookup, aggregation, time_based, multi_table_join, or comparative."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0
            )
            
            intent = response.choices[0].message.content.strip().lower()
            
            # Validate and normalize intent
            valid_intents = list(INTENT_CATEGORIES.keys())
            if intent not in valid_intents:
                # Try to extract from response
                for valid_intent in valid_intents:
                    if valid_intent in intent:
                        intent = valid_intent
                        break
                else:
                    # Default based on keywords if unclear
                    intent = self._fallback_intent_classification(question)
                    print(f"⚠️  Could not determine intent, using fallback: '{intent}'")
            
            return intent
            
        except Exception as e:
            print(f"❌ Error classifying intent: {e}")
            return self._fallback_intent_classification(question)
    
    def _fallback_intent_classification(self, question: str) -> str:
        """
        Fallback intent classification using keyword matching.
        
        Args:
            question: User's natural language question
            
        Returns:
            Intent category
        """
        question_lower = question.lower()
        
        # Check for comparative keywords
        if any(kw in question_lower for kw in ["compare", "vs", "versus", "change", "growth", "trend", "mom", "yoy"]):
            return "comparative"
        
        # Check for aggregation keywords
        if any(kw in question_lower for kw in ["sum", "count", "average", "total", "aggregate", "calculate"]):
            return "aggregation"
        
        # Check for time-based keywords
        if any(kw in question_lower for kw in ["month", "year", "date", "time", "period"]):
            return "time_based"
        
        # Check for multi-table keywords
        if any(kw in question_lower for kw in ["join", "across", "multiple", "related", "combine"]):
            return "multi_table_join"
        
        # Default to lookup
        return "lookup"
    
    def classify_complexity(self, question: str, intent: str) -> str:
        """
        Classify the complexity of a user question based on intent.
        
        Args:
            question: User's natural language question
            intent: Classified intent category
            
        Returns:
            Complexity level: "easy", "medium", or "hard"
        """
        try:
            prompt = self._build_complexity_classification_prompt(question, intent)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a Complexity Classification Agent. Use step-by-step reasoning to classify query complexity. Respond with only one word: easy, medium, or hard."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0
            )
            
            complexity = response.choices[0].message.content.strip().lower()
            
            # Validate and normalize
            if complexity not in ["easy", "medium", "hard"]:
                if "easy" in complexity:
                    complexity = "easy"
                elif "medium" in complexity:
                    complexity = "medium"
                elif "hard" in complexity:
                    complexity = "hard"
                else:
                    # Default based on intent
                    complexity = self._default_complexity_for_intent(intent)
                    print(f"⚠️  Could not determine complexity, defaulting to '{complexity}' for intent '{intent}'")
            
            return complexity
            
        except Exception as e:
            print(f"❌ Error classifying complexity: {e}")
            return self._default_complexity_for_intent(intent)
    
    def _default_complexity_for_intent(self, intent: str) -> str:
        """
        Get default complexity level for an intent.
        
        Args:
            intent: Intent category
            
        Returns:
            Default complexity level
        """
        defaults = {
            "lookup": "easy",
            "aggregation": "medium",
            "time_based": "medium",
            "multi_table_join": "hard",
            "comparative": "hard"
        }
        return defaults.get(intent, "medium")
    
    def generate_strategy_metadata(self, question: str, intent: str, complexity: str) -> Dict[str, any]:
        """
        Generate strategy metadata based on intent and complexity.
        
        Args:
            question: User's natural language question
            intent: Classified intent category
            complexity: Classified complexity level
            
        Returns:
            Dictionary with strategy metadata
        """
        try:
            prompt = self._build_strategy_metadata_prompt(question, intent, complexity)
            
            # Try with JSON response format first (if supported by model)
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a Strategy Metadata Generator. Return ONLY a valid JSON object, no explanations or markdown."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"}
                )
            except Exception:
                # Fallback if response_format not supported
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a Strategy Metadata Generator. Return ONLY a valid JSON object, no explanations or markdown."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.0
                )
            
            metadata_str = response.choices[0].message.content.strip()
            
            # Parse JSON
            try:
                metadata = json.loads(metadata_str)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code blocks
                json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', metadata_str, re.DOTALL)
                if json_match:
                    metadata = json.loads(json_match.group(1))
                else:
                    # Try to find JSON object in response
                    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', metadata_str, re.DOTALL)
                    if json_match:
                        try:
                            metadata = json.loads(json_match.group(0))
                        except json.JSONDecodeError:
                            # Fallback to intent-based defaults
                            metadata = self._default_strategy_metadata(intent, complexity)
                    else:
                        # Fallback to intent-based defaults
                        metadata = self._default_strategy_metadata(intent, complexity)
            
            # Ensure required fields exist
            required_fields = ["schema_parts", "max_joins", "requires_reasoning"]
            for field in required_fields:
                if field not in metadata:
                    metadata = self._default_strategy_metadata(intent, complexity)
                    break
            
            # Add reasoning_tools if missing
            if "reasoning_tools" not in metadata:
                metadata["reasoning_tools"] = []
                if metadata.get("requires_reasoning"):
                    if intent == "comparative":
                        metadata["reasoning_tools"].append("comparative_analysis")
                    elif intent == "multi_table_join":
                        metadata["reasoning_tools"].append("multi_table_join")
            
            return metadata
            
        except Exception as e:
            print(f"❌ Error generating strategy metadata: {e}")
            return self._default_strategy_metadata(intent, complexity)
    
    def _default_strategy_metadata(self, intent: str, complexity: str) -> Dict[str, any]:
        """
        Generate default strategy metadata based on intent.
        
        Args:
            intent: Intent category
            complexity: Complexity level
            
        Returns:
            Default strategy metadata dictionary
        """
        intent_info = INTENT_CATEGORIES.get(intent, INTENT_CATEGORIES["lookup"])
        
        metadata = {
            "schema_parts": intent_info["schema_parts"].copy(),
            "max_joins": intent_info["max_joins"],
            "requires_reasoning": intent_info["requires_reasoning"],
            "reasoning_tools": []
        }
        
        # Adjust max_joins based on complexity
        if complexity == "hard":
            metadata["max_joins"] = min(metadata["max_joins"] + 2, 10)
        elif complexity == "easy":
            metadata["max_joins"] = min(metadata["max_joins"], 1)
        
        # Add reasoning tools if required
        if metadata["requires_reasoning"]:
            if intent == "comparative":
                metadata["reasoning_tools"] = ["comparative_analysis", "time_series_analysis"]
            elif intent == "multi_table_join":
                metadata["reasoning_tools"] = ["multi_table_join"]
            elif intent == "aggregation" and complexity == "hard":
                metadata["reasoning_tools"] = ["aggregation_planning"]
        
        # Add schema parts based on intent
        if intent == "time_based":
            if "months" not in metadata["schema_parts"]:
                metadata["schema_parts"].append("months")
            if "files" not in metadata["schema_parts"]:
                metadata["schema_parts"].append("files")
        elif intent == "multi_table_join":
            metadata["schema_parts"] = ["all_tables", "relationships"]
        
        return metadata
    
    def classify_intent_and_complexity(self, question: str) -> Dict[str, any]:
        """
        Complete agentic classification: intent + complexity + strategy metadata.
        
        Args:
            question: User's natural language question
            
        Returns:
            Dictionary with:
            - intent: Intent category
            - complexity: Complexity level
            - strategy: Strategy metadata dictionary
        """
        print("   🤖 [Agent] Step 1: Classifying intent...")
        intent = self.classify_intent(question)
        print(f"      → Intent: {intent.upper()}")
        
        print("   🤖 [Agent] Step 2: Classifying complexity...")
        complexity = self.classify_complexity(question, intent)
        print(f"      → Complexity: {complexity.upper()}")
        
        print("   🤖 [Agent] Step 3: Generating strategy metadata...")
        strategy = self.generate_strategy_metadata(question, intent, complexity)
        print(f"      → Strategy: {strategy['schema_parts']} schema parts, max {strategy['max_joins']} joins, reasoning: {strategy['requires_reasoning']}")
        
        return {
            "intent": intent,
            "complexity": complexity,
            "strategy": strategy
        }


class QueryClassifier:
    """
    Classifies user queries and generates embeddings.
    Uses LLM to determine complexity and predict SQL query generation.
    """
    
    def __init__(self):
        """Initialize the query classifier with Groq LLM and embedding model."""
        if not GROQ_API_KEY:
            raise EnvironmentError("GROQ_API_KEY is missing. Set it in your .env file.")
        
        # Configure Groq
        self.client = Groq(api_key=GROQ_API_KEY)
        self.model = GROQ_MODEL
        
        # Initialize embedding model
        self.embeddings_model = get_embeddings()
        
        # Initialize Intent & Complexity Agent
        self.intent_complexity_agent = IntentComplexityAgent(self.client, self.model)
        
        print(f"✅ QueryClassifier initialized with {GROQ_MODEL}")
        print(f"✅ Intent & Complexity Agent initialized")
    
    def _build_sql_prediction_prompt(self, question: str) -> str:
        """
        Build prompt for SQL query prediction (internal use, not shown to user).
        
        Args:
            question: User's natural language question
            
        Returns:
            Formatted prompt string
        """
        prompt = f"""You are a SQL query generator. Based on the following natural language question, generate the corresponding SQL query.

Database Schema:
{SCHEMA_CONTEXT}

Important Rules:
- Use proper table names: "Mastersheet-Keyword_report" (with quotes and hyphens)
- Use proper JOIN syntax: JOIN files f ON k.file_id = f.file_id
- Use proper column references with table aliases
- Include proper WHERE clauses based on the question
- Use appropriate aggregations if needed (COUNT, SUM, AVG, etc.)
- End queries with semicolon

User Question: {question}

Generate the SQL query that would answer this question. Return ONLY the SQL query, no explanations."""
        
        return prompt
    
    def classify_complexity(self, question: str) -> str:
        """
        Classify the complexity of a user question using agentic reasoning.
        (Backward compatibility method - now uses Intent & Complexity Agent)
        
        Args:
            question: User's natural language question
            
        Returns:
            Complexity level: "easy", "medium", or "hard"
        """
        try:
            # Use agent for classification
            agent_result = self.intent_complexity_agent.classify_intent_and_complexity(question)
            return agent_result["complexity"]
        except Exception as e:
            print(f"❌ Error classifying complexity: {e}")
            # Default to medium on error
            return "medium"
    
    def predict_sql_query(self, question: str) -> str:
        """
        Predict SQL query for the question (internal use, not shown to user).
        
        Args:
            question: User's natural language question
            
        Returns:
            Predicted SQL query string
        """
        try:
            prompt = self._build_sql_prediction_prompt(question)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a SQL query generator. Return ONLY the SQL query, no explanations or markdown."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0
            )
            sql_query = response.choices[0].message.content.strip()
            
            # Clean up SQL query (remove markdown code blocks if present)
            if sql_query.startswith("```sql"):
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            elif sql_query.startswith("```"):
                sql_query = sql_query.replace("```", "").strip()
            
            # Remove any leading/trailing backticks
            sql_query = sql_query.strip('`').strip()
            
            # Remove any prefixes like "Query:", "SQL:", etc.
            for prefix in ['Query:', 'SQL:', 'Answer:', 'Output:', 'Result:']:
                if sql_query.startswith(prefix):
                    sql_query = sql_query[len(prefix):].strip()
            
            return sql_query
            
        except Exception as e:
            print(f"❌ Error predicting SQL query: {e}")
            return ""
    
    def process_query(self, question: str) -> Dict[str, any]:
        """
        Process a user question: classify intent/complexity using agentic reasoning,
        generate embedding, and predict SQL query.
        
        Args:
            question: User's natural language question
            
        Returns:
            Dictionary with:
            - question_text: Original question
            - embedding: Generated embedding vector
            - intent: Classified intent category (lookup, aggregation, time_based, multi_table_join, comparative)
            - complexity: Classified complexity level (easy, medium, hard)
            - strategy: Strategy metadata dictionary with:
                - schema_parts: List of required schema parts
                - max_joins: Maximum number of joins allowed
                - requires_reasoning: Boolean indicating if reasoning tools are needed
                - reasoning_tools: List of required reasoning tools
            - embedding_dimension: Dimension of the embedding
        """
        if not question or not question.strip():
            raise ValueError("Question cannot be empty")
        
        print(f"\n🔍 Processing query: {question[:60]}...")
        
        # Step 1: Agentic Intent & Complexity Classification
        print("   🤖 Running Intent & Complexity Agent...")
        agent_result = self.intent_complexity_agent.classify_intent_and_complexity(question)
        intent = agent_result["intent"]
        complexity = agent_result["complexity"]
        strategy = agent_result["strategy"]
        
        # Step 2: Predict SQL query (internal, not shown)
        # Use strategy metadata to inform SQL prediction
        print("   📝 Predicting SQL query (internal)...")
        predicted_sql = self.predict_sql_query(question)
        
        # Step 3: Generate embedding (using question + predicted SQL)
        print("   🔮 Generating embedding...")
        if predicted_sql:
            embedding = self.embeddings_model.generate_embedding(question, predicted_sql)
        else:
            # Fallback: use question only if SQL prediction fails
            embedding = self.embeddings_model.generate_embedding(question, "")
        
        result = {
            "question_text": question,
            "embedding": embedding,
            "intent": intent,
            "complexity": complexity,
            "strategy": strategy,
            "embedding_dimension": len(embedding)
        }
        
        print(f"   ✅ Intent: {intent.upper()}, Complexity: {complexity.upper()}, Embedding dimension: {len(embedding)}")
        print(f"   ✅ Strategy: {strategy['schema_parts']} schema parts, max {strategy['max_joins']} joins")
        if strategy['requires_reasoning']:
            print(f"   ✅ Reasoning tools: {', '.join(strategy.get('reasoning_tools', []))}")
        
        return result
    
    def get_intent(self, question: str) -> str:
        """
        Get intent classification for a question.
        
        Args:
            question: User's natural language question
            
        Returns:
            Intent category: "lookup", "aggregation", "time_based", "multi_table_join", or "comparative"
        """
        return self.intent_complexity_agent.classify_intent(question)
    
    def get_strategy(self, question: str) -> Dict[str, any]:
        """
        Get strategy metadata for a question.
        
        Args:
            question: User's natural language question
            
        Returns:
            Strategy metadata dictionary
        """
        agent_result = self.intent_complexity_agent.classify_intent_and_complexity(question)
        return agent_result["strategy"]
    
    def process_query_batch(self, questions: list) -> list:
        """
        Process multiple questions in batch.
        
        Args:
            questions: List of user questions
            
        Returns:
            List of result dictionaries
        """
        results = []
        for question in questions:
            try:
                result = self.process_query(question)
                results.append(result)
            except Exception as e:
                print(f"❌ Error processing question '{question[:50]}...': {e}")
                continue
        
        return results


# -------------------------
# Main (for testing)
# -------------------------
if __name__ == "__main__":
    print("="*60)
    print("Query Classifier and Embedding Generator")
    print("="*60)
    
    # Initialize classifier
    classifier = QueryClassifier()
    
    # Test queries covering different intents
    test_questions = [
        # ("Lookup", "What are all the keywords in the database?"),
        # ("Time-based", "Show keywords for ABC in March 2025 with their search volumes."),
        # ("Aggregation", "What is the average search volume for keywords of client EFG across all months?"),
        # ("Multi-table JOIN", "Show all keywords with their client names and month information."),
        # ("Comparative", "Compare the search volume for keywords in March 2025 vs April 2025."),
        ("Comparative (MoM)", "What is the month-over-month growth in keyword rankings for client ABC?")
    ]
    
    print("\n📋 Testing with sample questions:\n")
    
    for i, (intent_type, question) in enumerate(test_questions, 1):
        print(f"\n{'='*60}")
        print(f"Test {i} [{intent_type}]: {question}")
        print("="*60)
        
        try:
            result = classifier.process_query(question)
            
            print(f"\n✅ Results:")
            print(f"   Question: {result['question_text']}")
            print(f"   Intent: {result['intent'].upper()}")
            print(f"   Complexity: {result['complexity'].upper()}")
            print(f"   Strategy Metadata:")
            print(f"      - Schema parts: {', '.join(result['strategy']['schema_parts'])}")
            print(f"      - Max joins: {result['strategy']['max_joins']}")
            print(f"      - Requires reasoning: {result['strategy']['requires_reasoning']}")
            if result['strategy'].get('reasoning_tools'):
                print(f"      - Reasoning tools: {', '.join(result['strategy']['reasoning_tools'])}")
            print(f"   Embedding dimension: {result['embedding_dimension']}")
            print(f"   Embedding (first 5 values): {result['embedding'][:5]}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

