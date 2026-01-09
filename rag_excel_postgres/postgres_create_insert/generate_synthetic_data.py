"""
Generate synthetic question-SQL pairs for the knowledge base.
Based on the schema: months, files, Mastersheet-Keyword_report
Now includes intent classification and strategy metadata.
"""

import random
import re
import json
from typing import List, Dict, Optional

# Sample client names (hardcoded for synthetic data generation)
SAMPLE_CLIENTS = ["ABC", "EFG", "FGH", "XYZ", "KLM", "Stewart"]

# Sample months and years
MONTHS = ["January", "February", "March", "April", "May", "June", 
          "July", "August", "September", "October", "November", "December"]
YEARS = [2024, 2025]

# Sample keywords (hardcoded for synthetic data generation)
SAMPLE_KEYWORDS = [
    "commercial moving company", "office relocation services", 
    "how to move antique furniture", "moving a grandfather clock",
    "packing and moving services", "secure storage solutions",
    "long distance moving services", "cross country movers",
    "storage services near me", "professional piano movers",
    "fragile item moving service", "local movers near me"
]

# Complexity levels
COMPLEXITY_LEVELS = ["easy", "medium", "hard"]

# Intent categories
INTENTS = ["lookup", "aggregation", "time_based", "multi_table_join", "comparative"]

# Intent strategy templates based on query patterns
INTENT_STRATEGIES = {
    "lookup": {
        "schema_parts": ["Mastersheet-Keyword_report"],
        "max_joins": 0,
        "requires_reasoning": False,
        "reasoning_tools": []
    },
    "aggregation": {
        "schema_parts": ["Mastersheet-Keyword_report", "files"],
        "max_joins": 2,
        "requires_reasoning": False,
        "reasoning_tools": []
    },
    "time_based": {
        "schema_parts": ["months", "files", "Mastersheet-Keyword_report"],
        "max_joins": 2,
        "requires_reasoning": False,
        "reasoning_tools": []
    },
    "multi_table_join": {
        "schema_parts": ["all_tables", "relationships"],
        "max_joins": 10,
        "requires_reasoning": True,
        "reasoning_tools": ["multi_table_join"]
    },
    "comparative": {
        "schema_parts": ["months", "time_series_columns", "aggregation_columns"],
        "max_joins": 3,
        "requires_reasoning": True,
        "reasoning_tools": ["comparative_analysis", "time_series_analysis"]
    }
}


def analyze_sql_query(sql: str) -> Dict[str, any]:
    """
    Analyze SQL query to extract metadata: tables, joins, filters, aggregations.
    
    Args:
        sql: SQL query string
        
    Returns:
        Dictionary with extracted metadata
    """
    sql_upper = sql.upper()
    sql_lower = sql.lower()
    
    # Extract tables used
    tables = set()
    # Handle quoted table names (e.g., "Mastersheet-Keyword_report") and unquoted names
    table_patterns = [
        r'FROM\s+"([^"]+)"',  # Quoted table names
        r'JOIN\s+"([^"]+)"',  # Quoted in JOIN
        r'INNER\s+JOIN\s+"([^"]+)"',
        r'LEFT\s+JOIN\s+"([^"]+)"',
        r'RIGHT\s+JOIN\s+"([^"]+)"',
        r'FROM\s+([a-zA-Z0-9_-]+)(?:\s|;|$)',  # Unquoted table names
        r'JOIN\s+([a-zA-Z0-9_-]+)(?:\s|ON|;|$)',  # Unquoted in JOIN
        r'INNER\s+JOIN\s+([a-zA-Z0-9_-]+)(?:\s|ON|;|$)',
        r'LEFT\s+JOIN\s+([a-zA-Z0-9_-]+)(?:\s|ON|;|$)',
        r'RIGHT\s+JOIN\s+([a-zA-Z0-9_-]+)(?:\s|ON|;|$)'
    ]
    for pattern in table_patterns:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        for match in matches:
            # Normalize table names (lowercase, remove quotes)
            table_name = match.lower().strip('"').strip("'")
            if table_name:
                tables.add(table_name)
    
    # Count JOINs
    join_count = len(re.findall(r'\bJOIN\b', sql_upper))
    
    # Check for aggregations
    has_aggregation = bool(re.search(r'\b(SUM|COUNT|AVG|MAX|MIN|GROUP\s+BY)\b', sql_upper))
    aggregation_types = re.findall(r'\b(SUM|COUNT|AVG|MAX|MIN)\b', sql_upper)
    
    # Check for time filters
    has_time_filter = bool(re.search(r'\b(month|year|date|time|period)\b', sql_lower, re.IGNORECASE))
    time_filters = re.findall(r'(month|year|date|time|period)', sql_lower, re.IGNORECASE)
    
    # Check for WHERE clauses
    has_where = bool(re.search(r'\bWHERE\b', sql_upper))
    
    # Extract filters
    filters = []
    where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)', sql_upper, re.IGNORECASE)
    if where_match:
        where_clause = where_match.group(1)
        # Extract basic filter patterns
        filter_patterns = re.findall(r'(\w+)\s*(=|>|<|>=|<=|!=|LIKE|IN)\s*[\'"\w]+', where_clause)
        filters = [f"{col} {op}" for col, op in filter_patterns]
    
    # Check for comparative patterns
    has_comparative = bool(re.search(r'\b(compare|vs|versus|change|growth|trend|mom|yoy|difference)\b', sql_lower, re.IGNORECASE))
    
    return {
        "tables": list(tables),
        "join_count": join_count,
        "has_aggregation": has_aggregation,
        "aggregation_types": list(set(aggregation_types)),
        "has_time_filter": has_time_filter,
        "time_filters": list(set(time_filters)),
        "has_where": has_where,
        "filters": filters,
        "has_comparative": has_comparative
    }


def classify_intent_from_sql(sql: str, question: str = "") -> str:
    """
    Classify intent based on SQL query structure and question.
    
    Args:
        sql: SQL query string
        question: Optional question text for additional context
        
    Returns:
        Intent category string
    """
    analysis = analyze_sql_query(sql)
    sql_upper = sql.upper()
    sql_lower = sql.lower()
    question_lower = question.lower()
    
    # Check for comparative queries
    if analysis["has_comparative"] or any(kw in question_lower for kw in ["compare", "vs", "versus", "change", "growth", "trend", "mom", "yoy"]):
        return "comparative"
    
    # Check for aggregations
    if analysis["has_aggregation"] or "GROUP BY" in sql_upper or "COUNT" in sql_upper or "SUM" in sql_upper or "AVG" in sql_upper:
        if analysis["has_time_filter"]:
            return "comparative"  # Time-based aggregations are often comparative
        return "aggregation"
    
    # Check for multi-table joins (3+ tables)
    if analysis["join_count"] >= 3:
        return "multi_table_join"
    
    # Check for time-based filters
    if analysis["has_time_filter"] or any(kw in question_lower for kw in ["month", "year", "date", "period"]):
        return "time_based"
    
    # Check for simple joins (2 tables)
    if analysis["join_count"] >= 2:
        return "time_based"  # Often time-based when joining multiple tables
    
    # Default to lookup for simple queries
    return "lookup"


def generate_strategy_metadata(sql: str, intent: str, complexity: str) -> Dict[str, any]:
    """
    Generate strategy metadata based on SQL analysis and intent.
    
    Args:
        sql: SQL query string
        intent: Intent category
        complexity: Complexity level
        
    Returns:
        Strategy metadata dictionary
    """
    analysis = analyze_sql_query(sql)
    base_strategy = INTENT_STRATEGIES.get(intent, INTENT_STRATEGIES["lookup"]).copy()
    
    # Adjust based on actual SQL analysis
    actual_tables = analysis["tables"]
    if actual_tables:
        # Map table names to schema parts
        schema_parts = []
        for table in actual_tables:
            table_lower = table.lower()
            if "mastersheet" in table_lower or "keyword" in table_lower or "keyword_report" in table_lower:
                schema_parts.append("Mastersheet-Keyword_report")
            elif table_lower == "files":
                schema_parts.append("files")
            elif table_lower == "months":
                schema_parts.append("months")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_schema_parts = []
        for part in schema_parts:
            if part not in seen:
                seen.add(part)
                unique_schema_parts.append(part)
        
        if unique_schema_parts:
            base_strategy["schema_parts"] = unique_schema_parts
    
    # Adjust max_joins based on actual join count
    actual_joins = analysis["join_count"]
    if actual_joins > base_strategy["max_joins"]:
        base_strategy["max_joins"] = min(actual_joins + 1, 10)
    elif complexity == "easy" and actual_joins == 0:
        base_strategy["max_joins"] = 0
    
    # Add reasoning tools based on complexity and intent
    if complexity == "hard" and intent == "aggregation":
        if "aggregation_planning" not in base_strategy["reasoning_tools"]:
            base_strategy["reasoning_tools"].append("aggregation_planning")
    
    # Add table metadata
    base_strategy["tables_used"] = actual_tables
    base_strategy["join_paths"] = actual_tables if actual_joins > 0 else []
    base_strategy["filters_applied"] = analysis["filters"]
    base_strategy["aggregation_type"] = analysis["aggregation_types"] if analysis["has_aggregation"] else None
    base_strategy["time_granularity"] = analysis["time_filters"] if analysis["has_time_filter"] else None
    
    return base_strategy


def generate_synthetic_data(num_examples: int = 20) -> List[Dict[str, any]]:
    """
    Generate synthetic question-SQL pairs for the knowledge base with intent and strategy metadata.
    
    Args:
        num_examples: Number of examples to generate
        
    Returns:
        List of dictionaries with:
        - question_text: User question
        - sql_query: SQL query string
        - complexity: Complexity level (easy, medium, hard)
        - intent: Intent category (lookup, aggregation, time_based, multi_table_join, comparative)
        - strategy: Strategy metadata dictionary with:
            - schema_parts: List of required schema parts
            - max_joins: Maximum number of joins
            - requires_reasoning: Boolean indicating if reasoning tools are needed
            - reasoning_tools: List of required reasoning tools
            - tables_used: List of tables used in the query
            - join_paths: List of tables in join paths
            - filters_applied: List of filters applied
            - aggregation_type: List of aggregation types (if any)
            - time_granularity: List of time filters (if any)
    """
    examples = []
    
    # Easy queries - Simple SELECT from single table
    easy_queries = [
        {
            "question": "What are all the keywords in the database?",
            "sql": "SELECT keyword FROM \"Mastersheet-Keyword_report\";",
            "complexity": "easy"
        },
        {
            "question": "List all client names from the files table.",
            "sql": "SELECT DISTINCT client_name FROM files;",
            "complexity": "easy"
        },
        {
            "question": "Show all months and years in the database.",
            "sql": "SELECT month_name, year FROM months ORDER BY year, month_id;",
            "complexity": "easy"
        },
        {
            "question": "What are all the file names?",
            "sql": "SELECT file_name FROM files;",
            "complexity": "easy"
        },
        {
            "question": "Get all keywords with their search volumes.",
            "sql": "SELECT keyword, search_volume FROM \"Mastersheet-Keyword_report\";",
            "complexity": "easy"
        }
    ]
    
    # Medium queries - JOINs and WHERE clauses
    medium_queries = [
        {
            "question": f"What keywords are associated with {random.choice(SAMPLE_CLIENTS)}?",
            "sql": f"SELECT k.keyword FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id WHERE f.client_name = '{random.choice(SAMPLE_CLIENTS)}';",
            "complexity": "medium"
        },
        {
            "question": f"Show all keywords for {random.choice(MONTHS)} {random.choice(YEARS)}.",
            "sql": f"SELECT k.keyword FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id JOIN months m ON f.month_pk = m.month_pk WHERE m.month_name = '{random.choice(MONTHS)}' AND m.year = {random.choice(YEARS)};",
            "complexity": "medium"
        },
        {
            "question": f"Find all files for client {random.choice(SAMPLE_CLIENTS)}.",
            "sql": f"SELECT file_name, created_at FROM files WHERE client_name = '{random.choice(SAMPLE_CLIENTS)}';",
            "complexity": "medium"
        },
        {
            "question": "What are the keywords with ranking changes greater than 10?",
            "sql": "SELECT keyword, change FROM \"Mastersheet-Keyword_report\" WHERE change > 10;",
            "complexity": "medium"
        },
        {
            "question": f"List keywords for {random.choice(SAMPLE_CLIENTS)} in {random.choice(MONTHS)}.",
            "sql": f"SELECT k.keyword FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id JOIN months m ON f.month_pk = m.month_pk WHERE f.client_name = '{random.choice(SAMPLE_CLIENTS)}' AND m.month_name = '{random.choice(MONTHS)}';",
            "complexity": "medium"
        },
        {
            "question": "Show all keywords with their current ranking and search volume.",
            "sql": "SELECT keyword, current_ranking, search_volume FROM \"Mastersheet-Keyword_report\" WHERE current_ranking IS NOT NULL;",
            "complexity": "medium"
        },
        {
            "question": "What are the files created in 2025?",
            "sql": "SELECT f.file_name, f.client_name, m.month_name, m.year FROM files f JOIN months m ON f.month_pk = m.month_pk WHERE m.year = 2025;",
            "complexity": "medium"
        }
    ]
    
    # Hard queries - Aggregations, multiple JOINs, complex conditions
    hard_queries = [
        {
            "question": f"What is the average search volume for keywords of client {random.choice(SAMPLE_CLIENTS)}?",
            "sql": f"SELECT AVG(k.search_volume) as avg_search_volume FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id WHERE f.client_name = '{random.choice(SAMPLE_CLIENTS)}' AND k.search_volume IS NOT NULL;",
            "complexity": "hard"
        },
        {
            "question": "Which client has the most keywords?",
            "sql": "SELECT f.client_name, COUNT(k.id) as keyword_count FROM files f JOIN \"Mastersheet-Keyword_report\" k ON f.file_id = k.file_id GROUP BY f.client_name ORDER BY keyword_count DESC LIMIT 1;",
            "complexity": "hard"
        },
        {
            "question": f"What is the total search volume for all keywords in {random.choice(MONTHS)} {random.choice(YEARS)}?",
            "sql": f"SELECT SUM(k.search_volume) as total_search_volume FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id JOIN months m ON f.month_pk = m.month_pk WHERE m.month_name = '{random.choice(MONTHS)}' AND m.year = {random.choice(YEARS)} AND k.search_volume IS NOT NULL;",
            "complexity": "hard"
        },
        {
            "question": "Show the top 5 keywords with the highest search volume across all clients.",
            "sql": "SELECT k.keyword, k.search_volume, f.client_name FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id WHERE k.search_volume IS NOT NULL ORDER BY k.search_volume DESC LIMIT 5;",
            "complexity": "hard"
        },
        {
            "question": f"What is the average ranking change for keywords of client {random.choice(SAMPLE_CLIENTS)}?",
            "sql": f"SELECT AVG(k.change) as avg_change FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id WHERE f.client_name = '{random.choice(SAMPLE_CLIENTS)}' AND k.change IS NOT NULL;",
            "complexity": "hard"
        },
        {
            "question": "Which month has the most keywords across all clients?",
            "sql": "SELECT m.month_name, m.year, COUNT(k.id) as keyword_count FROM months m JOIN files f ON m.month_pk = f.month_pk JOIN \"Mastersheet-Keyword_report\" k ON f.file_id = k.file_id GROUP BY m.month_pk, m.month_name, m.year ORDER BY keyword_count DESC LIMIT 1;",
            "complexity": "hard"
        },
        {
            "question": "Show clients with their total number of keywords and average search volume.",
            "sql": "SELECT f.client_name, COUNT(k.id) as keyword_count, AVG(k.search_volume) as avg_search_volume FROM files f JOIN \"Mastersheet-Keyword_report\" k ON f.file_id = k.file_id WHERE k.search_volume IS NOT NULL GROUP BY f.client_name ORDER BY keyword_count DESC;",
            "complexity": "hard"
        },
        {
            "question": "What are the keywords that improved their ranking (positive change) for a specific client?",
            "sql": "SELECT k.keyword, k.change, f.client_name FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id WHERE k.change > 0 ORDER BY k.change DESC;",
            "complexity": "hard"
        },
        {
            "question": f"Compare the search volume for keywords between {random.choice(MONTHS)} and {random.choice(MONTHS)}.",
            "sql": f"SELECT m1.month_name as month1, m2.month_name as month2, k1.search_volume as vol1, k2.search_volume as vol2 FROM \"Mastersheet-Keyword_report\" k1 JOIN files f1 ON k1.file_id = f1.file_id JOIN months m1 ON f1.month_pk = m1.month_pk JOIN \"Mastersheet-Keyword_report\" k2 ON k1.keyword = k2.keyword JOIN files f2 ON k2.file_id = f2.file_id JOIN months m2 ON f2.month_pk = m2.month_pk WHERE m1.month_name = '{random.choice(MONTHS)}' AND m2.month_name = '{random.choice(MONTHS)}';",
            "complexity": "hard"
        },
        {
            "question": f"Show month-over-month growth in keyword rankings for client {random.choice(SAMPLE_CLIENTS)}.",
            "sql": f"SELECT m1.month_name as prev_month, m2.month_name as curr_month, k1.current_ranking as prev_rank, k2.current_ranking as curr_rank, (k2.current_ranking - k1.current_ranking) as rank_change FROM \"Mastersheet-Keyword_report\" k1 JOIN files f1 ON k1.file_id = f1.file_id JOIN months m1 ON f1.month_pk = m1.month_pk JOIN \"Mastersheet-Keyword_report\" k2 ON k1.keyword = k2.keyword JOIN files f2 ON k2.file_id = f2.file_id JOIN months m2 ON f2.month_pk = m2.month_pk WHERE f1.client_name = '{random.choice(SAMPLE_CLIENTS)}' AND f2.client_name = '{random.choice(SAMPLE_CLIENTS)}' AND m2.month_id = m1.month_id + 1;",
            "complexity": "hard"
        },
        {
            "question": "What is the year-over-year change in search volume for all keywords?",
            "sql": "SELECT k1.keyword, k1.search_volume as vol_2024, k2.search_volume as vol_2025, (k2.search_volume - k1.search_volume) as yoy_change FROM \"Mastersheet-Keyword_report\" k1 JOIN files f1 ON k1.file_id = f1.file_id JOIN months m1 ON f1.month_pk = m1.month_pk JOIN \"Mastersheet-Keyword_report\" k2 ON k1.keyword = k2.keyword JOIN files f2 ON k2.file_id = f2.file_id JOIN months m2 ON f2.month_pk = m2.month_pk WHERE m1.year = 2024 AND m2.year = 2025 AND m1.month_id = m2.month_id;",
            "complexity": "hard"
        }
    ]
    
    # Combine all queries
    all_queries = easy_queries + medium_queries + hard_queries
    
    # Select random queries up to num_examples
    selected_queries = random.sample(all_queries, min(num_examples, len(all_queries)))
    
    # Format as list of dictionaries with intent and strategy
    for query in selected_queries:
        question = query["question"]
        sql = query["sql"]
        complexity = query["complexity"]
        
        # Classify intent based on SQL structure and question
        intent = classify_intent_from_sql(sql, question)
        
        # Generate strategy metadata
        strategy = generate_strategy_metadata(sql, intent, complexity)
        
        # Create example dictionary
        example = {
            "question_text": question,
            "sql_query": sql,
            "complexity": complexity,
            "intent": intent,
            "strategy": strategy
        }
        
        examples.append(example)
    
    return examples


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    print("📝 Generating Synthetic Data for Knowledge Base\n")
    
    # Generate 20 examples
    synthetic_data = generate_synthetic_data(num_examples=20)
    
    print(f"✅ Generated {len(synthetic_data)} synthetic examples\n")
    print("="*60)
    print("Sample Examples:")
    print("="*60)
    
    for i, example in enumerate(synthetic_data[:5], 1):
        print(f"\n{i}. Intent: {example.get('intent', 'N/A').upper()}, Complexity: {example['complexity']}")
        print(f"   Question: {example['question_text']}")
        print(f"   SQL: {example['sql_query'][:80]}...")
        if 'strategy' in example and example['strategy']:
            strategy = example['strategy']
            print(f"   Strategy: {strategy.get('schema_parts', [])} schema parts, "
                  f"max {strategy.get('max_joins', 0)} joins, "
                  f"tables: {strategy.get('tables_used', [])}")
    
    print(f"\n... and {len(synthetic_data) - 5} more examples")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("📊 Summary Statistics:")
    print("="*60)
    
    intent_counts = {}
    for example in synthetic_data:
        intent = example.get('intent', 'unknown')
        intent_counts[intent] = intent_counts.get(intent, 0) + 1
    
    print("\nIntent Distribution:")
    for intent, count in sorted(intent_counts.items()):
        print(f"   • {intent}: {count}")
    
    complexity_counts = {}
    for example in synthetic_data:
        complexity = example.get('complexity', 'unknown')
        complexity_counts[complexity] = complexity_counts.get(complexity, 0) + 1
    
    print("\nComplexity Distribution:")
    for complexity, count in sorted(complexity_counts.items()):
        print(f"   • {complexity}: {count}")

