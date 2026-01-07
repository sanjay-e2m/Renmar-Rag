"""
Generate synthetic question-SQL pairs for the knowledge base.
Based on the schema: months, files, Mastersheet-Keyword_report
"""

import random
from typing import List, Dict

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


def generate_synthetic_data(num_examples: int = 20) -> List[Dict[str, str]]:
    """
    Generate synthetic question-SQL pairs for the knowledge base.
    
    Args:
        num_examples: Number of examples to generate
        
    Returns:
        List of dictionaries with 'question_text', 'sql_query', and 'complexity'
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
        }
    ]
    
    # Combine all queries
    all_queries = easy_queries + medium_queries + hard_queries
    
    # Select random queries up to num_examples
    selected_queries = random.sample(all_queries, min(num_examples, len(all_queries)))
    
    # Format as list of dictionaries
    for query in selected_queries:
        examples.append({
            "question_text": query["question"],
            "sql_query": query["sql"],
            "complexity": query["complexity"]
        })
    
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
        print(f"\n{i}. Complexity: {example['complexity']}")
        print(f"   Question: {example['question_text']}")
        print(f"   SQL: {example['sql_query'][:80]}...")
    
    print(f"\n... and {len(synthetic_data) - 5} more examples")

