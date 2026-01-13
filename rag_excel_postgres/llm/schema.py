"""
Database Schema Definitions
Contains detailed table and column descriptions for SQL Planning Agent context.
"""

import json
from typing import Dict, Any, List

# ============================================================================
# Semantic Model - Table Descriptions with Use Cases
# ============================================================================

SEMANTIC_MODEL = {
    "tables": [
        {
            "table_name": "months",
            "table_description": "Time dimension table storing month and year information for organizing data by time periods.",
            "use_cases": [
                "Filtering data by specific months",
                "Time-based aggregations (monthly, yearly)",
                "Joining with files table to get time context",
                "Year-over-year comparisons",
                "Month-over-month analysis"
            ],
            "columns": [
                {
                    "column_name": "month_pk",
                    "data_type": "SERIAL PRIMARY KEY",
                    "description": "Primary key for the months table, auto-incrementing integer",
                    "usage": "Used as foreign key in files table to link data to time periods"
                },
                {
                    "column_name": "month_name",
                    "data_type": "TEXT NOT NULL",
                    "description": "Name of the month (e.g., 'January', 'February', 'March')",
                    "usage": "Used for filtering and displaying month names in queries"
                },
                {
                    "column_name": "year",
                    "data_type": "INTEGER NOT NULL",
                    "description": "Year value (e.g., 2024, 2025)",
                    "usage": "Used for filtering by year and time-based analysis"
                },
                {
                    "column_name": "month_id",
                    "data_type": "INTEGER NOT NULL",
                    "description": "Numeric month identifier (1-12, where 1=January, 12=December)",
                    "usage": "Used for ordering months chronologically and month arithmetic"
                },
                {
                    "column_name": "created_at",
                    "data_type": "TIMESTAMP",
                    "description": "Timestamp when the record was created",
                    "usage": "Audit trail and record tracking"
                }
            ],
            "relationships": [
                {
                    "type": "one-to-many",
                    "related_table": "files",
                    "foreign_key": "files.month_pk",
                    "description": "One month can have many files"
                }
            ],
            "indexes": [
                "UNIQUE (month_name, year) - Ensures no duplicate month-year combinations"
            ]
        },
        {
            "table_name": "files",
            "table_description": "File metadata table storing information about uploaded files, linking them to clients and time periods.",
            "use_cases": [
                "Filtering data by client name",
                "Finding files for specific months",
                "Joining with keyword data to get client context",
                "Tracking file uploads over time",
                "Client-specific analysis"
            ],
            "columns": [
                {
                    "column_name": "file_id",
                    "data_type": "INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
                    "description": "Primary key for files table, auto-generated unique identifier",
                    "usage": "Used as foreign key in Mastersheet-Keyword_report table"
                },
                {
                    "column_name": "client_name",
                    "data_type": "TEXT NOT NULL",
                    "description": "Name of the client associated with the file (e.g., 'ABC', 'EFG', 'Stewart')",
                    "usage": "Primary filter for client-specific queries, used in WHERE clauses"
                },
                {
                    "column_name": "file_name",
                    "data_type": "TEXT NOT NULL",
                    "description": "Original name of the uploaded file",
                    "usage": "Display and tracking purposes, rarely used in filtering"
                },
                {
                    "column_name": "month_pk",
                    "data_type": "INTEGER NOT NULL",
                    "description": "Foreign key referencing months.month_pk",
                    "usage": "Links files to time periods, used in JOINs with months table"
                },
                {
                    "column_name": "created_at",
                    "data_type": "TIMESTAMP",
                    "description": "Timestamp when the file record was created",
                    "usage": "Audit trail and file upload tracking"
                }
            ],
            "relationships": [
                {
                    "type": "many-to-one",
                    "related_table": "months",
                    "foreign_key": "month_pk",
                    "description": "Many files belong to one month"
                },
                {
                    "type": "one-to-many",
                    "related_table": "Mastersheet-Keyword_report",
                    "foreign_key": "Mastersheet-Keyword_report.file_id",
                    "description": "One file can have many keyword records"
                }
            ],
            "constraints": [
                "FOREIGN KEY (month_pk) REFERENCES months(month_pk) ON DELETE CASCADE"
            ]
        },
        {
            "table_name": "Mastersheet-Keyword_report",
            "table_description": "Main keyword data table containing SEO keyword rankings, search volumes, and performance metrics for clients.",
            "use_cases": [
                "Keyword search volume analysis",
                "Ranking tracking and changes",
                "Client keyword performance",
                "Time-based keyword trends",
                "Aggregations (SUM, COUNT, AVG) on search volumes and rankings",
                "Top keyword identification",
                "Ranking improvement analysis"
            ],
            "columns": [
                {
                    "column_name": "id",
                    "data_type": "SERIAL PRIMARY KEY",
                    "description": "Primary key for keyword records, auto-incrementing integer",
                    "usage": "Unique identifier for each keyword record"
                },
                {
                    "column_name": "file_id",
                    "data_type": "INTEGER NOT NULL",
                    "description": "Foreign key referencing files.file_id",
                    "usage": "Links keywords to files and clients, used in JOINs with files table"
                },
                {
                    "column_name": "keyword",
                    "data_type": "TEXT NOT NULL",
                    "description": "The SEO keyword or search term",
                    "usage": "Primary data field, used in SELECT, WHERE, and GROUP BY clauses"
                },
                {
                    "column_name": "initial_ranking",
                    "data_type": "INTEGER",
                    "description": "Initial ranking position of the keyword (can be NULL)",
                    "usage": "Used for calculating ranking changes, comparisons, and trend analysis"
                },
                {
                    "column_name": "current_ranking",
                    "data_type": "INTEGER",
                    "description": "Current ranking position of the keyword (can be NULL)",
                    "usage": "Used for current performance analysis, filtering top rankings, and comparisons"
                },
                {
                    "column_name": "change",
                    "data_type": "INTEGER",
                    "description": "Change in ranking (positive = improvement, negative = decline, can be NULL)",
                    "usage": "Used for identifying ranking improvements/declines, filtering by change magnitude"
                },
                {
                    "column_name": "search_volume",
                    "data_type": "INTEGER",
                    "description": "Estimated monthly search volume for the keyword (can be NULL)",
                    "usage": "Primary metric for aggregations (SUM, AVG), filtering high-volume keywords, trend analysis"
                },
                {
                    "column_name": "map_ranking_gbp",
                    "data_type": "INTEGER",
                    "description": "Google Maps ranking in GBP (Google Business Profile) context (can be NULL)",
                    "usage": "Local SEO analysis, filtering by local ranking performance"
                },
                {
                    "column_name": "location",
                    "data_type": "TEXT",
                    "description": "Geographic location associated with the keyword (can be NULL)",
                    "usage": "Location-based filtering and analysis"
                },
                {
                    "column_name": "url",
                    "data_type": "TEXT",
                    "description": "URL associated with the keyword ranking (can be NULL)",
                    "usage": "Reference and tracking purposes"
                },
                {
                    "column_name": "difficulty",
                    "data_type": "INTEGER",
                    "description": "SEO difficulty score for the keyword (can be NULL)",
                    "usage": "Filtering by difficulty level, identifying easy/hard keywords"
                },
                {
                    "column_name": "search_intent",
                    "data_type": "TEXT",
                    "description": "Search intent category (e.g., 'informational', 'commercial', 'navigational') (can be NULL)",
                    "usage": "Intent-based filtering and analysis"
                },
                {
                    "column_name": "created_at",
                    "data_type": "TIMESTAMP",
                    "description": "Timestamp when the keyword record was created",
                    "usage": "Audit trail and record tracking"
                }
            ],
            "relationships": [
                {
                    "type": "many-to-one",
                    "related_table": "files",
                    "foreign_key": "file_id",
                    "description": "Many keywords belong to one file"
                }
            ],
            "constraints": [
                "FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE"
            ],
            "common_filters": [
                "WHERE search_volume IS NOT NULL - Filter out null search volumes",
                "WHERE change > 0 - Find keywords with ranking improvements",
                "WHERE current_ranking IS NOT NULL - Filter valid rankings"
            ],
            "common_aggregations": [
                "SUM(search_volume) - Total search volume",
                "AVG(search_volume) - Average search volume",
                "COUNT(*) - Count of keywords",
                "AVG(change) - Average ranking change"
            ]
        }
    ]
}


def get_semantic_model() -> Dict[str, Any]:
    """
    Get the semantic model with table descriptions.
    
    Returns:
        Dictionary containing semantic model
    """
    return SEMANTIC_MODEL


def get_semantic_model_str() -> str:
    """
    Get the semantic model as a formatted JSON string.
    
    Returns:
        JSON string of semantic model
    """
    return json.dumps(SEMANTIC_MODEL, indent=2)


def get_table_description(table_name: str) -> Dict[str, Any]:
    """
    Get detailed description for a specific table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table description or None if not found
    """
    for table in SEMANTIC_MODEL["tables"]:
        if table["table_name"] == table_name:
            return table
    return None


def get_all_table_schemas() -> str:
    """
    Get complete schema information for all tables with relationships.
    
    Returns:
        Formatted string with all table schemas and relationships
    """
    schema_text = """
═══════════════════════════════════════════════════════════════════════════════
COMPLETE DATABASE SCHEMA
═══════════════════════════════════════════════════════════════════════════════

"""
    
    for table in SEMANTIC_MODEL["tables"]:
        schema_text += f"""
TABLE: {table['table_name']}
{'─' * 70}
Description: {table['table_description']}

Columns:
"""
        for col in table['columns']:
            schema_text += f"  • {col['column_name']} ({col['data_type']}): {col['description']}\n"
            schema_text += f"    Usage: {col['usage']}\n"
        
        if table.get('relationships'):
            schema_text += "\nRelationships:\n"
            for rel in table['relationships']:
                schema_text += f"  • {rel['type']}: {rel['description']}\n"
                schema_text += f"    Foreign Key: {rel.get('foreign_key', 'N/A')}\n"
        
        if table.get('use_cases'):
            schema_text += "\nUse Cases:\n"
            for use_case in table['use_cases']:
                schema_text += f"  • {use_case}\n"
        
        schema_text += "\n" + "=" * 70 + "\n"
    
    # Add join paths
    schema_text += "\nJOIN PATHS:\n"
    join_paths = get_join_paths()
    for path in join_paths:
        schema_text += f"""
  {path.get('from_table')} → {path.get('to_table')}
    Type: {path.get('join_type', 'INNER')}
    Condition: {path.get('condition', 'N/A')}
    Description: {path.get('description', 'N/A')}
"""
    
    return schema_text


def get_join_paths() -> List[Dict[str, str]]:
    """
    Get common join paths between tables.
    
    Returns:
        List of join path dictionaries
    """
    join_paths = []
    
    # months -> files
    join_paths.append({
        "from_table": "months",
        "to_table": "files",
        "join_type": "INNER",
        "condition": "months.month_pk = files.month_pk",
        "description": "Join months to files to get time context for files"
    })
    
    # files -> Mastersheet-Keyword_report
    join_paths.append({
        "from_table": "files",
        "to_table": "Mastersheet-Keyword_report",
        "join_type": "INNER",
        "condition": "files.file_id = \"Mastersheet-Keyword_report\".file_id",
        "description": "Join files to keywords to get client and time context for keywords"
    })
    
    # months -> files -> Mastersheet-Keyword_report (common 3-table join)
    join_paths.append({
        "from_table": "months",
        "to_table": "files",
        "intermediate_table": "Mastersheet-Keyword_report",
        "join_type": "INNER",
        "condition": "months.month_pk = files.month_pk AND files.file_id = \"Mastersheet-Keyword_report\".file_id",
        "description": "Full join path from time to keywords with client context"
    })
    
    return join_paths


# ============================================================================
# Schema Context for LLM Prompts
# ============================================================================

SCHEMA_CONTEXT = """
Database Schema:
- months (month_pk, month_name, year, month_id)
- files (file_id, client_name, file_name, month_pk)
- Mastersheet-Keyword_report (id, file_id, keyword, initial_ranking, current_ranking, change, search_volume, map_ranking_gbp, location, url, difficulty, search_intent)
"""


if __name__ == "__main__":
    print("Database Schema Definitions")
    print("="*70)
    print(f"\nTables: {len(SEMANTIC_MODEL['tables'])}")
    for table in SEMANTIC_MODEL["tables"]:
        print(f"\n📊 {table['table_name']}")
        print(f"   Description: {table['table_description']}")
        print(f"   Columns: {len(table['columns'])}")
        print(f"   Use Cases: {len(table['use_cases'])}")
    
    print(f"\n\nJoin Paths: {len(get_join_paths())}")
    for path in get_join_paths():
        print(f"\n🔗 {path.get('from_table')} → {path.get('to_table')}")
        print(f"   {path.get('description')}")
