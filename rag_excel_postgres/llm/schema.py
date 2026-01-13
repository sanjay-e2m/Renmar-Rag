"""
Comprehensive Database Schema Description
Contains detailed table and column descriptions for LLM context
"""

def get_database_schema() -> str:
    """
    Get comprehensive database schema with detailed table and column descriptions.
    This is used as context for LLM to generate SQL queries.
    
    Returns:
    --------
    str
        Formatted schema description with detailed column information
    """
    schema = """
TABLE: reports_master
Stores keyword ranking reports. Each row = one keyword for one client in one month/year.

COLUMNS:
FILTERS: client_name (TEXT, lowercase), year (INTEGER), month (TEXT, capitalized full name), month_id (1-12)
METRICS: keyword (TEXT), initial_ranking (INTEGER), current_ranking (INTEGER), change (INTEGER: +improved, -declined)
         search_volume (INTEGER), map_ranking_gbp (INTEGER), location (TEXT), url (TEXT)
         difficulty (INTEGER 0-100), search_intent (TEXT: 'Informational', 'Commercial', 'Transactional')

NOTES:
- client_name: lowercase ('efg', not 'EFG')
- month: full capitalized ('December', not 'Dec')
- year: integer (2025, not '2025')
- Rankings: lower number = better (1 is best)
- change > 0 = improved, change < 0 = declined
- Indexed: client_name, year, month, keyword
"""
    return schema
