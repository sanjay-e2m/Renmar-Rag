import os
import psycopg2
from dotenv import load_dotenv
import json
from typing import Dict, List, Any

# Load environment variables
load_dotenv()

# -------------------------
# PostgreSQL Configuration
# -------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# -------------------------
# Schema Metadata Queries
# -------------------------

GET_TABLES_QUERY = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
    ORDER BY table_name;
"""

GET_FOREIGN_KEYS_QUERY = """
    SELECT 
      tc.table_name AS source_table,
      kcu.column_name AS source_column,
      ccu.table_name AS target_table,
      ccu.column_name AS target_column,
      tc.constraint_name AS constraint_name
    FROM 
      information_schema.table_constraints AS tc
    JOIN 
      information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN 
      information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE 
      tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = 'public'
    ORDER BY tc.table_name, kcu.column_name;
"""

GET_COLUMNS_QUERY = """
    SELECT 
      table_name,
      column_name,
      data_type,
      is_nullable,
      column_default
    FROM 
      information_schema.columns
    WHERE 
      table_schema = 'public'
    ORDER BY 
      table_name, ordinal_position;
"""

# -------------------------
# Schema Extraction Functions
# -------------------------

def fetch_tables() -> List[str]:
    """
    Fetch all table names from the public schema.
    
    Returns:
        List of table names
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        cursor.execute(GET_TABLES_QUERY)
        tables = [row[0] for row in cursor.fetchall()]
        
        return tables
    
    except Exception as e:
        print(f"❌ Error fetching tables: {e}")
        return []
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def fetch_foreign_keys() -> List[tuple]:
    """
    Fetch all foreign key relationships from the public schema.
    
    Returns:
        List of tuples: (source_table, source_column, target_table, target_column, constraint_name)
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        cursor.execute(GET_FOREIGN_KEYS_QUERY)
        relationships = cursor.fetchall()
        
        return relationships
    
    except Exception as e:
        print(f"❌ Error fetching foreign keys: {e}")
        return []
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def fetch_columns() -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch all column information for all tables.
    
    Returns:
        Dictionary mapping table names to lists of column info dictionaries
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        cursor.execute(GET_COLUMNS_QUERY)
        columns_data = cursor.fetchall()
        
        # Organize by table
        columns_by_table = {}
        for table_name, column_name, data_type, is_nullable, column_default in columns_data:
            if table_name not in columns_by_table:
                columns_by_table[table_name] = []
            
            columns_by_table[table_name].append({
                "column_name": column_name,
                "data_type": data_type,
                "is_nullable": is_nullable == "YES",
                "column_default": column_default
            })
        
        return columns_by_table
    
    except Exception as e:
        print(f"❌ Error fetching columns: {e}")
        return {}
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def fetch_schema_graph() -> Dict[str, List[Dict[str, str]]]:
    """
    Build a graph representation of the database schema.
    
    Returns:
        Dictionary where keys are table names and values are lists of relationships.
        Each relationship is a dict with 'to_table', 'from_column', and 'to_column'.
    """
    tables = fetch_tables()
    relationships = fetch_foreign_keys()
    
    # Initialize graph with all tables
    graph = {table: [] for table in tables}
    
    # Add relationships
    for src_table, src_col, tgt_table, tgt_col, constraint_name in relationships:
        if src_table in graph:
            graph[src_table].append({
                "to_table": tgt_table,
                "from_column": src_col,
                "to_column": tgt_col,
                "constraint_name": constraint_name
            })
    
    return graph


def get_full_schema_metadata() -> Dict[str, Any]:
    """
    Get complete schema metadata including tables, columns, and relationships.
    
    Returns:
        Dictionary with 'tables', 'columns', and 'graph' keys
    """
    tables = fetch_tables()
    columns = fetch_columns()
    graph = fetch_schema_graph()
    
    return {
        "tables": tables,
        "columns": columns,
        "graph": graph
    }


def print_schema_graph(graph: Dict[str, List[Dict[str, str]]]):
    """
    Pretty print the schema graph.
    """
    print("\n" + "="*60)
    print("Database Schema Graph")
    print("="*60)
    
    for table, relationships in graph.items():
        print(f"\n📊 Table: {table}")
        if relationships:
            for rel in relationships:
                print(f"   └─ {rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
        else:
            print("   └─ (no outgoing foreign keys)")
    
    print("\n" + "="*60)


def export_schema_to_json(output_path: str = "schema_metadata.json"):
    """
    Export complete schema metadata to a JSON file.
    
    Args:
        output_path: Path to save the JSON file
    """
    metadata = get_full_schema_metadata()
    
    try:
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"✅ Schema metadata exported to {output_path}")
    except Exception as e:
        print(f"❌ Error exporting schema: {e}")


# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    print("🔍 Extracting PostgreSQL schema metadata...\n")
    
    # Get schema graph
    graph = fetch_schema_graph()
    print_schema_graph(graph)
    
    # Get full metadata
    print("\n📋 Full Schema Metadata:")
    print("-" * 60)
    metadata = get_full_schema_metadata()
    
    print(f"\nTables found: {len(metadata['tables'])}")
    for table in metadata['tables']:
        col_count = len(metadata['columns'].get(table, []))
        rel_count = len(metadata['graph'].get(table, []))
        print(f"  • {table}: {col_count} columns, {rel_count} foreign key(s)")
    
    # Export to JSON (optional)
    print("\n💾 Exporting to JSON...")
    export_schema_to_json("schema_metadata.json")
    
    print("\n✅ Schema extraction complete!")

