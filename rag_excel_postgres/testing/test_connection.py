"""
Test Database Connection
Tests if the PostgreSQL database connection is successful using environment variables.
"""

import os
import psycopg2
from dotenv import load_dotenv

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




def test_connection():
    """
    Test the database connection and display connection details.
    """
    connection = None
    cursor = None
    
    print("=" * 60)
    print("Testing PostgreSQL Database Connection")
    print("=" * 60)
    print()
    
    # Display connection parameters (without password)
    print("Connection Parameters:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Port: {DB_CONFIG['port']}")
    print(f"  Database: {DB_CONFIG['dbname']}")
    print(f"  User: {DB_CONFIG['user']}")
    print(f"  Password: {'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else '(not set)'}")
    print()
    
    # Check if required environment variables are set
    missing_vars = []
    if not DB_CONFIG['host']:
        missing_vars.append("DB_HOST")
    if not DB_CONFIG['dbname']:
        missing_vars.append("DB_NAME")
    if not DB_CONFIG['user']:
        missing_vars.append("DB_USER")
    
    if missing_vars:
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables in your .env file.")
        return False
    
    # Attempt connection
    try:
        print("Attempting to connect...")
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Test query to verify connection
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        # Get database name
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        
        # Get PostgreSQL version
        cursor.execute("SELECT version();")
        pg_version = cursor.fetchone()[0]
        
        print("✅ Connection successful!")
        print()
        print("Connection Details:")
        print(f"  Database: {db_name}")
        print(f"  PostgreSQL Version: {pg_version.split(',')[0]}")
        print()
        
        # Test if we can query
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        if result[0] == 1:
            print("✅ Query test passed!")
        
        return True
        
    except psycopg2.OperationalError as e:
        print("❌ Connection failed!")
        print(f"   Error: {e}")
        print()
        print("Common issues:")
        print("  - Database does not exist (create it first)")
        print("  - PostgreSQL server is not running")
        print("  - Incorrect host, port, username, or password")
        print("  - Firewall blocking the connection")
        return False
        
    except psycopg2.Error as e:
        print("❌ Database error occurred!")
        print(f"   Error: {e}")
        return False
        
    except Exception as e:
        print("❌ Unexpected error occurred!")
        print(f"   Error: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("✅ Connection closed properly")


if __name__ == "__main__":
    success = test_connection()
    print()
    print("=" * 60)
    if success:
        print("Test Result: ✅ PASSED")
    else:
        print("Test Result: ❌ FAILED")
    print("=" * 60)
    exit(0 if success else 1)
