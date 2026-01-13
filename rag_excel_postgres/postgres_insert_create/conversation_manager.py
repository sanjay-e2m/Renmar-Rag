"""
Conversation Manager
Handles saving and retrieving conversation history from PostgreSQL
"""

import os
import psycopg2
from typing import List, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}


class ConversationManager:
    """Manages conversation history CRUD operations"""
    
    def __init__(self, db_config: Optional[Dict] = None):
        """
        Initialize ConversationManager.
        
        Parameters:
        -----------
        db_config : Optional[Dict], optional
            Database configuration. If None, uses environment variables.
        """
        self.db_config = db_config or DB_CONFIG
    
    def get_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    def save_conversation(
        self,
        session_id: str,
        user_query: str,
        system_response: str
    ) -> bool:
        """
        Save a conversation turn to the database.
        
        Parameters:
        -----------
        session_id : str
            Session identifier
        user_query : str
            User's question
        system_response : str
            System's response
            
        Returns:
        --------
        bool
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO conversation_history (session_id, user_query, system_response)
                VALUES (%s, %s, %s)
            """, (session_id, user_query, system_response))
            
            conn.commit()
            return True
            
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error saving conversation: {e}")
            return False
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def get_recent_conversations(
        self,
        session_id: str,
        limit: int = 5
    ) -> List[Dict[str, str]]:
        """
        Get recent conversation history for a session.
        
        Parameters:
        -----------
        session_id : str
            Session identifier
        limit : int, optional
            Number of recent conversations to retrieve. Default is 5.
            
        Returns:
        --------
        List[Dict[str, str]]
            List of conversation dictionaries with 'user_query' and 'system_response'
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT user_query, system_response
                FROM conversation_history
                WHERE session_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (session_id, limit))
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries, reverse to get chronological order
            conversations = []
            for row in reversed(results):
                conversations.append({
                    'user_query': row[0],
                    'system_response': row[1]
                })
            
            return conversations
            
        except Exception as e:
            print(f"Error fetching conversation history: {e}")
            return []
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def format_conversation_history(
        self,
        conversations: List[Dict[str, str]]
    ) -> str:
        """
        Format conversation history for LLM context.
        
        Parameters:
        -----------
        conversations : List[Dict[str, str]]
            List of conversation dictionaries
            
        Returns:
        --------
        str
            Formatted conversation history string
        """
        if not conversations:
            return ""
        
        formatted = "\nCONVERSATION HISTORY:\n"
        
        for i, conv in enumerate(conversations, 1):
            # Truncate responses more aggressively
            response_preview = conv['system_response'][:150] if len(conv['system_response']) > 150 else conv['system_response']
            formatted += f"{i}. Q: {conv['user_query'][:100]}\n   A: {response_preview}\n"
        
        formatted += "Use history for context if current question references previous queries.\n"
        
        return formatted
    
    def get_unique_clients(self) -> List[str]:
        """
        Get unique client names from reports_master table.
        
        Returns:
        --------
        List[str]
            List of unique client names, sorted alphabetically
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT client_name
                FROM reports_master
                ORDER BY client_name ASC
            """)
            
            results = cursor.fetchall()
            clients = [row[0] for row in results]
            
            return clients
            
        except Exception as e:
            print(f"Error fetching unique clients: {e}")
            return []
            
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def format_client_list(self, clients: List[str]) -> str:
        """
        Format client list for LLM context.
        
        Parameters:
        -----------
        clients : List[str]
            List of client names
            
        Returns:
        --------
        str
            Formatted client list string
        """
        if not clients:
            return ""
        
        formatted = "\nAVAILABLE CLIENTS: " + ", ".join([f"'{c}'" for c in clients])
        formatted += f"\nNote: Use lowercase client names in WHERE clauses. Example: WHERE client_name = 'efg'\n"
        
        return formatted
