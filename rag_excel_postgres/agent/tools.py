"""
Tool Wrappers
Wraps existing components as tools for the agent
"""

import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from rag_excel_postgres.llm.query_generator import QueryGenerator
from rag_excel_postgres.llm.query_executor import QueryExecutor
from rag_excel_postgres.llm.query_formatter import QueryFormatter
from rag_excel_postgres.postgres_insert_create.conversation_manager import ConversationManager


class AgentTools:
    """Wrapper class for agent tools"""
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize agent tools.
        
        Parameters:
        -----------
        groq_api_key : Optional[str]
            Groq API key
        groq_model : Optional[str]
            Groq model name
        db_config : Optional[Dict[str, Any]]
            Database configuration
        """
        self.query_generator = QueryGenerator(
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
        self.query_executor = QueryExecutor(
            query_generator=self.query_generator,
            db_config=db_config
        )
        self.conversation_manager = ConversationManager(db_config=db_config)
        self.query_formatter = QueryFormatter(
            groq_api_key=groq_api_key,
            groq_model=groq_model
        )
    
    def generate_session_id(self) -> str:
        """Generate a new session ID"""
        return f"session_{uuid.uuid4().hex[:12]}"
    
    def get_client_list(self) -> List[str]:
        """Get unique client list from database"""
        return self.conversation_manager.get_unique_clients()
    
    def get_conversation_history(self, session_id: str, limit: int = 5) -> List[Dict[str, str]]:
        """Get recent conversation history"""
        return self.conversation_manager.get_recent_conversations(session_id, limit)
    
    def save_conversation(
        self,
        session_id: str,
        user_query: str,
        system_response: str
    ) -> bool:
        """Save conversation to database"""
        return self.conversation_manager.save_conversation(
            session_id=session_id,
            user_query=user_query,
            system_response=system_response
        )
