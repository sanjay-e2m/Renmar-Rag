"""
Agent State Schema
Defines the state structure for the LangGraph agent
"""

from typing import TypedDict, List, Optional, Any, Dict
from datetime import datetime


class AgentState(TypedDict):
    """
    State schema for the SQL-RAG agent.
    All nodes read from and write to this state.
    """
    # User Input
    user_query: str  # Formatted query (after validation/formatting)
    original_query: Optional[str]  # Original query before formatting
    session_id: str
    
    # Query Formatting & Retry Tracking
    query_formatting_attempt: int  # 0 = not formatted, 1 = first format, 2 = reformat
    first_formatted_query: Optional[str]  # First formatted query (if formatting was used)
    
    # Query Generation
    generated_sql: Optional[str]
    sql_generation_attempts: int
    sql_generation_error: Optional[str]
    sql_valid: bool
    
    # Query Execution
    query_result: Optional[Any]  # List of dicts (from DataFrame), list, dict, or scalar (must be serializable)
    execution_error: Optional[str]
    execution_success: bool
    execution_retry_count: int  # Track execution retries
    
    # Answer Formatting
    formatted_answer: Optional[str]
    raw_data_summary: Optional[str]
    
    # Conversation History
    conversation_history: List[Dict[str, str]]  # Previous turns in session
    
    # Metadata
    metadata: Dict[str, Any]  # timestamps, client info, etc.
    
    # Error handling
    error: Optional[str]
    error_type: Optional[str]  # 'generation', 'validation', 'execution', 'formatting'
