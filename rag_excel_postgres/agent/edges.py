"""
Conditional Edges
Routing logic for the LangGraph agent
"""

import sys
from pathlib import Path
from typing import Literal
from langgraph.graph import END

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from rag_excel_postgres.agent.state import AgentState


def should_continue_after_validation(state: AgentState) -> Literal["execute_sql", "error_handler"]:
    """
    Route after SQL validation.
    
    Returns:
    --------
    "execute_sql" if SQL is valid, "error_handler" otherwise
    """
    if state.get("sql_valid", False):
        return "execute_sql"
    else:
        return "error_handler"


def should_continue_after_execution(state: AgentState) -> Literal["format_answer", "format_and_retry", "reformat_and_retry", "error_handler"]:
    """
    Route after SQL execution.
    
    Returns:
    --------
    "format_answer" if execution successful
    "format_and_retry" if failed and not yet formatted (1st retry)
    "reformat_and_retry" if failed and already formatted once (2nd retry)
    "error_handler" if all retries exhausted
    """
    if state.get("execution_success", False):
        return "format_answer"
    
    # Check formatting attempt count
    formatting_attempt = state.get("query_formatting_attempt", 0)
    execution_retry_count = state.get("execution_retry_count", 0)
    
    # First retry: format query and retry
    if formatting_attempt == 0 and execution_retry_count < 2:
        return "format_and_retry"
    
    # Second retry: reformat with error context
    if formatting_attempt == 1 and execution_retry_count < 3:
        return "reformat_and_retry"
    
    # All retries exhausted
    return "error_handler"


def should_retry_sql_generation(state: AgentState) -> Literal["generate_sql", END]:
    """
    Decide if SQL generation should be retried after error.
    
    Returns:
    --------
    "generate_sql" if retries available, END otherwise
    """
    attempts = state.get("sql_generation_attempts", 0)
    max_attempts = 3
    
    if attempts < max_attempts:
        return "generate_sql"
    else:
        return END
