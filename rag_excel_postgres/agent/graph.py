"""
LangGraph Agent Graph
Constructs and compiles the agent graph
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from rag_excel_postgres.agent.state import AgentState
from rag_excel_postgres.agent.nodes import (
    validate_input_node,
    generate_sql_node,
    validate_sql_node,
    execute_sql_node,
    format_answer_node,
    save_conversation_node,
    error_handler_node,
    format_and_retry_node,
    reformat_and_retry_node
)
from rag_excel_postgres.agent.edges import (
    should_continue_after_validation,
    should_continue_after_execution,
    should_retry_sql_generation
)
from rag_excel_postgres.agent.tools import AgentTools


class AgentGraph:
    """LangGraph Agent for SQL-RAG pipeline"""
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the agent graph.
        
        Parameters:
        -----------
        groq_api_key : Optional[str]
            Groq API key
        groq_model : Optional[str]
            Groq model name
        db_config : Optional[Dict[str, Any]]
            Database configuration
        """
        self.tools = AgentTools(
            groq_api_key=groq_api_key,
            groq_model=groq_model,
            db_config=db_config
        )
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        # Create graph with AgentState
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("validate_input", lambda state: validate_input_node(state, self.tools))
        workflow.add_node("generate_sql", lambda state: generate_sql_node(state, self.tools))
        workflow.add_node("validate_sql", lambda state: validate_sql_node(state, self.tools))
        workflow.add_node("execute_sql", lambda state: execute_sql_node(state, self.tools))
        workflow.add_node("format_answer", lambda state: format_answer_node(state, self.tools))
        workflow.add_node("save_conversation", lambda state: save_conversation_node(state, self.tools))
        workflow.add_node("error_handler", lambda state: error_handler_node(state, self.tools))
        workflow.add_node("format_and_retry", lambda state: format_and_retry_node(state, self.tools))
        workflow.add_node("reformat_and_retry", lambda state: reformat_and_retry_node(state, self.tools))
        
        # Set entry point
        workflow.set_entry_point("validate_input")
        
        # Add linear edges
        workflow.add_edge("validate_input", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")
        
        # Conditional edge after validation
        workflow.add_conditional_edges(
            "validate_sql",
            should_continue_after_validation,
            {
                "execute_sql": "execute_sql",
                "error_handler": "error_handler"
            }
        )
        
        # Conditional edge after execution
        workflow.add_conditional_edges(
            "execute_sql",
            should_continue_after_execution,
            {
                "format_answer": "format_answer",
                "format_and_retry": "format_and_retry",
                "reformat_and_retry": "reformat_and_retry",
                "error_handler": "error_handler"
            }
        )
        
        # Retry nodes route back to generate_sql
        workflow.add_edge("format_and_retry", "generate_sql")
        workflow.add_edge("reformat_and_retry", "generate_sql")
        
        # Linear edges for success path
        workflow.add_edge("format_answer", "save_conversation")
        workflow.add_edge("save_conversation", END)
        
        # Error handler can retry or end
        workflow.add_conditional_edges(
            "error_handler",
            should_retry_sql_generation,
            {
                "generate_sql": "generate_sql",
                END: END
            }
        )
        
        # Compile with memory for conversation persistence
        checkpointer = MemorySaver()
        return workflow.compile(checkpointer=checkpointer)
    
    def invoke(self, user_query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Invoke the agent with a user query.
        
        Parameters:
        -----------
        user_query : str
            User's natural language question
        session_id : Optional[str]
            Session identifier. If None, a new session is created.
            
        Returns:
        --------
        Dict[str, Any]
            Final state with formatted answer
        """
        # Initialize state
        initial_state: AgentState = {
            "user_query": user_query,
            "original_query": user_query,  # Will be updated by validate_input node
            "session_id": session_id or self.tools.generate_session_id(),
            "query_formatting_attempt": 0,
            "first_formatted_query": None,
            "generated_sql": None,
            "sql_generation_attempts": 0,
            "sql_generation_error": None,
            "sql_valid": False,
            "query_result": None,
            "execution_error": None,
            "execution_success": False,
            "execution_retry_count": 0,
            "formatted_answer": None,
            "raw_data_summary": None,
            "conversation_history": [],
            "metadata": {},
            "error": None,
            "error_type": None
        }
        
        # Run the graph
        config = {"configurable": {"thread_id": initial_state["session_id"]}}
        final_state = self.graph.invoke(initial_state, config=config)
        
        return final_state
    
    def stream(self, user_query: str, session_id: Optional[str] = None):
        """
        Stream the agent execution.
        
        Parameters:
        -----------
        user_query : str
            User's natural language question
        session_id : Optional[str]
            Session identifier
            
        Yields:
        -------
        Dict[str, Any]
            State at each step
        """
        initial_state: AgentState = {
            "user_query": user_query,
            "original_query": user_query,  # Will be updated by validate_input node
            "session_id": session_id or self.tools.generate_session_id(),
            "query_formatting_attempt": 0,
            "first_formatted_query": None,
            "generated_sql": None,
            "sql_generation_attempts": 0,
            "sql_generation_error": None,
            "sql_valid": False,
            "query_result": None,
            "execution_error": None,
            "execution_success": False,
            "execution_retry_count": 0,
            "formatted_answer": None,
            "raw_data_summary": None,
            "conversation_history": [],
            "metadata": {},
            "error": None,
            "error_type": None
        }
        
        config = {"configurable": {"thread_id": initial_state["session_id"]}}
        
        for step in self.graph.stream(initial_state, config=config, stream_mode="values"):
            yield step


def create_agent_graph(
    groq_api_key: Optional[str] = None,
    groq_model: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None
) -> AgentGraph:
    """
    Factory function to create an agent graph.
    
    Parameters:
    -----------
    groq_api_key : Optional[str]
        Groq API key
    groq_model : Optional[str]
        Groq model name
    db_config : Optional[Dict[str, Any]]
        Database configuration
        
    Returns:
    --------
    AgentGraph
        Compiled agent graph
    """
    return AgentGraph(
        groq_api_key=groq_api_key,
        groq_model=groq_model,
        db_config=db_config
    )
