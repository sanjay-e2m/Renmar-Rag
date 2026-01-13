"""
Agent Nodes
Individual node implementations for the LangGraph agent
"""

import sys
from pathlib import Path
import re
from typing import Dict, Any
import pandas as pd
from groq import Groq
import os
from dotenv import load_dotenv

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from rag_excel_postgres.agent.state import AgentState
from rag_excel_postgres.agent.tools import AgentTools

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


def validate_input_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 1: Validate input (NO formatting on first attempt).
    
    Actions:
    - Check query validity
    - Generate/load session_id
    - Load conversation history
    - Sanitize input
    - Store original query (formatting will be done later if needed)
    """
    print("\n🔵 DEBUG: [validate_input_node] Starting validation...")
    user_query = state.get("user_query", "").strip()
    print(f"   Input query: {user_query}")
    
    # Validate query
    if not user_query:
        print("   ❌ Query is empty")
        return {
            "error": "User query cannot be empty",
            "error_type": "validation"
        }
    
    # Store original query (no formatting on first attempt)
    original_query = user_query
    print(f"   📝 Storing original query: {original_query}")
    
    # Generate or use existing session_id
    session_id = state.get("session_id")
    if not session_id:
        session_id = tools.generate_session_id()
        print(f"   🆔 Generated new session ID: {session_id}")
    else:
        print(f"   🆔 Using existing session ID: {session_id}")
    
    # Load conversation history (limit to 3 for token efficiency)
    conversation_history = tools.get_conversation_history(session_id, limit=3)
    print(f"   📚 Loaded {len(conversation_history)} previous conversations")
    
    # Basic sanitization (remove any remaining problematic characters)
    sanitized_query = re.sub(r'[^\w\s\?\.\,\'\"]', '', user_query)
    print(f"   ✅ Validation complete - proceeding with original query (no formatting)")
    
    return {
        "user_query": sanitized_query,  # Use original query (no formatting yet)
        "original_query": original_query,  # Store original for reference
        "session_id": session_id,
        "conversation_history": conversation_history,
        "query_formatting_attempt": 0,  # 0 = not formatted yet
        "first_formatted_query": None,
        "execution_retry_count": 0,
        "metadata": {
            **state.get("metadata", {}),
            "input_validated_at": pd.Timestamp.now().isoformat()
        }
    }


def generate_sql_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 2: Generate SQL query from natural language.
    
    Actions:
    - Use QueryGenerator to generate SQL
    - Handle retries on failure
    - Track attempts
    """
    print("\n🟢 DEBUG: [generate_sql_node] Generating SQL...")
    user_query = state["user_query"]
    session_id = state["session_id"]
    attempts = state.get("sql_generation_attempts", 0)
    formatting_attempt = state.get("query_formatting_attempt", 0)
    max_attempts = 3
    
    print(f"   Query to use: {user_query}")
    print(f"   Formatting attempt: {formatting_attempt}")
    print(f"   SQL generation attempt: {attempts + 1}/{max_attempts}")
    
    if attempts >= max_attempts:
        return {
            "error": f"SQL generation failed after {max_attempts} attempts",
            "error_type": "generation",
            "sql_generation_error": state.get("sql_generation_error", "Max retries exceeded")
        }
    
    try:
        print(f"   🔄 Calling QueryGenerator.generate_sql_query()...")
        # Generate SQL with session context
        sql_query = tools.query_generator.generate_sql_query(
            user_question=user_query,
            session_id=session_id,
            max_retries=1  # Single attempt per node call
        )
        
        if sql_query:
            print(f"   ✅ SQL generated successfully")
            print(f"   📝 SQL: {sql_query[:150] + '...' if len(sql_query) > 150 else sql_query}")
            return {
                "generated_sql": sql_query,
                "sql_generation_attempts": attempts + 1,
                "sql_generation_error": None,
                "error": None
            }
        else:
            print(f"   ❌ SQL generation returned None")
            return {
                "sql_generation_attempts": attempts + 1,
                "sql_generation_error": "Failed to generate SQL query",
                "error": "SQL generation returned None",
                "error_type": "generation"
            }
    
    except Exception as e:
        return {
            "sql_generation_attempts": attempts + 1,
            "sql_generation_error": str(e),
            "error": f"SQL generation error: {str(e)}",
            "error_type": "generation"
        }


def validate_sql_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 3: Validate generated SQL before execution.
    
    Actions:
    - Security checks (no DROP, DELETE, etc.)
    - Basic syntax validation
    - Table/column verification
    """
    sql_query = state.get("generated_sql", "").strip().upper()
    
    if not sql_query:
        return {
            "sql_valid": False,
            "error": "No SQL query to validate",
            "error_type": "validation"
        }
    
    # Security checks - block dangerous operations
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    for keyword in dangerous_keywords:
        if keyword in sql_query:
            return {
                "sql_valid": False,
                "error": f"SQL query contains dangerous operation: {keyword}",
                "error_type": "validation"
            }
    
    # Must start with SELECT
    if not sql_query.startswith('SELECT'):
        return {
            "sql_valid": False,
            "error": "SQL query must start with SELECT",
            "error_type": "validation"
        }
    
    # Basic syntax check - must have FROM
    if 'FROM' not in sql_query:
        return {
            "sql_valid": False,
            "error": "SQL query must contain FROM clause",
            "error_type": "validation"
        }
    
    # Verify table name is reports_master
    if 'REPORTS_MASTER' not in sql_query:
        return {
            "sql_valid": False,
            "error": "SQL query must reference reports_master table",
            "error_type": "validation"
        }
    
    return {
        "sql_valid": True,
        "error": None
    }


def execute_sql_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 4: Execute SQL query on PostgreSQL.
    
    Actions:
    - Execute query via QueryExecutor
    - Handle database errors
    - Convert results to appropriate format
    """
    print("\n🟡 DEBUG: [execute_sql_node] Executing SQL...")
    sql_query = state.get("generated_sql")
    formatting_attempt = state.get("query_formatting_attempt", 0)
    
    print(f"   Formatting attempt: {formatting_attempt}")
    print(f"   SQL to execute: {sql_query[:150] + '...' if sql_query and len(sql_query) > 150 else sql_query}")
    
    if not sql_query:
        print("   ❌ No SQL query to execute")
        return {
            "execution_success": False,
            "execution_error": "No SQL query to execute",
            "error": "Missing SQL query",
            "error_type": "execution"
        }
    
    try:
        print(f"   🔄 Calling QueryExecutor.execute_sql_query()...")
        result = tools.query_executor.execute_sql_query(
            sql_query=sql_query,
            return_dataframe=True
        )
        
        # Convert DataFrame to serializable format (list of dicts) for LangGraph state
        # LangGraph checkpoint system cannot serialize pandas DataFrames
        if isinstance(result, pd.DataFrame):
            # Convert DataFrame to list of dictionaries for serialization
            serializable_result = result.to_dict('records')
            row_count = len(serializable_result)
            print(f"   📊 Query returned {row_count} rows")
            
            # Check if result is empty (0 rows)
            if len(serializable_result) == 0:
                print(f"   ⚠️  Query returned 0 rows - marking as failed")
                return {
                    "query_result": serializable_result,
                    "execution_success": False,  # Mark as failed if 0 rows
                    "execution_error": "Query returned 0 rows",
                    "error": "No data returned from query",
                    "error_type": "execution"
                }
        else:
            serializable_result = result
            # Check if result is empty
            if isinstance(serializable_result, list):
                row_count = len(serializable_result)
                print(f"   📊 Query returned {row_count} rows")
                if len(serializable_result) == 0:
                    print(f"   ⚠️  Query returned 0 rows - marking as failed")
                    return {
                        "query_result": serializable_result,
                        "execution_success": False,
                        "execution_error": "Query returned 0 rows",
                        "error": "No data returned from query",
                        "error_type": "execution"
                    }
            else:
                print(f"   📊 Query returned scalar/dict result")
        
        print(f"   ✅ SQL execution successful with {row_count if 'row_count' in locals() else 'data'} rows")
        return {
            "query_result": serializable_result,
            "execution_success": True,
            "execution_error": None,
            "error": None
        }
    
    except Exception as e:
        print(f"   ❌ SQL execution error: {str(e)}")
        return {
            "execution_success": False,
            "execution_error": str(e),
            "error": f"SQL execution error: {str(e)}",
            "error_type": "execution"
        }


def format_answer_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 5: Format query results into natural language response.
    
    Actions:
    - Use LLM to format results
    - Include conversation context
    - Generate natural language response
    """
    user_query = state["user_query"]
    query_result = state.get("query_result")
    sql_query = state.get("generated_sql", "")
    
    if query_result is None:
        return {
            "formatted_answer": "No results to format",
            "error": "Query result is None",
            "error_type": "formatting"
        }
    
    # Format data for context
    # Note: query_result is now a list of dicts (converted from DataFrame for serialization)
    if isinstance(query_result, list):
        if len(query_result) == 0:
            data_summary = "No data found (empty result set)"
        else:
            # Convert back to DataFrame for formatting (temporary, not stored in state)
            df = pd.DataFrame(query_result)
            data_summary = f"Results: {len(query_result)} rows × {len(df.columns)} columns\n"
            data_summary += df.head(20).to_string(index=False)
            if len(query_result) > 20:
                data_summary += f"\n... (showing first 20 of {len(query_result)} rows)"
    elif isinstance(query_result, dict):
        data_summary = str(query_result)
    else:
        data_summary = str(query_result)
    
    # Use Groq to format answer
    try:
        client = Groq(api_key=GROQ_API_KEY)
        model = GROQ_MODEL
        
        prompt = f"""You are a helpful data analyst assistant. Format the database query results into a clear, user-friendly answer.

USER QUESTION: {user_query}

SQL QUERY EXECUTED: {sql_query}

QUERY RESULT DATA:
{data_summary}

INSTRUCTIONS:
1. Provide a clear, conversational answer that directly addresses the user's question
2. If the result is empty, politely explain that no data was found matching the criteria
3. For tables with multiple rows: summarize key findings and highlight important numbers
4. Use professional but friendly tone
5. Don't repeat the SQL query unless specifically asked
6. Focus on insights and key information
7. Format numbers nicely (use commas for large numbers)

FORMAT YOUR ANSWER:"""

        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful data analyst assistant. You format database query results into clear, user-friendly answers."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.1
        )
        
        formatted_answer = response.choices[0].message.content.strip()
        
        return {
            "formatted_answer": formatted_answer,
            "raw_data_summary": data_summary,
            "error": None
        }
    
    except Exception as e:
        # Fallback to basic formatting
        return {
            "formatted_answer": data_summary,
            "raw_data_summary": data_summary,
            "error": f"Error formatting answer: {str(e)}",
            "error_type": "formatting"
        }


def save_conversation_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 6: Save conversation to database.
    
    Actions:
    - Determine which query version succeeded (original, formatted, or reformatted)
    - Save the successful query version, system_response, session_id
    - Update conversation_history in state
    """
    session_id = state["session_id"]
    formatted_answer = state.get("formatted_answer", "No answer generated")
    execution_success = state.get("execution_success", False)
    
    # Determine which query version succeeded
    formatting_attempt = state.get("query_formatting_attempt", 0)
    original_query = state.get("original_query", "")
    first_formatted_query = state.get("first_formatted_query")
    current_user_query = state.get("user_query", "")
    
    # Determine successful query version based on formatting attempt and execution success
    if execution_success:
        if formatting_attempt == 0:
            # Original query succeeded (no formatting was needed)
            successful_query = original_query
            print(f"💾 DEBUG: Storing original query (attempt 0): {successful_query}")
        elif formatting_attempt == 1:
            # First formatted query succeeded
            successful_query = first_formatted_query if first_formatted_query else current_user_query
            print(f"💾 DEBUG: Storing first formatted query (attempt 1): {successful_query}")
        elif formatting_attempt == 2:
            # Reformatted query succeeded
            successful_query = current_user_query
            print(f"💾 DEBUG: Storing reformatted query (attempt 2): {successful_query}")
        else:
            # Fallback to current user query
            successful_query = current_user_query
            print(f"💾 DEBUG: Storing current user query (fallback): {successful_query}")
    else:
        # Execution failed, store the last attempted query
        successful_query = current_user_query
        print(f"💾 DEBUG: Execution failed, storing last attempted query: {successful_query}")
    
    try:
        print(f"💾 DEBUG: Saving conversation to database...")
        print(f"   Session ID: {session_id}")
        print(f"   User Query (to save): {successful_query}")
        print(f"   Answer Length: {len(formatted_answer)} chars")
        
        success = tools.save_conversation(
            session_id=session_id,
            user_query=successful_query,  # Save the successful query version
            system_response=formatted_answer
        )
        
        if success:
            print(f"✅ DEBUG: Conversation saved successfully")
            # Update conversation history in state
            conversation_history = state.get("conversation_history", [])
            conversation_history.append({
                "user_query": successful_query,  # Store successful query version
                "system_response": formatted_answer
            })
            
            return {
                "conversation_history": conversation_history[-5:],  # Keep last 5
                "metadata": {
                    **state.get("metadata", {}),
                    "conversation_saved_at": pd.Timestamp.now().isoformat(),
                    "saved_query_version": successful_query,
                    "formatting_attempt_when_saved": formatting_attempt
                }
            }
        else:
            print(f"❌ DEBUG: Failed to save conversation")
            return {
                "error": "Failed to save conversation",
                "error_type": "save"
            }
    
    except Exception as e:
        print(f"❌ DEBUG: Exception saving conversation: {str(e)}")
        return {
            "error": f"Error saving conversation: {str(e)}",
            "error_type": "save"
        }


def format_and_retry_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node: Format query and retry SQL generation (2nd attempt).
    
    Actions:
    - Format the original query using QueryFormatter
    - Update user_query with formatted version
    - Reset SQL generation state for retry
    """
    print("\n🟣 DEBUG: [format_and_retry_node] Formatting query for retry...")
    original_query = state.get("original_query", state.get("user_query", ""))
    formatting_attempt = state.get("query_formatting_attempt", 0)
    
    print(f"   Original query: {original_query}")
    print(f"   Current formatting attempt: {formatting_attempt}")
    
    # Only format if this is the first formatting attempt
    if formatting_attempt == 0:
        print(f"   🔄 Calling QueryFormatter.format_query()...")
        format_result = tools.query_formatter.format_query(
            query=original_query,
            use_llm=True
        )
        
        formatted_query = format_result['formatted_query']
        if not formatted_query:
            formatted_query = original_query
        
        # Sanitize formatted query
        sanitized_query = re.sub(r'[^\w\s\?\.\,\'\"]', '', formatted_query)
        
        print(f"   ✅ Query formatted: {sanitized_query}")
        print(f"   📝 Changes made: {format_result.get('changes_made', [])}")
        print(f"   🔄 Resetting SQL generation state for retry...")
        
        return {
            "user_query": sanitized_query,
            "query_formatting_attempt": 1,
            "first_formatted_query": sanitized_query,
            "generated_sql": None,  # Reset SQL for regeneration
            "sql_generation_attempts": 0,  # Reset attempts
            "sql_generation_error": None,
            "execution_retry_count": state.get("execution_retry_count", 0) + 1,
            "metadata": {
                **state.get("metadata", {}),
                "formatting_changes": format_result.get("changes_made", []),
                "formatted_at": pd.Timestamp.now().isoformat()
            }
        }
    else:
        # Already formatted, return as-is
        print(f"   ⚠️  Already formatted (attempt {formatting_attempt}), skipping")
        return {}


def reformat_and_retry_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node: Reformat query with error context and retry SQL generation (3rd attempt).
    
    Actions:
    - Reformat query using reformat_query with error context
    - Update user_query with reformatted version
    - Reset SQL generation state for retry
    """
    print("\n🔴 DEBUG: [reformat_and_retry_node] Reformatting query with error context...")
    original_query = state.get("original_query", state.get("user_query", ""))
    first_formatted_query = state.get("first_formatted_query", "")
    execution_error = state.get("execution_error", "")
    sql_generation_error = state.get("sql_generation_error", "")
    
    print(f"   Original query: {original_query}")
    print(f"   First formatted query: {first_formatted_query}")
    print(f"   Execution error: {execution_error}")
    print(f"   SQL generation error: {sql_generation_error}")
    
    # Combine error context
    error_context = f"Execution Error: {execution_error}. SQL Generation Error: {sql_generation_error}"
    if not error_context.strip() or error_context.strip() == "Execution Error: . SQL Generation Error:":
        error_context = "Query returned 0 rows or SQL generation failed"
    
    print(f"   Error context: {error_context}")
    print(f"   🔄 Calling QueryFormatter.reformat_query()...")
    
    # Reformat with error context
    reformat_result = tools.query_formatter.reformat_query(
        original_query=original_query,
        previous_formatted_query=first_formatted_query if first_formatted_query else original_query,
        error_context=error_context,
        use_llm=True
    )
    
    reformatted_query = reformat_result['formatted_query']
    if not reformatted_query:
        reformatted_query = original_query
    
    # Sanitize reformatted query
    sanitized_query = re.sub(r'[^\w\s\?\.\,\'\"]', '', reformatted_query)
    
    print(f"   ✅ Query reformatted: {sanitized_query}")
    print(f"   📝 Changes made: {reformat_result.get('changes_made', [])}")
    print(f"   🔄 Resetting SQL generation state for retry...")
    
    return {
        "user_query": sanitized_query,
        "query_formatting_attempt": 2,
        "generated_sql": None,  # Reset SQL for regeneration
        "sql_generation_attempts": 0,  # Reset attempts
        "sql_generation_error": None,
        "execution_retry_count": state.get("execution_retry_count", 0) + 1,
        "metadata": {
            **state.get("metadata", {}),
            "reformatting_changes": reformat_result.get("changes_made", []),
            "reformatted_at": pd.Timestamp.now().isoformat(),
            "error_context_used": error_context
        }
    }


def error_handler_node(state: AgentState, tools: AgentTools) -> Dict[str, Any]:
    """
    Node 7: Handle errors gracefully.
    
    Actions:
    - Categorize error type
    - Generate user-friendly error message
    - Decide on retry vs. fail
    """
    error = state.get("error", "Unknown error")
    error_type = state.get("error_type", "unknown")
    
    # Generate user-friendly error message
    error_messages = {
        "validation": "I couldn't validate your query. Please try rephrasing your question.",
        "generation": "I had trouble generating a SQL query. Please try a different question.",
        "execution": "The query couldn't be executed. Please check your question and try again.",
        "formatting": "I retrieved the data but had trouble formatting it. Here's the raw result.",
        "save": "The query was successful, but I couldn't save it to history.",
        "unknown": "An unexpected error occurred. Please try again."
    }
    
    user_message = error_messages.get(error_type, error_messages["unknown"])
    
    return {
        "formatted_answer": f"❌ Error: {user_message}\n\nTechnical details: {error}",
        "error": error,
        "error_type": error_type
    }
