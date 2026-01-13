"""
Main Pipeline for SQL-RAG System
Uses LangGraph agent for agentic query processing
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Add project root to path for imports (when run as script)
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import LangGraph Agent
from rag_excel_postgres.agent import create_agent_graph, AgentGraph

# Get configuration from environment
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

if not GROQ_API_KEY:
    raise Exception("❌ Missing GROQ_API_KEY in environment variables")


class MainPipeline:
    """
    Main pipeline using LangGraph agent for agentic SQL-RAG processing.
    """
    
    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        groq_model: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize MainPipeline with LangGraph agent.
        
        Parameters:
        -----------
        groq_api_key : Optional[str], optional
            Groq API key. If None, uses environment variable.
        groq_model : Optional[str], optional
            Groq model name. If None, uses environment variable.
        db_config : Optional[Dict[str, Any]], optional
            Database configuration dictionary. If None, uses environment variables.
        """
        # Initialize LangGraph agent
        self.agent: AgentGraph = create_agent_graph(
            groq_api_key=groq_api_key,
            groq_model=groq_model,
            db_config=db_config
        )
        
        # Initialize Groq client for legacy format_answer method (fallback)
        api_key = groq_api_key or GROQ_API_KEY
        model = groq_model or GROQ_MODEL
        try:
            from groq import Groq
            self.client = Groq(api_key=api_key)
            self.model = model
        except Exception:
            self.client = None
            self.model = None
    
    def format_data_for_context(self, result: Any) -> str:
        """
        Format query result data for LLM context.
        
        Parameters:
        -----------
        result : Any
            Query result (DataFrame, list, or scalar)
        
        Returns:
        --------
        str
            Formatted string representation of the data
        """
        if isinstance(result, pd.DataFrame):
            if len(result) == 0:
                return "No data found (empty result set)"
            
            # Convert DataFrame to formatted string
            # Limit to first 50 rows for context
            df_display = result.head(50)
            
            # Create a readable format
            formatted_lines = []
            formatted_lines.append(f"Data Summary: {len(result)} rows × {len(result.columns)} columns")
            formatted_lines.append("\nColumn Names:")
            formatted_lines.append(", ".join(result.columns.tolist()))
            formatted_lines.append("\nData:")
            
            # Format as table
            formatted_lines.append(df_display.to_string(index=False))
            
            if len(result) > 50:
                formatted_lines.append(f"\n... (showing first 50 of {len(result)} rows)")
            
            return "\n".join(formatted_lines)
        
        elif isinstance(result, list):
            if len(result) == 0:
                return "No data found (empty result set)"
            
            df = pd.DataFrame(result)
            return self.format_data_for_context(df)
        
        elif isinstance(result, dict):
            # Handle dictionary results (like aggregations)
            formatted_lines = []
            for key, value in result.items():
                formatted_lines.append(f"{key}: {value}")
            return "\n".join(formatted_lines)
        
        else:
            # Scalar value
            return f"Result: {result}"
    
    def format_answer(
        self,
        user_question: str,
        sql_query: str,
        query_result: Any,
        max_retries: int = 3
    ) -> str:
        """
        Format the answer in a user-friendly manner using LLM.
        
        Parameters:
        -----------
        user_question : str
            Original user question
        sql_query : str
            Generated SQL query
        query_result : Any
            Query result data
        max_retries : int, optional
            Maximum number of retry attempts. Default is 3.
        
        Returns:
        --------
        str
            Formatted user-friendly answer
        """
        if self.client is None:
            # Fallback: return basic formatted result
            return self.format_data_for_context(query_result)
        
        # Format data for context
        data_context = self.format_data_for_context(query_result)
        
        # Determine result type
        if isinstance(query_result, pd.DataFrame):
            result_type = "table"
            row_count = len(query_result)
            column_count = len(query_result.columns)
        elif isinstance(query_result, list):
            result_type = "list"
            row_count = len(query_result)
            column_count = len(query_result[0]) if query_result else 0
        elif isinstance(query_result, dict):
            result_type = "aggregation"
            row_count = 1
            column_count = len(query_result)
        else:
            result_type = "scalar"
            row_count = 1
            column_count = 1
        
        prompt = f"""You are a helpful data analyst assistant. Your task is to format database query results into a clear, user-friendly answer.

USER QUESTION: {user_question}

SQL QUERY EXECUTED: {sql_query}

QUERY RESULT DATA:
{data_context}

RESULT TYPE: {result_type}
ROW COUNT: {row_count}
COLUMN COUNT: {column_count}

INSTRUCTIONS:
1. Provide a clear, conversational answer that directly addresses the user's question
2. If the result is empty, politely explain that no data was found matching the criteria
3. For tables/lists with multiple rows:
   - Summarize key findings
   - Highlight important numbers or trends
   - Present data in a readable format
   - Use bullet points or numbered lists when appropriate
4. For aggregations (counts, sums, averages):
   - State the result clearly
   - Provide context if helpful
   - Use natural language (e.g., "There are 25 keywords" instead of just "25")
5. For single values:
   - Answer directly
   - Provide context if relevant
6. Use professional but friendly tone
7. Don't repeat the SQL query unless specifically asked
8. Focus on insights and key information
9. Format numbers nicely (use commas for large numbers)
10. If showing rankings or comparisons, highlight the top/bottom items

FORMAT YOUR ANSWER:"""

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": """You are a helpful data analyst assistant. You format database query results into clear, 
user-friendly answers. You communicate in a professional but friendly tone. You focus on providing insights 
and answering the user's question directly."""
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1  # Lower temperature for more consistent formatting
                )
                
                formatted_answer = response.choices[0].message.content.strip()
                return formatted_answer
                
            except Exception as e:
                error_str = str(e)
                
                # Check for quota errors
                if "429" in error_str or "quota" in error_str.lower() or "rate limit" in error_str.lower():
                    if attempt < max_retries - 1:
                        import time
                        wait_time = 60 * (attempt + 1)
                        time.sleep(wait_time)
                        continue
                    else:
                        return self.format_data_for_context(query_result)
                else:
                    return self.format_data_for_context(query_result)
        
        # Final fallback
        return self.format_data_for_context(query_result)
    
    def process_user_question(
        self,
        user_question: str,
        session_id: Optional[str] = None,
        format_answer: bool = True,
        show_sql: bool = False
    ) -> Dict[str, Any]:
        """
        Complete workflow: Process user question using LangGraph agent.
        
        Parameters:
        -----------
        user_question : str
            Natural language question
        session_id : Optional[str], optional
            Session identifier for conversation history. If None, creates new session.
        format_answer : bool, optional
            Whether to format answer using LLM. Default is True. (Always True with agent)
        show_sql : bool, optional
            Whether to include SQL query in response. Default is False.
        
        Returns:
        --------
        Dict[str, Any]
            Dictionary containing:
            - 'success': Boolean indicating success
            - 'question': User question
            - 'sql_query': Generated SQL query
            - 'raw_result': Raw query result
            - 'formatted_answer': Formatted user-friendly answer
            - 'error': Error message if failed
            - 'session_id': Session identifier
        """
        print("\n" + "=" * 80)
        print("🔍 DEBUG: Starting process_user_question")
        print("=" * 80)
        print(f"📝 Original User Question: {user_question}")
        print(f"🆔 Session ID: {session_id or 'New session will be created'}")
        print(f"🔧 Format Answer: {format_answer}")
        print(f"👁️  Show SQL: {show_sql}")
        
        try:
            print("\n🚀 DEBUG: Invoking LangGraph agent...")
            # Invoke LangGraph agent
            final_state = self.agent.invoke(
                user_query=user_question,
                session_id=session_id
            )
            print("✅ DEBUG: Agent invocation completed")
            
            # Extract results from state
            print("\n📊 DEBUG: Extracting results from final state...")
            success = final_state.get("execution_success", False) and final_state.get("formatted_answer") is not None
            sql_query = final_state.get("generated_sql")
            raw_result = final_state.get("query_result")
            formatted_answer = final_state.get("formatted_answer", "No answer generated")
            error = final_state.get("error")
            session_id = final_state.get("session_id")
            
            # Debug: Show query formatting attempts
            formatting_attempt = final_state.get("query_formatting_attempt", 0)
            original_query = final_state.get("original_query", user_question)
            first_formatted_query = final_state.get("first_formatted_query")
            current_user_query = final_state.get("user_query", user_question)
            execution_retry_count = final_state.get("execution_retry_count", 0)
            
            print(f"📈 Query Formatting Attempt: {formatting_attempt}")
            print(f"📝 Original Query: {original_query}")
            if first_formatted_query:
                print(f"✏️  First Formatted Query: {first_formatted_query}")
            print(f"💬 Current User Query (used for SQL): {current_user_query}")
            print(f"🔄 Execution Retry Count: {execution_retry_count}")
            print(f"✅ Execution Success: {final_state.get('execution_success', False)}")
            print(f"📊 Query Result: {len(raw_result) if isinstance(raw_result, list) else 'N/A'} rows")
            print(f"🔍 SQL Query: {sql_query[:100] + '...' if sql_query and len(sql_query) > 100 else sql_query}")
            
            # Determine which query version succeeded
            successful_query = user_question  # Default to original
            if success:
                if formatting_attempt == 0:
                    successful_query = original_query
                    print(f"✅ SUCCESS: Original query succeeded - storing: {successful_query}")
                elif formatting_attempt == 1:
                    successful_query = first_formatted_query if first_formatted_query else current_user_query
                    print(f"✅ SUCCESS: Formatted query succeeded - storing: {successful_query}")
                elif formatting_attempt == 2:
                    successful_query = current_user_query
                    print(f"✅ SUCCESS: Reformatted query succeeded - storing: {successful_query}")
            else:
                print(f"❌ FAILED: No successful query to store")
            
            print("=" * 80 + "\n")
            
            return {
                'success': success,
                'question': successful_query if success else user_question,  # Store successful query version
                'sql_query': sql_query if show_sql else None,
                'raw_result': raw_result,
                'formatted_answer': formatted_answer,
                'error': error,
                'session_id': session_id,
                'debug_info': {
                    'formatting_attempt': formatting_attempt,
                    'original_query': original_query,
                    'first_formatted_query': first_formatted_query,
                    'current_user_query': current_user_query,
                    'execution_retry_count': execution_retry_count,
                    'successful_query': successful_query if success else None
                }
            }
            
        except Exception as e:
            print(f"\n❌ DEBUG: Exception occurred: {str(e)}")
            print("=" * 80 + "\n")
            return {
                'success': False,
                'question': user_question,
                'sql_query': None,
                'raw_result': None,
                'formatted_answer': f"Error: {str(e)}",
                'error': str(e),
                'session_id': session_id
            }
    
    def display_result(self, result: Dict[str, Any]) -> None:
        """
        Display the result in a formatted manner.
        
        Parameters:
        -----------
        result : Dict[str, Any]
            Result dictionary from process_user_question()
        """
        if result['success']:
            if result.get('sql_query'):
                print("\nSQL Query:")
                print(result['sql_query'])
                print("\n" + "-" * 80)
            
            print("\nAnswer:")
            print(result['formatted_answer'])
        else:
            print(f"\nError: {result.get('formatted_answer', result.get('error', 'Unknown error'))}")


# Interactive CLI usage
if __name__ == "__main__":
    import sys
    
    try:
        pipeline = MainPipeline()
        print("✅ LangGraph Agent initialized successfully!")
        print("💡 Type 'q', 'quit', or 'exit' to stop\n")
    except Exception as e:
        print(f"❌ Error: Failed to initialize pipeline - {e}")
        sys.exit(1)
    
    session_id = None
    
    while True:
        try:
            user_input = input("\nQuestion: ").strip()
            
            if user_input.lower() in ['q', 'quit', 'exit']:
                break
            
            if not user_input:
                continue
            
            result = pipeline.process_user_question(
                user_question=user_input,
                session_id=session_id,
                format_answer=True,
                show_sql=True
            )
            
            # Update session_id for next iteration
            if result.get('session_id'):
                session_id = result['session_id']
            
            pipeline.display_result(result)
            
        except KeyboardInterrupt:
            break
        except EOFError:
            break
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            continue

