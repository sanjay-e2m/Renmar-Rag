"""
Example usage of the LangGraph Agent
Demonstrates how to use the agentic SQL-RAG pipeline
"""

import os
import sys
from pathlib import Path

# Add project root to path for imports
_current_file = Path(__file__).resolve()
_project_root = _current_file.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from rag_excel_postgres.agent import create_agent_graph


def main():
    """Example usage of the agent"""
    
    # Initialize agent
    print("🚀 Initializing LangGraph Agent...")
    agent = create_agent_graph()
    print("✅ Agent initialized!\n")
    
    # Example 1: Simple query
    print("=" * 80)
    print("Example 1: Simple Query")
    print("=" * 80)
    question1 = "Show me top 5 keywords with highest search volume for efg in December 2025"
    print(f"Question: {question1}\n")
    
    result1 = agent.invoke(question1)
    print(f"Answer: {result1.get('formatted_answer', 'No answer')}\n")
    print(f"SQL: {result1.get('generated_sql', 'No SQL')}\n")
    print(f"Session ID: {result1.get('session_id')}\n")
    
    # Example 2: Query with conversation history
    print("=" * 80)
    print("Example 2: Query with Conversation History")
    print("=" * 80)
    session_id = result1.get('session_id')
    
    question2 = "What about for client abc?"
    print(f"Question: {question2}\n")
    
    result2 = agent.invoke(question2, session_id=session_id)
    print(f"Answer: {result2.get('formatted_answer', 'No answer')}\n")
    print(f"SQL: {result2.get('generated_sql', 'No SQL')}\n")
    
    # Example 3: Stream execution
    print("=" * 80)
    print("Example 3: Stream Execution")
    print("=" * 80)
    question3 = "How many keywords are tracked for efg in December 2025?"
    print(f"Question: {question3}\n")
    print("Streaming execution...\n")
    
    for step in agent.stream(question3):
        node_name = list(step.keys())[0] if step else "unknown"
        print(f"Step: {node_name}")
        if 'formatted_answer' in step:
            print(f"Answer: {step['formatted_answer']}")


if __name__ == "__main__":
    main()
