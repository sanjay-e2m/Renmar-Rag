"""
LangChain-based chatbot with memory for conversational question-answering.
Uses semantic search and Gemini for generating answers.
"""

from __future__ import annotations

from textwrap import dedent
from typing import List, Optional

from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.documents import Document
import google.generativeai as genai

if __package__ in {None, ""}:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generation.semantic_search import semantic_search
from supabase_pipeline.config import settings


class ChatBot:
    """
    A chatbot with conversational memory that uses semantic search
    and Gemini to answer questions based on document context.
    """

    def __init__(self, top_k: int = 2):
        """
        Initialize the chatbot with memory and LLM.

        Args:
            top_k: Number of documents to retrieve for context (default: 2)
        """
        if not settings.gemini_api_key:
            raise EnvironmentError("GEMINI_API_KEY is missing. Set it in your .env.")

        self.top_k = top_k
        # Simple in-memory chat history
        self.chat_history: List = []

        # Initialize Gemini model directly (like in chat.py)
        genai.configure(api_key=settings.gemini_api_key)
        model_name = settings.gemini_model or "gemini-1.5-flash"
        self.model = genai.GenerativeModel(model_name)

    def _format_context(self, docs: List[Document]) -> str:
        """Format retrieved documents into a context string."""
        if not docs:
            return "No relevant documents found."

        context_blocks = []
        for idx, doc in enumerate(docs, start=1):
            context_blocks.append(
                dedent(
                    f"""
                    ### Document {idx}
                    Doc ID: {doc.metadata.get('doc_id', 'N/A')}
                    PDF ID: {doc.metadata.get('pdf_id', 'N/A')}
                    Page: {doc.metadata.get('page_no', 'N/A')}
                    Similarity: {f"{doc.metadata.get('similarity'):.4f}" if doc.metadata.get('similarity') is not None else 'N/A'}
                    Content:
                    {doc.page_content}
                    """
                ).strip()
            )

        return "\n\n".join(context_blocks)

    def _build_prompt(self, question: str, docs: List[Document], chat_history: List) -> str:
        """Build the prompt with context, chat history, and question."""
        # Format chat history
        history_text = ""
        if chat_history:
            history_parts = []
            for msg in chat_history[-6:]:  # Keep last 6 messages (3 exchanges)
                if isinstance(msg, HumanMessage):
                    history_parts.append(f"User: {msg.content}")
                elif isinstance(msg, AIMessage):
                    history_parts.append(f"Assistant: {msg.content}")
            if history_parts:
                history_text = "\n\n## Previous Conversation:\n" + "\n".join(history_parts) + "\n"

        # Format context documents
        context = self._format_context(docs)

        # Build the full prompt
        instruction = dedent("""
            You are a helpful business analytics assistant. You must answer questions
            using ONLY the information contained in the provided documents. 
            
            If the answer is not explicitly stated in the documents, respond with:
            "I don't have enough information in the provided documents to answer that."
            
            Never use outside knowledge, assumptions, or prior training data beyond
            what is provided in the context documents.
            
            When answering, be concise and evidence-based, citing the relevant document/page
            when possible. You can reference previous conversation context to provide
            more natural follow-up answers.
        """).strip()

        prompt = (
            f"{instruction}\n\n"
            f"## Context Documents:\n{context}\n\n"
            f"{history_text}"
            f"## User Question:\n{question}\n\n"
            "Provide a concise, evidence-based answer based on the documents above.\n"
        )
        return prompt

    def chat(self, question: str) -> dict:
        """
        Process a user question and return an answer with context.

        Args:
            question: User's question

        Returns:
            Dictionary with 'answer' and 'context_docs' keys
        """
        if not question.strip():
            return {
                "answer": "Please provide a valid question.",
                "context_docs": []
            }

        # Perform semantic search
        docs = semantic_search(question, top_k=self.top_k)
        
        # Build prompt with context and chat history
        prompt = self._build_prompt(question, docs, self.chat_history)
        
        # Generate response using Gemini
        response = self.model.generate_content(prompt)
        answer = response.text.strip() if hasattr(response, 'text') else str(response)

        # Save to memory
        self.chat_history.append(HumanMessage(content=question))
        self.chat_history.append(AIMessage(content=answer))

        return {"answer": answer, "context_docs": docs}

    def clear_memory(self):
        """Clear the conversation memory."""
        self.chat_history = []

    def get_chat_history(self) -> List:
        """Get the current chat history."""
        return self.chat_history


def create_chatbot(top_k: int = 2) -> ChatBot:
    """
    Factory function to create a new chatbot instance.

    Args:
        top_k: Number of documents to retrieve for context

    Returns:
        ChatBot instance
    """
    return ChatBot(top_k=top_k)


if __name__ == "__main__":
    # Simple CLI test
    chatbot = create_chatbot(top_k=2)
    print("Chatbot initialized. Type 'exit' to quit, 'clear' to clear memory.\n")

    while True:
        question = input("You: ").strip()
        if not question:
            continue
        if question.lower() == "exit":
            break
        if question.lower() == "clear":
            chatbot.clear_memory()
            print("Memory cleared.\n")
            continue

        result = chatbot.chat(question)
        print(f"\nBot: {result['answer']}\n")
        if result['context_docs']:
            print(f"Retrieved {len(result['context_docs'])} document(s).\n")

