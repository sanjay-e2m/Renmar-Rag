
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

    def __init__(self, top_k: int = settings.retriever_top_k):

        if not settings.gemini_api_key:
            raise EnvironmentError("GEMINI_API_KEY is missing. Set it in your .env.")

        self.top_k = top_k
        # Simple in-memory chat history
        self.chat_history: List = []

        # Initialize Gemini model directly (like in chat.py)
        genai.configure(api_key=settings.gemini_api_key)
        model_name = settings.gemini_model
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

        context = self._format_context(docs)


        instruction = dedent("""
            You are a helpful business analytics assistant. You must answer questions
            using ONLY the information contained in the provided documents.
            
            If the answer is not explicitly stated in the documents, check the provided
            conversation history for a prior answer. If neither the documents nor the
            history contain the needed information, respond with:
            "I don't have enough information in the provided documents to answer that."
            
            Never use outside knowledge, assumptions, or prior training data beyond
            what is provided in the context documents or conversation history.
            
            When answering, be concise and evidence-based, citing the relevant document/page
            or referencing prior conversation when possible.
        """).strip()

        prompt = (
            f"{instruction}\n\n"
            f"## Context Documents:\n{context}\n\n"
            f"## Conversation History:\n{history_text}\n\n"
            f"## User Question:\n{question}\n\n"
            "Provide a concise, evidence-based answer based on the documents above.\n"
        )
        return prompt

    def chat(self, question: str) -> dict:

        if not question.strip():
            return {
                "answer": "Please provide a valid question.",
                "context_docs": []
            }


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


def create_chatbot() -> ChatBot:
    return ChatBot()


if __name__ == "__main__":

    chatbot = create_chatbot()
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

