"""
Console chatbot that retrieves top-2 Supabase documents and asks Gemini to
answer strictly from those documents.
"""

from __future__ import annotations

from textwrap import dedent
from typing import List

import google.generativeai as genai
from langchain_core.documents import Document

if __package__ in {None, ""}:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generation.semantic_search import semantic_search
from supabase_pipeline.config import settings


def _ensure_gemini_model() -> genai.GenerativeModel:
    if not settings.gemini_api_key:
        raise EnvironmentError("GEMINI_API_KEY is missing. Set it in your .env.")
    genai.configure(api_key=settings.gemini_api_key)
    model_name = settings.gemini_model or "gemini-1.5-flash"
    return genai.GenerativeModel(model_name)


def _build_prompt(question: str, docs: List[Document]) -> str:
    context_blocks = []
    for idx, doc in enumerate(docs, start=1):
        context_blocks.append(
            dedent(
                f"""
                ### Document {idx}
                Doc ID: {doc.metadata.get('doc_id')}
                PDF ID: {doc.metadata.get('pdf_id')}
                Page: {doc.metadata.get('page_no')}
                Content:
                {doc.page_content}
                """
            ).strip()
        )

    context = "\n\n".join(context_blocks)
    instruction = dedent(
        """
        You are a helpful business analytics assistant. You must answer the question
        using ONLY the information contained in the provided documents. If the answer
        is not explicitly stated in the documents, respond with:
        "I don’t have enough information in the provided documents to answer that."
        Never use outside knowledge, assumptions, or prior training data.
        """
    ).strip()

    prompt = (
        f"{instruction}\n\n"
        f"## Documents\n{context}\n\n"
        f"## User Question\n{question}\n\n"
        "Provide a concise, evidence-based answer, citing the relevant document/page.\n"
    )
    return prompt


def ask(question: str) -> str:
    docs = semantic_search(question, top_k=2)
    if not docs:
        return "No supporting documents were found for this query."

    model = _ensure_gemini_model()
    prompt = _build_prompt(question, docs)
    response = model.generate_content(prompt)
    return response.text.strip() if hasattr(response, "text") else str(response)


def _cli() -> None:
    question = input("Enter your question: ").strip()
    if not question:
        print("Question cannot be empty.")
        return

    answer = ask(question)
    print("\nGemini answer:")
    print(answer)


if __name__ == "__main__":
    _cli()

