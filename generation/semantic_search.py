"""
Simple semantic search utility that queries Supabase vectors ingested via
`supabase_pipeline`.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, List

from langchain_core.documents import Document
from supabase import create_client

try:
    from huggingface_hub import login as hf_login
except ImportError:  # pragma: no cover - optional dependency guard
    hf_login = None  # type: ignore


# Ensure package-relative imports work whether this file is run as a module or script.
if __package__ in {None, ""}:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from supabase_pipeline.config import settings
from supabase_pipeline.langchain_gemma_embeddings import LangChainGemmaEmbeddings


def _maybe_login_huggingface() -> None:
    """Authenticate with Hugging Face Hub if a token is provided."""
    if not settings.huggingface_token or hf_login is None:
        return
    try:
        hf_login(token=settings.huggingface_token)
    except Exception as exc:  # pragma: no cover - network specific
        raise RuntimeError("Failed to authenticate with Hugging Face Hub.") from exc


def semantic_search(query: str, top_k: int = 2) -> List[Document]:
    """
    Perform a basic semantic search against Supabase-stored vectors.

    Args:
        query: Natural language query text.
        top_k: Number of results to return (defaults to 2).

    Returns:
        List of LangChain Documents ranked by similarity.
    """
    settings.validate_vector_store()
    _maybe_login_huggingface()
    client = create_client(settings.supabase_url, settings.supabase_key)
    embeddings = LangChainGemmaEmbeddings(model_name=settings.embedding_model)
    query_vector = embeddings.embed_query(query)

    rpc_payload: Dict[str, Any] = {
        "query_embedding": query_vector,
        "match_count": top_k,
        "match_threshold": settings.supabase_match_threshold,
    }

    response = client.rpc(settings.supabase_query_fn, rpc_payload).execute()
    records: List[Dict[str, Any]] = response.data or []

    documents: List[Document] = []
    for row in records:
        metadata = row.get("metadata") or {}
        metadata["similarity"] = row.get("similarity")
        documents.append(
            Document(
                page_content=row.get("content", ""),
                metadata=metadata,
            )
        )
    return documents


def _cli() -> None:
    query = input("Enter your query: ").strip()
    if not query:
        print("Query cannot be empty.")
        return

    docs = semantic_search(query, top_k=2)
    if not docs:
        print("No documents found.")
        return

    for idx, doc in enumerate(docs, start=1):
        print(f"\nResult #{idx}")
        print("-" * 40)
        print(f"Doc ID   : {doc.metadata.get('doc_id')}")
        print(f"PDF ID   : {doc.metadata.get('pdf_id')}")
        print(f"Page No. : {doc.metadata.get('page_no')}")
        print("Content  :")
        print(doc.page_content)


if __name__ == "__main__":
    _cli()

