"""
Ingest page summaries into Supabase as a multi-vector retriever.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

from langchain_core.documents import Document
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import SupabaseVectorStore
from supabase import Client, create_client

from .config import settings
from .docstore import LocalJSONDocStore


def _load_summary_files() -> Sequence[Path]:
    if not settings.summaries_dir.exists():
        raise FileNotFoundError(f"Summaries directory not found: {settings.summaries_dir}")
    files = sorted(settings.summaries_dir.glob("*_summary.json"))
    if not files:
        raise FileNotFoundError(
            f"No summary JSON files found in {settings.summaries_dir}. "
            "Generate summaries first."
        )
    return files


def _build_documents() -> Tuple[List[Document], List[Tuple[str, Document]]]:
    vector_docs: List[Document] = []
    docstore_entries: List[Tuple[str, Document]] = []

    for summary_path in _load_summary_files():
        with summary_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        pdf_id = payload.get("pdf_id") or summary_path.stem.replace("_summary", "")
        for page in payload.get("pages", []):
            summary_text = (page.get("summary") or "").strip()
            if not summary_text:
                continue
            page_no = page.get("page_no")
            doc_id = f"{pdf_id}_p{int(page_no):03d}"
            image_path = page.get("image_path", "")

            metadata = {
                "doc_id": doc_id,
                "pdf_id": pdf_id,
                "page_no": page_no,
                "image_path": image_path,
                "created_at": page.get("created_at"),
            }

            embedding_text = (
                f"{pdf_id} page {page_no} summary:\n{summary_text}"
            )
            vector_docs.append(Document(page_content=embedding_text, metadata=metadata))

            doc_content = (
                f"PDF: {pdf_id}\n"
                f"Page: {page_no}\n"
                f"Image Path: {image_path}\n\n"
                f"Summary:\n{summary_text}"
            )
            docstore_entries.append((doc_id, Document(page_content=doc_content, metadata=metadata)))

    if not vector_docs:
        raise ValueError("No valid summaries found to ingest.")

    return vector_docs, docstore_entries


def _create_vector_store(client: Client) -> SupabaseVectorStore:
    embeddings = GoogleGenerativeAIEmbeddings(
        model=settings.gemini_embedding_model,
        google_api_key=settings.gemini_api_key,
    )
    return SupabaseVectorStore(
        client=client,
        table_name=settings.supabase_table,
        query_name=settings.supabase_query_fn,
        embedding=embeddings,
    )


def ingest() -> None:
    settings.validate_vector_store()

    vector_docs, docstore_entries = _build_documents()
    doc_ids = [doc.metadata["doc_id"] for doc in vector_docs]

    docstore = LocalJSONDocStore(settings.docstore_dir)
    docstore.mset(docstore_entries)

    client = create_client(settings.supabase_url, settings.supabase_key)
    vector_store = _create_vector_store(client)

    # Remove existing vectors for the same doc_ids to avoid duplicates
    if doc_ids:
        vector_store.delete(ids=doc_ids)

    vector_store.add_documents(vector_docs)

    print(f"✅ Ingested {len(vector_docs)} page summaries into Supabase.")


if __name__ == "__main__":
    ingest()

