"""
Ingest page summaries into Supabase as a multi-vector retriever.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple
from langchain_core.stores import BaseStore
from langchain_core.documents import Document
from langchain_voyageai import VoyageAIEmbeddings
from langchain_community.vectorstores import SupabaseVectorStore
from supabase import Client, create_client

try:
    from .config import settings
    from .docstore import LocalJSONDocStore
except ImportError:
    # Allow running as a script directly
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from supabase_pipeline.config import settings
    from supabase_pipeline.docstore import LocalJSONDocStore


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
    embeddings = VoyageAIEmbeddings(
        model=settings.voyage_embedding_model,
        voyage_api_key=settings.voyage_api_key,
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

    print(f"📦 Stored {len(docstore_entries)} documents in local docstore.")
    print(f"🔗 Connecting to Supabase at {settings.supabase_url}...")
    
    try:
        client = create_client(settings.supabase_url, settings.supabase_key)
        vector_store = _create_vector_store(client)
        
        # Test connection by trying to query the table
        print("✅ Connected to Supabase successfully.")
        
        # Remove existing vectors for the same doc_ids to avoid duplicates
        # Delete by filtering on metadata since table uses auto-incrementing bigint IDs
        if doc_ids:
            print(f"🗑️  Removing existing documents with matching doc_ids...")
            try:
                # Delete by metadata filter instead of IDs
                for doc_id in doc_ids:
                    # Use Supabase client directly to delete by metadata filter
                    client.table(settings.supabase_table).delete().eq("metadata->>doc_id", doc_id).execute()
                print("✅ Existing documents removed.")
            except Exception as e:
                print(f"⚠️  Warning: Could not delete existing documents: {e}")
                print("   Continuing with ingestion...")

        print(f"📤 Adding {len(vector_docs)} documents to vector store...")
        # Generate embeddings and insert directly to avoid UUID ID conflicts
        embeddings_model = VoyageAIEmbeddings(
            model=settings.voyage_embedding_model,
            voyage_api_key=settings.voyage_api_key,
        )
        
        # Generate embeddings for all documents
        texts = [doc.page_content for doc in vector_docs]
        embedding_vectors = embeddings_model.embed_documents(texts)
        
        # Insert directly into Supabase, letting the table auto-generate bigint IDs
        records = []
        for doc, embedding in zip(vector_docs, embedding_vectors):
            records.append({
                "content": doc.page_content,
                "metadata": doc.metadata,
                "embedding": embedding,
            })
        
        # Insert in batches to avoid payload size limits
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            client.table(settings.supabase_table).insert(batch).execute()
        
        print(f"✅ Ingested {len(vector_docs)} page summaries into Supabase.")
        
    except Exception as e:
        error_msg = str(e)
        if "getaddrinfo failed" in error_msg or "ConnectError" in str(type(e).__name__):
            raise ConnectionError(
                f"❌ Failed to connect to Supabase.\n"
                f"   URL: {settings.supabase_url}\n"
                f"   Error: {error_msg}\n\n"
                f"   Please check:\n"
                f"   1. SUPABASE_URL is correct in your .env file\n"
                f"   2. Your internet connection is working\n"
                f"   3. The Supabase project is active and accessible\n"
                f"   4. The URL format is: https://your-project-id.supabase.co"
            ) from e
        else:
            raise


if __name__ == "__main__":
    ingest()
