"""
Vector store utilities for storing embeddings in Supabase.
"""

from __future__ import annotations

from typing import List, Tuple, Optional, Callable
from pathlib import Path

from langchain_core.documents import Document
from supabase import Client, create_client

try:
    from huggingface_hub import login as hf_login
except ImportError:
    hf_login = None

if __package__ in {None, ""}:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from SyncDocuments.config import settings
from supabase_pipeline.langchain_gemma_embeddings import LangChainGemmaEmbeddings


def _maybe_login_huggingface() -> None:
    """Authenticate with Hugging Face Hub if a token is provided."""
    if not settings.huggingface_token or hf_login is None:
        return
    try:
        hf_login(token=settings.huggingface_token)
    except Exception as exc:
        raise RuntimeError("Failed to authenticate with Hugging Face Hub.") from exc


def build_documents_from_summary(summary_data: dict) -> Tuple[List[Document], List[Tuple[str, Document]]]:
    """
    Build LangChain documents from summary data.
    
    Args:
        summary_data: Dictionary with pdf_id, total_pages, and pages
        
    Returns:
        Tuple of (vector_docs, docstore_entries)
    """
    vector_docs: List[Document] = []
    docstore_entries: List[Tuple[str, Document]] = []
    
    pdf_id = summary_data.get("pdf_id")
    if not pdf_id:
        raise ValueError("summary_data must contain 'pdf_id'")
    
    for page in summary_data.get("pages", []):
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
        
        # Text for embedding
        embedding_text = f"{pdf_id} page {page_no} summary:\n{summary_text}"
        vector_docs.append(Document(page_content=embedding_text, metadata=metadata))
        
        # Full content for docstore
        doc_content = (
            f"PDF: {pdf_id}\n"
            f"Page: {page_no}\n"
            f"Image Path: {image_path}\n\n"
            f"Summary:\n{summary_text}"
        )
        docstore_entries.append((doc_id, Document(page_content=doc_content, metadata=metadata)))
    
    return vector_docs, docstore_entries


def store_in_supabase(
    summary_data: dict,
    check_cancel: Optional[Callable[[], bool]] = None
) -> None:
    """
    Store document embeddings in Supabase vector store.
    
    Args:
        summary_data: Dictionary with pdf_id, total_pages, and pages
        check_cancel: Optional callback to check if processing should be cancelled
    """
    settings.validate()
    _maybe_login_huggingface()
    
    # Build documents
    vector_docs, docstore_entries = build_documents_from_summary(summary_data)
    
    if not vector_docs:
        print("⚠️  No valid summaries to store")
        return
    
    print(f"📦 Preparing {len(vector_docs)} documents for storage...")
    
    # Connect to Supabase
    client = create_client(settings.supabase_url, settings.supabase_key)
    embeddings_model = LangChainGemmaEmbeddings(model_name=settings.embedding_model)
    
    # Get doc IDs to check for existing documents
    doc_ids = [doc.metadata["doc_id"] for doc in vector_docs]
    
    # Remove existing documents with same doc_ids
    print("🗑️  Removing existing documents with matching doc_ids...")
    for doc_id in doc_ids:
        if check_cancel and check_cancel():
            print("⚠️  Storage cancelled by user")
            return
            
        try:
            client.table(settings.supabase_table).delete().eq(
                "metadata->>doc_id", doc_id
            ).execute()
        except Exception as e:
            print(f"⚠️  Warning: Could not delete doc_id {doc_id}: {e}")
    
    # Generate embeddings
    print("🔢 Generating embeddings...")
    texts = [doc.page_content for doc in vector_docs]
    embedding_vectors = embeddings_model.embed_documents(texts)
    
    # Prepare records for insertion
    records = []
    for doc, embedding in zip(vector_docs, embedding_vectors):
        records.append({
            "content": doc.page_content,
            "metadata": doc.metadata,
            "embedding": embedding,
        })
    
    # Insert in batches
    print(f"📤 Inserting {len(records)} documents into Supabase...")
    batch_size = 100
    for i in range(0, len(records), batch_size):
        if check_cancel and check_cancel():
            print("⚠️  Storage cancelled by user")
            return
            
        batch = records[i:i + batch_size]
        client.table(settings.supabase_table).insert(batch).execute()
        print(f"  ✅ Inserted batch {i//batch_size + 1}/{(len(records)-1)//batch_size + 1}")
    
    print(f"✅ Successfully stored {len(records)} documents in Supabase")

