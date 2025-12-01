"""
Streamlit-compatible pipeline wrapper with logging and cancellation support.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Callable
import sys

from supabase import create_client

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from SyncDocuments.config import settings
from SyncDocuments.drive_sync import (
    connect_drive,
    list_files_in_folder,
    download_file,
    get_existing_pdf_ids,
    filter_new_pdfs
)
from SyncDocuments.pdf_processor import process_pdf
from SyncDocuments.vector_store import store_in_supabase


def sync_documents_streamlit(
    folder_id: str,
    log_callback: Optional[Callable[[str], None]] = None,
    check_cancel: Optional[Callable[[], bool]] = None
) -> Dict:
    """
    Main pipeline to sync documents from Google Drive to Supabase with Streamlit support.
    
    Args:
        folder_id: Google Drive folder ID to sync from
        log_callback: Function to call for logging messages
        check_cancel: Function to check if cancellation was requested
        
    Returns:
        Dictionary with sync results
    """
    def log(message: str):
        """Log message using callback or print."""
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    def should_cancel() -> bool:
        """Check if cancellation was requested."""
        if check_cancel:
            return check_cancel()
        return False
    
    log("="*80)
    log("🔄 DOCUMENT SYNC PIPELINE")
    log("="*80)
    
    results = {
        "total_files": 0,
        "new_files": 0,
        "processed": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        # Validate settings
        settings.validate()
        log("✅ Configuration validated")
        
        if should_cancel():
            log("⚠️  Sync cancelled by user")
            return results
        
        # Step 1: Connect to Google Drive
        log("\n" + "="*80)
        log("STEP 1: CONNECTING TO GOOGLE DRIVE")
        log("="*80)
        service = connect_drive()
        log("✅ Connected to Google Drive")
        
        if should_cancel():
            log("⚠️  Sync cancelled by user")
            return results
        
        # Step 2: List files in folder
        log("\n" + "="*80)
        log("STEP 2: LISTING FILES IN GOOGLE DRIVE FOLDER")
        log("="*80)
        drive_files = list_files_in_folder(service, folder_id)
        results["total_files"] = len(drive_files)
        log(f"📁 Found {len(drive_files)} file(s) in folder")
        
        if not drive_files:
            log("⚠️  No files found in the specified folder")
            return results
        
        if should_cancel():
            log("⚠️  Sync cancelled by user")
            return results
        
        # Step 3: Check existing PDFs in database
        log("\n" + "="*80)
        log("STEP 3: CHECKING EXISTING DOCUMENTS IN DATABASE")
        log("="*80)
        supabase_client = create_client(settings.supabase_url, settings.supabase_key)
        existing_pdf_ids = get_existing_pdf_ids(supabase_client, log_callback=log_callback)
        log(f"📊 Found {len(existing_pdf_ids)} unique PDF ID(s) in database")
        
        if should_cancel():
            log("⚠️  Sync cancelled by user")
            return results
        
        # Step 4: Filter new PDFs
        log("\n" + "="*80)
        log("STEP 4: FILTERING NEW DOCUMENTS")
        log("="*80)
        new_pdfs = filter_new_pdfs(drive_files, existing_pdf_ids, log_callback=log_callback)
        results["new_files"] = len(new_pdfs)
        log(f"🆕 Found {len(new_pdfs)} new PDF(s) to process")
        
        if not new_pdfs:
            log("✅ All documents are already in the database. Nothing to sync.")
            return results
        
        # Step 5: Process each new PDF
        log("\n" + "="*80)
        log("STEP 5: PROCESSING NEW DOCUMENTS")
        log("="*80)
        
        for idx, pdf_file in enumerate(new_pdfs, 1):
            if should_cancel():
                log(f"\n⚠️  Sync cancelled by user. Processed {results['processed']} of {len(new_pdfs)} files.")
                return results
            
            log(f"\n[{idx}/{len(new_pdfs)}] Processing: {pdf_file['name']}")
            
            try:
                # Download PDF
                log("  📥 Downloading from Google Drive...")
                pdf_path = download_file(service, pdf_file["id"], pdf_file["name"])
                log(f"  ✅ Downloaded to: {pdf_path}")
                
                if should_cancel():
                    log("⚠️  Sync cancelled by user")
                    return results
                
                # Process PDF (convert to images, create summaries)
                log("  🔄 Processing PDF...")
                summary_data = process_pdf(pdf_path, check_cancel=should_cancel)
                
                if not summary_data:
                    log(f"  ❌ Failed to process {pdf_file['name']}")
                    results["failed"] += 1
                    results["errors"].append(f"Failed to process {pdf_file['name']}")
                    continue
                
                if should_cancel():
                    log("⚠️  Sync cancelled by user")
                    return results
                
                # Store in Supabase
                log("  💾 Storing in vector database...")
                store_in_supabase(summary_data, check_cancel=should_cancel)
                log(f"  ✅ Successfully synced {pdf_file['name']}")
                
                results["processed"] += 1
                
            except Exception as e:
                log(f"  ❌ Error processing {pdf_file['name']}: {str(e)}")
                results["failed"] += 1
                results["errors"].append(f"{pdf_file['name']}: {str(e)}")
        
        # Summary
        log("\n" + "="*80)
        log("📊 SYNC SUMMARY")
        log("="*80)
        log(f"Total files in folder: {results['total_files']}")
        log(f"New files found: {results['new_files']}")
        log(f"Successfully processed: {results['processed']}")
        log(f"Failed: {results['failed']}")
        
        if results["errors"]:
            log("\n❌ Errors encountered:")
            for error in results["errors"]:
                log(f"  - {error}")
        
        return results
        
    except Exception as e:
        log(f"\n❌ Pipeline error: {str(e)}")
        results["errors"].append(f"Pipeline error: {str(e)}")
        return results

