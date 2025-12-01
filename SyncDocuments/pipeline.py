"""
Main pipeline for syncing documents from Google Drive to Supabase.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

from supabase import create_client

if __package__ in {None, ""}:
    import sys
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


def _cleanup_document_assets(pdf_path: Path, pages_data: Iterable[Dict]) -> None:
    """
    Remove downloaded PDF, generated summary JSON, and temp images.

    Args:
        pdf_path: Path to the downloaded PDF.
        pages_data: Iterable of page metadata with image paths.
    """
    try:
        # Delete the downloaded PDF
        if pdf_path.exists():
            pdf_path.unlink()

        # Delete the summary JSON created for this PDF
        summary_file = settings.summaries_dir / f"{pdf_path.stem}_summary.json"
        if summary_file.exists():
            summary_file.unlink()

        # Delete any generated page images
        for page in pages_data:
            image_path = page.get("image_path")
            if not image_path:
                continue
            img = Path(image_path)
            if img.exists():
                img.unlink()
    except Exception as cleanup_error:
        print(f"  ⚠️  Cleanup issue for {pdf_path.name}: {cleanup_error}")


def sync_documents(folder_id: str) -> Dict:
    """
    Main pipeline to sync documents from Google Drive to Supabase.
    
    Args:
        folder_id: Google Drive folder ID to sync from
        
    Returns:
        Dictionary with sync results
    """
    print("="*80)
    print("🔄 DOCUMENT SYNC PIPELINE")
    print("="*80)
    
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
        print("✅ Configuration validated")
        
        # Step 1: Connect to Google Drive
        print("\n" + "="*80)
        print("STEP 1: CONNECTING TO GOOGLE DRIVE")
        print("="*80)
        service = connect_drive()
        print("✅ Connected to Google Drive")
        
        # Step 2: List files in folder
        print("\n" + "="*80)
        print("STEP 2: LISTING FILES IN GOOGLE DRIVE FOLDER")
        print("="*80)
        drive_files = list_files_in_folder(service, folder_id)
        results["total_files"] = len(drive_files)
        print(f"📁 Found {len(drive_files)} file(s) in folder")
        
        if not drive_files:
            print("⚠️  No files found in the specified folder")
            return results
        
        # Step 3: Check existing PDFs in database
        print("\n" + "="*80)
        print("STEP 3: CHECKING EXISTING DOCUMENTS IN DATABASE")
        print("="*80)
        supabase_client = create_client(settings.supabase_url, settings.supabase_key)
        existing_pdf_ids = get_existing_pdf_ids(supabase_client, log_callback=None)
        print(f"📊 Found {len(existing_pdf_ids)} unique PDF ID(s) in database")
        
        # Step 4: Filter new PDFs
        print("\n" + "="*80)
        print("STEP 4: FILTERING NEW DOCUMENTS")
        print("="*80)
        new_pdfs = filter_new_pdfs(drive_files, existing_pdf_ids, log_callback=None)
        results["new_files"] = len(new_pdfs)
        print(f"🆕 Found {len(new_pdfs)} new PDF(s) to process")
        
        if not new_pdfs:
            print("✅ All documents are already in the database. Nothing to sync.")
            return results
        
        # Step 5: Process each new PDF
        print("\n" + "="*80)
        print("STEP 5: PROCESSING NEW DOCUMENTS")
        print("="*80)
        
        for idx, pdf_file in enumerate(new_pdfs, 1):
            print(f"\n[{idx}/{len(new_pdfs)}] Processing: {pdf_file['name']}")
            
            try:
                # Download PDF
                print("  📥 Downloading from Google Drive...")
                pdf_path = download_file(service, pdf_file["id"], pdf_file["name"])
                print(f"  ✅ Downloaded to: {pdf_path}")
                
                # Process PDF (convert to images, create summaries)
                print("  🔄 Processing PDF...")
                summary_data = process_pdf(pdf_path)
                
                if not summary_data:
                    print(f"  ❌ Failed to process {pdf_file['name']}")
                    results["failed"] += 1
                    results["errors"].append(f"Failed to process {pdf_file['name']}")
                    continue
                
                # Store in Supabase
                print("  💾 Storing in vector database...")
                store_in_supabase(summary_data)
                print(f"  ✅ Successfully synced {pdf_file['name']}")
                
                # Remove local artifacts for this PDF
                _cleanup_document_assets(pdf_path, summary_data.get("pages", []))

                results["processed"] += 1
                
            except Exception as e:
                print(f"  ❌ Error processing {pdf_file['name']}: {str(e)}")
                results["failed"] += 1
                results["errors"].append(f"{pdf_file['name']}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        # Summary
        print("\n" + "="*80)
        print("📊 SYNC SUMMARY")
        print("="*80)
        print(f"Total files in folder: {results['total_files']}")
        print(f"New files found: {results['new_files']}")
        print(f"Successfully processed: {results['processed']}")
        print(f"Failed: {results['failed']}")
        
        if results["errors"]:
            print("\n❌ Errors encountered:")
            for error in results["errors"]:
                print(f"  - {error}")
        
        return results
        
    except Exception as e:
        print(f"\n❌ Pipeline error: {str(e)}")
        import traceback
        traceback.print_exc()
        results["errors"].append(f"Pipeline error: {str(e)}")
        return results

