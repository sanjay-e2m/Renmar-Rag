#!/usr/bin/env python3
"""
Complete JSON Pipeline
Orchestrates the entire workflow:
1. Downloads files from Google Drive using integrated_pipeline
2. Processes all PDFs in the folder using pdf_converter/main.py
3. Generates JSON summaries for all files

Usage:
    python completejson.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import required modules
from fetch_data_google_drive.auth import connect_drive
from fetch_data_google_drive.drive import list_files_in_folder, download_file
from config.settings import FOLDER_ID, Config

# Import PDF processing function
try:
    from pdf_converter.main import process_folder
except ImportError:
    print("⚠️ Warning: Could not import from pdf_converter.main")
    print("Make sure pdf_converter/main.py exists and is properly configured")
    sys.exit(1)


def download_all_files_from_drive():
    """
    Download all files from Google Drive folder.
    
    Returns:
        list: List of paths to downloaded PDF files
    """
    print("\n" + "="*80)
    print("STEP 1: DOWNLOADING FILES FROM GOOGLE DRIVE")
    print("="*80 + "\n")
    
    try:
        # Authenticate with Google Drive
        service = connect_drive()
        print("✅ Successfully connected to Google Drive!")
        
        if not FOLDER_ID:
            print("❌ Error: FOLDER_ID not configured in environment variables")
            return []
        
        # List files in the configured folder
        files = list_files_in_folder(service, FOLDER_ID)
        
        if not files:
            print("⚠️ No files found in the configured Google Drive folder")
            return []
        
        print(f"Found {len(files)} file(s) in Google Drive:")
        for i, file in enumerate(files, 1):
            print(f"  {i}. {file['name']} ({file['mimeType']})")
        
        # Download each file
        downloaded_pdfs = []
        download_count = 0
        
        for file in files:
            try:
                print(f"\n--- Downloading: {file['name']} ---")
                
                # Download file
                local_path = download_file(service, file["id"], file["name"])
                print(f"✅ Downloaded to: {local_path}")
                
                # If it's a PDF, add to the list for processing
                if file['name'].lower().endswith('.pdf'):
                    downloaded_pdfs.append(local_path)
                
                download_count += 1
                
            except Exception as e:
                print(f"❌ Error downloading {file['name']}: {e}")
        
        print(f"\n✅ Downloaded {download_count} file(s)")
        print(f"📄 Found {len(downloaded_pdfs)} PDF file(s) for processing")
        
        return downloaded_pdfs
        
    except Exception as e:
        print(f"❌ Error in download process: {e}")
        import traceback
        traceback.print_exc()
        return []


def process_all_pdfs():
    """
    Process all PDFs in the downloaded folder using pdf_converter/main.py
    
    Returns:
        list: List of paths to generated JSON files
    """
    print("\n" + "="*80)
    print("STEP 2: PROCESSING PDF FILES TO JSON")
    print("="*80 + "\n")
    
    try:
        # Get the downloaded folder path
        downloaded_folder = Config.DATA_DIR / "downloaded"
        
        if not downloaded_folder.exists():
            print(f"❌ Downloaded folder not found: {downloaded_folder}")
            return []
        
        # Process all PDFs in the folder
        json_files = process_folder(str(downloaded_folder))
        
        return json_files if json_files else []
        
    except Exception as e:
        print(f"❌ Error processing PDFs: {e}")
        import traceback
        traceback.print_exc()
        return []


def run_complete_pipeline():
    """
    Run the complete pipeline:
    1. Download files from Google Drive
    2. Process all PDFs to generate JSON summaries
    """
    print("\n" + "="*80)
    print("COMPLETE JSON PIPELINE - STARTING")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    try:
        # Step 1: Download files from Google Drive
        downloaded_pdfs = download_all_files_from_drive()
        
        # Step 2: Process all PDFs to JSON
        json_files = process_all_pdfs()
        
        # Final Summary
        print("\n" + "="*80)
        print("COMPLETE PIPELINE FINISHED")
        print("="*80)
        print(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ Downloaded files: {len(downloaded_pdfs)}")
        print(f"✅ Generated JSON files: {len(json_files)}")
        
        if json_files:
            print("\nGenerated JSON files:")
            for i, json_file in enumerate(json_files, 1):
                print(f"  {i}. {json_file}")
        
        print("="*80 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error in complete pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the complete JSON pipeline."""
    run_complete_pipeline()


if __name__ == "__main__":
    main()
