#!/usr/bin/env python3
"""
Integrated Pipeline for Google Drive Document Processing
Downloads documents from Google Drive, saves them locally, and processes their content.

Usage:
    python integrated_pipeline.py
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import utilities
from fetch_data_google_drive.auth import connect_drive
from fetch_data_google_drive.drive import list_files_in_folder, download_file
from fetch_data_google_drive.file_reader import read_pdf, read_txt, read_docx
from config.settings import FOLDER_ID


def process_file(file_path):
    """
    Process a file based on its extension.
    
    Args:
        file_path (str): Path to the file to process
        
    Returns:
        str: Extracted content from the file
    """
    print(f"Processing file: {file_path}")
    
    if file_path.endswith(".pdf"):
        try:
            content = read_pdf(file_path)
            print(f"Successfully read PDF: {file_path}")
            return content
        except Exception as e:
            print(f"Error reading PDF {file_path}: {e}")
            return ""

    elif file_path.endswith(".txt"):
        try:
            content = read_txt(file_path)
            print(f"Successfully read TXT: {file_path}")
            return content
        except Exception as e:
            print(f"Error reading TXT {file_path}: {e}")
            return ""

    elif file_path.endswith(".docx"):
        try:
            content = read_docx(file_path)
            print(f"Successfully read DOCX: {file_path}")
            return content
        except Exception as e:
            print(f"Error reading DOCX {file_path}: {e}")
            return ""

    else:
        print(f"Unsupported file format: {file_path}")
        return ""


def save_processed_content(file_name, content):
    """
    Save processed content to a text file.
    
    Args:
        file_name (str): Original file name
        content (str): Extracted content
    """
    from config.settings import Config
    
    # Create processed directory if it doesn't exist
    processed_dir = Config.DATA_DIR / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Create output file path
    output_file_name = f"{Path(file_name).stem}_processed.txt"
    output_path = processed_dir / output_file_name
    
    # Save content
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Saved processed content to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error saving processed content: {e}")
        return None


def run_document_pipeline():
    """
    Run the complete document processing pipeline.
    """
    print("=" * 80)
    print("GOOGLE DRIVE DOCUMENT PROCESSING PIPELINE")
    print("=" * 80)
    
    try:
        # Step 1: Authenticate with Google Drive
        print("\n" + "=" * 80)
        print("STEP 1: AUTHENTICATING WITH GOOGLE DRIVE")
        print("=" * 80 + "\n")
        
        service = connect_drive()
        print("✅ Successfully connected to Google Drive!")
        
        # Step 2: List files in the configured folder
        print("\n" + "=" * 80)
        print("STEP 2: LISTING FILES IN GOOGLE DRIVE FOLDER")
        print("=" * 80 + "\n")
        
        if not FOLDER_ID:
            print("❌ Error: FOLDER_ID not configured in environment variables")
            sys.exit(1)
            
        files = list_files_in_folder(service, FOLDER_ID)
        
        if not files:
            print("⚠️  No files found in the configured Google Drive folder")
            return
            
        print(f"Found {len(files)} file(s) in the folder:")
        for i, file in enumerate(files, 1):
            print(f"  {i}. {file['name']} ({file['mimeType']})")
        
        # Step 3: Download and process each file
        print("\n" + "=" * 80)
        print("STEP 3: DOWNLOADING AND PROCESSING FILES")
        print("=" * 80 + "\n")
        
        processed_count = 0
        
        for file in files:
            try:
                print(f"\n--- Processing: {file['name']} ---")
                
                # Download file
                local_path = download_file(service, file["id"], file["name"])
                print(f"✅ Downloaded to: {local_path}")
                
                # Process content
                content = process_file(local_path)
                
                if content:
                    # Save processed content
                    output_path = save_processed_content(file["name"], content)
                    if output_path:
                        print(f"✅ Processed and saved: {output_path}")
                        processed_count += 1
                else:
                    print(f"⚠️  No content extracted from: {file['name']}")
                    
            except Exception as e:
                print(f"❌ Error processing {file['name']}: {e}")
                import traceback
                traceback.print_exc()
        
        # Final summary
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED")
        print("=" * 80)
        print(f"✅ Successfully processed: {processed_count} files")
        print(f"❌ Failed to process: {len(files) - processed_count} files")
        print("=" * 80 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error in pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the integrated pipeline."""
    run_document_pipeline()


if __name__ == "__main__":
    main()