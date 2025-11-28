# main.py
"""
Main PDF Processing Pipeline
Converts PDF -> images (with base64 data URIs) -> calls Gemini using base64 only -> writes JSON
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
import argparse

# Ensure project root is importable
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, continue anyway


# Import with try-except to handle both direct execution and module import
try:
    from .pdf_to_images import convert_pdf_to_images_pymupdf
    from .analyze_with_gemini import analyze_image_base64
except ImportError:
    # If relative import fails, use absolute import
    from pdf_to_images import convert_pdf_to_images_pymupdf
    from analyze_with_gemini import analyze_image_base64


# Optional config fallback; update to your config.settings if you have it.
try:
    from config.settings import Config
    # Add missing attribute if not present
    if not hasattr(Config, 'TEMP_IMAGES_DIR'):
        Config.TEMP_IMAGES_DIR = Config.BASE_DIR / "temp" / "pdf_images"
    if not hasattr(Config, 'INCLUDE_BASE64_IN_JSON'):
        Config.INCLUDE_BASE64_IN_JSON = True  # Changed to True to include actual base64 data
except Exception:
    class Config:
        BASE_DIR = Path(__file__).parent.parent
        SUMMARIES_DIR = BASE_DIR / "data" / "summaries"
        TEMP_IMAGES_DIR = BASE_DIR / "temp" / "pdf_images"
        INCLUDE_BASE64_IN_JSON = True  # Changed to True to include actual base64 data
        GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Set your folder path here (containing PDF files)
folder_path = "/Users/dhyeybhimani/Desktop/RAG_v01/data/downloaded"

def process_pdf_pipeline(pdf_path, output_dir=None):
    """Process a single PDF file through the pipeline"""
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        print(f"❌ PDF file not found: {pdf_path}")
        return None

    output_dir = Path(output_dir or Config.SUMMARIES_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"📄 Processing: {pdf_path.name}")
    print(f"{'='*60}")

    try:
        # Convert PDF to images and get base64 data-URIs
        image_results = convert_pdf_to_images_pymupdf(
            pdf_path=str(pdf_path),
            output_folder=str(Config.TEMP_IMAGES_DIR),
            dpi=200,
            image_format='PNG',
            include_base64=True
        )

        pages_data = []
        total_pages = len(image_results)

        # Ensure Gemini configured
        if Config.GEMINI_API_KEY:
            import google.generativeai as genai
            genai.configure(api_key=Config.GEMINI_API_KEY)
            print("✅ Gemini API key loaded successfully")
        else:
            print("⚠️ GEMINI_API_KEY not found in Config or environment. Gemini calls may fail.")

        for item in image_results:
            page_no = item["page_no"]
            original_image_path = item["image_path"]
            data_url = item.get("data_url")  # this is 'data:image/png;base64,...'

            print(f"Processing page {page_no}/{total_pages}")

            if not data_url:
                summary = "No base64 data available for this page."
                used_payload = None
            else:
                # Call Gemini analyzer directly with base64/data-uri (no file writes)
                analysis = analyze_image_base64(
                    base64_or_data_uri=data_url,
                    page_number=page_no,
                    total_pages=total_pages,
                    mime_type_hint="image/png"
                )
                summary = analysis.get("summary", "")
                used_payload = analysis.get("used_payload")

            page_entry = {
                "pdf_id": pdf_path.stem,
                "page_no": page_no,
                "original_image_path": original_image_path,
                "base64_image_url": data_url if Config.INCLUDE_BASE64_IN_JSON and data_url else ("present" if data_url else None),
                "summary": summary,
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
            }

            pages_data.append(page_entry)

        final_data = {
            "pdf_id": pdf_path.stem,
            "total_pages": total_pages,
            "pages": pages_data
        }

        output_file = Path(output_dir) / f"{pdf_path.stem}_summary.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Processing complete! Output saved to: {output_file}")
        return str(output_file)
    
    except Exception as e:
        print(f"❌ Error processing {pdf_path.name}: {str(e)}")
        return None


def process_folder(folder_path, output_dir=None):
    """Process all PDF files in the specified folder"""
    folder_path = Path(folder_path)
    
    if not folder_path.exists():
        print(f"❌ Folder not found: {folder_path}")
        return []
    
    if not folder_path.is_dir():
        print(f"❌ Path is not a directory: {folder_path}")
        return []
    
    # Find all PDF files in the folder
    pdf_files = list(folder_path.glob("*.pdf"))
    
    if not pdf_files:
        print(f"⚠️ No PDF files found in: {folder_path}")
        return []
    
    print(f"\n🔍 Found {len(pdf_files)} PDF file(s) in {folder_path.name}")
    print("="*60)
    
    results = []
    successful = 0
    failed = 0
    
    for idx, pdf_file in enumerate(pdf_files, 1):
        print(f"\n[{idx}/{len(pdf_files)}] Processing: {pdf_file.name}")
        result = process_pdf_pipeline(pdf_file, output_dir)
        
        if result:
            results.append(result)
            successful += 1
        else:
            failed += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successfully processed: {successful} file(s)")
    print(f"❌ Failed: {failed} file(s)")
    print(f"📁 Total files: {len(pdf_files)}")
    
    return results


def main():
    """Main entry point - process all PDFs in the folder"""
    # Process all PDFs in the hardcoded folder_path
    process_folder(folder_path)

if __name__ == "__main__":
    main()