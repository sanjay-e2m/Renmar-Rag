"""
PDF processing utilities.
Converts PDFs to images and creates summaries using Gemini.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import google.generativeai as genai

if __package__ in {None, ""}:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from SyncDocuments.config import settings

# Import PDF conversion and analysis functions
try:
    from pdf_converter.pdf_to_images import convert_pdf_to_images_pymupdf
    from pdf_converter.analyze_with_gemini import analyze_image_base64
except ImportError:
    # Fallback if relative imports don't work
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from pdf_converter.pdf_to_images import convert_pdf_to_images_pymupdf
    from pdf_converter.analyze_with_gemini import analyze_image_base64


def process_pdf(pdf_path: Path) -> Optional[Dict]:
    """
    Process a PDF file: convert to images and create summaries.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        Dictionary with pdf_id, total_pages, and pages data, or None if error
    """
    if not pdf_path.exists():
        print(f"❌ PDF file not found: {pdf_path}")
        return None
    
    print(f"\n{'='*60}")
    print(f"📄 Processing: {pdf_path.name}")
    print(f"{'='*60}")
    
    try:
        # Configure Gemini
        if not settings.gemini_api_key:
            raise EnvironmentError("GEMINI_API_KEY not configured")
        
        genai.configure(api_key=settings.gemini_api_key)
        
        # Convert PDF to images
        print("🖼️  Converting PDF to images...")
        image_results = convert_pdf_to_images_pymupdf(
            pdf_path=str(pdf_path),
            output_folder=str(settings.temp_images_dir),
            dpi=200,
            image_format='PNG',
            include_base64=True
        )
        
        if not image_results:
            print("❌ No images generated from PDF")
            return None
        
        total_pages = len(image_results)
        print(f"✅ Converted {total_pages} pages to images")
        
        # Process each page with Gemini
        pages_data = []
        print("\n📝 Generating summaries with Gemini...")
        
        for idx, item in enumerate(image_results, 1):
            page_no = item["page_no"]
            original_image_path = item["image_path"]
            data_url = item.get("data_url")
            
            print(f"  Processing page {page_no}/{total_pages}...", end=" ")
            
            if not data_url:
                summary = "No base64 data available for this page."
                print("⚠️  No base64 data")
            else:
                # Analyze image with Gemini
                analysis = analyze_image_base64(
                    base64_or_data_uri=data_url,
                    page_number=page_no,
                    total_pages=total_pages,
                    mime_type_hint="image/png"
                )
                summary = analysis.get("summary", "")
                print("✅")
            
            page_entry = {
                "pdf_id": pdf_path.stem,
                "page_no": page_no,
                "image_path": str(original_image_path),
                "summary": summary,
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
            }
            
            pages_data.append(page_entry)
        
        # Create final data structure
        final_data = {
            "pdf_id": pdf_path.stem,
            "total_pages": total_pages,
            "pages": pages_data
        }
        
        # Save summary JSON
        settings.summaries_dir.mkdir(parents=True, exist_ok=True)
        output_file = settings.summaries_dir / f"{pdf_path.stem}_summary.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Summary saved to: {output_file}")
        return final_data
    
    except Exception as e:
        print(f"❌ Error processing {pdf_path.name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

