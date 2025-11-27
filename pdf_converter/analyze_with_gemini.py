"""
PDF Image Analysis with Gemini Vision API
Analyzes each PDF page image and generates detailed summaries.
"""

import os
import json
from pathlib import Path
from datetime import datetime
import google.generativeai as genai
from PIL import Image


from dotenv import load_dotenv
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
# Configure Gemini API

genai.configure(api_key=GEMINI_API_KEY)


def analyze_image_with_gemini(image_path, page_number, total_pages):
    """
    Analyze a PDF page image using Gemini Vision API.
    
    Args:
        image_path (str): Path to the image file
        page_number (int): Current page number
        total_pages (int): Total number of pages in the PDF
        
    Returns:
        str: Generated summary for the page
    """
    # Load the image
    img = Image.open(image_path)
    
    # Initialize Gemini model with vision capabilities
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    # Create a prompt based on page position
    if page_number == 1:
        # First page is likely an introduction/cover page
        prompt = """
        Analyze this page from a business report. This appears to be page 1.
        
        If this is an introduction, cover, or title page:
        - Provide a BRIEF summary (2-3 sentences maximum) stating the document type, company/client name, and reporting period.
        
        If this page contains actual data or detailed content:
        - Provide a DETAILED summary including all key metrics, data points, charts, and insights.
        - Extract specific numbers, percentages, trends, and notable findings.
        
        Focus on accuracy and completeness of information.
        """
    else:
        # Other pages likely contain data
        prompt = f"""
        Analyze this page {page_number} of {total_pages} from a business report.
        
        Provide a DETAILED summary that includes:
        - Section title or heading
        - All key metrics, KPIs, and data points with specific values
        - Any charts, graphs, or visualizations described in detail
        - Trends, insights, and notable patterns
        - Comparisons (month-over-month, year-over-year, etc.)
        - Any recommendations or action items
        
        Be thorough and extract ALL important information. Include actual numbers and percentages.
        """
    
    # Generate content
    response = model.generate_content([prompt, img])
    
    return response.text


def analyze_all_images(images_folder, pdf_name):
    """
    Analyze all images from a PDF and create page-wise summaries.
    
    Args:
        images_folder (str): Folder containing the images
        pdf_name (str): Name of the PDF (for filtering images)
        
    Returns:
        dict: Analysis result with page-wise entries
    """
    images_path = Path(images_folder)
    
    # Get all images for this PDF, sorted by page number
    image_files = sorted([
        f for f in images_path.iterdir()
        if f.name.startswith(pdf_name) and f.suffix.lower() in ['.png', '.jpg', '.jpeg']
    ])
    
    if not image_files:
        print(f"❌ No images found for PDF: {pdf_name}")
        return None
    
    print(f"📄 Analyzing {len(image_files)} pages from: {pdf_name}")
    print("=" * 80)
    
    pages = []
    
    # Analyze each image
    for idx, image_file in enumerate(image_files, start=1):
        print(f"\n🔍 Analyzing page {idx}/{len(image_files)}: {image_file.name}")
        
        try:
            summary = analyze_image_with_gemini(
                str(image_file),
                page_number=idx,
                total_pages=len(image_files)
            )
            
            # Create page entry with required format
            page_entry = {
                "pdf_id": pdf_name,
                "page_no": idx,
                "image_path": str(image_file),
                "summary": summary.strip(),
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
            }
            
            pages.append(page_entry)
            
            print(f"✅ Page {idx} analyzed successfully")
            print(f"Summary preview: {summary[:100]}...")
            
        except Exception as e:
            print(f"❌ Error analyzing page {idx}: {str(e)}")
            page_entry = {
                "pdf_id": pdf_name,
                "page_no": idx,
                "image_path": str(image_file),
                "summary": f"Error analyzing page: {str(e)}",
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
            }
            pages.append(page_entry)
    
    # Create final result with page-wise structure
    result = {
        "pdf_id": pdf_name,
        "total_pages": len(pages),
        "pages": pages
    }
    
    return result


def main():
    """Main function to run the analysis."""
    
    # Define paths
    project_root = Path(__file__).parent.parent
    images_folder = project_root / "temp" / "pdf_images"
    output_folder = project_root / "data" / "summaries"
    
    # Create output folder
    output_folder.mkdir(parents=True, exist_ok=True)
    
    # PDF name to analyze
    pdf_name = "Copy of Affordable_Dental_Monthly_Report_2025-08-01_2025-08-31"
    
    # Analyze all images
    result = analyze_all_images(str(images_folder), pdf_name)
    
    if result:
        # Save to JSON file
        output_file = output_folder / f"{pdf_name}_summary.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print("\n" + "=" * 80)
        print(f"✅ Analysis complete!")
        print(f"📁 Summary saved to: {output_file}")
        print(f"📊 Total pages analyzed: {result['total_pages']}")
        print(f"📄 Pages stored individually with separate summaries")
        print("\n" + "=" * 80)
        
        # Print a preview of the first page summary
        if result['pages']:
            print("\n📝 First Page Summary Preview:")
            print("-" * 80)
            first_page = result['pages'][0]
            print(f"Page {first_page['page_no']}: {first_page['summary'][:300]}...")
            print("-" * 80)
    else:
        print("❌ Analysis failed!")


if __name__ == "__main__":
    main()
