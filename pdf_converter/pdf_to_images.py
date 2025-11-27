"""
PDF to Images Converter
Converts each page of a PDF file into separate image files.
"""

import os
from pathlib import Path
from pdf2image import convert_from_path
from PIL import Image


def convert_pdf_to_images(pdf_path, output_folder, dpi=200, image_format='PNG'):
    """
    Convert each page of a PDF to an image.
    
    Args:
        pdf_path (str): Path to the PDF file
        output_folder (str): Directory where images will be saved
        dpi (int): Resolution for the output images (default: 200)
        image_format (str): Output image format (default: 'PNG')
    
    Returns:
        list: List of paths to the generated images
    """
    # Create output folder if it doesn't exist
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Get the PDF filename without extension
    pdf_name = Path(pdf_path).stem
    
    print(f"Converting PDF: {pdf_path}")
    print(f"Output folder: {output_folder}")
    print(f"DPI: {dpi}")
    
    try:
        # Convert PDF to images
        images = convert_from_path(pdf_path, dpi=dpi)
        
        print(f"Total pages found: {len(images)}")
        
        # Save each page as an image
        image_paths = []
        for i, image in enumerate(images, start=1):
            # Create filename with page number
            image_filename = f"{pdf_name}_page_{i:03d}.{image_format.lower()}"
            image_path = output_path / image_filename
            
            # Save the image
            image.save(image_path, image_format)
            image_paths.append(str(image_path))
            
            print(f"  ✓ Saved page {i}/{len(images)}: {image_filename}")
        
        print(f"\n✅ Successfully converted {len(images)} pages!")
        return image_paths
    
    except Exception as e:
        print(f"❌ Error converting PDF: {str(e)}")
        raise


def main():
    """Main function to demonstrate PDF to images conversion."""
    
    # Define paths
    project_root = Path(__file__).parent.parent
    pdf_path = project_root / "data" / "downloaded" / "Copy of Affordable_Dental_Monthly_Report_2025-08-01_2025-08-31.pdf"
    output_folder = project_root / "temp" / "pdf_images"
    
    # Check if PDF exists
    if not pdf_path.exists():
        print(f"❌ PDF file not found: {pdf_path}")
        return
    
    # Convert PDF to images
    try:
        image_paths = convert_pdf_to_images(
            pdf_path=str(pdf_path),
            output_folder=str(output_folder),
            dpi=200,  # Higher DPI = better quality but larger files
            image_format='PNG'
        )
        
        print(f"\n📁 Images saved to: {output_folder}")
        print(f"📊 Total images created: {len(image_paths)}")
        
    except Exception as e:
        print(f"❌ Failed to convert PDF: {str(e)}")


if __name__ == "__main__":
    main()
