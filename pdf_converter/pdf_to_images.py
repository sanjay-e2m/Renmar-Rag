# pdf_converter/pdf_to_images.py
"""
PDF to Images Converter
Converts each page of a PDF file into separate image files and returns both
file paths and base64 data-URLs for each page image.

Provides two implementations:
1. convert_pdf_to_images - Uses pdf2image (requires poppler)
2. convert_pdf_to_images_pymupdf - Uses PyMuPDF (no external dependencies)
"""

import os
from pathlib import Path
from PIL import Image
import base64
import io

try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

def _image_to_data_uri(pil_img, image_format="PNG"):
    """
    Convert a PIL Image to a data URI like: data:image/png;base64,<base64>
    """
    buffer = io.BytesIO()
    pil_img.save(buffer, format=image_format)
    buffer.seek(0)
    img_bytes = buffer.read()
    b64 = base64.b64encode(img_bytes).decode('utf-8')
    mime = "image/png" if image_format.upper() == "PNG" else f"image/{image_format.lower()}"
    return f"data:{mime};base64,{b64}"

def convert_pdf_to_images(pdf_path, output_folder, dpi=200, image_format='PNG', include_base64=True):
    """
    Convert each page of a PDF to an image and optionally return base64 data-URIs.
    Uses pdf2image (requires poppler).
    Returns: list of dicts: [{"image_path": str, "data_url": str}, ...]
    """
    if not PDF2IMAGE_AVAILABLE:
        raise ImportError(
            "pdf2image is not installed. "
            "Install it with: pip install pdf2image, or use convert_pdf_to_images_pymupdf instead"
        )
    
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    pdf_path = Path(pdf_path)
    pdf_name = pdf_path.stem

    print(f"Converting PDF: {pdf_path}")
    print(f"Output folder: {output_path}")
    print(f"DPI: {dpi}")

    try:
        images = convert_from_path(str(pdf_path), dpi=dpi)
        print(f"Total pages found: {len(images)}")

        results = []
        for i, image in enumerate(images, start=1):
            image_filename = f"{pdf_name}_page_{i:03d}.{image_format.lower()}"
            image_path = output_path / image_filename
            image.save(image_path, image_format)
            print(f"  ✓ Saved page {i}/{len(images)}: {image_filename}")

            data_url = None
            if include_base64:
                try:
                    data_url = _image_to_data_uri(image, image_format=image_format)
                except Exception as e:
                    print(f"    ⚠️ Failed to convert image to base64 for page {i}: {e}")
                    data_url = None

            results.append({
                "page_no": i,
                "image_path": str(image_path),
                "data_url": data_url
            })

        print(f"\n✅ Successfully converted {len(images)} pages!")
        return results

    except Exception as e:
        print(f"❌ Error converting PDF: {str(e)}")
        raise


def convert_pdf_to_images_pymupdf(pdf_path, output_folder, dpi=200, image_format='PNG', include_base64=True):
    """
    Convert each page of a PDF to an image using PyMuPDF (fitz).
    This implementation doesn't require poppler installation.
    
    Args:
        pdf_path: Path to the PDF file
        output_folder: Directory to save the images
        dpi: Resolution for the images (default: 200)
        image_format: Image format (PNG, JPEG, etc.)
        include_base64: Whether to include base64 data URLs in the results
        
    Returns: 
        list of dicts: [{"page_no": int, "image_path": str, "data_url": str}, ...]
    """
    if not PYMUPDF_AVAILABLE:
        raise ImportError(
            "PyMuPDF (fitz) is not installed. "
            "Install it with: pip install pymupdf"
        )
    
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    pdf_path = Path(pdf_path)
    pdf_name = pdf_path.stem

    print(f"Converting PDF: {pdf_path}")
    print(f"Output folder: {output_path}")
    print(f"DPI: {dpi}")

    try:
        # Open the PDF
        pdf_document = fitz.open(str(pdf_path))
        total_pages = len(pdf_document)
        print(f"Total pages found: {total_pages}")

        results = []
        
        # Calculate zoom factor from DPI (72 is the default DPI)
        zoom = dpi / 72
        mat = fitz.Matrix(zoom, zoom)

        for page_num in range(total_pages):
            page = pdf_document[page_num]
            
            # Render page to an image (pixmap)
            pix = page.get_pixmap(matrix=mat)
            
            # Convert pixmap to PIL Image
            # PyMuPDF pixmaps can be in different color spaces, ensure RGB
            if pix.n < 4:  # Gray or RGB
                img_mode = "RGB" if pix.n == 3 else "L"
            else:  # RGBA
                img_mode = "RGBA"
            
            # Get the image data - pix.samples contains the raw pixel data
            pil_image = Image.frombytes(img_mode, (pix.width, pix.height), pix.samples)
            
            # Convert to RGB if needed (for consistency)
            if pil_image.mode != "RGB":
                pil_image = pil_image.convert("RGB")
            
            # Save the image
            image_filename = f"{pdf_name}_page_{page_num + 1:03d}.{image_format.lower()}"
            image_path = output_path / image_filename
            pil_image.save(image_path, image_format)
            print(f"  ✓ Saved page {page_num + 1}/{total_pages}: {image_filename}")

            # Generate base64 data URL if requested
            data_url = None
            if include_base64:
                try:
                    data_url = _image_to_data_uri(pil_image, image_format=image_format)
                except Exception as e:
                    print(f"    ⚠️ Failed to convert image to base64 for page {page_num + 1}: {e}")
                    data_url = None

            results.append({
                "page_no": page_num + 1,
                "image_path": str(image_path),
                "data_url": data_url
            })

        pdf_document.close()
        print(f"\n✅ Successfully converted {total_pages} pages using PyMuPDF!")
        return results

    except Exception as e:
        print(f"❌ Error converting PDF with PyMuPDF: {str(e)}")
        raise

