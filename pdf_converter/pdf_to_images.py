"""
PDF to Images Converter
Converts each page of every PDF file inside data/downloaded into images.
"""

from pathlib import Path
from typing import List
import fitz  # PyMuPDF


def convert_pdf_to_images(
    pdf_path: str,
    output_folder: str,
    dpi: int = 200,
    image_format: str = "PNG",
) -> List[str]:
    """
    Convert each page of a PDF to an image.
    """
    pdf_path = Path(pdf_path)
    output_path = Path(output_folder)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    output_path.mkdir(parents=True, exist_ok=True)
    pdf_name = pdf_path.stem

    print(f"\nConverting PDF: {pdf_path}")
    print(f"Output folder: {output_folder}")
    print(f"DPI: {dpi}")

    try:
        zoom = dpi / 72
        matrix = fitz.Matrix(zoom, zoom)

        image_paths: List[str] = []
        with fitz.open(str(pdf_path)) as pdf_doc:
            print(f"Total pages found: {pdf_doc.page_count}")

            for page_index in range(pdf_doc.page_count):
                page = pdf_doc.load_page(page_index)
                pix = page.get_pixmap(matrix=matrix, alpha=False)

                image_filename = f"{pdf_name}_page_{page_index + 1:03d}.{image_format.lower()}"
                image_path = output_path / image_filename

                pix.save(str(image_path))
                image_paths.append(str(image_path))

                print(f"  ✓ Saved page {page_index + 1}/{pdf_doc.page_count}: {image_filename}")

        print(f"✅ Successfully converted {len(image_paths)} pages!")
        return image_paths

    except Exception as e:
        print(f"❌ Error converting PDF: {str(e)}")
        raise


def convert_all_pdfs_in_folder(input_folder: str, output_base_folder: str, dpi: int = 200, image_format: str = "PNG"):
    """
    Converts all PDFs inside a folder into images.
    Each PDF gets its own output subfolder.
    """
    input_folder = Path(input_folder)
    output_base_folder = Path(output_base_folder)

    if not input_folder.exists():
        print(f"❌ Input folder does not exist: {input_folder}")
        return

    pdf_files = list(input_folder.glob("*.pdf"))
    if not pdf_files:
        print("❌ No PDF files found in directory.")
        return

    print(f"Found {len(pdf_files)} PDF(s) in {input_folder}")

    for pdf_file in pdf_files:
        output_folder = output_base_folder / pdf_file.stem
        output_folder.mkdir(parents=True, exist_ok=True)

        try:
            convert_pdf_to_images(
                pdf_path=str(pdf_file),
                output_folder=str(output_folder),
                dpi=dpi,
                image_format=image_format
            )
        except Exception as e:
            print(f"❌ Failed to convert {pdf_file.name}: {str(e)}")


def main():
    """Main: converts all PDFs in data/downloaded/"""

    input_folder = Path("../data/downloaded")
    output_base_folder = Path("../temp/pdf_images")

    convert_all_pdfs_in_folder(
        input_folder=input_folder,
        output_base_folder=output_base_folder,
        dpi=200,
        image_format="PNG",
    )


if __name__ == "__main__":
    main()
