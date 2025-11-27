# PDF to Images Converter

This module converts PDF files into individual image files (one image per page).

## Requirements

Install the required dependencies:

```bash
pip install pdf2image pillow
```

### Additional System Requirements

The `pdf2image` library requires `poppler-utils` to be installed on your system:

**macOS:**
```bash
brew install poppler
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install poppler-utils
```

**Windows:**
Download and install poppler from: https://github.com/oschwartz10612/poppler-windows/releases/

## Usage

### Run the converter:

```bash
python pdf_converter/pdf_to_images.py
```

This will convert the PDF at:
- Input: `data/downloaded/Copy of Affordable_Dental_Monthly_Report_2025-08-01_2025-08-31.pdf`
- Output: `temp/pdf_images/` (images will be saved here)

### Customize the conversion:

```python
from pdf_converter.pdf_to_images import convert_pdf_to_images

# Convert with custom settings
image_paths = convert_pdf_to_images(
    pdf_path="path/to/your/file.pdf",
    output_folder="path/to/output",
    dpi=300,  # Higher quality
    image_format='PNG'  # or 'JPEG', 'TIFF', etc.
)
```

## Output

- Each page of the PDF is saved as a separate image
- Images are named: `{pdf_name}_page_{number}.png`
- Default resolution: 200 DPI
- Default format: PNG

## Example Output

For a 5-page PDF named "report.pdf", you'll get:
```
temp/pdf_images/
├── report_page_001.png
├── report_page_002.png
├── report_page_003.png
├── report_page_004.png
└── report_page_005.png
```
