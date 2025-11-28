#!/usr/bin/env python3
"""
Quick test script to verify PyMuPDF implementation works
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pdf_converter.main import process_pdf_pipeline

# Test with one PDF
test_pdf = Path("data/downloaded/Copy of CB_Distributors_Monthly_Report_2025-08-01_2025-08-31.pdf")

if test_pdf.exists():
    print(f"Testing with: {test_pdf.name}")
    result = process_pdf_pipeline(test_pdf)
    if result:
        print(f"\n✅ SUCCESS! Generated: {result}")
    else:
        print("\n❌ FAILED!")
else:
    print(f"❌ Test PDF not found: {test_pdf}")
