# pdf_converter/analyze_with_gemini.py
"""
Analyze Base64 (data URI or raw base64) directly with Gemini Vision API.
Does NOT write image to disk. Receives the data-URI or base64 string and
passes it to the Gemini SDK as an image payload.
"""

import os
from datetime import datetime
import google.generativeai as genai

# Load environment variables and configure Gemini
try:
    from config.settings import Config
    GEMINI_API_KEY = Config.GEMINI_API_KEY
except ImportError:
    # Fallback to environment variable if config not available
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# Configure Gemini API key
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# Helper to sanitize/strip data URI prefix if present
def _strip_data_uri(data_uri_or_b64):
    if data_uri_or_b64 is None:
        return None
    if data_uri_or_b64.startswith("data:"):
        # data:<mime>;base64,<data>
        try:
            return data_uri_or_b64.split(",", 1)[1]
        except Exception:
            return data_uri_or_b64
    return data_uri_or_b64

def _build_prompt(page_number: int, total_pages: int) -> str:
    if page_number == 1:
        return """
        Analyze this page from a business report. This appears to be page 1.

        If this is an introduction, cover, or title page:
        - Provide a BRIEF summary (2-3 sentences maximum) stating the document type, company/client name, and reporting period.

        If this page contains actual data or detailed content:
        - Provide a DETAILED summary including all key metrics, data points, charts, and insights.
        - Extract specific numbers, percentages, trends, and notable findings.

        Focus on accuracy and completeness of information.
        """
    else:
        return f"""
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

def analyze_image_base64(base64_or_data_uri: str, page_number: int, total_pages: int, mime_type_hint="image/png"):
    """
    Analyze a base64 string or data URI directly with Gemini.
    - base64_or_data_uri: either "data:image/png;base64,..." or raw base64 (no prefix)
    - returns: {"summary": str, "used_payload": dict}
    """

    if not base64_or_data_uri:
        return {"summary": "No base64 image provided", "used_payload": None}

    # Check if Gemini is properly configured
    if not GEMINI_API_KEY:
        return {"summary": "Error: GEMINI_API_KEY not configured. Please set the API key in your environment or config.", "used_payload": None}

    try:
        model = genai.GenerativeModel('gemini-2.0-flash')
    except Exception as e:
        return {"summary": f"Error initializing Gemini model: {e}", "used_payload": None}

    # If it's a data URI, strip the prefix to get raw base64
    raw_b64 = _strip_data_uri(base64_or_data_uri)

    # Prepare the prompt and image payload.
    prompt = _build_prompt(page_number, total_pages)

    # Some genai SDKs accept a dict for image part; keep the same shape your earlier code used:
    image_part = {
        "mime_type": mime_type_hint,
        "data": raw_b64
    }

    try:
        # Call Gemini: pass the prompt and the image part. This avoids writing to disk.
        response = model.generate_content([prompt, image_part])
        summary_text = response.text if hasattr(response, "text") else str(response)
        return {"summary": summary_text.strip(), "used_payload": {"mime_type": mime_type_hint, "data_length": len(raw_b64)}}
    except Exception as e:
        return {"summary": f"Error calling Gemini: {e}", "used_payload": {"mime_type": mime_type_hint, "data_length": len(raw_b64)}}