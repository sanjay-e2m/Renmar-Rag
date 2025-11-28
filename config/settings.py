import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Google Drive scopes
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Google Drive Folder ID
FOLDER_ID = os.getenv("FOLDER_ID")

# Path to credentials
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials", "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "credentials", "token.json")

# Download directory
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloaded")


class Config:
    """Centralized configuration for the application"""
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    SUMMARIES_DIR = DATA_DIR / "summaries"
    TEMP_IMAGES_DIR = BASE_DIR / "temp" / "pdf_images"
    INCLUDE_BASE64_IN_JSON = True
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
    
    # Embedding model configuration
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "google/gemma-2-2b-it")
    HF_TOKEN = os.getenv("HF_TOKEN", "")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "32"))
