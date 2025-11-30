"""
Configuration for SyncDocuments pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

try:
    from config.settings import Config
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config.settings import Config

load_dotenv()


@dataclass(frozen=True)
class SyncSettings:
    """Settings for the document sync pipeline."""
    
    base_dir: Path = Path(__file__).parent.parent
    
    # Google Drive settings
    google_drive_credentials: Path = base_dir / "credentials" / "credentials.json"
    google_drive_token: Path = base_dir / "credentials" / "token.json"
    google_drive_scopes: tuple = ('https://www.googleapis.com/auth/drive.readonly',)
    
    # Directories
    download_dir: Path = base_dir / "data" / "downloaded"
    summaries_dir: Path = base_dir / "data" / "summaries"
    temp_images_dir: Path = base_dir / "temp" / "pdf_images"
    docstore_dir: Path = base_dir / "temp" / "pipeline_docstore"
    
    # Supabase settings
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_ANON_KEY", "")
    supabase_table: str = os.getenv("SUPABASE_TABLE", "page_summaries")
    supabase_query_fn: str = os.getenv("SUPABASE_QUERY_FN", "match_page_summaries")
    supabase_match_threshold: float = float(os.getenv("SUPABASE_MATCH_THRESHOLD", "0.0"))
    
    # Gemini settings
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    
    # Embedding settings
    embedding_model: str = os.getenv("EMBEDDING_MODEL", Config.EMBEDDING_MODEL)
    huggingface_token: str = os.getenv("HUGGINGFACE_HUB_TOKEN", "")
    
    def validate(self) -> None:
        """Validate required settings."""
        if not self.gemini_api_key:
            raise EnvironmentError("GEMINI_API_KEY must be set in the environment.")
        if not self.supabase_url or not self.supabase_key:
            raise EnvironmentError(
                "SUPABASE_URL and SUPABASE_ANON_KEY must be set in the environment."
            )
        if not self.google_drive_credentials.exists():
            raise FileNotFoundError(
                f"Google Drive credentials not found: {self.google_drive_credentials}"
            )


settings = SyncSettings()

