"""
Centralized configuration for the pipeline package.
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
class Settings:
    base_dir: Path = Path(__file__).parent.parent
    summaries_dir: Path = base_dir / "data" / "summaries"
    docstore_dir: Path = base_dir / "temp" / "pipeline_docstore"

    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_ANON_KEY", "")
    supabase_table: str = os.getenv("SUPABASE_TABLE", "page_summaries")
    supabase_query_fn: str = os.getenv("SUPABASE_QUERY_FN", "match_page_summaries")
    supabase_match_threshold: float = float(os.getenv("SUPABASE_MATCH_THRESHOLD", "0.0"))

    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    embedding_model: str = os.getenv("EMBEDDING_MODEL", Config.EMBEDDING_MODEL)
    huggingface_token: str = os.getenv("HUGGINGFACE_HUB_TOKEN", "")

    retriever_top_k: int = int(os.getenv("RETRIEVER_TOP_K", 2))

    def validate_vector_store(self) -> None:
        if not self.supabase_url or not self.supabase_key:
            raise EnvironmentError(
                "SUPABASE_URL and SUPABASE_ANON_KEY must be set in the environment."
            )
        if not self.gemini_api_key:
            raise EnvironmentError("GEMINI_API_KEY must be set in the environment.")


settings = Settings()
