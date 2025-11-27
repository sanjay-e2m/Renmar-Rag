"""
Simple JSON-backed document store for MultiVectorRetriever.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from langchain.storage import BaseStore
from langchain_core.documents import Document


class LocalJSONDocStore(BaseStore[str, Document]):
    def __init__(self, store_dir: Path):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)

    def _doc_path(self, key: str) -> Path:
        safe_key = key.replace("/", "_")
        return self.store_dir / f"{safe_key}.json"

    def mget(self, keys: Iterable[str]) -> List[Optional[Document]]:
        docs: List[Optional[Document]] = []
        for key in keys:
            path = self._doc_path(key)
            if not path.exists():
                docs.append(None)
                continue
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            docs.append(
                Document(
                    page_content=payload["page_content"],
                    metadata=payload["metadata"],
                )
            )
        return docs

    def mset(self, key_value_pairs: Iterable[Tuple[str, Document]]) -> None:
        for key, doc in key_value_pairs:
            path = self._doc_path(key)
            payload = {
                "page_content": doc.page_content,
                "metadata": doc.metadata,
            }
            with path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

    def mdelete(self, keys: Iterable[str]) -> None:
        for key in keys:
            path = self._doc_path(key)
            if path.exists():
                path.unlink()

