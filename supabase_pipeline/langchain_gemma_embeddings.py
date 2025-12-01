"""
LangChain-compatible wrapper for GemmaEmbeddings
"""
from typing import List
from langchain_core.embeddings import Embeddings
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_pipeline.embeddings import GemmaEmbeddings


class LangChainGemmaEmbeddings(Embeddings):
    """
    LangChain-compatible wrapper for GemmaEmbeddings.
    This allows GemmaEmbeddings to be used with LangChain vector stores.
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize the LangChain-compatible Gemma embeddings
        
        Args:
            model_name: Name of the embedding model to use
        """
        self.gemma_embeddings = GemmaEmbeddings(model_name=model_name)
    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a list of documents
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings (each embedding is a list of floats)
        """
        return self.gemma_embeddings.embed_batch(texts)
    
    def embed_query(self, text: str) -> List[float]:
        """
        Embed a single query text
        
        Args:
            text: Query text to embed
            
        Returns:
            Embedding as a list of floats
        """
        return self.gemma_embeddings.embed_query(text)
    
    def get_dimension(self) -> int:
        """Get the dimension of the embeddings"""
        return self.gemma_embeddings.get_dimension()
