from typing import List, Union
import numpy as np
from sentence_transformers import SentenceTransformer
from config.settings import Config


class GemmaEmbeddings:
    """
    Wrapper for generating embeddings using HuggingFace sentence-transformers
    Supports both standard models and embeddinggemma
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize the embedding model
        
        Args:
            model_name: Name of the model to use (default from Config)
        """
        self.model_name = model_name or Config.EMBEDDING_MODEL
        print(f"Loading embedding model: {self.model_name}")
        
        # Initialize the model
        # NOTE: EmbeddingGemma requires float32 or bfloat16, not float16
        self.model = SentenceTransformer(
            self.model_name,
            trust_remote_code=True
        )
        
        # Set HuggingFace token if needed for private models
        if Config.HF_TOKEN:
            import os
            os.environ['HF_TOKEN'] = Config.HF_TOKEN
        
        # Check if this is embeddinggemma model
        self.is_embeddinggemma = 'embeddinggemma' in self.model_name.lower()
        
        print(f"Model loaded successfully. Embedding dimension: {self.get_dimension()}")
        if self.is_embeddinggemma:
            print("Using embeddinggemma-specific encode_document() method")
    
    def embed_text(self, text: str) -> List[float]:
        """
        Generate embedding for a single text
        
        Args:
            text: Input text to embed
            
        Returns:
            List of floats representing the embedding
        """
        if self.is_embeddinggemma:
            # Use encode_document for embeddinggemma
            embedding = self.model.encode_document(text, convert_to_numpy=True)
        else:
            # Use standard encode for other models
            embedding = self.model.encode(text, convert_to_numpy=True)
        
        return embedding.tolist()
    
    def embed_batch(self, texts: List[str], batch_size: int = None) -> List[List[float]]:
        """
        Generate embeddings for multiple texts
        
        Args:
            texts: List of input texts to embed
            batch_size: Batch size for processing (default from Config)
            
        Returns:
            List of embeddings
        """
        batch_size = batch_size or Config.BATCH_SIZE
        
        if self.is_embeddinggemma:
            # Use encode_document for embeddinggemma
            embeddings = self.model.encode_document(
                texts,
                batch_size=batch_size,
                convert_to_numpy=True,
                show_progress_bar=True
            )
        else:
            # Use standard encode for other models
            embeddings = self.model.encode(
                texts,
                batch_size=batch_size,
                convert_to_numpy=True,
                show_progress_bar=True
            )
        
        return embeddings.tolist()
    
    def embed_query(self, query: str) -> List[float]:
        """
        Generate embedding for a query (for embeddinggemma)
        
        Args:
            query: Query text to embed
            
        Returns:
            List of floats representing the embedding
        """
        if self.is_embeddinggemma:
            # Use encode_query for embeddinggemma
            embedding = self.model.encode_query(query, convert_to_numpy=True)
        else:
            # Fall back to regular encode
            embedding = self.model.encode(query, convert_to_numpy=True)
        
        return embedding.tolist()
    
    def get_dimension(self) -> int:
        """Get the dimension of the embeddings"""
        return self.model.get_sentence_embedding_dimension()


# Singleton instance
_embeddings_instance = None


def get_embeddings() -> GemmaEmbeddings:
    """Get or create singleton embeddings instance"""
    global _embeddings_instance
    
    if _embeddings_instance is None:
        _embeddings_instance = GemmaEmbeddings()
    
    return _embeddings_instance
