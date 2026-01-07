import os
import sys
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Load environment variables
load_dotenv()

# -------------------------
# Embedding Configuration
# -------------------------
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "google/embeddinggemma-300m")
HF_TOKEN = os.getenv("HF_TOKEN", "")


class KnowledgeBaseEmbeddings:
    """
    Generate embeddings for knowledge base queries using Gemma embedding model.
    Combines question_text and sql_query for embedding generation.
    """
    
    def __init__(self, model_name: str = None):
        """
        Initialize the embedding model.
        
        Args:
            model_name: Name of the model to use (default from env)
        """
        self.model_name = model_name or EMBEDDING_MODEL
        print(f"Loading embedding model: {self.model_name}")
        
        # Set HuggingFace token if needed
        if HF_TOKEN:
            os.environ['HF_TOKEN'] = HF_TOKEN
        
        # Initialize the model
        self.model = SentenceTransformer(
            self.model_name,
            trust_remote_code=True
        )
        
        # Check if this is embeddinggemma model
        self.is_embeddinggemma = 'embeddinggemma' in self.model_name.lower()
        
        print(f"Model loaded successfully. Embedding dimension: {self.get_dimension()}")
        if self.is_embeddinggemma:
            print("Using embeddinggemma-specific encode_document() method")
    
    def generate_embedding(self, question_text: str, sql_query: str) -> List[float]:
        """
        Generate embedding for a combined question and SQL query.
        
        Args:
            question_text: Natural language question
            sql_query: Corresponding SQL query
            
        Returns:
            List of floats representing the embedding
        """
        # Combine question and SQL query
        combined_text = f"Question: {question_text}\nSQL: {sql_query}"
        
        if self.is_embeddinggemma:
            # Use encode_document for embeddinggemma
            embedding = self.model.encode_document(combined_text, convert_to_numpy=True)
        else:
            # Use standard encode for other models
            embedding = self.model.encode(combined_text, convert_to_numpy=True)
        
        return embedding.tolist()
    
    def generate_embeddings_batch(self, data_list: List[dict], batch_size: int = 8) -> List[List[float]]:
        """
        Generate embeddings for a batch of question-SQL pairs.
        
        Args:
            data_list: List of dicts with 'question_text' and 'sql_query' keys
            batch_size: Batch size for processing
            
        Returns:
            List of embeddings
        """
        # Combine questions and SQL queries
        combined_texts = [
            f"Question: {item['question_text']}\nSQL: {item['sql_query']}"
            for item in data_list
        ]
        
        if self.is_embeddinggemma:
            # Use encode_document for embeddinggemma
            embeddings = self.model.encode_document(
                combined_texts,
                batch_size=batch_size,
                convert_to_numpy=True,
                show_progress_bar=True
            )
        else:
            # Use standard encode for other models
            embeddings = self.model.encode(
                combined_texts,
                batch_size=batch_size,
                convert_to_numpy=True,
                show_progress_bar=True
            )
        
        return embeddings.tolist()
    
    def get_dimension(self) -> int:
        """Get the dimension of the embeddings"""
        return self.model.get_sentence_embedding_dimension()


# Singleton instance
_embeddings_instance = None


def get_embeddings() -> KnowledgeBaseEmbeddings:
    """Get or create singleton embeddings instance"""
    global _embeddings_instance
    
    if _embeddings_instance is None:
        _embeddings_instance = KnowledgeBaseEmbeddings()
    
    return _embeddings_instance


# -------------------------
# Main (for testing)
# -------------------------
if __name__ == "__main__":
    # Test the embedding generation
    embeddings = get_embeddings()
    
    test_question = "What are all the keywords for client ABC in March 2025?"
    test_sql = "SELECT k.keyword FROM \"Mastersheet-Keyword_report\" k JOIN files f ON k.file_id = f.file_id JOIN months m ON f.month_pk = m.month_pk WHERE f.client_name = 'ABC' AND m.month_name = 'March' AND m.year = 2025;"
    
    print("\n" + "="*60)
    print("Testing Embedding Generation")
    print("="*60)
    print(f"Question: {test_question}")
    print(f"SQL: {test_sql}")
    
    embedding = embeddings.generate_embedding(test_question, test_sql)
    print(f"\n✅ Embedding generated successfully!")
    print(f"   Dimension: {len(embedding)}")
    print(f"   First 5 values: {embedding[:5]}")

