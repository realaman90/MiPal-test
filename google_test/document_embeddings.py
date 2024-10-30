from typing import Dict, List, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from llama_index.vector_stores.neo4jvector import Neo4jVectorStore
from llama_index.core import VectorStoreIndex, Document, StorageContext
import openai

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentEmbeddingStore:
    def __init__(self):
        """Initialize Neo4j connection and vector store"""
        # Neo4j credentials
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        # OpenAI credentials
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize Neo4j vector store
        self.embed_dim = 1536  # OpenAI embedding dimension
        self.vector_store = Neo4jVectorStore(
            username=self.user,
            password=self.password,
            url=self.uri,
            embed_dim=self.embed_dim,
            index_name="document_summaries",  # Custom index name
            text_node_property="summary",     # Property containing the text
            hybrid_search=True               # Enable hybrid search
        )
        
        # Initialize regular Neo4j connection
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        """Close connections"""
        self.driver.close()

    def create_embeddings_for_documents(self, user_id: str) -> bool:
        """Create embeddings for all document summaries for a user"""
        try:
            # Get all documents for the user
            query = """
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            WHERE d.summary IS NOT NULL AND d.summary <> ''
            RETURN d.id as id, d.title as title, d.summary as summary, 
                   d.doc_type as doc_type, d.created_time as created_time
            """
            
            documents = []
            with self.driver.session() as session:
                results = session.run(query, user_id=user_id)
                for record in results:
                    # Create metadata
                    metadata = {
                        "doc_id": record["id"],
                        "title": record["title"],
                        "doc_type": record["doc_type"],
                        "created_time": record["created_time"],
                        "_node_type": "Document",
                        "_node_content": record["summary"]
                    }
                    
                    # Create Document object
                    doc = Document(
                        text=record["summary"],
                        metadata=metadata
                    )
                    documents.append(doc)
            
            if not documents:
                logger.warning(f"No documents found for user {user_id}")
                return False
            
            # Create storage context with our vector store
            storage_context = StorageContext.from_defaults(
                vector_store=self.vector_store
            )
            
            # Create the index
            index = VectorStoreIndex.from_documents(
                documents,
                storage_context=storage_context
            )
            
            logger.info(f"Created embeddings for {len(documents)} documents")
            return True
            
        except Exception as e:
            logger.error(f"Error creating embeddings: {e}")
            return False

    def search_documents(self, user_id: str, query: str, limit: int = 5) -> List[Dict]:
        """Search documents using hybrid search"""
        try:
            # Create query engine
            index = VectorStoreIndex.from_vector_store(self.vector_store)
            query_engine = index.as_query_engine()
            
            # Custom retrieval query to filter by user_id
            retrieval_query = f"""
            MATCH (u:User {{user_id: '{user_id}'}})-[:OWNS]->(d:Document)
            WITH d, score
            RETURN d.summary AS text, score, d.id AS id,
            {{
                title: d.title,
                doc_type: d.doc_type,
                created_time: d.created_time,
                _node_type: 'Document',
                _node_content: d.summary
            }} AS metadata
            LIMIT {limit}
            """
            
            # Update vector store with custom query
            self.vector_store.retrieval_query = retrieval_query
            
            # Perform search
            response = query_engine.query(query)
            
            # Format results
            source_nodes = response.source_nodes
            results = []
            for node in source_nodes:
                results.append({
                    'title': node.metadata.get('title'),
                    'doc_type': node.metadata.get('doc_type'),
                    'created_time': node.metadata.get('created_time'),
                    'summary': node.text,
                    'score': node.score if hasattr(node, 'score') else None
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return []

def main():
    """Test document embeddings"""
    try:
        embedding_store = DocumentEmbeddingStore()
        test_user_id = "aman"
        
        # Create embeddings
        print("\nCreating embeddings for documents...")
        success = embedding_store.create_embeddings_for_documents(test_user_id)
        
        if success:
            # Test search
            print("\nTesting document search...")
            test_query = "Find documents about machine learning"
            results = embedding_store.search_documents(test_user_id, test_query)
            
            print(f"\nSearch results for: {test_query}")
            for i, result in enumerate(results, 1):
                print(f"\n{i}. {result['title']}")
                print(f"Type: {result['doc_type']}")
                print(f"Score: {result['score']}")
                print(f"Summary: {result['summary'][:200]}...")
                print("-" * 80)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        embedding_store.close()

if __name__ == "__main__":
    main() 