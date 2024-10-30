import logging
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from typing import Dict, List

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingVerifier:
    def __init__(self):
        """Initialize Neo4j connection"""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

    def verify_document_embeddings(self, user_id: str = None) -> Dict:
        """Verify document embeddings and their chunks"""
        try:
            # Different queries for user-specific and all documents
            if user_id:
                query = """
                // Match user's documents
                MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
                OPTIONAL MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
                
                WITH d,
                     collect(e) as embedding_chunks,
                     size(collect(e)) as chunk_count
                
                // Collect statistics
                RETURN 
                    count(d) as total_docs,
                    sum(CASE WHEN chunk_count > 0 THEN 1 ELSE 0 END) as docs_with_embeddings,
                    sum(CASE WHEN chunk_count = 0 THEN 1 ELSE 0 END) as docs_without_embeddings,
                    avg(chunk_count) as avg_chunks_per_doc,
                    collect({
                        title: d.title,
                        doc_type: d.doc_type,
                        chunk_count: chunk_count,
                        chunk_sizes: [c in embedding_chunks | size(c.embedding_chunk)]
                    }) as doc_details
                """
            else:
                query = """
                // Match all documents
                MATCH (d:Document)
                OPTIONAL MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
                
                WITH d,
                     collect(e) as embedding_chunks,
                     size(collect(e)) as chunk_count
                
                // Collect statistics
                RETURN 
                    count(d) as total_docs,
                    sum(CASE WHEN chunk_count > 0 THEN 1 ELSE 0 END) as docs_with_embeddings,
                    sum(CASE WHEN chunk_count = 0 THEN 1 ELSE 0 END) as docs_without_embeddings,
                    avg(chunk_count) as avg_chunks_per_doc,
                    collect({
                        title: d.title,
                        doc_type: d.doc_type,
                        chunk_count: chunk_count,
                        chunk_sizes: [c in embedding_chunks | size(c.embedding_chunk)]
                    }) as doc_details
                """
            
            with self.driver.session() as session:
                result = session.run(query, user_id=user_id).single()
                
                if not result:
                    return {"error": "No documents found"}
                
                stats = {
                    "total_documents": result["total_docs"],
                    "with_embeddings": result["docs_with_embeddings"],
                    "without_embeddings": result["docs_without_embeddings"],
                    "average_chunks": round(result["avg_chunks_per_doc"], 2),
                    "documents": result["doc_details"]
                }
                
                # Print summary
                print("\nEmbedding Statistics:")
                print(f"Total Documents: {stats['total_documents']}")
                print(f"Documents with Embeddings: {stats['with_embeddings']}")
                print(f"Documents without Embeddings: {stats['without_embeddings']}")
                print(f"Average Chunks per Document: {stats['average_chunks']}")
                
                # Print details for documents with unusual chunk counts
                print("\nDetailed Analysis:")
                for doc in stats["documents"]:
                    if doc["chunk_count"] == 0:
                        print(f"\nWarning: No embeddings for document '{doc['title']}' ({doc['doc_type']})")
                    elif len(set(doc["chunk_sizes"])) > 1:
                        print(f"\nWarning: Inconsistent chunk sizes for '{doc['title']}':")
                        print(f"Chunk sizes: {doc['chunk_sizes']}")
                
                return stats
                
        except Exception as e:
            logger.error(f"Error verifying embeddings: {e}")
            return {"error": str(e)}

    def check_vector_index(self) -> Dict:
        """Check vector index configuration"""
        try:
            query = """
            SHOW INDEXES
            YIELD name, type, labelsOrTypes, properties, options
            WHERE name = 'document_embeddings'
            """
            
            with self.driver.session() as session:
                result = session.run(query).single()
                
                if not result:
                    return {"error": "Vector index not found"}
                
                index_info = {
                    "name": result["name"],
                    "type": result["type"],
                    "labels": result["labelsOrTypes"],
                    "properties": result["properties"],
                    "options": result["options"]
                }
                
                print("\nVector Index Configuration:")
                print(f"Name: {index_info['name']}")
                print(f"Type: {index_info['type']}")
                print(f"Labels: {index_info['labels']}")
                print(f"Properties: {index_info['properties']}")
                print(f"Options: {index_info['options']}")
                
                return index_info
                
        except Exception as e:
            logger.error(f"Error checking vector index: {e}")
            return {"error": str(e)}

def main():
    """Verify embeddings and index setup"""
    verifier = EmbeddingVerifier()
    
    try:
        # Check vector index configuration
        print("\nChecking vector index...")
        index_info = verifier.check_vector_index()
        if "error" in index_info:
            print(f"Error: {index_info['error']}")
            return
        
        # Verify embeddings for specific user
        test_user_id = "aman"
        print(f"\nVerifying embeddings for user {test_user_id}...")
        user_stats = verifier.verify_document_embeddings(test_user_id)
        
        # Verify all embeddings
        print("\nVerifying all embeddings...")
        all_stats = verifier.verify_document_embeddings()
        
    except Exception as e:
        logger.error(f"Error in verification: {e}")
        raise
    
    finally:
        verifier.close()

if __name__ == "__main__":
    main() 