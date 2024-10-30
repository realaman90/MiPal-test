from typing import Dict, List, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import openai
from llama_index.embeddings.openai import OpenAIEmbedding

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentRetriever:
    def __init__(self):
        """Initialize Neo4j connection and embedding model"""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        # Initialize OpenAI
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize Neo4j connection
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        
        # Initialize embedding model
        self.embed_model = OpenAIEmbedding()

    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

    def search_documents(self, user_id: str, query: str, limit: int = 5, similarity_threshold: float = 0.3) -> List[Dict]:
        """Search for documents using embedding similarity"""
        try:
            # Generate query embedding and chunk it
            full_query_embedding = self.embed_model.get_text_embedding(query)
            chunk_size = 500  # Same size as stored chunks
            query_chunks = [full_query_embedding[i:i + chunk_size] for i in range(0, len(full_query_embedding), chunk_size)]
            
            # Search query using vector similarity
            search_query = """
            // Match documents owned by the user
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            
            // Match document embeddings with same chunk index
            MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
            WHERE e.chunk_index = $chunk_index
            
            // Calculate similarity for each document
            WITH d, e, gds.similarity.cosine(
                e.embedding_chunk,
                $query_chunk
            ) AS chunk_score
            
            // Group by document and get average score
            WITH d, avg(chunk_score) as similarity_score
            WHERE similarity_score > $threshold
            
            // Return document details
            RETURN DISTINCT
                d.id as id,
                d.title as title,
                d.doc_type as doc_type,
                d.summary as summary,
                d.created_time as created_time,
                d.modified_time as modified_time,
                d.web_view_link as url,
                similarity_score
            ORDER BY similarity_score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                # Search with first chunk (most significant part)
                result = session.run(
                    search_query,
                    user_id=user_id,
                    query_chunk=query_chunks[0],  # Use first chunk
                    chunk_index=0,  # Match with first chunk of documents
                    threshold=similarity_threshold,
                    limit=limit
                )
                
                documents = []
                for record in result:
                    documents.append({
                        'id': record['id'],
                        'title': record.get('title') or 'Untitled',
                        'doc_type': record['doc_type'],
                        'summary': record['summary'],
                        'created_time': record['created_time'],
                        'modified_time': record['modified_time'],
                        'url': record['url'],
                        'similarity_score': record['similarity_score']
                    })
                
                if not documents:
                    logger.info(f"No similar documents found for query: {query}")
                else:
                    logger.info(f"Found {len(documents)} similar documents")
                
                return documents
                
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return []

    def get_document_recommendations(self, user_id: str, doc_id: str, limit: int = 3) -> List[Dict]:
        """
        Get similar document recommendations based on a source document
        
        Args:
            user_id: User identifier
            doc_id: Source document ID
            limit: Maximum number of recommendations
            
        Returns:
            List of similar documents
        """
        try:
            recommendation_query = """
            // Match source document and its embeddings
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(source:Document {id: $doc_id})-[:HAS_EMBEDDING]->(source_emb:DocumentEmbedding)
            
            // Match other documents owned by the user
            MATCH (u)-[:OWNS]->(other:Document)-[:HAS_EMBEDDING]->(other_emb:DocumentEmbedding)
            WHERE other.id <> source.id
            
            // Calculate similarity between chunks
            WITH other, other_emb, source_emb,
                 gds.similarity.cosine(other_emb.embedding_chunk, source_emb.embedding_chunk) AS chunk_similarity
            
            // Average similarity scores for each document
            WITH other, avg(chunk_similarity) as similarity_score
            WHERE similarity_score > 0.7
            
            // Return recommended documents
            RETURN 
                other.id as id,
                other.title as title,
                other.doc_type as doc_type,
                other.summary as summary,
                other.web_view_link as url,
                similarity_score
            ORDER BY similarity_score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    recommendation_query,
                    user_id=user_id,
                    doc_id=doc_id,
                    limit=limit
                )
                
                recommendations = []
                for record in result:
                    recommendations.append({
                        'id': record['id'],
                        'title': record['title'],
                        'doc_type': record['doc_type'],
                        'summary': record['summary'],
                        'url': record['url'],
                        'similarity_score': record['similarity_score']
                    })
                
                return recommendations
                
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []

    def search_by_topic(self, user_id: str, topic: str, limit: int = 5) -> Dict[str, List[Dict]]:
        """Search documents by topic and group by type"""
        try:
            # Generate topic embedding
            topic_embedding = self.embed_model.get_text_embedding(topic)
            topic_size = len(topic_embedding)
            
            topic_query = """
            // Match user's documents and their embeddings
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
            WHERE size(e.embedding_chunk) = $topic_size
            
            // Calculate similarity scores
            WITH d, e, gds.similarity.cosine(e.embedding_chunk, $topic_embedding) AS chunk_score
            
            // Average scores per document
            WITH d, avg(chunk_score) as similarity_score
            WHERE similarity_score > 0.7
            
            // Group by document type
            WITH COALESCE(d.doc_type, split(d.mime_type, '/')[1]) as doc_type,
                 collect({
                    id: d.id,
                    title: COALESCE(d.title, d.name, 'Untitled'),
                    summary: d.summary,
                    url: d.web_view_link,
                    score: similarity_score
                 })[0..$limit] as docs
            WHERE size(docs) > 0
            
            // Return grouped results
            RETURN doc_type, docs
            ORDER BY doc_type
            """
            
            with self.driver.session() as session:
                result = session.run(
                    topic_query,
                    user_id=user_id,
                    topic_embedding=topic_embedding,
                    topic_size=topic_size,
                    limit=limit
                )
                
                results_by_type = {}
                for record in result:
                    doc_type = record['doc_type']
                    docs = record['docs']
                    if docs:  # Only include document types with results
                        results_by_type[doc_type] = docs
                
                return results_by_type
                
        except Exception as e:
            logger.error(f"Error searching by topic: {e}")
            return {}

    def verify_embeddings(self, user_id: str) -> Dict:
        """Verify embedding chunks for user's documents"""
        try:
            verify_query = """
            // Match documents and their embeddings
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            OPTIONAL MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
            
            // Collect statistics about embeddings
            WITH d, collect(e) as embeddings
            RETURN 
                count(d) as total_docs,
                sum(CASE WHEN size(embeddings) > 0 THEN 1 ELSE 0 END) as docs_with_embeddings,
                sum(CASE WHEN size(embeddings) = 0 THEN 1 ELSE 0 END) as docs_without_embeddings,
                collect({
                    title: d.title,
                    doc_type: d.doc_type,
                    embedding_count: size(embeddings),
                    chunk_sizes: [c in embeddings | size(c.embedding_chunk)]
                }) as doc_details
            """
            
            with self.driver.session() as session:
                result = session.run(verify_query, user_id=user_id).single()
                
                if result:
                    print("\nEmbedding Statistics:")
                    print(f"Total Documents: {result['total_docs']}")
                    print(f"With Embeddings: {result['docs_with_embeddings']}")
                    print(f"Without Embeddings: {result['docs_without_embeddings']}")
                    
                    print("\nDocument Details:")
                    for doc in result['doc_details']:
                        if doc['embedding_count'] == 0:
                            print(f"\nWarning: No embeddings for {doc['title']} ({doc['doc_type']})")
                        else:
                            print(f"\nDocument: {doc['title']}")
                            print(f"Type: {doc['doc_type']}")
                            print(f"Embedding chunks: {doc['embedding_count']}")
                            print(f"Chunk sizes: {doc['chunk_sizes']}")
                    
                    return {
                        'total_docs': result['total_docs'],
                        'with_embeddings': result['docs_with_embeddings'],
                        'without_embeddings': result['docs_without_embeddings'],
                        'details': result['doc_details']
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error verifying embeddings: {e}")
            return None

    def debug_embeddings(self, user_id: str) -> None:
        """Debug embeddings for a user's documents"""
        try:
            debug_query = """
            // Match documents and their embeddings
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            OPTIONAL MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
            WITH d, collect(e) as embeddings
            
            // Return detailed information
            RETURN 
                d.title as title,
                d.doc_type as type,
                d.summary as summary,
                size(embeddings) as num_embeddings,
                [e in embeddings | size(e.embedding_chunk)] as chunk_sizes,
                [e in embeddings | e.embedding_chunk[0..5]] as chunk_samples
            """
            
            with self.driver.session() as session:
                results = session.run(debug_query, user_id=user_id)
                
                print("\nEmbedding Debug Information:")
                for record in results:
                    print(f"\nDocument: {record['title']}")
                    print(f"Type: {record['type']}")
                    print(f"Number of embeddings: {record['num_embeddings']}")
                    print(f"Chunk sizes: {record['chunk_sizes']}")
                    print(f"First 5 values of each chunk: {record['chunk_samples']}")
                    print("-" * 80)
                
        except Exception as e:
            logger.error(f"Error debugging embeddings: {e}")

def main():
    """Test document retrieval functionality"""
    try:
        retriever = DocumentRetriever()
        test_user_id = "dev"
        
        # Debug embeddings
        print("\nDebugging embeddings...")
        retriever.debug_embeddings(test_user_id)
        
        # Test semantic search with different queries
        queries = [
           
            {
                'text': "Fastlane Devs logo image",
                'threshold': 0.9

            },
            
        ]
        
        for query_info in queries:
            query = query_info['text']
            threshold = query_info['threshold']
            
            print(f"\nSearching for: '{query}' (threshold: {threshold})")
            results = retriever.search_documents(
                test_user_id, 
                query, 
                limit=5,
                similarity_threshold=threshold
            )
            
            if results:
                print(f"\nFound {len(results)} matching documents:")
                for i, doc in enumerate(results, 1):
                    print(f"\n{i}. {doc['title']}")
                    print(f"Type: {doc['doc_type']}")
                    print(f"Score: {doc['similarity_score']:.3f}")
                    print(f"Summary excerpt: {doc['summary'][:200]}...")
                    print("-" * 80)
            else:
                print("No matching documents found")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        retriever.close()

if __name__ == "__main__":
    main() 