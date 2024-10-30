from typing import Dict, List, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
from google_test.drive_document_summarizer import DriveDocumentSummarizer
import os
from dotenv import load_dotenv
from llama_index.core import Document, VectorStoreIndex, StorageContext
from llama_index.vector_stores.neo4jvector import Neo4jVectorStore
import openai
from llama_index.embeddings.openai import OpenAIEmbedding

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentGraphStore:
    def __init__(self):
        """Initialize Neo4j connection and vector store"""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        # Initialize OpenAI
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize Neo4j connections
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        
        # Initialize vector store for embeddings
        self.embed_dim = 1536  # OpenAI embedding dimension
        self.vector_store = Neo4jVectorStore(
            url=self.uri,
            username=self.user,
            password=self.password,
            embedding_dimension=self.embed_dim,  # This is the required parameter
            node_label="Document",              
            embedding_field="embedding",        
            text_field="summary",              
            keyword_field="title",             
            distance_metric="cosine"           # Changed from embedding_distance_metric
        )
        
        # Initialize embedding model
        self.embed_model = OpenAIEmbedding()

    def close(self):
        """Close the Neo4j driver connection"""
        self.driver.close()

    def _get_file_metadata(self, drive_service, file_id: str) -> Dict:
        """Get file metadata from Google Drive"""
        try:
            file = drive_service.files().get(
                fileId=file_id,
                fields='createdTime,modifiedTime,owners,lastModifyingUser,size,webViewLink'
            ).execute()
            
            return {
                'created_time': file.get('createdTime'),
                'modified_time': file.get('modifiedTime'),
                'owner_email': file.get('owners', [{}])[0].get('emailAddress', 'Unknown'),
                'last_modifier_email': file.get('lastModifyingUser', {}).get('emailAddress', 'Unknown'),
                'size': file.get('size', '0'),
                'web_view_link': file.get('webViewLink', '')
            }
        except Exception as e:
            logger.error(f"Error getting file metadata: {e}")
            return {}

    def _store_embedding_chunks(self, tx, doc_id: str, embedding: List[float], chunk_size: int = 500):
        """Store embedding in chunks"""
        query = """
        MATCH (d:Document {id: $doc_id})
        WITH d, $chunks as chunks
        UNWIND range(0, size(chunks)-1) as i
        CREATE (e:DocumentEmbedding {
            id: apoc.create.uuid(),
            chunk_index: i,
            total_chunks: size(chunks),
            embedding_chunk: chunks[i]
        })
        CREATE (d)-[:HAS_SUMMARY_EMBEDDING]->(e)
        """
        
        # Split embedding into chunks
        chunks = [embedding[i:i + chunk_size] for i in range(0, len(embedding), chunk_size)]
        
        tx.run(query, doc_id=doc_id, chunks=chunks)

    def _create_document_node(self, tx, user_id: str, doc_data: Dict):
        """Create document node and store embeddings separately"""
        # First create document node (without embedding)
        query = """
        MERGE (u:User {user_id: $user_id})
        CREATE (d:Document {
            id: apoc.create.uuid(),
            origin_source_id: $origin_id,
            mime_type: $mime_type,
            origin_source: $origin_source,
            title: $title,
            doc_type: $doc_type,
            summary: $summary,
            created_time: $created_time,
            modified_time: $modified_time,
            owner_email: $owner_email,
            last_modifier_email: $last_modifier_email,
            size: $size,
            web_view_link: $web_view_link,
            indexed_at: $indexed_at,
            summary_status: CASE 
                WHEN $summary IS NULL OR $summary = '' 
                THEN 'FAILED' 
                ELSE 'SUCCESS' 
            END
        })
        CREATE (u)-[:OWNS]->(d)
        WITH d
        MERGE (o:User {email: $owner_email})
        CREATE (o)-[:CREATED]->(d)
        WITH d
        MERGE (m:User {email: $last_modifier_email})
        CREATE (m)-[:LAST_MODIFIED]->(d)
        RETURN d
        """
        
        # Get document type from MIME type
        doc_type = next(
            (dt for dt, mt in DriveDocumentSummarizer.MIME_TYPES.items() 
             if mt == doc_data['mime_type']),
            'unknown'
        )
        
        # Create document
        result = tx.run(query, 
                       user_id=user_id,
                       origin_id=doc_data['id'],
                       mime_type=doc_data['mime_type'],
                       doc_type=doc_type,  # Added doc_type here
                       origin_source='google_drive',
                       title=doc_data['name'],
                       summary=doc_data['summary'],
                       created_time=doc_data['metadata'].get('created_time'),
                       modified_time=doc_data['metadata'].get('modified_time'),
                       owner_email=doc_data['metadata'].get('owner_email'),
                       last_modifier_email=doc_data['metadata'].get('last_modifier_email'),
                       size=doc_data['metadata'].get('size'),
                       web_view_link=doc_data['metadata'].get('web_view_link'),
                       indexed_at=datetime.now().isoformat())
        doc_node = result.single()['d']
        
        # Generate and store embedding if summary exists
        summary = doc_data.get('summary', '')
        if summary and len(summary.strip()) > 0:
            try:
                embedding = self.embed_model.get_text_embedding(summary)
                self._store_embedding_chunks(tx, doc_node['id'], embedding)
            except Exception as e:
                logger.error(f"Error generating/storing embedding: {e}")
        
        return doc_node

    def store_user_documents(self, user_id: str, summarizer: DriveDocumentSummarizer):
        """Store all documents for a user in Neo4j"""
        try:
            # Get summaries from DriveDocumentSummarizer with user_id
            summaries = summarizer.summarize_all_files(user_id)  # Pass user_id here
            
            with self.driver.session() as session:
                for doc_type, documents in summaries.items():
                    for doc in documents:
                        # Add mime_type to the document data
                        doc['mime_type'] = summarizer.MIME_TYPES[doc_type]
                        
                        # Get file metadata
                        metadata = self._get_file_metadata(
                            summarizer.drive_service, 
                            doc['id']
                        )
                        doc['metadata'] = metadata
                        
                        # Create document node
                        session.execute_write(self._create_document_node, user_id, doc)
                        logger.info(f"Created document node for {doc['name']}")
                
            return True
        
        except Exception as e:
            logger.error(f"Error storing documents: {e}")
            return False

    def get_user_documents(self, user_id: str) -> List[Dict]:
        """Retrieve all documents for a user from Neo4j"""
        query = """
        MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
        OPTIONAL MATCH (creator)-[:CREATED]->(d)
        OPTIONAL MATCH (modifier)-[:LAST_MODIFIED]->(d)
        RETURN d, creator.email as creator_email, modifier.email as modifier_email
        ORDER BY d.modified_time DESC
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, user_id=user_id)
                documents = []
                for record in result:
                    doc = dict(record["d"])
                    if 'name' not in doc and 'title' in doc:
                        doc['name'] = doc['title']
                    doc['creator_email'] = record["creator_email"]
                    doc['modifier_email'] = record["modifier_email"]
                    documents.append(doc)
                return documents
        
        except Exception as e:
            logger.error(f"Error retrieving documents: {e}")
            return []

    def get_document_history(self, doc_id: str) -> Dict:
        """Get document modification history"""
        query = """
        MATCH (d:Document {origin_source_id: $doc_id})
        OPTIONAL MATCH (creator)-[:CREATED]->(d)
        OPTIONAL MATCH (modifier)-[:LAST_MODIFIED]->(d)
        RETURN d, creator.email as creator_email, modifier.email as modifier_email
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, doc_id=doc_id).single()
                if result:
                    doc = dict(result["d"])
                    doc['creator_email'] = result["creator_email"]
                    doc['modifier_email'] = result["modifier_email"]
                    return doc
                return None
        
        except Exception as e:
            logger.error(f"Error retrieving document history: {e}")
            return None

    def search_similar_documents(self, user_id: str, query_text: str, limit: int = 5) -> List[Dict]:
        """Search using chunked embeddings"""
        try:
            query_embedding = self.embed_model.get_text_embedding(query_text)
            
            # Modified query to work with chunked embeddings
            query = """
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
            WITH d, e, gds.similarity.cosine(e.embedding_chunk, $query_embedding) AS chunk_score
            WITH d, avg(chunk_score) as avg_score  // Average scores across chunks
            WHERE avg_score > 0.7
            RETURN d.title as title, d.doc_type as type, d.summary as summary,
                   d.created_time as created_time, avg_score as score
            ORDER BY score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    query,
                    user_id=user_id,
                    query_embedding=query_embedding,
                    limit=limit
                )
                
                return [{
                    'title': record['title'],
                    'type': record['type'],
                    'summary': record['summary'],
                    'created_time': record['created_time'],
                    'similarity_score': record['score']
                } for record in result]
                
        except Exception as e:
            logger.error(f"Error searching similar documents: {e}")
            return []

    def update_embeddings(self, user_id: str = None) -> Dict:
        """
        Update embeddings for all documents or specific user's documents
        
        Args:
            user_id: Optional user identifier to update specific user's documents
            
        Returns:
            Dictionary containing update status
        """
        try:
            # Query to get documents without embeddings
            query = """
            MATCH (d:Document)
            WHERE d.embedding IS NULL AND d.summary IS NOT NULL
            {}
            RETURN d.id as id, d.summary as summary
            """.format("AND EXISTS((u:User {user_id: $user_id})-[:OWNS]->(d))" if user_id else "")
            
            updated = 0
            failed = 0
            
            with self.driver.session() as session:
                # Get documents needing embeddings
                docs = session.run(query, user_id=user_id if user_id else None)
                
                # Update each document
                for doc in docs:
                    try:
                        # Generate embedding
                        embedding = self.embed_model.get_text_embedding(doc['summary'])
                        
                        # Update document
                        session.run(
                            """
                            MATCH (d:Document {id: $id})
                            SET d.embedding = $embedding
                            """,
                            id=doc['id'],
                            embedding=embedding
                        )
                        updated += 1
                        
                    except Exception as e:
                        logger.error(f"Error updating embedding for document {doc['id']}: {e}")
                        failed += 1
            
            return {
                'status': 'success',
                'updated': updated,
                'failed': failed,
                'message': f'Updated {updated} documents, {failed} failed'
            }
            
        except Exception as e:
            logger.error(f"Error updating embeddings: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

def main():
    """Test document storage and similarity search"""
    try:
        doc_store = DocumentGraphStore()
        test_user_id = "aman"
        
        # Test similarity search
        print("\nTesting similarity search...")
        query = "machine learning documents"
        similar_docs = doc_store.search_similar_documents(test_user_id, query)
        
        print(f"\nResults for query: '{query}'")
        for doc in similar_docs:
            print(f"\nTitle: {doc['title']}")
            print(f"Type: {doc['type']}")
            print(f"Score: {doc['similarity_score']:.3f}")
            print(f"Summary: {doc['summary'][:200]}...")
            print("-" * 80)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        doc_store.close()

if __name__ == "__main__":
    main() 