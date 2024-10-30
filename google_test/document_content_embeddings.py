from typing import Dict, List, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import openai
from llama_index.embeddings.openai import OpenAIEmbedding
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import tempfile
import base64
from docx import Document as DocxDocument
from pptx import Presentation
import PyPDF2
from neo4j_test.user_store import UserStore

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentContentEmbedding:
    SUPPORTED_TYPES = {
        'document': 'application/vnd.google-apps.document',
        'presentation': 'application/vnd.google-apps.presentation',
        'pdf': 'application/pdf',
        'word': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'powerpoint': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'image': 'image/jpeg',
        'image_png': 'image/png'
    }

    def __init__(self):
        """Initialize connections and services"""
        # Neo4j setup
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        
        # OpenAI setup
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize embedding model
        self.embed_model = OpenAIEmbedding()
        
        # Initialize UserStore for Google credentials
        self.user_store = UserStore()

    def close(self):
        """Close connections"""
        self.driver.close()
        self.user_store.close()

    def _get_google_services(self, user_id: str):
        """Initialize Google services for a user"""
        try:
            # Get credentials from Neo4j using UserStore
            credentials = self.user_store.get_google_credentials(user_id)
            if not credentials:
                raise ValueError(f"No valid Google credentials found for user {user_id}")
            
            # Build services
            drive_service = build('drive', 'v3', credentials=credentials)
            docs_service = build('docs', 'v1', credentials=credentials)
            slides_service = build('slides', 'v1', credentials=credentials)
            
            return drive_service, docs_service, slides_service
            
        except Exception as e:
            logger.error(f"Error getting Google services: {e}")
            raise

    def _extract_google_doc_content(self, docs_service, doc_id: str) -> str:
        """Extract content from Google Doc"""
        try:
            doc = docs_service.documents().get(documentId=doc_id).execute()
            content = ""
            
            for element in doc.get('body', {}).get('content', []):
                if 'paragraph' in element:
                    for para_element in element['paragraph']['elements']:
                        if 'textRun' in para_element:
                            content += para_element['textRun'].get('content', '')
            
            return content
        except Exception as e:
            logger.error(f"Error extracting Google Doc content: {e}")
            return ""

    def create_content_embedding(self, user_id: str, doc_id: str, doc_type: str) -> bool:
        """Create and store content embedding for a document"""
        temp_file = None
        try:
            if doc_type not in self.SUPPORTED_TYPES:
                raise ValueError(f"Unsupported document type: {doc_type}")
            
            # Get Google services
            drive_service, docs_service, slides_service = self._get_google_services(user_id)
            
            # Extract content based on document type
            content = ""
            if doc_type == 'document':
                content = self._extract_google_doc_content(docs_service, doc_id)
            else:
                # Handle other document types...
                pass
            
            if not content.strip():
                logger.warning(f"No content extracted from document {doc_id}")
                return False
            
            # Generate embedding
            content_embedding = self.embed_model.get_text_embedding(content)
            
            # Store content and embedding
            with self.driver.session() as session:
                session.execute_write(
                    self._store_content_embedding,
                    doc_id,
                    content,
                    content_embedding
                )
            
            logger.info(f"Successfully created content embedding for document {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating content embedding: {e}")
            return False

    def _store_content_embedding(self, tx, doc_id: str, content: str, content_embedding: List[float]):
        """Store content and its embedding in Neo4j"""
        query = """
        MATCH (d:Document {origin_source_id: $doc_id})
        
        // Store content and full embedding
        SET d.content = $content,
            d.content_embedding = $embedding,
            d.content_embedding_created = datetime()
        
        // Create chunked embeddings
        WITH d, $embedding as full_embedding, 500 as chunk_size
        UNWIND range(0, size(full_embedding)-1, chunk_size) as start_idx
        WITH d, 
             start_idx,
             CASE 
                 WHEN start_idx + 500 > size(full_embedding)
                 THEN full_embedding[start_idx..]
                 ELSE full_embedding[start_idx..start_idx + 500]
             END as chunk,
             size(full_embedding) as total_size,
             chunk_size
        
        // Create embedding chunk node
        CREATE (e:DocumentEmbedding {
            id: apoc.create.uuid(),
            chunk_index: start_idx / chunk_size,
            total_chunks: ceil(toFloat(total_size) / chunk_size),
            embedding_chunk: chunk
        })
        CREATE (d)-[:HAS_CONTENT_EMBEDDING]->(e)
        """
        
        tx.run(query, 
               doc_id=doc_id,
               content=content,
               embedding=content_embedding)

def main():
    """Test content embedding creation"""
    try:
        embedder = DocumentContentEmbedding()
        test_user_id = "dev"
        
        # Test documents
        test_docs = [
            {
                'id': '1sM18dltbJSMHLA6TFP6yUo7M9aF1DNWtk_SWIl2_T94',
                'type': 'document'
            }
        ]
        
        for doc in test_docs:
            print(f"\nProcessing document: {doc['id']}")
            success = embedder.create_content_embedding(
                test_user_id,
                doc['id'],
                doc['type']
            )
            print(f"Embedding creation {'successful' if success else 'failed'}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        embedder.close()

if __name__ == "__main__":
    main() 