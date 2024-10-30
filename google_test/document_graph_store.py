from typing import Dict, List, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
from google_test.drive_document_summarizer import DriveDocumentSummarizer
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentGraphStore:
    def __init__(self):
        """Initialize Neo4j connection using environment variables"""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

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

    def _create_document_node(self, tx, user_id: str, doc_data: Dict):
        """Create a document node in Neo4j"""
        query = """
        MERGE (u:User {user_id: $user_id})
        CREATE (d:Document {
            id: apoc.create.uuid(),  // Generate unique ID
            origin_source_id: $origin_id,
            mime_type: $mime_type,
            origin_source: $origin_source,
            title: $title,
            doc_type: $doc_type,
            summary: CASE 
                WHEN $summary IS NULL OR $summary = '' 
                THEN 'Summary generation failed or not supported for this document type' 
                ELSE $summary 
            END,
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
        
        result = tx.run(query, 
                       user_id=user_id,
                       origin_id=doc_data['id'],
                       mime_type=doc_data['mime_type'],
                       doc_type=doc_type,
                       origin_source='google_drive',
                       title=doc_data['name'],
                       summary=doc_data.get('summary', ''),
                       created_time=doc_data['metadata'].get('created_time'),
                       modified_time=doc_data['metadata'].get('modified_time'),
                       owner_email=doc_data['metadata'].get('owner_email'),
                       last_modifier_email=doc_data['metadata'].get('last_modifier_email'),
                       size=doc_data['metadata'].get('size'),
                       web_view_link=doc_data['metadata'].get('web_view_link'),
                       indexed_at=datetime.now().isoformat())
        return result

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

    def _store_documents(self, tx, user_id: str, documents: Dict[str, List[Dict]]):
        """Store documents and their summaries in Neo4j"""
        query = """
        MATCH (u:User {user_id: $user_id})
        WITH u
        UNWIND $documents as doc
        MERGE (d:Document {id: doc.id})
        SET 
            d.name = doc.name,
            d.mime_type = doc.mime_type,
            d.summary = doc.summary,
            d.created_time = doc.created_time,
            d.modified_time = doc.modified_time,
            d.owner_email = doc.owner_email,
            d.size = doc.size,
            d.summary_status = doc.summary_status
        MERGE (u)-[r:OWNS]->(d)
        RETURN d
        """
        
        # Prepare documents for storage
        flat_documents = []
        for doc_type, docs in documents.items():
            for doc in docs:
                flat_documents.append({
                    'id': doc['id'],  # Use the existing ID from Google Drive
                    'name': doc['name'],
                    'mime_type': doc['mime_type'],
                    'summary': doc.get('summary', ''),
                    'created_time': doc.get('created_time', ''),
                    'modified_time': doc.get('modified_time', ''),
                    'owner_email': doc.get('owner_email', ''),
                    'size': doc.get('size', 0),
                    'summary_status': doc.get('summary_status', 'UNKNOWN')
                })
        
        result = tx.run(
            query,
            user_id=user_id,
            documents=flat_documents
        )
        return result

def main():
    """Example usage of DocumentGraphStore"""
    try:
        # Initialize the document store
        doc_store = DocumentGraphStore()
        
        # Initialize the summarizer
        summarizer = DriveDocumentSummarizer()
        
        # Store documents for a test user
        test_user_id = "test_user_123"
        success = doc_store.store_user_documents(test_user_id, summarizer)
        
        if success:
            # Retrieve and print the stored documents
            documents = doc_store.get_user_documents(test_user_id)
            for doc in documents:
                print(f"\nDocument Title: {doc['title']}")
                print(f"Created: {doc['created_time']}")
                print(f"Modified: {doc['modified_time']}")
                print(f"Owner: {doc['owner_email']}")
                print(f"Last modified by: {doc['last_modifier_email']}")
                print(f"Size: {doc['size']} bytes")
                print(f"Web View: {doc['web_view_link']}")
                print(f"Summary: {doc['summary'][:200]}...")
                print("-" * 80)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
    
    finally:
        doc_store.close()

if __name__ == "__main__":
    main() 