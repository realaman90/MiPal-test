import logging
from neo4j_test.user_store import UserStore
from google_test.drive_document_summarizer import DriveDocumentSummarizer
from google_test.document_graph_store import DocumentGraphStore
import json
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Test the complete document flow"""
    user_store = None
    doc_store = None
    
    try:
        # Initialize components
        user_store = UserStore()
        doc_store = DocumentGraphStore()
        test_user_id = "dev"  # Use existing user ID
        
        # Get existing credentials
        print("\nFetching Google credentials...")
        credentials = user_store.get_google_credentials(test_user_id)
        if not credentials:
            logger.error("No valid credentials found. Please set up Google integration first.")
            return
            
        # Convert credentials to dict properly
        credentials_dict = {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes
        }
        
        # Initialize summarizer with credentials
        print("\nInitializing document summarizer...")
        summarizer = DriveDocumentSummarizer(
            credentials_dict=credentials_dict,
            config={
                'test_mode': True,
                'test_file_limit': 4,
                'max_files_per_type': 3,
                'max_total_files': 80
            }
        )
        
        # Check drive statistics
        print("\nChecking drive statistics...")
        stats = summarizer.check_drive_stats(test_user_id)
        print(f"Total files: {stats.get('total_files', 'N/A')}")
        print(f"Storage used: {stats.get('total_size_human', 'N/A')}")
        
        # Store documents and summaries
        print("\nStoring documents and summaries...")
        success = doc_store.store_user_documents(test_user_id, summarizer)
        
        if success:
            # Retrieve stored documents
            print("\nRetrieving stored documents...")
            documents = doc_store.get_user_documents(test_user_id)
            
            print("\nStored Documents:")
            for doc in documents:
                print(f"\nDocument: {doc['name']}")
                print(f"Created: {doc.get('created_time', 'N/A')}")
                print(f"Modified: {doc.get('modified_time', 'N/A')}")
                print(f"Owner: {doc.get('owner_email', 'N/A')}")
                print(f"Size: {doc.get('size', '0')} bytes")
                print(f"Summary: {doc.get('summary', '')[:200]}...")
                print("-" * 80)
        else:
            print("Failed to store documents")
        
    except Exception as e:
        logger.error(f"Error in document flow: {e}")
        raise
    
    finally:
        if user_store:
            user_store.close()
        if doc_store:
            doc_store.close()

if __name__ == "__main__":
    main() 