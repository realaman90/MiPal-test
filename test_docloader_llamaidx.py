import logging
import sys
from typing import Optional, Dict
from google.oauth2.credentials import Credentials
from llama_index.core import SummaryIndex
from llama_index.readers.google import GoogleDocsReader
from google.auth.transport.requests import Request
import json

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))

class CustomGoogleDocsReader(GoogleDocsReader):
    def __init__(self, credentials_dict: Dict):
        """
        Initialize the reader with credentials from database
        Args:
            credentials_dict: Dictionary containing OAuth2 credentials from database
        """
        # Convert the dictionary to Credentials object
        credentials = Credentials.from_authorized_user_info(
            credentials_dict,
            scopes=["https://www.googleapis.com/auth/documents.readonly"]
        )
        
        # Refresh token if expired
        if credentials.expired:
            credentials.refresh(Request())
            
            # Here you would typically update the refreshed credentials in your database
            # update_credentials_in_db(credentials.to_json())
        
        super().__init__(credentials=credentials)

def get_credentials_from_db() -> Dict:
    """
    Mock function to demonstrate getting credentials from database
    In real implementation, replace this with actual database query
    """
    # Example implementation - replace with your database logic
    try:
        # This is where you'd query your database
        # Example: credentials = db.query("SELECT credentials FROM user_credentials WHERE user_id = ?", [user_id])
        
        # For demonstration, reading from token.json
        with open("token.json", "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error fetching credentials from database: {e}")
        raise

def update_credentials_in_db(credentials_json: str):
    """
    Mock function to demonstrate updating credentials in database
    In real implementation, replace this with actual database update
    """
    # Example implementation - replace with your database logic
    try:
        # This is where you'd update your database
        # Example: db.execute("UPDATE user_credentials SET credentials = ? WHERE user_id = ?", 
        #                    [credentials_json, user_id])
        
        # For demonstration, writing to token.json
        with open("token.json", "w") as f:
            f.write(credentials_json)
    except Exception as e:
        logging.error(f"Error updating credentials in database: {e}")
        raise

def process_document(document_ids: list[str], credentials_dict: Optional[Dict] = None) -> str:
    """
    Process Google Docs with credentials from database
    """
    try:
        # If no credentials provided, fetch from database
        if credentials_dict is None:
            credentials_dict = get_credentials_from_db()
        
        # Initialize custom reader with credentials
        reader = CustomGoogleDocsReader(credentials_dict)
        
        # Load documents
        documents = reader.load_data(document_ids=document_ids)
        
        # Create index and query engine
        index = SummaryIndex.from_documents(documents)
        query_engine = index.as_query_engine()
        
        # Query the document
        response = query_engine.query("what is the content of the document?")
        
        return str(response)
        
    except Exception as e:
        logging.error(f"Error processing document: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    document_ids = ["1W52MVD8jZNVtYo_cTxf_t0PnZE0W8QNSHjTX3lSTG8E"]
    
    try:
        # Process document with credentials from database
        response = process_document(document_ids)
        print(response)
        
    except Exception as e:
        print(f"Error: {e}")
