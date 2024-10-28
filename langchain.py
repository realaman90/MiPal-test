import os
import json
import logging
from typing import Any, List

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from llama_index.core.schema import Document

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Docs API scopes (reuse from the first code)
SCOPES = ["https://www.googleapis.com/auth/documents.readonly"]

def load_credentials() -> Credentials:
    """Load Google API credentials from token.json."""
    print("Loading credentials...")
    creds = None
    if os.path.exists("token.json"):
        print("token.json found, loading credentials from file.")
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    else:
        print("token.json not found, starting new authorization flow.")
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired credentials.")
            creds.refresh(Request())
        else:
            print("No valid credentials, starting authorization flow.")
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    print("Credentials loaded successfully.")
    return creds

class GoogleDocsReader:
    """Google Docs reader for loading document content based on provided document IDs."""

    def __init__(self):
        print("Initializing GoogleDocsReader...")
        self.credentials = load_credentials()
        print("Building Google Docs API service...")
        self.docs_service = build("docs", "v1", credentials=self.credentials)
        print("Google Docs API service created successfully.")

    def load_data(self, document_ids: List[str]) -> List[Document]:
        """Load content from Google Docs based on document IDs."""
        print("Starting to load data from Google Docs...")
        results = []
        for document_id in document_ids:
            print(f"Loading document with ID: {document_id}")
            try:
                google_doc = self.docs_service.documents().get(documentId=document_id).execute()
                print(f"Document {document_id} loaded successfully.")
                doc_content = self._parse_content(google_doc.get("body").get("content"))
                metadata = {"document_id": document_id}
                results.append(Document(text=doc_content, metadata=metadata))
            except Exception as e:
                print(f"Error loading document {document_id}: {e}")
        print("Finished loading data from Google Docs.")
        return results

    def _parse_content(self, elements: List[Any]) -> str:
        """Parse the document's structural elements into a text string."""
        print("Parsing document content...")
        text = ""
        for element in elements:
            if "paragraph" in element:
                for elem in element.get("paragraph").get("elements"):
                    text += elem.get("textRun", {}).get("content", "")
        print("Document content parsed successfully.")
        return text

if __name__ == "__main__":
    # Example: Provide a list of document IDs from Google Drive
    document_ids = [
        "1d2q8-9EXpm__pTx1u7yaIQvQTtomr7ITNnl3FbvDhZ8",
        "1GW2AjMu4w3DJFglnJXIi-r5GyQQgxuU0yRYQCrp_0Zc",
        # Add other document IDs here
    ]
    
    # Initialize the GoogleDocsReader and load the documents
    print("Initializing GoogleDocsReader instance.")
    reader = GoogleDocsReader()
    print("Loading documents...")
    documents = reader.load_data(document_ids)

    # Display document content and metadata
    for doc in documents:
        print(f"\nDocument ID: {doc.metadata['document_id']}")
        print(f"Content Snippet: {doc.text[:200]}...")  # Show the first 200 characters of content




#oogle Docs files don't have "binary content" for a typical download. 
# Instead, they need to be exported to a compatible format like .txt or .pdf to be read.