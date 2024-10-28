import logging
import sys
from typing import Dict
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import json
from llama_index.readers.google import GoogleDocsReader

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

def list_docs_and_sheets():
    """List all Google Docs and Spreadsheets in Drive"""
    try:
        # Get credentials from token.json
        with open("token.json", "r") as f:
            creds_dict = json.load(f)
        
        credentials = Credentials.from_authorized_user_info(
            creds_dict,
            scopes=[
                "https://www.googleapis.com/auth/drive.metadata.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/documents.readonly",
                "https://www.googleapis.com/auth/spreadsheets.readonly"
            ]
        )
        
        if credentials.expired:
            credentials.refresh(Request())

        # Build the Drive API service
        drive_service = build('drive', 'v3', credentials=credentials)
        docs_reader = GoogleDocsReader(credentials=credentials)
        
        # List files with Google's native MIME types
        query = "mimeType='application/vnd.google-apps.document' or mimeType='application/vnd.google-apps.spreadsheet'"
        
        print("\nQuerying Google Drive...")
        results = drive_service.files().list(
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType)",
            q=query
        ).execute()
        
        items = results.get('files', [])
        
        if not items:
            print("No Google Docs or Spreadsheets found.")
            
            # Let's list all files to debug
            print("\nListing all files in Drive to debug:")
            all_files = drive_service.files().list(
                pageSize=10,  # Limit to 10 files for debugging
                fields="files(id, name, mimeType)"
            ).execute().get('files', [])
            
            for file in all_files:
                print(f"Name: {file['name']}")
                print(f"MIME Type: {file['mimeType']}")
                print(f"ID: {file['id']}")
                print("-" * 50)
            return
        
        print("\nGoogle Docs:")
        print("-" * 50)
        doc_ids = []
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.document':
                print(f"Name: {item['name']}")
                print(f"ID: {item['id']}")
                print(f"MIME Type: {item['mimeType']}")
                print("-" * 50)
                doc_ids.append(item['id'])
        
        print("\nGoogle Spreadsheets:")
        print("-" * 50)
        sheet_ids = []
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                print(f"Name: {item['name']}")
                print(f"ID: {item['id']}")
                print(f"MIME Type: {item['mimeType']}")
                print("-" * 50)
                sheet_ids.append(item['id'])

        # Try to read the first document
        if doc_ids:
            print("\nTrying to read the first document:")
            try:
                documents = docs_reader.load_data(document_ids=[doc_ids[0]])
                if documents:
                    print(f"Successfully read document content. Length: {len(documents[0].text)}")
                    print("First 500 characters:")
                    print(documents[0].text[:500])
                else:
                    print("No content found in document")
            except Exception as e:
                print(f"Error reading document: {e}")

    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise

if __name__ == "__main__":
    list_docs_and_sheets()
