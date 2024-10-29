from typing import Dict, Optional
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import json
import logging
import os
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleAuthManager:
    """Handles Google OAuth2 authentication and token management"""
    
    # Define required scopes for different Google services
    SCOPES = {
        'drive': [
            "https://www.googleapis.com/auth/drive.metadata.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/documents.readonly",
            "https://www.googleapis.com/auth/spreadsheets.readonly"
        ]
    }

    def __init__(self, credentials_dir: str = "credentials"):
        """
        Initialize GoogleAuthManager
        
        Args:
            credentials_dir: Directory to store credential files
        """
        self.credentials_dir = Path(credentials_dir)
        self.credentials_dir.mkdir(exist_ok=True)

    def get_credentials_path(self, user_id: str) -> Path:
        """Get path for user's credentials file"""
        return self.credentials_dir / f"{user_id}_token.json"

    def save_credentials(self, user_id: str, credentials: Dict) -> bool:
        """Save credentials to file"""
        try:
            creds_path = self.get_credentials_path(user_id)
            with open(creds_path, 'w') as f:
                json.dump(credentials, f)
            logger.info(f"Saved credentials for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error saving credentials: {e}")
            return False

    def load_credentials(self, user_id: str) -> Optional[Dict]:
        """Load credentials from file"""
        try:
            creds_path = self.get_credentials_path(user_id)
            if creds_path.exists():
                with open(creds_path, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            logger.error(f"Error loading credentials: {e}")
            return None

    def credentials_to_dict(self, credentials: Credentials) -> Dict:
        """Convert Google Credentials object to dictionary"""
        return {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes,
            'expiry': credentials.expiry.isoformat() if credentials.expiry else None
        }

    def dict_to_credentials(self, credentials_dict: Dict) -> Credentials:
        """Convert dictionary to Google Credentials object"""
        return Credentials.from_authorized_user_info(
            credentials_dict,
            credentials_dict['scopes']
        )

    def authenticate(self, user_id: str, client_secrets_file: str) -> Optional[Dict]:
        """
        Authenticate user with Google OAuth2
        
        Args:
            user_id: User identifier
            client_secrets_file: Path to client secrets file
            
        Returns:
            Dictionary containing credentials if successful, None otherwise
        """
        try:
            # Check for existing credentials
            creds_dict = self.load_credentials(user_id)
            if creds_dict:
                credentials = self.dict_to_credentials(creds_dict)
                
                # Refresh if expired
                if credentials.expired and credentials.refresh_token:
                    credentials.refresh(Request())
                    creds_dict = self.credentials_to_dict(credentials)
                    self.save_credentials(user_id, creds_dict)
                    
                return creds_dict

            # If no valid credentials exist, run OAuth2 flow
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file, 
                self.SCOPES['drive']
            )
            credentials = flow.run_local_server(port=0)
            
            # Save and return new credentials
            creds_dict = self.credentials_to_dict(credentials)
            self.save_credentials(user_id, creds_dict)
            return creds_dict

        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None

    def get_service(self, service_name: str, version: str, credentials_dict: Dict):
        """
        Get Google API service instance
        
        Args:
            service_name: Name of the Google service (e.g., 'drive', 'docs')
            version: API version
            credentials_dict: Dictionary containing credentials
            
        Returns:
            Google service instance
        """
        try:
            credentials = self.dict_to_credentials(credentials_dict)
            return build(service_name, version, credentials=credentials)
        except Exception as e:
            logger.error(f"Error creating service {service_name}: {e}")
            raise

def main():
    """Test Google authentication"""
    try:
        # Initialize auth manager
        auth_manager = GoogleAuthManager()
        
        # Test user authentication
        test_user_id = "test123"
        # Use absolute path to credentials.json
        client_secrets_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "credentials.json")
        
        if not os.path.exists(client_secrets_file):
            raise FileNotFoundError(f"credentials.json not found at {client_secrets_file}")
        
        # Authenticate user
        print(f"Authenticating user {test_user_id}...")
        creds_dict = auth_manager.authenticate(test_user_id, client_secrets_file)
        
        if creds_dict:
            print("Authentication successful!")
            
            # Test creating Drive service
            drive_service = auth_manager.get_service('drive', 'v3', creds_dict)
            
            # Test API call
            results = drive_service.files().list(
                pageSize=10,
                fields="nextPageToken, files(id, name)"
            ).execute()
            
            files = results.get('files', [])
            print("\nFiles found:")
            for file in files:
                print(f"- {file['name']} ({file['id']})")
                
        else:
            print("Authentication failed!")
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 