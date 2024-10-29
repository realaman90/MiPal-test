from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import RedirectResponse
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
import json
import os
from typing import Dict
from datetime import datetime

from neo4j_test.user_store import UserStore
from google_test.google_auth_manager import GoogleAuthManager

router = APIRouter(prefix="/api/auth/google", tags=["google"])

# Initialize managers
auth_manager = GoogleAuthManager()
user_store = UserStore()

# Load client secrets
CLIENT_SECRETS_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "credentials.json")

# Configure OAuth2 flow
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/documents.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]

@router.get("/url")
async def get_authorization_url(user_id: str):
    """
    Generate Google OAuth2 authorization URL
    
    Args:
        user_id: User identifier
        
    Returns:
        Dict containing authorization URL and state
    """
    try:
        # Create flow instance
        flow = Flow.from_client_secrets_file(
            CLIENT_SECRETS_FILE,
            scopes=SCOPES,
            redirect_uri="http://localhost:8000/api/auth/google/callback"  # Update with your redirect URI
        )
        
        # Generate authorization URL
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        
        # Store state and user_id mapping (you might want to use Redis or similar)
        state_data = {
            'user_id': user_id,
            'timestamp': datetime.now().isoformat()
        }
        auth_manager.save_credentials(state, state_data)  # Reusing save_credentials method
        
        return {
            "authorization_url": authorization_url,
            "state": state
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/callback")
async def oauth2_callback(code: str, state: str, error: str = None):
    """
    Handle OAuth2 callback from Google
    
    Args:
        code: Authorization code
        state: State parameter for security
        error: Error message if any
        
    Returns:
        Redirect to frontend with success/error message
    """
    try:
        if error:
            return RedirectResponse(
                url=f"http://localhost:3000/settings?error={error}"  # Update with your frontend URL
            )
        
        # Get user_id from state
        state_data = auth_manager.load_credentials(state)
        if not state_data:
            raise HTTPException(status_code=400, detail="Invalid state parameter")
        
        user_id = state_data['user_id']
        
        # Create flow instance
        flow = Flow.from_client_secrets_file(
            CLIENT_SECRETS_FILE,
            scopes=SCOPES,
            redirect_uri="http://localhost:8000/api/auth/google/callback"
        )
        
        # Exchange code for credentials
        flow.fetch_token(code=code)
        credentials = flow.credentials
        
        # Convert credentials to dict
        credentials_dict = {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes
        }
        
        # Store credentials in Neo4j
        result = user_store.update_integration_status(
            user_id=user_id,
            provider='google_drive',
            status_data={
                'enabled': True,
                'last_connected': datetime.now().isoformat(),
                'token': credentials_dict
            }
        )
        
        if result:
            return RedirectResponse(
                url="http://localhost:3000/settings?success=true"
            )
        else:
            return RedirectResponse(
                url="http://localhost:3000/settings?error=failed_to_store_credentials"
            )
            
    except Exception as e:
        return RedirectResponse(
            url=f"http://localhost:3000/settings?error={str(e)}"
        )

@router.get("/status/{user_id}")
async def get_integration_status(user_id: str):
    """
    Get Google Drive integration status for a user
    
    Args:
        user_id: User identifier
        
    Returns:
        Dict containing integration status
    """
    try:
        credentials = user_store.get_google_credentials(user_id)
        if credentials:
            return {
                "status": "connected",
                "last_checked": datetime.now().isoformat()
            }
        return {
            "status": "disconnected"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/disconnect/{user_id}")
async def disconnect_integration(user_id: str):
    """
    Disconnect Google Drive integration for a user
    
    Args:
        user_id: User identifier
        
    Returns:
        Dict containing operation status
    """
    try:
        result = user_store.update_integration_status(
            user_id=user_id,
            provider='google_drive',
            status_data={
                'enabled': False,
                'token': None,
                'last_disconnected': datetime.now().isoformat()
            }
        )
        
        if result:
            return {
                "status": "success",
                "message": "Successfully disconnected Google Drive integration"
            }
        return {
            "status": "error",
            "message": "Failed to disconnect integration"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 