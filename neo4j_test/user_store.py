from typing import Dict, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
import json
import os
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UserStore:
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

    def _create_or_update_user(self, tx, user_data: Dict):
        """Create or update a user node in Neo4j"""
        query = """
        MERGE (u:User {user_id: $user_id})
        SET 
            u.email = $email,
            u.name = $name,
            u.role = $role,
            u.department = $department,
            u.integrations = $integrations,
            u.created_at = CASE
                WHEN u.created_at IS NULL THEN $current_time
                ELSE u.created_at
            END,
            u.updated_at = $current_time,
            u.last_login = $current_time
        WITH u
        
        // Create or merge company node and relationship
        MERGE (c:Company {name: $company_name})
        SET 
            c.industry = $company_industry,
            c.size = $company_size,
            c.created_at = CASE
                WHEN c.created_at IS NULL THEN $current_time
                ELSE c.created_at
            END,
            c.updated_at = $current_time
            
        // Create relationship between user and company
        MERGE (u)-[r:WORKS_FOR]->(c)
        SET r.role = $role,
            r.department = $department,
            r.joined_at = CASE
                WHEN r.joined_at IS NULL THEN $current_time
                ELSE r.joined_at
            END
            
        RETURN u, c, r
        """
        
        current_time = datetime.now().isoformat()
        integrations_str = json.dumps(user_data.get('integrations', {}))
        company_data = user_data.get('company', {})
        if isinstance(company_data, str):
            company_data = {'name': company_data}
        
        result = tx.run(
            query,
            user_id=user_data['user_id'],
            email=user_data.get('email'),
            name=user_data.get('name'),
            role=user_data.get('role'),
            department=user_data.get('department'),
            integrations=integrations_str,
            company_name=company_data.get('name'),
            company_industry=company_data.get('industry'),
            company_size=company_data.get('size'),
            current_time=current_time
        )
        return result.single()

    def create_or_update_user(self, user_data: Dict) -> Optional[Dict]:
        """Create or update a user in the database"""
        try:
            with self.driver.session() as session:
                result = session.execute_write(self._create_or_update_user, user_data)
                if result:
                    user_node = result['u']
                    company_node = result['c']
                    relationship = result['r']
                    
                    integrations = json.loads(user_node['integrations']) if user_node['integrations'] else {}
                    
                    return {
                        'user_id': user_node['user_id'],
                        'email': user_node['email'],
                        'name': user_node['name'],
                        'role': relationship['role'],
                        'department': relationship['department'],
                        'company': {
                            'name': company_node['name'],
                            'industry': company_node.get('industry'),
                            'size': company_node.get('size')
                        },
                        'joined_at': relationship['joined_at'],
                        'integrations': integrations,
                        'created_at': user_node['created_at'],
                        'updated_at': user_node['updated_at'],
                        'last_login': user_node['last_login']
                    }
                return None
        except Exception as e:
            logger.error(f"Error creating/updating user: {e}")
            raise

    def _update_google_integration(self, tx, user_id: str, credentials_dict: Dict):
        """Update Google integration status and credentials in Neo4j"""
        query = """
        MATCH (u:User {user_id: $user_id})
        WITH u, u.integrations as current_integrations
        SET u.integrations = $new_integrations,
            u.updated_at = $current_time
        RETURN u
        """
        
        current_result = tx.run(
            "MATCH (u:User {user_id: $user_id}) RETURN u.integrations",
            user_id=user_id
        ).single()
        
        if not current_result:
            raise ValueError(f"User {user_id} not found")
            
        current_integrations = json.loads(current_result['u.integrations'] or '{}')
        current_integrations['google_drive'] = {
            'enabled': True,
            'last_connected': datetime.now().isoformat(),
            'token': credentials_dict,
            'scopes': [
                "https://www.googleapis.com/auth/drive.metadata.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/documents.readonly",
                "https://www.googleapis.com/auth/spreadsheets.readonly"
            ]
        }
        
        result = tx.run(
            query,
            user_id=user_id,
            new_integrations=json.dumps(current_integrations),
            current_time=datetime.now().isoformat()
        )
        return result.single()
    
    #to be checked with frontend team this function is for backend only

    def setup_google_integration(self, user_id: str, client_secrets_file: str) -> Dict:
        """Set up Google Drive integration for a user"""
        try:
            SCOPES = [
                "https://www.googleapis.com/auth/drive.metadata.readonly",
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/documents.readonly",
                "https://www.googleapis.com/auth/spreadsheets.readonly"
            ]
            
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file, SCOPES)
            credentials = flow.run_local_server(port=0)
            
            credentials_dict = {
                'token': credentials.token,
                'refresh_token': credentials.refresh_token,
                'token_uri': credentials.token_uri,
                'client_id': credentials.client_id,
                'client_secret': credentials.client_secret,
                'scopes': credentials.scopes
            }
            
            with self.driver.session() as session:
                result = session.execute_write(
                    self._update_google_integration,
                    user_id,
                    credentials_dict
                )
                
                if result:
                    logger.info(f"Successfully set up Google integration for user {user_id}")
                    return {
                        'status': 'success',
                        'message': 'Google Drive integration successful',
                        'user_id': user_id,
                        'integration_data': json.loads(result['u']['integrations'])['google_drive']
                    }
                
            return {
                'status': 'error',
                'message': 'Failed to store integration data'
            }
            
        except Exception as e:
            logger.error(f"Error setting up Google integration: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

    def get_google_credentials(self, user_id: str) -> Optional[Credentials]:
        """Retrieve and refresh Google credentials for a user"""
        try:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (u:User {user_id: $user_id}) RETURN u.integrations",
                    user_id=user_id
                ).single()
                
                if not result:
                    return None
                
                integrations = json.loads(result['u.integrations'])
                if 'google_drive' not in integrations:
                    return None
                
                creds_dict = integrations['google_drive']['token']
                credentials = Credentials.from_authorized_user_info(
                    creds_dict,
                    integrations['google_drive']['scopes']
                )
                
                if credentials.expired:
                    credentials.refresh(Request())
                    new_creds_dict = {
                        'token': credentials.token,
                        'refresh_token': credentials.refresh_token,
                        'token_uri': credentials.token_uri,
                        'client_id': credentials.client_id,
                        'client_secret': credentials.client_secret,
                        'scopes': credentials.scopes
                    }
                    
                    self.update_integration_status(
                        user_id,
                        'google_drive',
                        {'token': new_creds_dict}
                    )
                
                return credentials
                
        except Exception as e:
            logger.error(f"Error getting Google credentials: {e}")
            return None

    def update_integration_status(self, user_id: str, provider: str, status_data: Dict) -> Optional[Dict]:
        """Update integration status for a user"""
        try:
            with self.driver.session() as session:
                current_result = session.run(
                    "MATCH (u:User {user_id: $user_id}) RETURN u.integrations",
                    user_id=user_id
                ).single()
                
                if not current_result:
                    raise ValueError(f"User {user_id} not found")
                    
                current_integrations = json.loads(current_result['u.integrations'] or '{}')
                if provider not in current_integrations:
                    current_integrations[provider] = {}
                
                current_integrations[provider].update(status_data)
                
                result = session.run(
                    """
                    MATCH (u:User {user_id: $user_id})
                    SET u.integrations = $new_integrations,
                        u.updated_at = $current_time
                    RETURN u
                    """,
                    user_id=user_id,
                    new_integrations=json.dumps(current_integrations),
                    current_time=datetime.now().isoformat()
                ).single()
                
                if result:
                    user_node = result['u']
                    integrations = json.loads(user_node['integrations']) if user_node['integrations'] else {}
                    return {
                        'user_id': user_node['user_id'],
                        'integrations': integrations,
                        'updated_at': user_node['updated_at']
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error updating integration status: {e}")
            raise

    def delete_user(self, user_id: str) -> Dict:
        """
        Delete a user and all their relationships from Neo4j
        
        Args:
            user_id: User identifier
            
        Returns:
            Dictionary containing status of deletion
        """
        try:
            query = """
            MATCH (u:User {user_id: $user_id})
            OPTIONAL MATCH (u)-[r1:WORKS_FOR]->(c:Company)
            OPTIONAL MATCH (u)-[r2:OWNS]->(d:Document)
            WITH u, r1, r2, d, c,
                 CASE WHEN NOT EXISTS((c)<-[:WORKS_FOR]-(:User)) THEN c ELSE null END as companyToDelete
            DETACH DELETE u, r1, r2, d
            WITH companyToDelete
            WHERE companyToDelete IS NOT NULL
            DELETE companyToDelete
            """
            
            with self.driver.session() as session:
                # First check if user exists
                check_query = "MATCH (u:User {user_id: $user_id}) RETURN u"
                user_exists = session.run(check_query, user_id=user_id).single()
                
                if not user_exists:
                    return {
                        'status': 'error',
                        'message': f'User {user_id} not found'
                    }
                
                # Execute deletion
                session.run(query, user_id=user_id)
                
                # Verify deletion
                verify = session.run(check_query, user_id=user_id).single()
                if verify is None:
                    return {
                        'status': 'success',
                        'message': f'User {user_id} and all related data successfully deleted'
                    }
                else:
                    return {
                        'status': 'error',
                        'message': f'Failed to delete user {user_id}'
                    }
                    
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

def main():
    """Test the UserStore functionality"""
    try:
        user_store = UserStore()
        
        test_user = {
            'user_id': 'aman',
            'email': 'aman@fastlane.ai',
            'name': 'Aman',
            'company': {
                'name': 'Fastlane',
                'industry': 'Technology',
                'size': '100-500'
            },
            'role': 'Admin',
            'department': 'R&D'
        }
        
        print("Creating test user...")
        created_user = user_store.create_or_update_user(test_user)
        print(f"Created user: {json.dumps(created_user, indent=2)}")
        
        print("\nSetting up Google integration...")
        client_secrets_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "credentials.json")
        
        if not os.path.exists(client_secrets_file):
            raise FileNotFoundError(f"credentials.json not found at {client_secrets_file}")
            
        integration_result = user_store.setup_google_integration(
            'aman',  # Use same user_id as created user
            client_secrets_file
        )
        print(f"Integration result: {json.dumps(integration_result, indent=2)}")
        
        print("\nTesting credentials retrieval...")
        credentials = user_store.get_google_credentials('aman')  # Use same user_id
        if credentials:
            print("Successfully retrieved and validated credentials")
            drive_service = build('drive', 'v3', credentials=credentials)
            print("Successfully created Drive service")
        
        # Test user deletion
        # print("\nTesting user deletion...")
        # deletion_result = user_store.delete_user('test123')
        # print(f"Deletion result: {json.dumps(deletion_result, indent=2)}")
        
        # # Verify deletion by trying to get credentials
        # print("\nVerifying deletion...")
        # deleted_user_creds = user_store.get_google_credentials('aman')
        # if deleted_user_creds is None:
        #     print("Verification successful: User and credentials were deleted")
        # else:
        #     print("Warning: User credentials still exist")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        user_store.close()

if __name__ == "__main__":
    main() 