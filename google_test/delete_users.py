from typing import Dict, List
import logging
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UserCleaner:
    def __init__(self):
        """Initialize Neo4j connection"""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

    def list_users(self) -> List[Dict]:
        """List all users and their connected nodes"""
        query = """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:OWNS]->(d:Document)
        OPTIONAL MATCH (d)-[:HAS_EMBEDDING|HAS_CONTENT_EMBEDDING|HAS_SUMMARY_EMBEDDING]->(e)
        WITH u, 
             count(DISTINCT d) as doc_count,
             count(DISTINCT e) as embedding_count
        RETURN 
            u.user_id as user_id,
            u.email as email,
            u.role as role,
            u.department as department,
            doc_count,
            embedding_count,
            u.last_login as last_login
        ORDER BY u.last_login DESC
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [{
                    'user_id': record['user_id'],
                    'email': record['email'],
                    'role': record['role'],
                    'department': record['department'],
                    'documents': record['doc_count'],
                    'embeddings': record['embedding_count'],
                    'last_login': record['last_login']
                } for record in result]
        except Exception as e:
            logger.error(f"Error listing users: {e}")
            return []

    def delete_user(self, user_id: str, confirm: bool = True) -> Dict:
        """Delete a user and all connected nodes"""
        try:
            # First get user details and connected nodes
            count_query = """
            MATCH (u:User {user_id: $user_id})
            OPTIONAL MATCH (u)-[:OWNS]->(d:Document)
            OPTIONAL MATCH (d)-[:HAS_EMBEDDING|HAS_CONTENT_EMBEDDING|HAS_SUMMARY_EMBEDDING]->(e)
            OPTIONAL MATCH (u)-[:WORKS_FOR]->(c:Company)
            WITH u, d, e, c,
                 count(DISTINCT d) as doc_count,
                 count(DISTINCT e) as embedding_count,
                 count(DISTINCT c) as company_count
            RETURN u.email as email,
                   doc_count,
                   embedding_count,
                   company_count
            """
            
            with self.driver.session() as session:
                count_result = session.run(count_query, user_id=user_id).single()
                
                if not count_result:
                    return {
                        'status': 'error',
                        'message': f'User {user_id} not found'
                    }
                
                # Ask for confirmation
                if confirm:
                    print(f"\nWARNING: This will delete:")
                    print(f"- User: {user_id} ({count_result['email']})")
                    print(f"- {count_result['doc_count']} documents")
                    print(f"- {count_result['embedding_count']} embeddings")
                    print(f"- {count_result['company_count']} company relationships")
                    
                    confirmation = input("\nDo you want to continue? (yes/no): ").lower()
                    if confirmation != 'yes':
                        return {
                            'status': 'cancelled',
                            'message': 'Operation cancelled by user'
                        }
                
                # Delete user and all connected nodes
                delete_query = """
                // Match user and connected nodes
                MATCH (u:User {user_id: $user_id})
                
                // Match and collect all relationships and nodes to delete
                OPTIONAL MATCH (u)-[:OWNS]->(d:Document)
                OPTIONAL MATCH (d)-[r:HAS_EMBEDDING|HAS_CONTENT_EMBEDDING|HAS_SUMMARY_EMBEDDING]->(e:DocumentEmbedding)
                OPTIONAL MATCH (u)-[w:WORKS_FOR]->(c:Company)
                
                // Delete embeddings first
                WITH u, d, e, r, w, c
                WHERE e IS NOT NULL
                DELETE r, e
                
                // Delete documents
                WITH u, d, w, c
                WHERE d IS NOT NULL
                DELETE d
                
                // Delete company relationship
                WITH u, w, c
                WHERE w IS NOT NULL
                DELETE w
                
                // Delete orphaned company if no other users
                WITH u, c
                WHERE c IS NOT NULL
                AND NOT EXISTS((c)<-[:WORKS_FOR]-(:User))
                DELETE c
                
                // Finally delete user
                DELETE u
                """
                
                session.run(delete_query, user_id=user_id)
                
                # Verify deletion
                verify = session.run(
                    "MATCH (u:User {user_id: $user_id}) RETURN u",
                    user_id=user_id
                ).single()
                
                if verify is None:
                    return {
                        'status': 'success',
                        'message': f'Successfully deleted user {user_id} and all connected data'
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

    def delete_all_users(self, confirm: bool = True) -> Dict:
        """Delete all users and their data"""
        try:
            # First get total counts
            count_query = """
            MATCH (u:User)
            OPTIONAL MATCH (u)-[:OWNS]->(d:Document)
            OPTIONAL MATCH (d)-[:HAS_EMBEDDING|HAS_CONTENT_EMBEDDING|HAS_SUMMARY_EMBEDDING]->(e:DocumentEmbedding)
            OPTIONAL MATCH (u)-[:WORKS_FOR]->(c:Company)
            RETURN count(DISTINCT u) as user_count,
                   count(DISTINCT d) as doc_count,
                   count(DISTINCT e) as embedding_count,
                   count(DISTINCT c) as company_count
            """
            
            with self.driver.session() as session:
                counts = session.run(count_query).single()
                
                if not counts or counts['user_count'] == 0:
                    return {
                        'status': 'info',
                        'message': 'No users found in the database'
                    }
                
                # Ask for confirmation
                if confirm:
                    print(f"\nWARNING: This will delete ALL:")
                    print(f"- {counts['user_count']} users")
                    print(f"- {counts['doc_count']} documents")
                    print(f"- {counts['embedding_count']} embeddings")
                    print(f"- {counts['company_count']} companies")
                    
                    confirmation = input("\nDo you want to continue? (yes/no): ").lower()
                    if confirmation != 'yes':
                        return {
                            'status': 'cancelled',
                            'message': 'Operation cancelled by user'
                        }
                
                # Delete everything in the correct order
                delete_query = """
                // First delete all embedding relationships and nodes
                MATCH (e:DocumentEmbedding)
                DETACH DELETE e
                
                // Then delete all documents and their relationships
                WITH 1 as dummy
                MATCH (d:Document)
                DETACH DELETE d
                
                // Delete company relationships
                WITH 1 as dummy
                MATCH (u:User)-[r:WORKS_FOR]->(c:Company)
                DELETE r
                
                // Delete orphaned companies
                WITH 1 as dummy
                MATCH (c:Company)
                WHERE NOT EXISTS((c)<-[:WORKS_FOR]-(:User))
                DELETE c
                
                // Finally delete all users and any remaining relationships
                WITH 1 as dummy
                MATCH (u:User)
                DETACH DELETE u
                """
                
                session.run(delete_query)
                
                # Verify deletion
                verify = session.run("MATCH (u:User) RETURN count(u) as count").single()
                
                if verify['count'] == 0:
                    return {
                        'status': 'success',
                        'message': 'Successfully deleted all users and related data'
                    }
                else:
                    return {
                        'status': 'error',
                        'message': f"{verify['count']} users remain after deletion attempt"
                    }
                    
        except Exception as e:
            logger.error(f"Error deleting all users: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

def main():
    """Command line interface for user deletion"""
    cleaner = UserCleaner()
    
    try:
        # First list all users
        print("\nCurrent users in database:")
        users = cleaner.list_users()
        
        if not users:
            print("No users found in database")
            return
        
        for user in users:
            print(f"\nUser ID: {user['user_id']}")
            print(f"Email: {user['email']}")
            print(f"Role: {user['role']}")
            print(f"Documents: {user['documents']}")
            print(f"Embeddings: {user['embeddings']}")
            print(f"Last Login: {user['last_login']}")
            print("-" * 50)
        
        # Ask what to do
        print("\nOptions:")
        print("1. Delete specific user")
        print("2. Delete all users")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ")
        
        if choice == '1':
            user_id = input("\nEnter user ID to delete: ")
            result = cleaner.delete_user(user_id)
            print(f"\nResult: {result['status']}")
            print(f"Message: {result['message']}")
            
        elif choice == '2':
            result = cleaner.delete_all_users()
            print(f"\nResult: {result['status']}")
            print(f"Message: {result['message']}")
            
        else:
            print("\nExiting...")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        cleaner.close()

if __name__ == "__main__":
    main() 