import logging
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import argparse
from typing import List, Dict

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentCleaner:
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

    def _get_document_count(self, tx, user_id: str):
        """Get count of documents owned by user"""
        query = """
        MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
        WITH count(d) as doc_count
        OPTIONAL MATCH (:User {user_id: $user_id})-[:OWNS]->(:Document)<-[:CREATED]-(creator:User)
        WHERE creator.user_id IS NULL
        WITH doc_count, count(DISTINCT creator) as orphaned_creators
        OPTIONAL MATCH (:User {user_id: $user_id})-[:OWNS]->(:Document)<-[:LAST_MODIFIED]-(modifier:User)
        WHERE modifier.user_id IS NULL
        RETURN doc_count, orphaned_creators, count(DISTINCT modifier) as orphaned_modifiers
        """
        result = tx.run(query, user_id=user_id).single()
        return {
            'doc_count': result["doc_count"] if result else 0,
            'orphaned_creators': result["orphaned_creators"] if result else 0,
            'orphaned_modifiers': result["orphaned_modifiers"] if result else 0
        }

    def _delete_user_documents(self, tx, user_id: str):
        """Delete all documents owned by user and cleanup orphaned users"""
        query = """
        // First collect all documents owned by the user
        MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
        WITH collect(d) as docs
        
        // For each document, find its relationships and orphaned users
        UNWIND docs as d
        
        // Find creator relationships
        OPTIONAL MATCH (d)<-[cr:CREATED]-(creator:User)
        WHERE creator.user_id IS NULL
        
        // Find modifier relationships
        OPTIONAL MATCH (d)<-[mr:LAST_MODIFIED]-(modifier:User)
        WHERE modifier.user_id IS NULL
        
        // Delete relationships and document
        DELETE cr, mr, d
        
        // Collect orphaned users for deletion
        WITH DISTINCT creator, modifier
        WHERE creator IS NOT NULL OR modifier IS NOT NULL
        
        // Delete orphaned creators
        WITH creator, modifier
        WHERE creator IS NOT NULL AND creator.user_id IS NULL
        AND NOT EXISTS((creator)-[:CREATED]->(:Document))
        AND NOT EXISTS((creator)-[:LAST_MODIFIED]->(:Document))
        DELETE creator
        
        // Delete orphaned modifiers
        WITH modifier
        WHERE modifier IS NOT NULL AND modifier.user_id IS NULL
        AND NOT EXISTS((modifier)-[:CREATED]->(:Document))
        AND NOT EXISTS((modifier)-[:LAST_MODIFIED]->(:Document))
        DELETE modifier
        """
        tx.run(query, user_id=user_id)

    def delete_documents(self, user_id: str, confirm: bool = True) -> dict:
        """
        Delete all documents owned by a user and cleanup orphaned users
        
        Args:
            user_id: User identifier
            confirm: Whether to ask for confirmation before deleting
            
        Returns:
            Dictionary containing operation status
        """
        try:
            with self.driver.session() as session:
                # Get document and orphaned user count
                counts = session.execute_read(self._get_document_count, user_id)
                
                if counts['doc_count'] == 0:
                    return {
                        'status': 'info',
                        'message': f'No documents found for user {user_id}'
                    }
                
                # Ask for confirmation if required
                if confirm:
                    confirmation = input(
                        f"\nWARNING: This will delete:"
                        f"\n- {counts['doc_count']} documents"
                        f"\n- {counts['orphaned_creators']} orphaned creator users"
                        f"\n- {counts['orphaned_modifiers']} orphaned modifier users"
                        f"\nfor user {user_id}."
                        f"\nThis action cannot be undone."
                        f"\nDo you want to continue? (yes/no): "
                    ).lower()
                    
                    if confirmation != 'yes':
                        return {
                            'status': 'cancelled',
                            'message': 'Operation cancelled by user'
                        }
                
                # Delete documents and cleanup orphaned users
                session.execute_write(self._delete_user_documents, user_id)
                
                # Verify deletion
                remaining = session.execute_read(self._get_document_count, user_id)
                
                if remaining['doc_count'] == 0:
                    return {
                        'status': 'success',
                        'message': (
                            f'Successfully deleted {counts["doc_count"]} documents, '
                            f'{counts["orphaned_creators"]} orphaned creators, and '
                            f'{counts["orphaned_modifiers"]} orphaned modifiers '
                            f'for user {user_id}'
                        )
                    }
                else:
                    return {
                        'status': 'error',
                        'message': (
                            f'Failed to delete all data. '
                            f'{remaining["doc_count"]} documents remaining.'
                        )
                    }
                
        except Exception as e:
            logger.error(f"Error deleting documents: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
        
        finally:
            self.close()

    def list_user_documents(self, user_id: str) -> list:
        """List all documents owned by user"""
        query = """
        MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
        RETURN d.name as name, d.doc_type as type, 
               d.created_time as created, d.modified_time as modified
        ORDER BY d.modified_time DESC
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query, user_id=user_id)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Error listing documents: {e}")
            return []

    def list_orphaned_users(self) -> List[Dict]:
        """List all orphaned users (users with only email and no relationships)"""
        query = """
        MATCH (u:User)
        WHERE u.user_id IS NULL
        OPTIONAL MATCH (u)-[r]-()
        WITH u, COLLECT(r) as rels
        WHERE SIZE(rels) <= 1
        RETURN u.email as email, SIZE(rels) as relationship_count
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [
                    {
                        'email': record['email'],
                        'relationship_count': record['relationship_count']
                    }
                    for record in result
                ]
        except Exception as e:
            logger.error(f"Error listing orphaned users: {e}")
            return []

    def delete_all_documents(self, confirm: bool = True) -> dict:
        """
        Delete all documents and orphaned users from Neo4j
        
        Args:
            confirm: Whether to ask for confirmation before deleting
            
        Returns:
            Dictionary containing operation status
        """
        try:
            # First get document count and orphaned user count
            count_query = """
            MATCH (d:Document)
            WITH count(d) as doc_count
            OPTIONAL MATCH (u:User)
            WHERE u.user_id IS NULL
            WITH doc_count, count(u) as orphaned_users
            RETURN doc_count, orphaned_users
            """
            
            with self.driver.session() as session:
                count_result = session.run(count_query).single()
                if not count_result or count_result["doc_count"] == 0:
                    return {
                        'status': 'info',
                        'message': 'No documents found in the database'
                    }
                
                doc_count = count_result["doc_count"]
                orphaned_users = count_result["orphaned_users"]
                
                # Ask for confirmation if required
                if confirm:
                    confirmation = input(
                        f"\nWARNING: This will delete:"
                        f"\n- ALL {doc_count} documents"
                        f"\n- {orphaned_users} orphaned users"
                        f"\nfrom the database."
                        f"\nThis action cannot be undone."
                        f"\nDo you want to continue? (yes/no): "
                    ).lower()
                    
                    if confirmation != 'yes':
                        return {
                            'status': 'cancelled',
                            'message': 'Operation cancelled by user'
                        }
                
                # Delete all documents and cleanup orphaned users
                delete_query = """
                // First collect all documents
                MATCH (d:Document)
                
                // Get all relationships
                OPTIONAL MATCH (d)<-[r:OWNS]-(:User)
                OPTIONAL MATCH (d)<-[cr:CREATED]-(creator:User)
                OPTIONAL MATCH (d)<-[mr:LAST_MODIFIED]-(modifier:User)
                
                // Delete document and its relationships
                DELETE r, cr, mr, d
                
                // Clean up orphaned users
                WITH DISTINCT creator, modifier
                WHERE creator IS NOT NULL OR modifier IS NOT NULL
                
                // Delete orphaned creators
                WITH creator, modifier
                WHERE creator IS NOT NULL AND creator.user_id IS NULL
                AND NOT EXISTS((creator)-[:CREATED]->(:Document))
                AND NOT EXISTS((creator)-[:LAST_MODIFIED]->(:Document))
                DELETE creator
                
                // Delete orphaned modifiers
                WITH modifier
                WHERE modifier IS NOT NULL AND modifier.user_id IS NULL
                AND NOT EXISTS((modifier)-[:CREATED]->(:Document))
                AND NOT EXISTS((modifier)-[:LAST_MODIFIED]->(:Document))
                DELETE modifier
                """
                
                session.run(delete_query)
                
                # Verify deletion
                verify_result = session.run(count_query).single()
                remaining_docs = verify_result["doc_count"]
                remaining_orphans = verify_result["orphaned_users"]
                
                if remaining_docs == 0:
                    return {
                        'status': 'success',
                        'message': (
                            f'Successfully deleted {doc_count} documents and '
                            f'{orphaned_users - remaining_orphans} orphaned users'
                        )
                    }
                else:
                    return {
                        'status': 'error',
                        'message': (
                            f'Failed to delete all data. '
                            f'{remaining_docs} documents and '
                            f'{remaining_orphans} orphaned users remaining.'
                        )
                    }
                
        except Exception as e:
            logger.error(f"Error deleting all documents: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

def main():
    """Command line interface for document deletion"""
    parser = argparse.ArgumentParser(description='Delete user documents from Neo4j')
    parser.add_argument('user_id', nargs='?', help='User ID whose documents should be deleted')
    parser.add_argument('--force', action='store_true', help='Delete without confirmation')
    parser.add_argument('--list', action='store_true', help='List documents without deleting')
    parser.add_argument('--orphaned', action='store_true', help='List orphaned users')
    parser.add_argument('--all', action='store_true', help='Delete all documents from all users')
    
    args = parser.parse_args()
    
    cleaner = DocumentCleaner()
    
    try:
        if args.all:
            # Delete all documents
            print("\nDeleting all documents...")
            result = cleaner.delete_all_documents(confirm=not args.force)
            print(f"\nResult: {result['status']}")
            print(f"Message: {result['message']}")
            return
            
        if not args.user_id and not args.orphaned:
            parser.error("user_id is required unless --all or --orphaned is specified")
            
        if args.orphaned:
            # List orphaned users
            print("\nListing orphaned users:")
            orphaned = cleaner.list_orphaned_users()
            
            if not orphaned:
                print("No orphaned users found")
                return
                
            for user in orphaned:
                print(f"Email: {user['email']}")
                print(f"Relationships: {user['relationship_count']}")
                print("-" * 50)
            
            print(f"\nTotal orphaned users: {len(orphaned)}")
            
        elif args.list:
            # Just list the documents
            print(f"\nListing documents for user {args.user_id}:")
            documents = cleaner.list_user_documents(args.user_id)
            
            if not documents:
                print("No documents found")
                return
                
            for doc in documents:
                print(f"\nName: {doc['name']}")
                print(f"Type: {doc['type']}")
                print(f"Created: {doc['created']}")
                print(f"Modified: {doc['modified']}")
                print("-" * 50)
            
            print(f"\nTotal documents: {len(documents)}")
            
        else:
            # Delete documents
            result = cleaner.delete_documents(args.user_id, confirm=not args.force)
            print(f"\nResult: {result['status']}")
            print(f"Message: {result['message']}")
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        cleaner.close()

if __name__ == "__main__":
    main() 