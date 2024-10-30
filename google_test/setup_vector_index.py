import logging
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VectorIndexSetup:
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

    def setup_vector_index(self):
        """Create vector index for document embeddings"""
        try:
            with self.driver.session() as session:
                # First, check if index exists
                check_query = """
                SHOW INDEXES
                YIELD name, type
                WHERE name = 'document_embeddings'
                RETURN count(*) as count
                """
                result = session.run(check_query).single()
                
                if result and result['count'] > 0:
                    logger.info("Vector index already exists")
                    return True
                
                # Create vector index with correct provider
                create_index_query = """
                CREATE VECTOR INDEX document_embeddings IF NOT EXISTS
                FOR (e:DocumentEmbedding)
                ON (e.embedding_chunk)
                OPTIONS {
                    indexProvider: 'vector-2.0',
                    indexConfig: {
                        `vector.dimensions`: 500,
                        `vector.similarity_function`: 'cosine'
                    }
                }
                """
                
                session.run(create_index_query)
                logger.info("Vector index created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error creating vector index: {e}")
            return False

    def verify_embeddings(self, user_id: str = None):
        """Verify document embeddings exist"""
        try:
            with self.driver.session() as session:
                # Query to check embeddings
                query = """
                MATCH (d:Document)
                {}
                OPTIONAL MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
                WITH d, count(e) as embedding_count
                RETURN 
                    count(d) as total_docs,
                    sum(CASE WHEN embedding_count > 0 THEN 1 ELSE 0 END) as docs_with_embeddings,
                    sum(CASE WHEN embedding_count = 0 THEN 1 ELSE 0 END) as docs_without_embeddings
                """.format("WHERE EXISTS((u:User {user_id: $user_id})-[:OWNS]->(d))" if user_id else "")
                
                result = session.run(query, user_id=user_id if user_id else None).single()
                
                if result:
                    print("\nEmbedding Statistics:")
                    print(f"Total documents: {result['total_docs']}")
                    print(f"Documents with embeddings: {result['docs_with_embeddings']}")
                    print(f"Documents without embeddings: {result['docs_without_embeddings']}")
                    
                    if result['docs_without_embeddings'] > 0:
                        print("\nWarning: Some documents don't have embeddings!")
                else:
                    print("No documents found")
                
        except Exception as e:
            logger.error(f"Error verifying embeddings: {e}")

def main():
    """Set up vector index and verify embeddings"""
    setup = VectorIndexSetup()
    
    try:
        # Create vector index
        print("\nSetting up vector index...")
        if setup.setup_vector_index():
            print("Vector index setup successful")
        else:
            print("Failed to set up vector index")
            return
        
        # Verify embeddings for specific user
        print("\nVerifying embeddings for user 'aman'...")
        setup.verify_embeddings("aman")
        
        # Verify all embeddings
        print("\nVerifying all embeddings...")
        setup.verify_embeddings()
        
    except Exception as e:
        logger.error(f"Error in setup: {e}")
        raise
    
    finally:
        setup.close()

if __name__ == "__main__":
    main() 