from typing import Dict, List, Optional
from neo4j import GraphDatabase
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import openai
from llama_index.embeddings.openai import OpenAIEmbedding

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentRetriever:
    def __init__(self):
        """Initialize Neo4j connection and embedding model"""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = os.getenv("NEO4J_USER", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        
        # Initialize OpenAI
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize Neo4j connection
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
        
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        
        # Initialize embedding model
        self.embed_model = OpenAIEmbedding()

    def close(self):
        """Close Neo4j connection"""
        self.driver.close()

    def search_content(self, user_id: str, query: str, limit: int = 5, similarity_threshold: float = 0.3) -> List[Dict]:
        """Search for documents using content embedding similarity"""
        try:
            # Generate query embedding and chunk it
            full_query_embedding = self.embed_model.get_text_embedding(query)
            chunk_size = 500  # Same size as stored chunks
            query_chunks = [full_query_embedding[i:i + chunk_size] for i in range(0, len(full_query_embedding), chunk_size)]
            
            # Search query using vector similarity
            search_query = """
            // Match documents owned by the user
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            
            // Match document content embeddings
            MATCH (d)-[:HAS_CONTENT_EMBEDDING]->(e:DocumentEmbedding)
            WHERE size(e.embedding_chunk) = size($query_chunk)
            
            // Calculate similarity for each chunk
            WITH d, e, gds.similarity.cosine(
                e.embedding_chunk,
                $query_chunk
            ) AS chunk_score
            
            // Group by document and get max similarity score
            WITH d, max(chunk_score) as similarity_score
            WHERE similarity_score > $threshold
            
            // Return document details with content
            RETURN DISTINCT
                d.origin_source_id as drive_id,
                d.title as title,
                d.doc_type as doc_type,
                d.content as content,  // Include full content
                d.summary as summary,
                d.created_time as created_time,
                d.modified_time as modified_time,
                d.web_view_link as url,
                similarity_score
            ORDER BY similarity_score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    search_query,
                    user_id=user_id,
                    query_chunk=query_chunks[0],  # Use first chunk
                    threshold=similarity_threshold,
                    limit=limit
                )
                
                documents = []
                for record in result:
                    # Extract relevant text from content
                    content = record.get('content', '')
                    relevant_text = self._extract_relevant_text(content, query) if content else record.get('summary', '')
                    
                    documents.append({
                        'id': record['drive_id'],
                        'title': record.get('title') or 'Untitled',
                        'doc_type': record['doc_type'],
                        'relevant_text': relevant_text,
                        'summary': record['summary'],
                        'created_time': record['created_time'],
                        'modified_time': record['modified_time'],
                        'url': record['url'],
                        'similarity_score': record['similarity_score']
                    })
                
                if not documents:
                    logger.info(f"No similar documents found for query: {query}")
                else:
                    logger.info(f"Found {len(documents)} similar documents")
                
                return documents
                
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return []

    def _extract_relevant_text(self, content: str, query: str, context_window: int = 500) -> str:
        """Extract relevant text around query terms from content"""
        try:
            if not content:
                return ""
            
            # Split content into sentences or paragraphs
            paragraphs = content.split('\n')
            
            # Use OpenAI to find most relevant section
            messages = [
                {"role": "system", "content": "You are a helpful assistant that finds the most relevant section of text based on a query."},
                {"role": "user", "content": f"""
                Given this query: "{query}"
                
                Find the most relevant section from this text, maintaining context:
                
                {content[:4000]}  # Limit content length for API
                
                Return only the relevant section, no explanations.
                """}
            ]
            
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=messages,
                max_tokens=500,
                temperature=0
            )
            
            relevant_text = response.choices[0].message.content.strip()
            return relevant_text
            
        except Exception as e:
            logger.error(f"Error extracting relevant text: {e}")
            return content[:context_window] + "..."  # Fallback to first section

    def get_content_recommendations(self, user_id: str, doc_id: str, limit: int = 3) -> List[Dict]:
        """Get similar documents based on content similarity"""
        try:
            recommendation_query = """
            // Match source document and its content embeddings
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(source:Document {origin_source_id: $doc_id})
            MATCH (source)-[:HAS_CONTENT_EMBEDDING]->(source_emb:DocumentEmbedding)
            
            // Match other documents owned by the user
            MATCH (u)-[:OWNS]->(other:Document)
            WHERE other.origin_source_id <> source.origin_source_id
            MATCH (other)-[:HAS_CONTENT_EMBEDDING]->(other_emb:DocumentEmbedding)
            WHERE size(other_emb.embedding_chunk) = size(source_emb.embedding_chunk)
            
            // Calculate similarity between chunks
            WITH other, other_emb, source_emb,
                 gds.similarity.cosine(other_emb.embedding_chunk, source_emb.embedding_chunk) AS chunk_similarity
            
            // Group by document and get max similarity score
            WITH other, max(chunk_similarity) as similarity_score
            WHERE similarity_score > 0.7
            
            // Return recommended documents
            RETURN DISTINCT
                other.origin_source_id as drive_id,
                other.title as title,
                other.doc_type as doc_type,
                other.summary as summary,
                other.content as content,
                other.web_view_link as url,
                similarity_score
            ORDER BY similarity_score DESC
            LIMIT $limit
            """
            
            with self.driver.session() as session:
                result = session.run(
                    recommendation_query,
                    user_id=user_id,
                    doc_id=doc_id,
                    limit=limit
                )
                
                recommendations = []
                for record in result:
                    recommendations.append({
                        'id': record['drive_id'],
                        'title': record['title'],
                        'doc_type': record['doc_type'],
                        'summary': record['summary'],
                        'content': record['content'],
                        'url': record['url'],
                        'similarity_score': record['similarity_score']
                    })
                
                return recommendations
                
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []

    def search_by_topic(self, user_id: str, topic: str, limit: int = 5) -> Dict[str, List[Dict]]:
        """Search and group documents by topic using content similarity"""
        try:
            # Generate topic embedding
            topic_embedding = self.embed_model.get_text_embedding(topic)
            
            topic_query = """
            // Match user's documents and their content embeddings
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            MATCH (d)-[:HAS_CONTENT_EMBEDDING]->(e:DocumentEmbedding)
            WHERE size(e.embedding_chunk) = size($topic_embedding)
            
            // Calculate similarity scores
            WITH d, e, gds.similarity.cosine(e.embedding_chunk, $topic_embedding) AS chunk_score
            
            // Group by document and get max similarity score
            WITH d, max(chunk_score) as similarity_score
            WHERE similarity_score > 0.7
            
            // Group by document type
            WITH d.doc_type as doc_type,
                 collect({
                    id: d.origin_source_id,
                    title: d.title,
                    summary: d.summary,
                    content: d.content,
                    url: d.web_view_link,
                    score: similarity_score
                 })[0..$limit] as docs
            WHERE size(docs) > 0
            
            // Return grouped results
            RETURN doc_type, docs
            ORDER BY doc_type
            """
            
            with self.driver.session() as session:
                result = session.run(
                    topic_query,
                    user_id=user_id,
                    topic_embedding=topic_embedding,
                    limit=limit
                )
                
                results_by_type = {}
                for record in result:
                    doc_type = record['doc_type']
                    docs = record['docs']
                    if docs:  # Only include document types with results
                        results_by_type[doc_type] = docs
                
                return results_by_type
                
        except Exception as e:
            logger.error(f"Error searching by topic: {e}")
            return {}

    def get_context_for_llm(self, user_id: str, query: str, max_docs: int = 3, max_context_length: int = 4000) -> str:
        """Get relevant context from documents for LLM"""
        try:
            # Generate query embedding and chunk it
            full_query_embedding = self.embed_model.get_text_embedding(query)
            chunk_size = 500
            query_chunks = [full_query_embedding[i:i + chunk_size] for i in range(0, len(full_query_embedding), chunk_size)]
            
            # Search query to get most relevant documents
            search_query = """
            // Match documents owned by the user
            MATCH (u:User {user_id: $user_id})-[:OWNS]->(d:Document)
            
            // Match document embeddings with matching chunk index
            MATCH (d)-[:HAS_EMBEDDING]->(e:DocumentEmbedding)
            WHERE e.chunk_index = 0
            AND size(e.embedding_chunk) = size($query_chunk)
            
            // Calculate similarity for first chunk
            WITH d, e, gds.similarity.cosine(
                e.embedding_chunk,
                $query_chunk
            ) AS similarity_score
            WHERE similarity_score > 0.3
            
            // Return document details
            RETURN DISTINCT
                d.title as title,
                d.doc_type as doc_type,
                d.summary as summary,
                d.web_view_link as url,
                similarity_score
            ORDER BY similarity_score DESC
            LIMIT $max_docs
            """
            
            with self.driver.session() as session:
                result = session.run(
                    search_query,
                    user_id=user_id,
                    query_chunk=query_chunks[0],
                    max_docs=max_docs
                )
                
                # Build context string
                context_parts = []
                total_length = 0
                
                for record in result:
                    # Format document information
                    doc_context = f"\nDocument: {record['title']}\n"
                    doc_context += f"Type: {record['doc_type']}\n"
                    
                    # Add summary
                    if record['summary']:
                        remaining_length = max_context_length - total_length - len(doc_context)
                        if remaining_length > 100:
                            truncated_summary = record['summary'][:remaining_length]
                            doc_context += f"Summary: {truncated_summary}\n"
                    
                    # Add source URL
                    if record['url']:
                        doc_context += f"Source: {record['url']}\n"
                    
                    # Add separator
                    doc_context += "-" * 80 + "\n"
                    
                    # Check if adding this document would exceed max length
                    if total_length + len(doc_context) <= max_context_length:
                        context_parts.append(doc_context)
                        total_length += len(doc_context)
                    else:
                        break
                
                if not context_parts:
                    return "No relevant context found."
                
                # Combine all context parts
                full_context = "Here is the relevant context from the documents:\n\n"
                full_context += "".join(context_parts)
                
                return full_context
                
        except Exception as e:
            logger.error(f"Error getting context: {e}")
            return f"Error retrieving context: {str(e)}"

def main():
    """Test content retrieval functionality"""
    try:
        retriever = ContentRetriever()
        test_user_id = "dev"
        
        # Test semantic search with different queries
        queries = [
            {
                'text': "Leaflogic platform features and capabilities",
                'threshold': 0.3
            }
        ]
        
        for query_info in queries:
            query = query_info['text']
            threshold = query_info['threshold']
            
            print(f"\nSearching for: '{query}' (threshold: {threshold})")
            results = retriever.search_content(
                test_user_id, 
                query, 
                limit=3,
                similarity_threshold=threshold
            )
            
            if results:
                print(f"\nFound {len(results)} matching documents:")
                for i, doc in enumerate(results, 1):
                    print(f"\n{i}. {doc['title']}")
                    print(f"Type: {doc['doc_type']}")
                    print(f"Score: {doc['similarity_score']:.3f}")
                    print("\nRelevant Text:")
                    print(doc['relevant_text'])
                    print("-" * 80)
            else:
                print("No matching documents found")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        retriever.close()

if __name__ == "__main__":
    main() 