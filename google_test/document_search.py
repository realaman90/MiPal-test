# document_search.py

import logging
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from typing import Dict, List
from neo4j import GraphDatabase # type: ignore

class DocumentSearch:
        # Initialize the Neo4j driver
        # self.uri = "bolt://localhost:7687"
        # self.user = "neo4j"
        # self.password = "your_neo4j_password" 
        # self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def recommend_similar_documents(self, doc_id: str, top_n: int = 5) -> List[Dict]:
        """
        Recommend similar documents based on summary embeddings.

        Args:
            doc_id (str): The origin_source_id of the document to find similarities for.
            top_n (int): Number of similar documents to return.

        Returns:
            List[Dict]: A list of similar documents with their similarity scores.
        """
        try:
            with self.driver.session() as session:
                # Retrieve the embedding of the document
                query_get_embedding = """
                MATCH (d:Document {origin_source_id: $doc_id})
                RETURN d.embedding AS embedding
                """
                result = session.run(query_get_embedding, parameters={"doc_id": doc_id}).single()
                if not result:
                    logging.error(f"No document found with origin_source_id: {doc_id}")
                    return []

                target_embedding = result["embedding"]
                if not target_embedding:
                    logging.error(f"No embedding found for document with origin_source_id: {doc_id}")
                    return []

                # Now similarity search within Neo4j using GDS cosine similarity
                query_similarity = """
                WITH $target_embedding AS targetEmbedding
                MATCH (d:Document)
                WHERE d.origin_source_id <> $doc_id AND d.embedding IS NOT NULL
                RETURN
                    d.origin_source_id AS doc_id,
                    d.name AS name,
                    gds.similarity.cosine(targetEmbedding, d.embedding) AS similarity
                ORDER BY similarity DESC
                LIMIT $top_n
                """

                results = session.run(
                    query_similarity,
                    parameters={
                        "doc_id": doc_id,
                        "target_embedding": target_embedding,
                        "top_n": top_n
                    }
                )

                similar_documents = []
                for record in results:
                    similar_documents.append({
                        "doc_id": record["doc_id"],
                        "name": record["name"],
                        "similarity": record["similarity"]
                    })

                return similar_documents

        except Exception as e:
            logging.error(f"Error recommending similar documents: {e}")
            return []

    def close(self):
        """Close the Neo4j driver connection"""
        self.driver.close()
