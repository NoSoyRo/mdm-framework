"""
Query utilities for COVID-19 graph database
"""
from typing import Dict, Any
from neo4j import GraphDatabase

class CovidGraphQuerier:
    """
    Utility class for querying COVID-19 graph database.
    Provides methods for common statistics and data exploration.
    """

    def __init__(self, uri: str = "bolt://localhost:7687", 
                 user: str = "neo4j", 
                 password: str = None):
        """
        Initialize the querier with database connection.
        
        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
        """
        if not password:
            raise ValueError("Database password is required")
            
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def get_general_statistics(self) -> Dict[str, Any]:
        """Get general statistics about the COVID-19 data"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Patient)
                RETURN 
                    count(p) as total_patients,
                    avg(p.age) as avg_age,
                    collect(distinct p.variant) as variants
            """)
            return result.single()

    def get_hospital_distribution(self) -> list:
        """Get patient distribution across hospitals"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Patient)-[:TREATED_AT]->(h:Hospital)
                RETURN 
                    h.name as hospital, 
                    count(p) as patients
                ORDER BY patients DESC
            """)
            return list(result)

    def get_duplicate_statistics(self) -> Dict[str, int]:
        """Get statistics about potential duplicate records"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p1:Patient)-[:POSSIBLE_DUPLICATE]->(p2:Patient)
                RETURN count(*) as duplicate_pairs
            """)
            return {"duplicate_pairs": result.single()["duplicate_pairs"]}

    def get_variant_timeline(self) -> list:
        """Get timeline of COVID variants"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Patient)
                RETURN 
                    p.variant as variant,
                    p.test_date as date,
                    count(*) as cases
                ORDER BY date
            """)
            return list(result)

    def get_contact_network(self, patient_id: str) -> Dict[str, Any]:
        """
        Get contact network for a specific patient
        
        Args:
            patient_id: ID of the patient to analyze
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Patient {id: $patient_id})-[r:HAD_CONTACT]->(c:Contact)
                RETURN 
                    p.name as patient_name,
                    collect({
                        name: c.name,
                        type: c.contact_type,
                        date: c.contact_date
                    }) as contacts
            """, {"patient_id": patient_id})
            return result.single()

    def close(self) -> None:
        """Close the database connection"""
        self.driver.close()
