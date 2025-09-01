"""
Graph database dummy data generator for COVID-19 MDM
"""
from typing import List, Dict, Any
from datetime import datetime, timedelta
import random
from dataclasses import dataclass
from neo4j import GraphDatabase
from faker import Faker

@dataclass
class HospitalConfig:
    """Configuration for hospital data generation"""
    names: List[str] = None

    def __post_init__(self):
        self.names = self.names or [
            "Hospital General",
            "IMSS",
            "ISSSTE",
            "Hospital Privado",
            "Centro COVID"
        ]

@dataclass
class CovidConfig:
    """Configuration for COVID-19 specific data"""
    variants: List[str] = None
    symptoms: List[str] = None
    source_systems: List[str] = None

    def __post_init__(self):
        self.variants = self.variants or [
            "Alpha", "Beta", "Gamma", "Delta", "Omicron"
        ]
        self.symptoms = self.symptoms or [
            "Fiebre", "Tos", "Fatiga", 
            "Pérdida de olfato", "Dificultad respiratoria"
        ]
        self.source_systems = self.source_systems or [
            "SALUD", "IMSS", "ISSSTE", "PRIVADO"
        ]

class CovidGraphGenerator:
    """
    Generator for COVID-19 dummy data in Neo4j graph database.
    
    This class creates a realistic dummy dataset for COVID-19 cases,
    including patients, hospitals, and contact tracing information.
    """

    def __init__(
        self, 
        uri: str = "bolt://localhost:7687", 
        user: str = "neo4j", 
        password: str = None,
        hospital_config: HospitalConfig = None,
        covid_config: CovidConfig = None
    ):
        """
        Initialize the generator with database connection and configurations.
        
        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
            hospital_config: Configuration for hospital data
            covid_config: Configuration for COVID-19 specific data
        """
        if not password:
            raise ValueError("Database password is required")
        
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.faker = Faker(['es_MX'])
        self.hospital_config = hospital_config or HospitalConfig()
        self.covid_config = covid_config or CovidConfig()

    def _create_hospitals(self, session) -> None:
        """Create hospital nodes in the database"""
        for hospital in self.hospital_config.names:
            session.run("""
                CREATE (h:Hospital {
                    name: $name,
                    location: $location,
                    capacity: $capacity
                })
            """, {
                "name": hospital,
                "location": self.faker.address(),
                "capacity": random.randint(100, 500)
            })

    def _create_patient(self, session) -> Dict[str, Any]:
        """Create a single patient node with random data"""
        patient_data = {
            "id": self.faker.unique.random_number(digits=8),
            "name": self.faker.name(),
            "age": random.randint(18, 90),
            "gender": random.choice(["M", "F"]),
            "curp": self.faker.unique.random_number(digits=18),
            "address": self.faker.address(),
            "phone": self.faker.phone_number(),
            "variant": random.choice(self.covid_config.variants),
            "symptoms": random.sample(
                self.covid_config.symptoms, 
                random.randint(1, len(self.covid_config.symptoms))
            ),
            "test_date": (
                datetime.now() - timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d"),
            "source_system": random.choice(self.covid_config.source_systems)
        }
        
        session.run("""
            CREATE (p:Patient {
                id: $id,
                name: $name,
                age: $age,
                gender: $gender,
                curp: $curp,
                address: $address,
                phone: $phone,
                variant: $variant,
                symptoms: $symptoms,
                test_date: $test_date,
                source_system: $source_system
            })
            RETURN p
        """, patient_data)
        
        return patient_data

    def _create_contacts(self, session, patient_id: str, num_contacts: int = None) -> None:
        """Create contact nodes and relationships for a patient"""
        num_contacts = num_contacts or random.randint(1, 5)
        
        for _ in range(num_contacts):
            contact_data = {
                "patient_id": patient_id,
                "contact_name": self.faker.name(),
                "contact_phone": self.faker.phone_number(),
                "contact_date": (
                    datetime.now() - timedelta(days=random.randint(0, 30))
                ).strftime("%Y-%m-%d"),
                "contact_type": random.choice(["FAMILIAR", "LABORAL", "SOCIAL"])
            }
            
            session.run("""
                MATCH (p:Patient {id: $patient_id})
                CREATE (c:Contact {
                    name: $contact_name,
                    phone: $contact_phone,
                    contact_date: $contact_date,
                    contact_type: $contact_type
                })
                CREATE (p)-[:HAD_CONTACT]->(c)
            """, contact_data)

    def create_covid_data(self, num_patients: int = 1000) -> None:
        """
        Create the complete dummy dataset.
        
        Args:
            num_patients: Number of patient nodes to create
        """
        with self.driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")
            
            # Create base data structures
            self._create_hospitals(session)
            
            # Create patients and their relationships
            for _ in range(num_patients):
                patient_data = self._create_patient(session)
                
                # Connect to random hospital
                session.run("""
                    MATCH (p:Patient {id: $patient_id})
                    MATCH (h:Hospital {name: $hospital})
                    CREATE (p)-[:TREATED_AT]->(h)
                """, {
                    "patient_id": patient_data["id"],
                    "hospital": random.choice(self.hospital_config.names)
                })
                
                # Create contact nodes
                self._create_contacts(session, patient_data["id"])
            
            # Create possible duplicates based on CURP similarity
            self._create_duplicate_relationships(session)

    def _create_duplicate_relationships(self, session) -> None:
        """Create relationships between potentially duplicate patient records"""
        session.run("""
            MATCH (p1:Patient), (p2:Patient)
            WHERE p1 <> p2 
            AND p1.curp STARTS WITH substring(p2.curp, 0, 4)
            CREATE (p1)-[:POSSIBLE_DUPLICATE]->(p2)
        """)

    def close(self) -> None:
        """Close the database connection"""
        self.driver.close()
