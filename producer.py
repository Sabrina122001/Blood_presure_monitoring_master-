import json
import time
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
import random

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "blood_pressure_topic"

# Initialize Faker
fake = Faker()

def create_blood_pressure_observation():
    """Generate a FHIR-like blood pressure observation"""
    systolic = random.uniform(80, 200)
    diastolic = fake.random_int(min=50, max=120)
    
    # Generate a random location (latitude and longitude)
    latitude, longitude = fake.latitude(), fake.longitude()

    return {
        "resourceType": "Observation",
        "id": fake.uuid4(),
        "status": "final",
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs",
                "display": "Vital Signs"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": "85354-9",
                "display": "Blood pressure panel"
            }]
        },
        "subject": {
            "reference": f"Patient/{fake.uuid4()}"
        },
        "effectiveDateTime": datetime.now().isoformat(),
        "component": [
            {
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8480-6",
                        "display": "Systolic blood pressure"
                    }]
                },
                "valueQuantity": {
                    "value": round(systolic, 1),
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mmHg"
                }
            },
            {
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8462-4",
                        "display": "Diastolic blood pressure"
                    }]
                },
                "valueQuantity": {
                    "value": round(diastolic, 1),
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mmHg"
                }
            }
        ],
        # Add geographical location as a geo_point
        "geo_point": f"{latitude},{longitude}"
    }



def create_producer():
    """Create and configure Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def main():
    # Allow time for Kafka to start
    time.sleep(20)
    
    producer = create_producer()
    print(f"Started producing blood pressure readings to {KAFKA_TOPIC}")
    
    while True:
        try:
            # Generate and send blood pressure reading
            observation = create_blood_pressure_observation()
            producer.send(KAFKA_TOPIC, observation)
            
            # Extract values for logging
            systolic = observation["component"][0]["valueQuantity"]["value"]
            diastolic = observation["component"][1]["valueQuantity"]["value"]
            patient_id = observation["subject"]["reference"]
            location = observation["geo_point"]
            
            print(f"Sent reading - Patient: {patient_id}, "
                  f"BP: {systolic}/{diastolic} mmHg, Location: {location}")
            
            # Wait before sending next reading
            time.sleep(5)
            
        except Exception as e:
            print(f"Error producing message: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()

    