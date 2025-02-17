import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import os

# Configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "blood_pressure_topic"
ES_HOST = "http://elasticsearch:9200"
NORMAL_DATA_DIR = "/app/data/normal"

# Blood pressure thresholds based on AHA guidelines
THRESHOLDS = {
    "hypotension": {"systolic": (0, 90), "diastolic": (0, 60)},
    "normal": {"systolic": (90, 120), "diastolic": (60, 80)},
    "elevated": {"systolic": (120, 129), "diastolic": (60, 79)},  # Diastolic must be < 80
    "hypertension_stage_1": {"systolic": (130, 139), "diastolic": (80, 89)},
    "hypertension_stage_2": {"systolic": (140, 179), "diastolic": (90, 119)},
    "hypertensive_crisis": {"systolic": (180, float("inf")), "diastolic": (120, float("inf"))}
}

def create_consumer():
    """Create and configure Kafka consumer"""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

def create_elasticsearch_client():
    """Create and configure Elasticsearch client"""
    return Elasticsearch([ES_HOST], verify_certs=False)

def classify_blood_pressure(systolic, diastolic):
    """Classify blood pressure reading based on AHA guidelines"""
    if systolic is None or diastolic is None:
        return None  # Ignore invalid readings

    try:
        systolic = float(systolic)
        diastolic = float(diastolic)
    except ValueError:
        return None  # Ignore non-numeric values

    if systolic >= 180 or diastolic >= 120:
        return "hypertensive_crisis"
    elif 140 <= systolic < 180 or 90 <= diastolic < 120:
        return "hypertension_stage_2"
    elif 130 <= systolic < 140 or 80 <= diastolic < 90:
        return "hypertension_stage_1"
    elif 120 <= systolic < 130 and diastolic < 80:
        return "elevated"
    elif 90 <= systolic < 120 and 60 <= diastolic < 80:
        return "normal"
    elif 0 < systolic < 90 or 0 < diastolic < 60:
        return "hypotension"
    
    return None  # If no category matches

def save_normal_reading(reading):
    """Save normal blood pressure reading to local file"""
    os.makedirs(NORMAL_DATA_DIR, exist_ok=True)
    filename = f"{NORMAL_DATA_DIR}/normal_{int(time.time())}.json"

    with open(filename, 'w') as f:
        json.dump(reading, f)

def process_reading(es, reading):
    """Process blood pressure reading"""
    try:
        components = reading.get("component", [])
        if len(components) < 2:
            print("Invalid reading: missing blood pressure components")
            return

        systolic = components[0].get("valueQuantity", {}).get("value")
        diastolic = components[1].get("valueQuantity", {}).get("value")

        if systolic is None or diastolic is None:
            print("Invalid reading: missing systolic or diastolic value")
            return

        category = classify_blood_pressure(systolic, diastolic)

        if category is None:
            print(f"Ignoring invalid reading - BP: {systolic}/{diastolic}")
            return  # Ignore readings that cannot be classified

        patient_id = reading["subject"]["reference"]
        geo_point = reading.get("geo_point", None)

        if category == "normal":
            save_normal_reading(reading)
            print(f"Normal reading - Patient: {patient_id}, BP: {systolic}/{diastolic} mmHg")
        else:
            document = {
                "patient_id": patient_id,
                "timestamp": reading["effectiveDateTime"],
                "systolic_pressure": systolic,
                "diastolic_pressure": diastolic,
                "category": category
            }

            if geo_point:
                try:
                    latitude, longitude = map(float, geo_point.split(","))
                    document["location"] = {"lat": latitude, "lon": longitude}
                except ValueError:
                    print(f"Invalid geo_point format: {geo_point}")

            es.index(index="blood_pressure_anomalies", document=document)
            print(f"Abnormal reading detected - Patient: {patient_id}, BP: {systolic}/{diastolic} mmHg, Category: {category}")

    except Exception as e:
        print(f"Error processing reading: {e}")

def main():
    # Allow time for Elasticsearch to start
    time.sleep(30)

    # Create Elasticsearch client and configure index
    es = create_elasticsearch_client()

    # Wait for Elasticsearch to be ready
    retries = 0
    while retries < 30:
        try:
            if es.ping():
                break
        except Exception:
            print("Waiting for Elasticsearch...")
            time.sleep(10)
            retries += 1

    # Create index if it doesn't exist
    if not es.indices.exists(index="blood_pressure_anomalies"):
        es.indices.create(
            index="blood_pressure_anomalies",
            mappings={
                "properties": {
                    "patient_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "systolic_pressure": {"type": "float"},
                    "diastolic_pressure": {"type": "float"},
                    "category": {"type": "keyword"},
                    "location": {"type": "geo_point"}  # Adding geo_point mapping for Elasticsearch
                }
            }
        )

    # Create and start consumer
    consumer = create_consumer()
    print(f"Started consuming blood pressure readings from {KAFKA_TOPIC}")

    for message in consumer:
        process_reading(es, message.value)

if __name__ == "__main__":
    main()
