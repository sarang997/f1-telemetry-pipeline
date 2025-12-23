import json
import time
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION, STORAGE_GROUP_ID, INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

def run_ingest_worker():
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id=STORAGE_GROUP_ID, 
        auto_offset_reset='earliest',
        api_version=KAFKA_API_VERSION
    )
    print(f"Connected to Kafka [Group: {STORAGE_GROUP_ID}]")

    # Initialize InfluxDB Client
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    
    # Verify Connection
    try:
        if not client.ping():
            print(f"Error: Could not connect to InfluxDB at {INFLUX_URL}")
            sys.exit(1)
        print(f"Connected to InfluxDB [{INFLUX_BUCKET}] (Health Check Passed)")
    except Exception as e:
        print(f"Error connecting to InfluxDB: {e}")
        sys.exit(1)

    # Verify Bucket Exists
    try:
        buckets_api = client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(INFLUX_BUCKET)
        if bucket is None:
            print(f"Bucket '{INFLUX_BUCKET}' not found. Attempting to create it...")
            try:
                buckets_api.create_bucket(bucket_name=INFLUX_BUCKET, org=INFLUX_ORG)
                print(f"Successfully created bucket '{INFLUX_BUCKET}'.")
            except Exception as e:
                print(f"Error: Failed to create bucket '{INFLUX_BUCKET}'.")
                print(f"Details: {e}")
                print("Suggestion: Your token might not have permission to create buckets.")
                print("Action: Create the bucket manually in InfluxDB UI or check permissions.")
                sys.exit(1)
    except Exception as check_e:
        print(f"Warning: Ensure bucket '{INFLUX_BUCKET}' exists. Detection failed: {check_e}")

    # Use SYNCHRONOUS write api for reliability (fail fast if DB is down)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    print("Starting Storage Ingest Worker...")

    count = 0
    batch = []
    BATCH_SIZE = 500
    
    try:
        for message in consumer:
            try:
                data = message.value
                
                # Check for critical timestamp, otherwise skip
                if 'timestamp' not in data:
                    continue

                # Create Point with safe getters
                p = Point("telemetry") \
                    .tag("car_id", "HAM_44") \
                    .tag("circuit", "silverstone") \
                    .field("speed_kmh", float(data.get('speed_kmh', 0.0))) \
                    .field("rpm", int(data.get('rpm', 0))) \
                    .field("gear", int(data.get('gear', 0))) \
                    .field("throttle", float(data.get('throttle', 0.0))) \
                    .field("brake", float(data.get('brake', 0.0))) \
                    .field("g_lat", float(data.get('g_lat', 0.0))) \
                    .field("g_long", float(data.get('g_long', 0.0))) \
                    .time(int(data['timestamp']))
                
                batch.append(p)
                
                # Write Batch
                if len(batch) >= BATCH_SIZE:
                    try:
                        write_api.write(bucket=INFLUX_BUCKET, record=batch)
                        count += len(batch)
                        
                        # Safe logging
                        speed = data.get('speed_kmh', 0.0)
                        print(f"Ingested {count} records. Latest Speed={speed:.1f}")
                        batch = [] # Reset batch
                    except Exception as e:
                        print(f"Failed to write batch to InfluxDB: {e}")
                        # In production, we might log this to an error topic or retry with backoff.
                        # For now, we clear the batch to prevent a loop if the data is bad.
                        batch = []
                        
            except Exception as msg_e:
                print(f"Skipping bad message: {msg_e}")
                continue
            
    except KeyboardInterrupt:
        print("\nStopping Ingest Worker...")
        # Flush remaining
        if batch:
            print(f"Flushing remaining {len(batch)} records...")
            write_api.write(bucket=INFLUX_BUCKET, record=batch)
            
        write_api.close()
        client.close()

if __name__ == "__main__":
    run_ingest_worker()
