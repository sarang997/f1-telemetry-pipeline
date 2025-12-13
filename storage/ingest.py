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
    write_api = client.write_api(write_options=WriteOptions(batch_size=500, flush_interval=1000))
    print(f"Connected to InfluxDB [{INFLUX_BUCKET}]")
    
    print("Starting Storage Ingest Worker...")

    count = 0
    
    try:
        for message in consumer:
            data = message.value
            
            # Create Point
            p = Point("telemetry") \
                .tag("car_id", "HAM_44") \
                .tag("circuit", "silverstone") \
                .field("speed_kmh", float(data['speed_kmh'])) \
                .field("rpm", int(data['rpm'])) \
                .field("gear", int(data['gear'])) \
                .field("throttle", float(data['throttle'])) \
                .field("brake", float(data['brake'])) \
                .field("g_lat", float(data['g_lat'])) \
                .field("g_long", float(data['g_long'])) \
                .time(time.time_ns())
                
            write_api.write(bucket=INFLUX_BUCKET, record=p)
            
            count += 1
            if count % 100 == 0:
                print(f"Ingested {count} records. Latest Speed={data['speed_kmh']:.1f}")

    except KeyboardInterrupt:
        print("Stopping Ingest Worker...")
        write_api.close()
        client.close()

if __name__ == "__main__":
    run_ingest_worker()
