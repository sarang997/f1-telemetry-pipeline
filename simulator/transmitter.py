import json
import time
import sys
import os

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION, PHYSICS_FREQUENCY
from simulator.track import SilverstoneTrack
from simulator.car import SmartCar

def json_serializer(data):
    # Data is now a dict, so json.dumps works directly
    return json.dumps(data).encode("utf-8")

def run_transmitter():
    # Initialize Physics & Sensors
    try:
        track = SilverstoneTrack()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return
    
    car = SmartCar(track)
    
    # Initialize Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
            acks=0,
            api_version=KAFKA_API_VERSION
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return
    
    target_dt = 1.0 / PHYSICS_FREQUENCY
    print(f"Starting F1 Telemetry Stream ({PHYSICS_FREQUENCY}Hz) on {track.name}...")
    print("Sensors Active: " + ", ".join([s.name for s in car.sensors.sensors]))
    
    last_time = time.time()
    
    try:
        while True:
            loop_start = time.time()
            
            current_time = time.time()
            actual_dt = current_time - last_time
            last_time = current_time
            
            actual_dt = min(actual_dt, 0.1)
            
            telemetry_payload = car.update(actual_dt)
            
            producer.send(KAFKA_TOPIC, telemetry_payload)
            
            elapsed = time.time() - loop_start
            sleep_time = max(0, target_dt - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\nStopping Stream.")
        producer.close()

if __name__ == "__main__":
    run_transmitter()