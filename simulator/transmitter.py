import json
import time
import sys
import os

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_API_VERSION, PHYSICS_FREQUENCY
from simulator.track import SilverstoneTrack
from simulator.physics import F1Sim

def json_serializer(data):
    if hasattr(data, 'to_json'):
        return data.to_json().encode("utf-8")
    return json.dumps(data).encode("utf-8")

def run_transmitter():
    # Initialize Physics
    track = SilverstoneTrack()
    sim = F1Sim(track)
    
    # Initialize Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
            acks=0, # Fire and forget
            api_version=KAFKA_API_VERSION
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    # Simulation Loop
    target_dt = 1.0 / PHYSICS_FREQUENCY
    
    print(f"Starting F1 Telemetry Stream ({PHYSICS_FREQUENCY}Hz)...")
    
    try:
        while True:
            start_time = time.time()
            
            # Step Physics (Returns TelemetryData object)
            state = sim.step(target_dt)
            
            # Send to Kafka
            producer.send(KAFKA_TOPIC, state)
            
            # Sleep to maintain frequency
            elapsed = time.time() - start_time
            sleep_time = max(0, target_dt - elapsed)
            
            if sleep_time > 0:
                time.sleep(sleep_time)
                
            # Log every 1 second
            if int(state.time * PHYSICS_FREQUENCY) % PHYSICS_FREQUENCY == 0:
                print(f"Sent: T={state.time:.2f} Speed={state.speed_kmh:.1f} km/h")
                
    except KeyboardInterrupt:
        print("\nStopping Stream.")
        producer.close()

if __name__ == "__main__":
    run_transmitter()


#TODO
#Add mechanism to add the sensors to the data (then send the data to the same message )