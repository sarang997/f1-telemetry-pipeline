"""
Kafka Topic Setup Utility

Creates and verifies the analytics Kafka topic before starting the streaming job.
"""

import sys
import os
import shutil

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import KAFKA_BOOTSTRAP_SERVERS, ANALYTICS_TOPIC, SPARK_CHECKPOINT_DIR


def create_analytics_topic():
    """
    Create the f1-analytics Kafka topic with optimized configuration.
    """
    print(f"[SETUP] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='analytics-setup'
        )
        
        # Define topic with performance-optimized settings
        topic = NewTopic(
            name=ANALYTICS_TOPIC,
            num_partitions=2,  # 2 partitions for parallel consumption
            replication_factor=1,  # Single broker setup
            topic_configs={
                'retention.ms': '604800000',  # 7 days retention
                'compression.type': 'snappy',  # Compress analytics data
                'cleanup.policy': 'delete'
            }
        )
        
        print(f"[SETUP] Creating topic: {ANALYTICS_TOPIC}")
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"[SETUP] ✓ Topic '{ANALYTICS_TOPIC}' created successfully!")
        
    except TopicAlreadyExistsError:
        print(f"[SETUP] ✓ Topic '{ANALYTICS_TOPIC}' already exists.")
    except Exception as e:
        print(f"[SETUP] ✗ Error creating topic: {e}")
        sys.exit(1)
    finally:
        admin_client.close()


def cleanup_checkpoints():
    """
    Clean up old Spark checkpoints to avoid state inconsistencies.
    """
    if os.path.exists(SPARK_CHECKPOINT_DIR):
        print(f"[SETUP] Removing old checkpoint directory: {SPARK_CHECKPOINT_DIR}")
        try:
            shutil.rmtree(SPARK_CHECKPOINT_DIR)
            print("[SETUP] ✓ Checkpoint directory cleaned.")
        except Exception as e:
            print(f"[SETUP] Warning: Could not remove checkpoint directory: {e}")


def verify_setup():
    """
    Verify Kafka broker is reachable and topic exists.
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='analytics-verify'
        )
        
        topics = admin_client.list_topics()
        
        if ANALYTICS_TOPIC in topics:
            print(f"[SETUP] ✓ Verified topic '{ANALYTICS_TOPIC}' exists.")
            return True
        else:
            print(f"[SETUP] ✗ Topic '{ANALYTICS_TOPIC}' not found!")
            return False
            
    except Exception as e:
        print(f"[SETUP] ✗ Error verifying setup: {e}")
        return False
    finally:
        admin_client.close()


def run_setup(clean_checkpoints=False):
    """
    Main setup routine.
    """
    print("=" * 60)
    print("F1 ANALYTICS ENGINE - KAFKA SETUP")
    print("=" * 60)
    
    create_analytics_topic()
    
    if clean_checkpoints:
        cleanup_checkpoints()
    
    if verify_setup():
        print("\n[SETUP] ✓ All checks passed! Ready to start analytics engine.")
        print(f"[SETUP] Run: python analytics/spark_streaming.py")
        return True
    else:
        print("\n[SETUP] ✗ Setup incomplete. Please fix errors above.")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Setup Kafka for F1 Analytics Engine')
    parser.add_argument('--clean', action='store_true', 
                       help='Clean old checkpoints before starting')
    args = parser.parse_args()
    
    success = run_setup(clean_checkpoints=args.clean)
    sys.exit(0 if success else 1)
