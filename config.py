# F1 Telemetry Configuration

# --- SYSTEM SETTINGS ---
PHYSICS_FREQUENCY = 100  # Hz
VISUALIZATION_FPS = 30   # FPS

# --- KAFKA CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9094"]
KAFKA_TOPIC = "f1-telemetry"
ANALYTICS_TOPIC = "f1-analytics"  # For aggregated lap-level analytics
KAFKA_API_VERSION = (2, 5, 0) # Fixed version to avoid auto-negotiation errors

# Consumer Groups
VIZ_GROUP_ID = None # None means transient/anonymous for liveviz
STORAGE_GROUP_ID = "storage-group"
ANALYTICS_GROUP_ID = "analytics-group"

# --- INFLUXDB CONFIGURATION ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "s5IVEsl-zcYdG8vEPsSp667toPP0TcHVG5_XgGESon4M15g-dK7jADd2kJpghAXf5yVuZF2_uCfEk9QFfxJSXA=="
INFLUX_ORG = "f1"
INFLUX_BUCKET = "f1-data"

# --- SPARK STREAMING CONFIGURATION ---
# Performance-optimized settings to prevent analytics from becoming a bottleneck
SPARK_MASTER = "local[4]"  # Use 4 cores for parallel processing
SPARK_SHUFFLE_PARTITIONS = 4  # Match core count for optimal partitioning
SPARK_TRIGGER_INTERVAL = "5 seconds"  # Micro-batch interval (balance latency vs throughput)
SPARK_CHECKPOINT_DIR = "/tmp/f1-analytics-checkpoint-v2"  # Checkpoint location for fault tolerance
SPARK_WATERMARK_DELAY = "10 seconds"  # Handle late-arriving data

# Analytics Processing
MAX_OFFSETS_PER_TRIGGER = 10000  # Limit records per micro-batch to prevent memory overflow 
