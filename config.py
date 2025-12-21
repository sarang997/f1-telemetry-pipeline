# F1 Telemetry Configuration

# --- SYSTEM SETTINGS ---
PHYSICS_FREQUENCY = 100  # Hz
VISUALIZATION_FPS = 30   # FPS

# --- KAFKA CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9094"]
KAFKA_TOPIC = "f1-telemetry"
KAFKA_API_VERSION = (2, 5, 0) # Fixed version to avoid auto-negotiation errors

# Consumer Groups
VIZ_GROUP_ID = None # None means transient/anonymous for liveviz
STORAGE_GROUP_ID = "storage-group"

# --- INFLUXDB CONFIGURATION ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "s5IVEsl-zcYdG8vEPsSp667toPP0TcHVG5_XgGESon4M15g-dK7jADd2kJpghAXf5yVuZF2_uCfEk9QFfxJSXA=="
INFLUX_ORG = "f1"
INFLUX_BUCKET = "f1-data"

#Mechanism to control the throughput of the data into the kafka 
#Mechanism to undetstand the performace of the kafka setup. 
