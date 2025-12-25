# F1 Telemetry Streaming Pipeline

A real-time distributed system that simulates an F1 car lapping Silverstone, streams telemetry data via **Apache Kafka**, visualizes it on a live dashboard, and persists it for historical analysis.

## 🏗️ Architecture Explained

This project mimics a professional event-driven architecture used in motorsport data pipelines. Instead of a monolithic app, the system is split into three decoupled services that communicate over a messaging bus.

### 1. 🚀 Simulator (The Producer)
*   **Path**: `simulator/transmitter.py`, `simulator/physics.py`
*   **Role**: Acts as the F1 Car.
*   **How it works**:
    - Runs a **Physical Model** (`simulator/physics.py`) that calculates drag, downforce, grip, and acceleration at **100Hz**.
    - Wraps this data into a standardized JSON packet (defined in `shared/schema.py`).
    - **Streams** these packets to the `f1-telemetry` **Kafka Topic**.
    - It fires-and-forgets events, unaware of who is listening.

### 2. 📡 The Backbone (Apache Kafka)
*   **Role**: The central nervous system.
*   **Why Kafka?**: In a real race, you might have 50 engineers looking at the data, plus ML models and storage systems. Kafka allows all of them to subscribe to the car's data stream simultaneously without slowing down the telemetry transmission.

### 3. 📊 Dashboard (The "Pit Wall" Consumer)
*   **Path**: `dashboard/app.py`
*   **Role**: Real-time visualization for the Race Engineer.
*   **Tech**: Python `matplotlib` (using `FuncAnimation`).
*   **How it works**:
    - Connects to Kafka as a **Consumer**.
    - Reads the latest telemetry packets.
    - Updates the Track Map, G-Force Circle, Speed Traces, and Pedal Gauges at **30 FPS**.
    - It uses a separate thread to poll Kafka vs rendering to ensure smooth playback.

### 4. 💾 Storage (The "Data Center" Consumer)
*   **Path**: `storage/ingest.py`
*   **Role**: Historical Archivist.
*   **Tech**: **InfluxDB** (Time Series Database).
*   **How it works**:
    - Shows how to have *multiple* consumers on the same topic: `storage` runs completely independently of `dashboard`.
    - It consumes the same Kafka stream but writes every single data point to an InfluxDB bucket for later analysis (e.g., comparing laps).

### 5. ⚡ Analytics Engine (The "Performance Analyst")
*   **Path**: `analytics/spark_streaming.py`
*   **Role**: Real-time Lap Aggregation & Insights.
*   **Tech**: **Apache Spark Structured Streaming**.
*   **How it works**:
    - Consumes the high-frequency telemetry stream (100Hz) from Kafka.
    - Uses **windowing** and **watermarking** to aggregate data by lap.
    - Computes lap-level metrics: avg/max speed, throttle %, fuel usage, G-forces.
    - Publishes aggregated analytics to `f1-analytics` Kafka topic.
    - Dashboard consumes from this topic for lap comparisons without impacting real-time telemetry.
    - Runs as **separate process** with micro-batching (5s intervals) for optimal performance.

---

## 🛠️ Setup & Usage

### Prerequisites
1.  **Python 3.10+**
2.  **Apache Kafka**: Running locally (configured in `config.py` as `localhost:9094`).
3.  **InfluxDB v2**: Running locally (configured in `config.py` as `localhost:8086`).
4.  **Apache Spark**: PySpark will be installed via pip (local mode).

### Installation
```bash
pip install -r requirements.txt
```

### 🏁 Running the Pipeline

You can run the full distributed system with **4 separate terminals**:

**Terminal 1: Start the Simulation**
```bash
python3 simulator/transmitter.py
```
> *Output: "Sent: T=12.45 Speed=280 km/h..."*

**Terminal 2: Start the Analytics Engine (Optional)**
```bash
# First-time setup: Create analytics Kafka topic
python3 analytics/setup_kafka.py

# Run the analytics streaming job
python3 analytics/spark_streaming.py
```
> *Output: Lap-level analytics printed to console and published to Kafka*

**Terminal 3: Open the Dashboard**
```bash
python3 dashboard/app.py
```
> *A window will open showing live telemetry. Click ANALYTICS tab to see lap metrics.*

**Terminal 4: Start Storage Ingestion (Optional)**
```bash
python3 storage/ingest.py
```
> *Output: "Ingested 500 records..."*

## ⚙️ Configuration
Check `config.py` to adjust:
- `PHYSICS_FREQUENCY`: Simulation update rate (default 100Hz).
- `VISUALIZATION_FPS`: Dashboard refresh rate.
- `SPARK_TRIGGER_INTERVAL`: Analytics micro-batch interval (default 5 seconds).
- `SPARK_MASTER`: Spark cores for analytics (default "local[4]").
- Kafka & InfluxDB connection strings and topic names.

## 📊 Analytics Performance

**Why Analytics Won't Bottleneck:**
- Runs in **separate process** from telemetry
- Uses **aggregation-only** workload (summaries, not full storage)
- **Micro-batching** (5s intervals) reduces overhead
- **Watermarking** prevents unbounded state growth
- Parallel execution across multiple cores
