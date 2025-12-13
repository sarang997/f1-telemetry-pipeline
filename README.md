# F1 Telemetry Streaming System

Real-time F1 physics simulation, telemetry streaming, and visualization.

## Architecture

This system mimics a professional distributed telemetry pipeline:

1.  **Simulator (`simulator/`)**: A physics-based engine runs at 100Hz, simulating an F1 car lapping Silverstone. It streams data to a Kafka topic.
2.  **Dashboard (`dashboard/`)**: A real-time visualization tool consumes the stream to render the track map, G-forces, and telemetry graphs at 30 FPS.
3.  **Storage (`storage/`)**: A separate worker persists all telemetry data into an InfluxDB Time Series Database for historical analysis.

## Setup

### Prerequisites
- Python 3.10+
- Kafka (Running on `localhost:9094`)
- InfluxDB v2 (Running on `localhost:8086`)

### Installation
```bash
pip install -r requirements.txt
```

## Running the System

Open 3 separate terminals:

**1. The Car (Physics Engine)**
```bash
python3 simulator/transmitter.py
```
*Streams data to `f1-telemetry` topic.*

**2. The Pit Wall (Visualizer)**
```bash
python3 dashboard/app.py
```
*Visualizes live data.*

**3. The Data Center (Storage)**
```bash
python3 storage/ingest.py
```
*Writes data to InfluxDB `f1-data` bucket.*

## Configuration
Edit `config.py` to adjust:
- Physics Frequency (default 100Hz)
- Kafka Brokers & Topics
- InfluxDB Credentials
