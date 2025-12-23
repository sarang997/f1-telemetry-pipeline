"""
PySpark Structured Streaming Job for F1 Telemetry Analytics

This job consumes high-frequency telemetry data from Kafka (100Hz),
aggregates it by lap, and produces lap-level analytics to a separate topic.
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SPARK_TRIGGER_INTERVAL

# CRITICAL: Set Java compatibility flags BEFORE importing PySpark
# This fixes Java 18+ compatibility with Hadoop's security module
java_opts = " ".join([
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
])

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--driver-java-options "{java_opts}" pyspark-shell'

# Now import PySpark (after environment is set)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, sum as spark_sum,
    first, last, count, from_json, to_json, struct, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    ANALYTICS_TOPIC,
    SPARK_MASTER,
    SPARK_SHUFFLE_PARTITIONS,
    SPARK_CHECKPOINT_DIR,
    SPARK_WATERMARK_DELAY,
    MAX_OFFSETS_PER_TRIGGER
)

# Define the schema for incoming telemetry data
telemetry_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("time", DoubleType(), True),
    StructField("lap_count", IntegerType(), True),
    StructField("lap_time", DoubleType(), True),
    StructField("last_lap_time", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("sector_name", StringType(), True),
    StructField("throttle", DoubleType(), True),
    StructField("brake", DoubleType(), True),
    StructField("gear", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("g_lat", DoubleType(), True),
    StructField("g_long", DoubleType(), True),
    StructField("fuel_kg", DoubleType(), True),
    StructField("tire_wear_pct", DoubleType(), True),
])

def create_spark_session():
    """
    Create and configure Spark session with performance optimizations.
    
    Key optimizations:
    - Multi-core processing for parallel aggregations
    - Reduced shuffle partitions to minimize overhead
    - Graceful shutdown for clean state management
    - Memory tuning to handle 100Hz telemetry stream
    - Kafka connector package automatically downloaded
    - Java 18+ compatibility (configured at module level)
    """
    return SparkSession.builder \
        .appName("F1-Telemetry-Analytics") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS)) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "5000") \
        .getOrCreate()

def run_analytics_job():
    """
    Main analytics job: Read from Kafka, aggregate by lap, write back to Kafka
    """
    print("[ANALYTICS] Starting PySpark Structured Streaming Job...")
    
    # Create Spark Session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    kafka_bootstrap = ",".join(KAFKA_BOOTSTRAP_SERVERS)
    
    print(f"[ANALYTICS] Connecting to Kafka: {kafka_bootstrap}")
    print(f"[ANALYTICS] Reading from topic: {KAFKA_TOPIC}")
    
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON telemetry
    telemetry_df = raw_stream \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), telemetry_schema).alias("data")) \
        .select("data.*")
    
    print("[ANALYTICS] Telemetry stream schema:")
    telemetry_df.printSchema()
    
    # -------------------------------------------------------------------------
    # AGGREGATION LOGIC: Window by lap_count
    # -------------------------------------------------------------------------
    # Use watermarking to handle late arrivals gracefully
    # Watermark based on event time derived from timestamp
    # Events older than watermark are dropped, preventing unbounded state growth
    
    # Convert millisecond timestamp to proper timestamp for watermarking
    telemetry_with_eventtime = telemetry_df \
        .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
    
    lap_analytics = telemetry_with_eventtime \
        .withWatermark("event_time", SPARK_WATERMARK_DELAY) \
        .groupBy("lap_count") \
        .agg(
            # Speed Metrics
            avg("speed_kmh").alias("avg_speed_kmh"),
            spark_max("speed_kmh").alias("max_speed_kmh"),
            spark_min("speed_kmh").alias("min_speed_kmh"),
            
            # Pedal Inputs - Average and High-Throttle Analysis
            avg("throttle").alias("avg_throttle"),
            avg("brake").alias("avg_brake"),
            spark_sum(
                (col("throttle") > 0.5).cast("int")
            ).alias("high_throttle_samples"),  # Count samples with >50% throttle
            
            # G-Forces
            spark_max("g_lat").alias("max_g_lat"),
            spark_max("g_long").alias("max_g_long"),
            spark_min("g_long").alias("min_g_long"),  # Max braking (negative G)
            
            # Fuel & Tires
            first("fuel_kg").alias("fuel_start_kg"),
            last("fuel_kg").alias("fuel_end_kg"),
            spark_max("tire_wear_pct").alias("tire_wear_pct"),
            first("tire_wear_pct").alias("tire_wear_start_pct"),
            
            # Lap Time
            spark_max("lap_time").alias("lap_time_s"),
            last("last_lap_time").alias("last_lap_time_s"),
            
            # Metadata - keep as long integers for proper serialization
            count("*").alias("sample_count"),
            first("timestamp").alias("lap_start_timestamp"),
            last("timestamp").alias("lap_end_timestamp")
        ) \
        .withColumn("fuel_used_kg", col("fuel_start_kg") - col("fuel_end_kg")) \
        .withColumn("throttle_pct", 
                   (col("high_throttle_samples") / col("sample_count") * 100)) \
        .withColumn("lap_duration_ms", 
                   col("lap_end_timestamp") - col("lap_start_timestamp")) \
        .withColumn("fuel_per_lap", col("fuel_used_kg")) \
        .withColumn("tire_deg_rate", col("tire_wear_pct") - col("tire_wear_start_pct")) \
        .withColumn("aggression_index", (col("avg_throttle") * 50 + col("avg_brake") * 50)) \
        .select(
            "lap_count",
            "avg_speed_kmh",
            "max_speed_kmh",
            "min_speed_kmh",
            "avg_throttle",
            "avg_brake",
            "throttle_pct",
            "max_g_lat",
            "max_g_long",
            "min_g_long",
            "fuel_start_kg",
            "fuel_end_kg",
            "fuel_used_kg",
            "fuel_per_lap",
            "tire_wear_pct",
            "tire_deg_rate",
            "aggression_index",
            "lap_time_s",
            "last_lap_time_s",
            "sample_count",
            "lap_start_timestamp",
            "lap_end_timestamp",
            "lap_duration_ms"
        )
    
    
    # Convert to JSON for Kafka output
    analytics_json = lap_analytics \
        .select(to_json(struct("*")).alias("value"))
    
    # Write to Kafka Analytics Topic
    print(f"[ANALYTICS] Writing to topic: {ANALYTICS_TOPIC}")
    print(f"[ANALYTICS] Trigger interval: {SPARK_TRIGGER_INTERVAL}")
    print(f"[ANALYTICS] Checkpoint location: {SPARK_CHECKPOINT_DIR}")
    
    # from pyspark.sql.streaming.trigger import Trigger    
    query = analytics_json \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", ANALYTICS_TOPIC) \
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
        .trigger(processingTime=SPARK_TRIGGER_INTERVAL) \
        .outputMode("update") \
        .start()
    
    print("[ANALYTICS] Streaming job started successfully!")
    print("[ANALYTICS] Aggregating telemetry by lap and publishing to Kafka...")
    print("[ANALYTICS] Press Ctrl+C to stop.")
    
    # Also print to console for debugging
    console_query = lap_analytics \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # Wait for termination
    try:
        query.awaitTermination()
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[ANALYTICS] Stopping job...")
        query.stop()
        console_query.stop()
        spark.stop()

if __name__ == "__main__":
    run_analytics_job()
