"""
Spark Structured Streaming processor.

Consumes JSON sensor readings from Kafka, applies:
  1. Schema validation & timestamp parsing
  2. Anomaly detection (rule-based thresholds)
  3. Writes raw enriched readings to InfluxDB (measurement: sensor_reading)
  4. Computes 1-minute sliding-window aggregations per location
     and writes them to InfluxDB (measurement: sensor_aggregation)
"""

import os
import time

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, lit, max as spark_max,
    min as spark_min, sum as spark_sum, to_timestamp, window,
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "sensor-data")
INFLUXDB_URL            = os.getenv("INFLUXDB_URL",            "http://localhost:8086")
INFLUXDB_TOKEN          = os.getenv("INFLUXDB_TOKEN",          "mytoken")
INFLUXDB_ORG            = os.getenv("INFLUXDB_ORG",            "sensororg")
INFLUXDB_BUCKET         = os.getenv("INFLUXDB_BUCKET",         "sensors")

# ── Schema ────────────────────────────────────────────────────────────────────
SENSOR_SCHEMA = StructType([
    StructField("sensor_id",   StringType(),  True),
    StructField("location",    StringType(),  True),
    StructField("latitude",    DoubleType(),  True),
    StructField("longitude",   DoubleType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("temperature", DoubleType(),  True),
    StructField("humidity",    DoubleType(),  True),
    StructField("co2_ppm",     DoubleType(),  True),
    StructField("pm2_5",       DoubleType(),  True),
    StructField("pm10",        DoubleType(),  True),
    StructField("aqi",         DoubleType(),  True),
])


def _influx_client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


# ── Batch writers (called by foreachBatch — run in the driver) ─────────────────

def write_raw(df, epoch_id: int) -> None:
    """Write individual sensor readings to InfluxDB."""
    records = df.collect()
    if not records:
        return

    client = _influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []

    for row in records:
        d = row.asDict()
        if d.get("sensor_id") is None:
            continue
        point = (
            Point("sensor_reading")
            .tag("sensor_id", d["sensor_id"])
            .tag("location",  d["location"] or "unknown")
            .field("temperature", float(d["temperature"] or 0))
            .field("humidity",    float(d["humidity"]    or 0))
            .field("co2_ppm",     float(d["co2_ppm"]    or 0))
            .field("pm2_5",       float(d["pm2_5"]      or 0))
            .field("pm10",        float(d["pm10"]       or 0))
            .field("aqi",         float(d["aqi"]        or 0))
            .field("is_anomaly",  1 if d.get("is_anomaly") else 0)
            .time(d["event_time"])
        )
        points.append(point)

    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        print(f"[processor] epoch={epoch_id} raw_points={len(points)}")

    client.close()


def write_aggregations(df, epoch_id: int) -> None:
    """Write windowed aggregations to InfluxDB."""
    records = df.collect()
    if not records:
        return

    client = _influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []

    for row in records:
        d = row.asDict()
        point = (
            Point("sensor_aggregation")
            .tag("location", d["location"] or "unknown")
            .field("avg_temperature", float(d["avg_temperature"] or 0))
            .field("avg_humidity",    float(d["avg_humidity"]    or 0))
            .field("avg_co2_ppm",     float(d["avg_co2_ppm"]    or 0))
            .field("avg_aqi",         float(d["avg_aqi"]        or 0))
            .field("max_temperature", float(d["max_temperature"] or 0))
            .field("min_temperature", float(d["min_temperature"] or 0))
            .field("anomaly_count",   int(d["anomaly_count"]    or 0))
            .field("reading_count",   int(d["reading_count"]    or 0))
            .time(d["window_end"])
        )
        points.append(point)

    if points:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
        print(f"[processor] epoch={epoch_id} agg_points={len(points)}")

    client.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("[processor] Starting Spark Structured Streaming processor…")
    # Give infrastructure a moment to be fully ready after healthcheck passes
    time.sleep(15)

    spark = (
        SparkSession.builder
        .appName("SensorPipelineProcessor")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read from Kafka ────────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON & enrich ────────────────────────────────────────────────
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn(
            "is_anomaly",
            (
                (col("temperature") > 38.0) |
                (col("temperature") < -5.0)  |
                (col("humidity")    > 92.0)  |
                (col("co2_ppm")     > 1000.0)|
                (col("pm2_5")       > 50.0)  |
                (col("aqi")         > 150.0)
            )
        )
        .filter(col("sensor_id").isNotNull())
    )

    # ── 3. Stream raw readings → InfluxDB ────────────────────────────────────
    raw_query = (
        parsed.writeStream
        .foreachBatch(write_raw)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/raw")
        .start()
    )

    # ── 4. Windowed aggregations (1 min window, 30 s slide) → InfluxDB ────────
    aggregated = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("location"),
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("co2_ppm").alias("avg_co2_ppm"),
            avg("aqi").alias("avg_aqi"),
            spark_max("temperature").alias("max_temperature"),
            spark_min("temperature").alias("min_temperature"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
            count(lit(1)).alias("reading_count"),
        )
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )

    agg_query = (
        aggregated.writeStream
        .foreachBatch(write_aggregations)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/aggregations")
        .start()
    )

    print("[processor] Both streaming queries running. Awaiting termination…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
