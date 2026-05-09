"""
Sensor data simulator.

Generates realistic environmental sensor readings for 8 Romanian cities and
publishes them to a Kafka topic every second. Injects occasional anomalies to
make the anomaly-detection pipeline visible in the dashboard.
"""

import json
import math
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")

SENSORS = [
    {"id": "sensor_01", "location": "Bucharest-North",   "lat": 44.4820, "lon": 26.0720},
    {"id": "sensor_02", "location": "Bucharest-Center",  "lat": 44.4268, "lon": 26.1025},
    {"id": "sensor_03", "location": "Bucharest-South",   "lat": 44.3730, "lon": 26.0820},
    {"id": "sensor_04", "location": "Cluj-Napoca",        "lat": 46.7712, "lon": 23.6236},
    {"id": "sensor_05", "location": "Timisoara",          "lat": 45.7489, "lon": 21.2087},
    {"id": "sensor_06", "location": "Iasi",               "lat": 47.1585, "lon": 27.6014},
    {"id": "sensor_07", "location": "Constanta",          "lat": 44.1598, "lon": 28.6348},
    {"id": "sensor_08", "location": "Brasov",             "lat": 45.6427, "lon": 25.5887},
]

# Base environmental profile per sensor (realistic for each city's climate)
BASE_PROFILES = {
    "sensor_01": {"temp": 18.0, "hum": 60.0, "co2": 420.0, "pm25": 18.0, "pm10": 32.0},
    "sensor_02": {"temp": 20.0, "hum": 55.0, "co2": 450.0, "pm25": 22.0, "pm10": 38.0},
    "sensor_03": {"temp": 17.5, "hum": 62.0, "co2": 410.0, "pm25": 15.0, "pm10": 28.0},
    "sensor_04": {"temp": 15.0, "hum": 58.0, "co2": 400.0, "pm25": 12.0, "pm10": 22.0},
    "sensor_05": {"temp": 19.0, "hum": 65.0, "co2": 415.0, "pm25": 14.0, "pm10": 25.0},
    "sensor_06": {"temp": 16.0, "hum": 70.0, "co2": 405.0, "pm25": 16.0, "pm10": 29.0},
    "sensor_07": {"temp": 21.0, "hum": 72.0, "co2": 395.0, "pm25": 10.0, "pm10": 18.0},
    "sensor_08": {"temp": 14.0, "hum": 68.0, "co2": 390.0, "pm25":  8.0, "pm10": 15.0},
}


def daily_cycle(tick: int) -> float:
    """Sinusoidal daily temperature cycle. tick advances once per second."""
    hour = (tick / 3600.0) % 24.0
    return math.sin((hour - 6.0) * math.pi / 12.0) * 8.0


def maybe_spike(value: float, prob: float = 0.015, factor: float = 2.8) -> float:
    """Randomly inject a spike anomaly."""
    return value * factor if random.random() < prob else value


def build_reading(sensor: dict, tick: int) -> dict:
    p = BASE_PROFILES[sensor["id"]]
    delta_t = daily_cycle(tick)

    temperature = round(maybe_spike(p["temp"] + delta_t + random.gauss(0, 0.5), prob=0.012), 2)
    humidity    = round(min(100.0, max(0.0, p["hum"]  + random.gauss(0, 2.0))), 2)
    co2_ppm     = round(max(300.0,           p["co2"]  + random.gauss(0, 15.0)), 1)
    pm25        = round(max(0.0, maybe_spike(p["pm25"] + random.gauss(0, 1.5))), 2)
    pm10        = round(max(0.0, maybe_spike(p["pm10"] + random.gauss(0, 2.0))), 2)
    # Simplified EPA AQI based on PM2.5 breakpoints
    aqi         = round(min(500.0, (pm25 / 35.4) * 150.0), 1)

    return {
        "sensor_id":  sensor["id"],
        "location":   sensor["location"],
        "latitude":   sensor["lat"],
        "longitude":  sensor["lon"],
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "temperature": temperature,
        "humidity":    humidity,
        "co2_ppm":    co2_ppm,
        "pm2_5":      pm25,
        "pm10":       pm10,
        "aqi":        aqi,
    }


def connect_producer() -> KafkaProducer:
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
            )
            print(f"[simulator] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[simulator] Kafka not ready (attempt {attempt}/30), retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 30 attempts.")


def main() -> None:
    producer = connect_producer()
    tick = 0

    print(f"[simulator] Streaming {len(SENSORS)} sensors → topic '{KAFKA_TOPIC}'")
    while True:
        for sensor in SENSORS:
            reading = build_reading(sensor, tick)
            producer.send(KAFKA_TOPIC, key=sensor["id"], value=reading)
            print(
                f"[{reading['sensor_id']}] {reading['location']:20s} | "
                f"T={reading['temperature']:6.2f}°C | "
                f"H={reading['humidity']:5.1f}% | "
                f"CO2={reading['co2_ppm']:6.1f}ppm | "
                f"AQI={reading['aqi']:5.1f}"
            )
        producer.flush()
        tick += 1
        time.sleep(1)


if __name__ == "__main__":
    main()
