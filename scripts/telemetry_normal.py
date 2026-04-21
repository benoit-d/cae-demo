"""
Telemetry Normal Readings — Scheduled every 30 seconds via Fabric Data Pipeline.

Generates one batch of normal-range sensor readings for all 3 simulators
and sends them to the SimulatorTelemetryStream Eventstream (Event Hub endpoint).

Usage in a Pipeline:
  - Activity type: Notebook or Script
  - Schedule: Every 30 seconds (or use a loop inside a pipeline with a 30s wait)
  - Parameters: EVENTHUB_CONNECTION_STRING (pipeline parameter or Key Vault reference)
"""

import csv
import io
import json
import math
import os
import random
import time
from datetime import datetime, timezone


def load_sensor_definitions(csv_path: str) -> list[dict]:
    """Load sensor definitions from CSV."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def generate_normal_value(sensor: dict, timestamp: float) -> float:
    """Generate a realistic value within the normal operating range."""
    normal_min = float(sensor["normal_min"])
    normal_max = float(sensor["normal_max"])
    midpoint = (normal_min + normal_max) / 2
    amplitude = (normal_max - normal_min) / 2

    # Slow sinusoidal drift + small noise — stays comfortably in normal range
    period = 120 + hash(sensor["sensor_id"]) % 180  # unique period per sensor
    phase = hash(sensor["sensor_id"]) % 1000 / 1000 * 2 * math.pi
    value = midpoint + amplitude * 0.5 * math.sin(2 * math.pi * timestamp / period + phase)
    noise = random.gauss(0, amplitude * 0.05)
    return round(value + noise, 4)


def build_telemetry_batch(sensors: list[dict]) -> list[dict]:
    """Build one batch of telemetry events for all sensors across all simulators."""
    now = datetime.now(timezone.utc)
    ts = now.isoformat()
    epoch = now.timestamp()

    events = []
    for sensor in sensors:
        value = generate_normal_value(sensor, epoch)
        events.append({
            "timestamp": ts,
            "simulator_id": sensor["simulator_id"],
            "sensor_id": sensor["sensor_id"],
            "sensor_category": sensor["sensor_category"],
            "sensor_name": sensor["sensor_name"],
            "value": value,
            "unit": sensor["unit"],
            "alert_level": "Normal",
            "is_anomaly": False,
        })
    return events


def send_to_eventhub(events: list[dict], connection_string: str) -> None:
    """Send events to the Eventstream via its Event Hub endpoint."""
    from azure.eventhub import EventData, EventHubProducerClient

    producer = EventHubProducerClient.from_connection_string(connection_string)
    with producer:
        batch = producer.create_batch()
        for event in events:
            batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)


# ---------------------------------------------------------------------------
# Main — runs once per invocation (pipeline schedules the 30-second cadence)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Resolve paths relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "..", "data", "telemetry", "sensor_definitions.csv")

    # Fallback: try Fabric Files path
    if not os.path.exists(csv_path):
        csv_path = "Files/data/telemetry/sensor_definitions.csv"

    sensors = load_sensor_definitions(csv_path)
    events = build_telemetry_batch(sensors)

    conn_str = os.environ.get("EVENTHUB_CONNECTION_STRING", "")

    if conn_str:
        send_to_eventhub(events, conn_str)
        print(f"[{datetime.now(timezone.utc).isoformat()}] Sent {len(events)} normal readings to Eventstream.")
    else:
        # Fallback: print to stdout (useful for testing or notebook %run)
        print(json.dumps(events[:3], indent=2))
        print(f"... ({len(events)} total events — set EVENTHUB_CONNECTION_STRING to send to Eventstream)")
