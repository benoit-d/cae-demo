"""
Telemetry Fault Injection — Run manually (or triggered) to simulate a failure on SIM-001.

Generates a sequence of degrading sensor readings on SIM-001 that should trigger
a maintenance detection. The fault profile simulates a hydraulic pump wearing out:

  1. Minutes 0–2:   Hydraulic pressure starts drifting down slowly
  2. Minutes 2–5:   Pressure drops faster, fluid temperature rises
  3. Minutes 5–8:   Vibration increases, flow rate degrades
  4. Minutes 8–10:  Multiple sensors hit Warning/Critical thresholds

The script sends readings every 30 seconds (matching the normal cadence) so
the anomaly blends into the real-time stream and the downstream Activator /
Agent can detect it naturally.

Usage:
  python telemetry_fault_injection.py                          # 10-minute fault, stdout
  python telemetry_fault_injection.py --duration 5             # 5-minute fault
  EVENTHUB_CONNECTION_STRING=... python telemetry_fault_injection.py  # send to Eventstream
"""

import argparse
import csv
import json
import math
import os
import random
import time
from datetime import datetime, timezone


TARGET_SIMULATOR = "SIM-001"
BATCH_INTERVAL_SECONDS = 30

# Fault profiles: sensor_name -> (start_minute, drift_per_minute)
FAULT_PROFILES = {
    "Hydraulic Pressure":           {"start_min": 0,  "drift_per_min": -60.0},   # PSI drop
    "Hydraulic Fluid Temperature":  {"start_min": 2,  "drift_per_min":  3.5},    # °C rise
    "Hydraulic Flow Rate":          {"start_min": 3,  "drift_per_min": -4.0},    # LPM drop
    "Motion Platform Vibration X":  {"start_min": 5,  "drift_per_min":  0.012},  # g increase
    "Motion Platform Vibration Y":  {"start_min": 5,  "drift_per_min":  0.010},  # g increase
    "Base Frame Vibration":         {"start_min": 6,  "drift_per_min":  0.006},  # g increase
    "Motion Platform Temperature":  {"start_min": 4,  "drift_per_min":  2.0},    # °C rise
    "Power Consumption":            {"start_min": 3,  "drift_per_min":  5.0},    # kW rise
}


def load_sensor_definitions(csv_path: str) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def generate_faulty_value(sensor: dict, elapsed_minutes: float) -> tuple[float, str]:
    """Generate a sensor value with optional fault drift applied."""
    normal_min = float(sensor["normal_min"])
    normal_max = float(sensor["normal_max"])
    warning_min = float(sensor["warning_min"])
    warning_max = float(sensor["warning_max"])
    critical_min = float(sensor["critical_min"])
    critical_max = float(sensor["critical_max"])

    midpoint = (normal_min + normal_max) / 2
    amplitude = (normal_max - normal_min) / 2

    # Base normal value
    epoch = time.time()
    period = 120 + hash(sensor["sensor_id"]) % 180
    phase = hash(sensor["sensor_id"]) % 1000 / 1000 * 2 * math.pi
    value = midpoint + amplitude * 0.5 * math.sin(2 * math.pi * epoch / period + phase)
    noise = random.gauss(0, amplitude * 0.05)
    value += noise

    # Apply fault drift if this sensor has a fault profile
    sensor_name = sensor["sensor_name"]
    if sensor["simulator_id"] == TARGET_SIMULATOR and sensor_name in FAULT_PROFILES:
        profile = FAULT_PROFILES[sensor_name]
        if elapsed_minutes >= profile["start_min"]:
            drift_time = elapsed_minutes - profile["start_min"]
            value += profile["drift_per_min"] * drift_time

    # Determine alert level
    if value < critical_min or value > critical_max:
        alert_level = "Critical"
    elif value < warning_min or value > warning_max:
        alert_level = "Warning"
    else:
        alert_level = "Normal"

    return round(value, 4), alert_level


def run_fault_injection(sensors: list[dict], duration_minutes: float, conn_str: str):
    """Run the fault injection loop."""
    target_sensors = [s for s in sensors if s["simulator_id"] == TARGET_SIMULATOR]
    start_time = time.time()
    batch_count = 0

    print(f"=== Fault Injection on {TARGET_SIMULATOR} ===")
    print(f"Duration: {duration_minutes} minutes")
    print(f"Sensors: {len(target_sensors)}")
    print(f"Fault profiles: {list(FAULT_PROFILES.keys())}")
    print(f"Output: {'Eventstream' if conn_str else 'stdout'}")
    print()

    while True:
        elapsed = (time.time() - start_time) / 60.0
        if elapsed >= duration_minutes:
            break

        now = datetime.now(timezone.utc).isoformat()
        events = []

        for sensor in target_sensors:
            value, alert_level = generate_faulty_value(sensor, elapsed)
            events.append({
                "timestamp": now,
                "simulator_id": TARGET_SIMULATOR,
                "sensor_id": sensor["sensor_id"],
                "sensor_category": sensor["sensor_category"],
                "sensor_name": sensor["sensor_name"],
                "value": value,
                "unit": sensor["unit"],
                "alert_level": alert_level,
                "is_anomaly": alert_level != "Normal",
            })

        # Send or print
        if conn_str:
            from azure.eventhub import EventData, EventHubProducerClient
            producer = EventHubProducerClient.from_connection_string(conn_str)
            with producer:
                batch = producer.create_batch()
                for event in events:
                    batch.add(EventData(json.dumps(event)))
                producer.send_batch(batch)

        # Log summary
        warnings = sum(1 for e in events if e["alert_level"] == "Warning")
        criticals = sum(1 for e in events if e["alert_level"] == "Critical")
        batch_count += 1
        print(f"  [{elapsed:5.1f} min] Batch {batch_count}: {len(events)} readings | "
              f"Normal: {len(events) - warnings - criticals}  Warning: {warnings}  Critical: {criticals}")

        if warnings + criticals > 0:
            for e in events:
                if e["alert_level"] != "Normal":
                    print(f"    ⚠ {e['sensor_name']}: {e['value']} {e['unit']} [{e['alert_level']}]")

        time.sleep(BATCH_INTERVAL_SECONDS)

    print(f"\n=== Fault injection complete. {batch_count} batches sent over {duration_minutes} minutes. ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject faulty telemetry on SIM-001")
    parser.add_argument("--duration", type=float, default=10.0, help="Fault duration in minutes (default: 10)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "..", "data", "telemetry", "sensor_definitions.csv")
    if not os.path.exists(csv_path):
        csv_path = "Files/data/telemetry/sensor_definitions.csv"

    sensors = load_sensor_definitions(csv_path)
    conn_str = os.environ.get("EVENTHUB_CONNECTION_STRING", "")

    run_fault_injection(sensors, args.duration, conn_str)
