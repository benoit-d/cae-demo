# Fabric notebook source
# %% [markdown]
# # Simulator Telemetry Emulator
#
# Streams **normal** sensor readings for 3 simulators (SIM-001, SIM-002, SIM-003)
# every 30 seconds to the `SimulatorTelemetryStream` Eventstream.
#
# **Run this notebook and leave it running** while you explore dashboards.
# Stop it when the demo is over.

# %%
import csv, json, math, os, random, time
from datetime import datetime, timezone

# Load sensor definitions from the staging Lakehouse
sensor_defs = []
try:
    df = spark.read.csv("Files/data/telemetry/sensor_definitions.csv", header=True, inferSchema=True)
    sensor_defs = [row.asDict() for row in df.collect()]
    print(f"Loaded {len(sensor_defs)} sensor definitions")
except Exception:
    print("Sensor definitions not found. Run PostDeploymentConfig first.")

# %%
# Eventstream connection
EVENTHUB_CONN_STR = spark.conf.get("spark.cae.telemetry.eventhub.connectionString", "")
USE_EVENTSTREAM = bool(EVENTHUB_CONN_STR)

if USE_EVENTSTREAM:
    print("Output → Eventstream")
else:
    print("Output → Delta table fallback (set connection string in PostDeploymentConfig for Eventstream)")

# %%
# Telemetry generation loop

INTERVAL = 30  # seconds between batches
MAX_MINUTES = 120

start = time.time()
batch_count = 0

print(f"Streaming every {INTERVAL}s for up to {MAX_MINUTES} min. Press Stop to end.\n")

try:
    while (time.time() - start) / 60 < MAX_MINUTES:
        ts = datetime.now(timezone.utc).isoformat()
        epoch = time.time()
        events = []

        for s in sensor_defs:
            nmin, nmax = float(s["normal_min"]), float(s["normal_max"])
            mid = (nmin + nmax) / 2
            amp = (nmax - nmin) / 2
            period = 120 + hash(s["sensor_id"]) % 180
            phase = hash(s["sensor_id"]) % 1000 / 1000 * 2 * math.pi
            val = mid + amp * 0.5 * math.sin(2 * math.pi * epoch / period + phase)
            val += random.gauss(0, amp * 0.05)

            events.append({
                "timestamp": ts,
                "simulator_id": s["simulator_id"],
                "sensor_id": s["sensor_id"],
                "sensor_category": s["sensor_category"],
                "sensor_name": s["sensor_name"],
                "value": round(val, 4),
                "unit": s["unit"],
                "alert_level": "Normal",
                "is_anomaly": False,
            })

        if USE_EVENTSTREAM:
            from azure.eventhub import EventHubProducerClient, EventData
            producer = EventHubProducerClient.from_connection_string(EVENTHUB_CONN_STR)
            with producer:
                batch = producer.create_batch()
                for e in events:
                    batch.add(EventData(json.dumps(e)))
                producer.send_batch(batch)
        else:
            df = spark.createDataFrame(events)
            df.write.format("delta").mode("append").save("Tables/simulator_telemetry_raw")

        batch_count += 1
        if batch_count % 4 == 0:
            elapsed = (time.time() - start) / 60
            print(f"  [{elapsed:.1f} min] {batch_count} batches, {batch_count * len(events)} events")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass

print(f"\nStopped. {batch_count} batches sent.")
