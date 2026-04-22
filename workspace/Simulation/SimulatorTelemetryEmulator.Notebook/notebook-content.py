# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Simulator Telemetry Emulator
# 
# Streams normal sensor readings for 3 simulators (SIM-001, SIM-002, SIM-003)
# every 30 seconds. Leave this running while you explore dashboards.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv, json, math, os, random, time
from datetime import datetime, timezone

# Load sensor definitions from the staging Lakehouse
sensor_defs = []
try:
    df = spark.read.csv("Files/data/telemetry/sensor_definitions.csv", header=True, inferSchema=True)
    sensor_defs = [row.asDict() for row in df.collect()]
    print(f"Loaded {len(sensor_defs)} sensor definitions")
except Exception as e:
    print(f"Sensor definitions not found: {e}")
    print("Run PostDeploymentConfig first.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Telemetry generation loop
INTERVAL = 30
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

        df_out = spark.createDataFrame(events)
        df_out.write.format("delta").mode("append").save("Tables/simulator_telemetry_raw")

        batch_count += 1
        if batch_count % 4 == 0:
            elapsed = (time.time() - start) / 60
            print(f"  [{elapsed:.1f} min] {batch_count} batches, {batch_count * len(events)} total events")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass

print(f"\nStopped. {batch_count} batches sent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
