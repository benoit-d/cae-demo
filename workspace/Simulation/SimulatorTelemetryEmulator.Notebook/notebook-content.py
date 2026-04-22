# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Simulator Telemetry - Single Batch
# 
# Generates ONE batch of normal sensor readings for 3 simulators and sends
# them to the SimulatorTelemetryStream Eventstream.
# 
# **This notebook is designed to be called by a Data Pipeline on a 1-minute schedule.**
# It does not loop or sleep - it sends one batch and exits.
# 
# To set up:
# 1. Create a Data Pipeline in the workspace
# 2. Add a Notebook activity pointing to this notebook
# 3. Set the schedule to run every 1 minute

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration - set the Eventstream connection string
# Get this from: Eventstream > Custom App source > Connection String
# Or leave empty to write to the Lakehouse staging area as fallback

EVENTHUB_CONNECTION_STRING = ""  # Paste your Eventstream connection string here

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, math, random, time, os, requests
from datetime import datetime, timezone
import notebookutils

# Discover Lakehouse for reading staged CSV sensor definitions
TOKEN = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except Exception:
        pass

headers = {"Authorization": f"Bearer {TOKEN}"}
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=headers)
items = resp.json().get("value", [])
lh = next((i for i in items if i.get("displayName") == "CAEManufacturing_LH"), None)

if not lh:
    raise RuntimeError("Lakehouse not found")

LH_ID = lh["id"]
BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}"

# Load sensor definitions
sensor_defs = [row.asDict() for row in
    spark.read.csv(f"{BASE}/Files/data/telemetry/sensor_definitions.csv", header=True, inferSchema=True).collect()]

print(f"Loaded {len(sensor_defs)} sensors")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generate ONE batch of normal readings
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

# Send to Eventstream or fallback to Lakehouse
if EVENTHUB_CONNECTION_STRING:
    from azure.eventhub import EventHubProducerClient, EventData
    producer = EventHubProducerClient.from_connection_string(EVENTHUB_CONNECTION_STRING)
    with producer:
        batch = producer.create_batch()
        for e in events:
            batch.add(EventData(json.dumps(e)))
        producer.send_batch(batch)
    print(f"Sent {len(events)} events to Eventstream at {ts}")
else:
    # Fallback: write to Lakehouse staging Delta table (configure Eventstream for production)
    df = spark.createDataFrame(events)
    df.write.format("delta").mode("append").save(f"{BASE}/Tables/simulator_telemetry_raw")
    print(f"Wrote {len(events)} events to Lakehouse staging at {ts}")
    print("(Set EVENTHUB_CONNECTION_STRING to route to Eventstream -> Eventhouse)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
