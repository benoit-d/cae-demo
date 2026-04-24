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
# Generates ONE batch of normal sensor readings for all machines and sends them
# to the **TelemetryEventStream** via its Event Hub-compatible custom endpoint.
# The EventStream routes data to the Eventhouse KQL Database automatically.
# 
# **Called by TelemetryPipeline on a 1-minute schedule.**
# Sends one batch and exits — no loop or sleep.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-eventhub"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration — EventStream connection string from the Custom Endpoint source.
# Leave empty to auto-discover from workspace items.
EVENTSTREAM_CONNECTION_STRING = ""
EVENTSTREAM_NAME = "TelemetryEventStream"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, math, random, time, os, requests, base64
from datetime import datetime, timezone
import notebookutils

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

# Auto-discover EventStream connection string if not set
if not EVENTSTREAM_CONNECTION_STRING:
    es = next((i for i in items if i.get("displayName") == EVENTSTREAM_NAME and i.get("type") == "Eventstream"), None)
    if es:
        es_def_resp = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/eventstreams/{es['id']}/definition",
            headers=headers
        )
        if es_def_resp.status_code == 200:
            for part in es_def_resp.json().get("definition", {}).get("parts", []):
                if part["path"] == "eventstream.json":
                    es_json = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                    for src in es_json.get("sources", []):
                        conn = src.get("properties", {}).get("connectionString", "")
                        if conn:
                            EVENTSTREAM_CONNECTION_STRING = conn
                            print(f"Auto-discovered EventStream connection string")
                            break
                    break

if not EVENTSTREAM_CONNECTION_STRING:
    raise RuntimeError(
        f"EventStream connection string not found. "
        f"Open '{EVENTSTREAM_NAME}' in Fabric UI → Custom Endpoint source → "
        f"copy the Event Hub connection string into EVENTSTREAM_CONNECTION_STRING."
    )

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
        "machine_id": s["machine_id"],
        "sensor_id": s["sensor_id"],
        "sensor_category": s["sensor_category"],
        "sensor_name": s["sensor_name"],
        "value": round(val, 4),
        "unit": s["unit"],
        "alert_level": "Normal",
        "is_anomaly": False,
    })

# Send to EventStream via Event Hub SDK
from azure.eventhub import EventHubProducerClient, EventData

producer = EventHubProducerClient.from_connection_string(EVENTSTREAM_CONNECTION_STRING)
with producer:
    batch = producer.create_batch()
    for e in events:
        batch.add(EventData(json.dumps(e)))
    producer.send_batch(batch)

print(f"Sent {len(events)} telemetry events to {EVENTSTREAM_NAME} at {ts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
