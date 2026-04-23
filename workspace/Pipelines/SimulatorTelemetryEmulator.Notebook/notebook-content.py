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
# Generates ONE batch of normal sensor readings for all machines and ingests
# directly into the KQL Database via the Kusto streaming ingestion API.
# 
# **This notebook is designed to be called by a Data Pipeline on a 1-minute schedule.**
# It does not loop or sleep - it sends one batch and exits.
# 
# No EventHub or Eventstream needed - uses the Kusto REST API directly.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration
# Leave KQL_URI empty to auto-discover from the Eventhouse in the workspace
KQL_URI = ""  # e.g. "https://xyz.z0.kusto.fabric.microsoft.com"

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
        "machine_id": s["machine_id"],
        "sensor_id": s["sensor_id"],
        "sensor_category": s["sensor_category"],
        "sensor_name": s["sensor_name"],
        "value": round(val, 4),
        "unit": s["unit"],
        "alert_level": "Normal",
        "is_anomaly": False,
    })

# Send to Eventhouse KQL Database via streaming ingestion
TOKEN_KQL = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")

# Auto-discover KQL URI from Eventhouse if not set
if not KQL_URI:
    eh = next((i for i in items if i.get("displayName") == "CAEManufacturingEH"), None)
    if eh:
        eh_props = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/eventhouses/{eh['id']}",
            headers=headers
        ).json()
        KQL_URI = eh_props.get("properties", {}).get("queryServiceUri", "")
        print(f"Auto-discovered KQL URI: {KQL_URI}")
    else:
        raise RuntimeError("CAEManufacturingEH not found and KQL_URI not set")

DB_NAME = "CAEManufacturingKQLDB"

# Build CSV payload for inline ingestion
csv_lines = []
for e in events:
    csv_lines.append(f"{e['timestamp']},{e['machine_id']},{e['sensor_id']},{e['sensor_category']},{e['sensor_name']},{e['value']},{e['unit']},{e['alert_level']},{e['is_anomaly']}")

csv_payload = "\n".join(csv_lines)

# Use Kusto streaming ingestion REST API
ingest_url = f"{KQL_URI}/v1/rest/ingest/{DB_NAME}/MachineTelemetry?streamFormat=Csv"
ingest_headers = {
    "Authorization": f"Bearer {TOKEN_KQL}",
    "Content-Type": "text/csv",
}
ingest_resp = requests.post(ingest_url, headers=ingest_headers, data=csv_payload)

if ingest_resp.status_code == 200:
    print(f"Ingested {len(events)} telemetry events at {ts}")
else:
    print(f"Ingestion failed ({ingest_resp.status_code}): {ingest_resp.text[:300]}")
    # Fallback: write to Lakehouse staging
    df = spark.createDataFrame(events)
    df.write.format("delta").mode("append").save(f"{BASE}/Tables/machine_telemetry_raw")
    print(f"Fallback: wrote {len(events)} events to Lakehouse staging")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
