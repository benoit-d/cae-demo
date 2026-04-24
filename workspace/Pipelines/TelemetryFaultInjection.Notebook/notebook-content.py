# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Telemetry Fault Injection
# 
# Simulates progressive machine failures by injecting degrading sensor
# readings into the **TelemetryEventStream** via its Event Hub-compatible
# custom endpoint. The EventStream routes data to the Eventhouse KQL Database.
# 
# **Fault Profiles:**
# - **CNC-003**: Spindle bearing failure — vibration increases, temperature rises,
#   coolant flow drops, axis accuracy degrades, power consumption climbs
# - *(Extensible to other machines via TARGET_MACHINE and FAULTS config)*
# 
# Run manually during a demo. Default: 10 batches over 10 minutes.
# Each batch sends one reading per sensor, then sleeps for INTERVAL seconds.
# 
# The injected data triggers the KQL health-scoring functions
# (CNC_BearingWearScore, CNC_CoolantFailScore) which feed MachineHealthAlerts().

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

# === CONFIGURATION ===
TARGET_MACHINE = "CNC-003"      # Machine to inject faults on
INTERVAL = 60                   # Seconds between batches
DURATION_MIN = 6.0              # Total fault injection duration in minutes

# EventStream connection string from the Custom Endpoint source.
# Must be set manually after fabric-cicd redeploys since EventStream endpoints
# are rotated. Copy from TelemetryEventStream → Custom Endpoint source in UI.
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
items_list = resp.json().get("value", [])
lh = next((i for i in items_list if i.get("displayName") == "CAEManufacturing_LH"), None)
if not lh:
    raise RuntimeError("Lakehouse not found (needed to read staged CSVs)")

LH_ID = lh["id"]
BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}"

sensor_defs = [row.asDict() for row in
    spark.read.csv(f"{BASE}/Files/data/telemetry/sensor_definitions.csv", header=True, inferSchema=True)
    .filter(f"machine_id = '{TARGET_MACHINE}'").collect()]
print(f"{len(sensor_defs)} sensors loaded for {TARGET_MACHINE}")

# Auto-discover EventStream connection string if not set
if not EVENTSTREAM_CONNECTION_STRING:
    es = next((i for i in items_list if i.get("displayName") == EVENTSTREAM_NAME and i.get("type") == "Eventstream"), None)
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

# Fault profile: spindle bearing failure on CNC mill.
# Rates are tuned so at least one sensor crosses Critical thresholds
# (vib>0.20g, temp>70C, power>26kW, coolant<3LPM) within ~5 minutes.
FAULTS = {
    "Spindle Vibration":       {"start": 0,  "rate": 0.04},     # g increase per minute
    "Spindle Temperature":     {"start": 1,  "rate": 7.0},      # C rise per minute
    "Coolant Flow Rate":       {"start": 2,  "rate": -2.5},     # LPM drop per minute
    "Axis Position Accuracy":  {"start": 3,  "rate": 0.003},    # mm drift per minute
    "Power Consumption":       {"start": 1,  "rate": 4.0},      # kW rise per minute
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.eventhub import EventHubProducerClient, EventData

start = time.time()
batch_num = 0

print(f"Injecting spindle bearing fault on {TARGET_MACHINE} for {DURATION_MIN} min...\n")

try:
    while True:
        elapsed = (time.time() - start) / 60
        if elapsed >= DURATION_MIN:
            break

        ts = datetime.now(timezone.utc).isoformat()
        events = []

        for s in sensor_defs:
            nmin, nmax = float(s["normal_min"]), float(s["normal_max"])
            wmin, wmax = float(s["warning_min"]), float(s["warning_max"])
            cmin, cmax = float(s["critical_min"]), float(s["critical_max"])
            mid = (nmin + nmax) / 2
            amp = (nmax - nmin) / 2
            val = mid + amp * 0.5 * math.sin(time.time() / 120) + random.gauss(0, amp * 0.05)

            f = FAULTS.get(s["sensor_name"])
            if f and elapsed >= f["start"]:
                val += f["rate"] * (elapsed - f["start"])

            if val < cmin or val > cmax:   lvl = "Critical"
            elif val < wmin or val > wmax: lvl = "Warning"
            else:                          lvl = "Normal"

            events.append({
                "timestamp": ts, "machine_id": TARGET_MACHINE,
                "sensor_id": s["sensor_id"], "sensor_category": s["sensor_category"],
                "sensor_name": s["sensor_name"], "value": round(val, 4),
                "unit": s["unit"], "alert_level": lvl, "is_anomaly": lvl != "Normal",
            })

        # Send to EventStream via Event Hub SDK
        producer = EventHubProducerClient.from_connection_string(EVENTSTREAM_CONNECTION_STRING)
        with producer:
            eh_batch = producer.create_batch()
            for e in events:
                eh_batch.add(EventData(json.dumps(e)))
            producer.send_batch(eh_batch)

        w = sum(1 for e in events if e["alert_level"] == "Warning")
        c = sum(1 for e in events if e["alert_level"] == "Critical")
        batch_num += 1

        print(f"  [{elapsed:5.1f} min] batch {batch_num}  Normal:{len(events)-w-c}  Warn:{w}  Crit:{c}  → {EVENTSTREAM_NAME}")
        for e in events:
            if e["alert_level"] != "Normal":
                print(f"      {e['sensor_name']}: {e['value']} {e['unit']} [{e['alert_level']}]")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass

print(f"\nFault injection complete. {batch_num} batches sent to {EVENTSTREAM_NAME}.")
print(f"Check alerts: MachineHealthAlerts() | where machine_id == '{TARGET_MACHINE}'")

# Release Spark resources so the notebook (and its parent pipeline) can end
# instead of holding the session open until idle timeout.
try:
    spark.stop()
except Exception:
    pass
try:
    notebookutils.session.stop()
except Exception:
    try:
        import mssparkutils
        mssparkutils.session.stop()
    except Exception:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
