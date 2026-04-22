# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Telemetry Fault Injection - SIM-001
# 
# Manually run during a demo to simulate a hydraulic pump failure on SIM-001.
# Sends degrading readings over 10 minutes (one batch per minute).
# 
# This is the ONE notebook that runs as a loop since the fault injection
# needs to be a continuous degradation visible in the dashboard.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EVENTHUB_CONNECTION_STRING = ""  # SimulatorTelemetryStream connection string

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, math, random, time, os, requests
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
    .filter("simulator_id = 'SIM-001'").collect()]
print(f"{len(sensor_defs)} SIM-001 sensors loaded")

FAULTS = {
    "Hydraulic Pressure":          {"start": 0,  "rate": -60.0},
    "Hydraulic Fluid Temperature": {"start": 2,  "rate":   3.5},
    "Hydraulic Flow Rate":         {"start": 3,  "rate":  -4.0},
    "Motion Platform Vibration X": {"start": 5,  "rate":   0.012},
    "Motion Platform Vibration Y": {"start": 5,  "rate":   0.010},
    "Base Frame Vibration":        {"start": 6,  "rate":   0.006},
    "Motion Platform Temperature": {"start": 4,  "rate":   2.0},
    "Power Consumption":           {"start": 3,  "rate":   5.0},
}

INTERVAL = 60  # 1 minute between batches
DURATION_MIN = 10.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start = time.time()
batch_num = 0

print(f"Injecting fault on SIM-001 for {DURATION_MIN} min (1 batch/min)...\n")

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
                "timestamp": ts, "simulator_id": "SIM-001",
                "sensor_id": s["sensor_id"], "sensor_category": s["sensor_category"],
                "sensor_name": s["sensor_name"], "value": round(val, 4),
                "unit": s["unit"], "alert_level": lvl, "is_anomaly": lvl != "Normal",
            })

        if EVENTHUB_CONNECTION_STRING:
            from azure.eventhub import EventHubProducerClient, EventData
            p = EventHubProducerClient.from_connection_string(EVENTHUB_CONNECTION_STRING)
            with p:
                b = p.create_batch()
                for e in events:
                    b.add(EventData(json.dumps(e)))
                p.send_batch(b)
        else:
            spark.createDataFrame(events).write.format("delta").mode("append").save(f"{BASE}/Tables/simulator_telemetry_raw")

        w = sum(1 for e in events if e["alert_level"] == "Warning")
        c = sum(1 for e in events if e["alert_level"] == "Critical")
        batch_num += 1
        print(f"  [{elapsed:5.1f} min] batch {batch_num}  Normal:{len(events)-w-c}  Warn:{w}  Crit:{c}")
        for e in events:
            if e["alert_level"] != "Normal":
                print(f"      {e['sensor_name']}: {e['value']} {e['unit']} [{e['alert_level']}]")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass

print(f"\nFault injection complete. {batch_num} batches over {DURATION_MIN} min.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
