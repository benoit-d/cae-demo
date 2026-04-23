# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Telemetry Fault Injection
# 
# Simulates progressive machine failures by injecting degrading sensor
# readings directly into the KQL Database via Kusto streaming ingestion.
# 
# **Fault Profiles:**
# - **CNC-001**: Spindle bearing failure — vibration increases, temperature rises,
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

# === CONFIGURATION ===
TARGET_MACHINE = "CNC-001"      # Machine to inject faults on
KQL_URI = ""                    # Leave empty to auto-discover from Eventhouse
INTERVAL = 60                   # Seconds between batches
DURATION_MIN = 10.0             # Total fault injection duration in minutes

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
    .filter(f"machine_id = '{TARGET_MACHINE}'").collect()]
print(f"{len(sensor_defs)} sensors loaded for {TARGET_MACHINE}")

# Auto-discover KQL URI from Eventhouse
TOKEN_KQL = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")
if not KQL_URI:
    eh = next((i for i in items_list if i.get("displayName") == "CAEManufacturingEH"), None)
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

# Fault profile: spindle bearing failure on CNC mill
FAULTS = {
    "Spindle Vibration":       {"start": 0,  "rate": 0.015},    # g increase per minute
    "Spindle Temperature":     {"start": 1,  "rate": 3.0},      # C rise per minute
    "Coolant Flow Rate":       {"start": 3,  "rate": -1.5},     # LPM drop per minute
    "Axis Position Accuracy":  {"start": 5,  "rate": 0.001},    # mm drift per minute
    "Power Consumption":       {"start": 2,  "rate": 2.0},      # kW rise per minute
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

        # Send to KQL Database via Kusto streaming ingestion
        csv_lines = []
        for e in events:
            csv_lines.append(
                f"{e['timestamp']},{e['machine_id']},{e['sensor_id']},"
                f"{e['sensor_category']},{e['sensor_name']},{e['value']},"
                f"{e['unit']},{e['alert_level']},{e['is_anomaly']}"
            )
        csv_payload = "\n".join(csv_lines)

        ingest_url = f"{KQL_URI}/v1/rest/ingest/{DB_NAME}/MachineTelemetry?streamFormat=Csv"
        ingest_headers = {
            "Authorization": f"Bearer {TOKEN_KQL}",
            "Content-Type": "text/csv",
        }
        ingest_resp = requests.post(ingest_url, headers=ingest_headers, data=csv_payload)

        w = sum(1 for e in events if e["alert_level"] == "Warning")
        c = sum(1 for e in events if e["alert_level"] == "Critical")
        batch_num += 1

        status = "OK" if ingest_resp.status_code == 200 else f"FAIL({ingest_resp.status_code})"
        print(f"  [{elapsed:5.1f} min] batch {batch_num}  Normal:{len(events)-w-c}  Warn:{w}  Crit:{c}  KQL:{status}")
        for e in events:
            if e["alert_level"] != "Normal":
                print(f"      {e['sensor_name']}: {e['value']} {e['unit']} [{e['alert_level']}]")

        if ingest_resp.status_code != 200:
            print(f"      Ingestion error: {ingest_resp.text[:200]}")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass

print(f"\nFault injection complete. {batch_num} batches sent to KQL.")
print(f"Check alerts: MachineHealthAlerts() | where machine_id == '{TARGET_MACHINE}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
