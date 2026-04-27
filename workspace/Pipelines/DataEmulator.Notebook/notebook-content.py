# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "94300b2e-c8c6-4807-8f2f-7de502f4349c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Emulator
# 
# Sends **telemetry** (107 sensors × 20 machines) and **clock-in events**
# (badge in/out, task start/complete) to their respective EventStreams every
# `INTERVAL_SEC` seconds for `DURATION_MIN` minutes.
# 
# One Spark session, one loop, both streams — no pipelines needed.
# 
# **Run All to start. Cancel the notebook to stop early.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
INTERVAL_SEC = 60        # Seconds between batches
DURATION_MIN = 480       # Total run time in minutes (0 = single batch, then exit)
print(f"Data Emulator: interval={INTERVAL_SEC}s, duration={DURATION_MIN}min")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load config + reference data
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
items = resp.json().get("value", [])
lh = next((i for i in items if i.get("displayName") == "CAEManufacturing_LH" and i.get("type") == "Lakehouse"), None)
if not lh:
    raise RuntimeError("Lakehouse not found")

LH_ID = lh["id"]
BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}"

# Load reference data for both emulators
sensor_defs = [row.asDict() for row in
    spark.read.csv(f"{BASE}/Files/data/telemetry/sensor_definitions.csv", header=True, inferSchema=True).collect()]
employees = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/hr/employees.csv", header=True).collect()]
tasks = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/plm/tasks.csv", header=True).collect()]
projects = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/plm/projects.csv", header=True).collect()]

proj_map = {p["Project_ID"]: p for p in projects}
workers = [e for e in employees if e["employee_id"] != "EMP-050"]
in_progress = [t for t in tasks if t.get("Complete_Percentage") and 0 < int(t["Complete_Percentage"]) < 100]

print(f"Loaded: {len(sensor_defs)} sensors, {len(workers)} workers, {len(in_progress)} in-progress tasks")

# Read connection strings from config
CONFIG_PATH = f"{BASE}/config/connections.json"
config = json.loads(notebookutils.fs.head(CONFIG_PATH, 10000))

TELEMETRY_CONN = config.get("TELEMETRY_EVENTSTREAM_CONNECTION_STRING", "")
CLOCKIN_CONN = config.get("CLOCKIN_EVENTSTREAM_CONNECTION_STRING", "")

if not TELEMETRY_CONN:
    raise RuntimeError("TELEMETRY_EVENTSTREAM_CONNECTION_STRING is empty in connections.json")
if not CLOCKIN_CONN:
    raise RuntimeError("CLOCKIN_EVENTSTREAM_CONNECTION_STRING is empty in connections.json")

print("Connection strings loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main loop — send telemetry + clock-in events every INTERVAL_SEC seconds
from azure.eventhub import EventHubProducerClient, EventData

def generate_telemetry(sensor_defs):
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
            "timestamp": ts, "machine_id": s["machine_id"],
            "sensor_id": s["sensor_id"], "sensor_category": s["sensor_category"],
            "sensor_name": s["sensor_name"], "value": round(val, 4),
            "unit": s["unit"], "alert_level": "Normal", "is_anomaly": False,
        })
    return events

def generate_clockin(workers, in_progress, proj_map):
    events = []
    for w in random.sample(workers, min(3, len(workers))):
        events.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": random.choice(["badge_in", "badge_out"]),
            "employee_email": w["email"],
            "employee_name": f"{w['first_name']} {w['last_name']}",
            "employee_id": w["employee_id"],
            "department": w["department"],
            "project_id": "", "task_id": "", "simulator_id": "",
            "details": "Shift event",
        })
    for t in random.sample(in_progress, min(2, len(in_progress))):
        e = next((w for w in workers if w["email"] == t["Resource_Login"]), None)
        p = proj_map.get(t["Parent_Project_ID"], {})
        if e:
            etype = random.choice(["task_start", "task_complete"])
            events.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": etype,
                "employee_email": e["email"],
                "employee_name": f"{e['first_name']} {e['last_name']}",
                "employee_id": e["employee_id"],
                "department": e["department"],
                "project_id": t["Parent_Project_ID"],
                "task_id": t["Task_ID"],
                "simulator_id": p.get("Simulator_ID", ""),
                "details": f"{etype}: {t['Task_Name']}",
            })
    return events

def send_batch(conn_str, events):
    producer = EventHubProducerClient.from_connection_string(conn_str)
    with producer:
        batch = producer.create_batch()
        for e in events:
            batch.add(EventData(json.dumps(e)))
        producer.send_batch(batch)

end_time = time.time() + DURATION_MIN * 60 if DURATION_MIN > 0 else 0
batch_count = 0

try:
    while True:
        # Telemetry
        telemetry = generate_telemetry(sensor_defs)
        send_batch(TELEMETRY_CONN, telemetry)

        # Clock-in
        clockin = generate_clockin(workers, in_progress, proj_map)
        send_batch(CLOCKIN_CONN, clockin)

        batch_count += 1
        print(f"[{batch_count}] {len(telemetry)} telemetry + {len(clockin)} clock-in events")

        if DURATION_MIN == 0:
            break
        if time.time() >= end_time:
            print(f"Duration reached ({DURATION_MIN} min). Stopping.")
            break
        time.sleep(INTERVAL_SEC)
except KeyboardInterrupt:
    print(f"Stopped after {batch_count} batches.")

print(f"Done. {batch_count} batches sent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
