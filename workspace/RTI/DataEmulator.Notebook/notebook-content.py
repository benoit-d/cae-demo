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
# Sends **telemetry** (107 sensors x 20 machines) and **clock-in events**
# (badge in/out, task start/complete) to their respective EventStreams every
# `INTERVAL_SEC` seconds for `DURATION_MIN` minutes.
# 
# **Fault injection** is built in. To trigger during a demo:
# 1. Open Lakehouse > Files > config > `connections.json`
# 2. Set `"FAULT_INJECTION": "true"`
# 3. The next loop iteration picks it up and starts a 10-minute bearing failure on CNC-003
# 4. After the fault window completes, it auto-resets `FAULT_INJECTION` back to `"false"`
# 
# One Spark session, one loop, all streams. **Run All to start. Cancel to stop.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
INTERVAL_SEC = 60          # Seconds between batches
DURATION_MIN = 480         # Total run time in minutes (0 = single batch then exit)
FAULT_DURATION_MIN = 10    # How long a fault injection lasts once triggered
FAULT_MACHINE = "CNC-003"  # Machine to inject faults on
print(f"Data Emulator: interval={INTERVAL_SEC}s, duration={DURATION_MIN}min, fault={FAULT_MACHINE}/{FAULT_DURATION_MIN}min")

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
CONFIG_PATH = f"{BASE}/config/connections.json"

# Load reference data
sensor_defs = [row.asDict() for row in
    spark.read.csv(f"{BASE}/Files/data/telemetry/sensor_definitions.csv", header=True, inferSchema=True).collect()]
employees = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/hr/employees.csv", header=True).collect()]
tasks = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/plm/tasks.csv", header=True).collect()]
projects = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/plm/projects.csv", header=True).collect()]

proj_map = {p["Project_ID"]: p for p in projects}
workers = [e for e in employees if e["employee_id"] != "EMP-050"]
in_progress = [t for t in tasks if t.get("Complete_Percentage") and 0 < int(t["Complete_Percentage"]) < 100]

# Read connection strings
config = json.loads(notebookutils.fs.head(CONFIG_PATH, 10000))
TELEMETRY_CONN = config.get("TELEMETRY_EVENTSTREAM_CONNECTION_STRING", "")
CLOCKIN_CONN = config.get("CLOCKIN_EVENTSTREAM_CONNECTION_STRING", "")

if not TELEMETRY_CONN:
    raise RuntimeError("TELEMETRY_EVENTSTREAM_CONNECTION_STRING is empty in connections.json")
if not CLOCKIN_CONN:
    raise RuntimeError("CLOCKIN_EVENTSTREAM_CONNECTION_STRING is empty in connections.json")

print(f"Loaded: {len(sensor_defs)} sensors, {len(workers)} workers, {len(in_progress)} in-progress tasks")
print("Connection strings loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main loop: telemetry + clock-in + fault injection (trigger via connections.json)
from azure.eventhub import EventHubProducerClient, EventData

# CNC-003 spindle bearing failure profile — rates tuned so sensors cross
# Critical thresholds (vib>0.20g, temp>70C, coolant<3LPM, power>26kW) in ~5 min
FAULT_PROFILE = {
    "Spindle Vibration":      {"start": 0,  "rate": 0.04},
    "Spindle Temperature":    {"start": 1,  "rate": 7.0},
    "Coolant Flow Rate":      {"start": 2,  "rate": -2.5},
    "Axis Position Accuracy": {"start": 3,  "rate": 0.003},
    "Power Consumption":      {"start": 1,  "rate": 4.0},
}

def generate_telemetry(sensor_defs, fault_active, fault_elapsed_min):
    ts = datetime.now(timezone.utc).isoformat()
    epoch = time.time()
    events = []
    for s in sensor_defs:
        nmin, nmax = float(s["normal_min"]), float(s["normal_max"])
        wmin, wmax = float(s["warning_min"]), float(s["warning_max"])
        cmin, cmax = float(s["critical_min"]), float(s["critical_max"])
        mid = (nmin + nmax) / 2
        amp = (nmax - nmin) / 2
        period = 120 + hash(s["sensor_id"]) % 180
        phase = hash(s["sensor_id"]) % 1000 / 1000 * 2 * math.pi
        val = mid + amp * 0.5 * math.sin(2 * math.pi * epoch / period + phase)
        val += random.gauss(0, amp * 0.05)

        lvl = "Normal"
        if fault_active and s["machine_id"] == FAULT_MACHINE:
            fp = FAULT_PROFILE.get(s["sensor_name"])
            if fp and fault_elapsed_min >= fp["start"]:
                val += fp["rate"] * (fault_elapsed_min - fp["start"])
            if val < cmin or val > cmax:   lvl = "Critical"
            elif val < wmin or val > wmax: lvl = "Warning"

        events.append({
            "timestamp": ts, "machine_id": s["machine_id"],
            "sensor_id": s["sensor_id"], "sensor_category": s["sensor_category"],
            "sensor_name": s["sensor_name"], "value": round(val, 4),
            "unit": s["unit"], "alert_level": lvl,
            "is_anomaly": str(lvl != "Normal"),
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
            "employee_id": w["employee_id"], "department": w["department"],
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
                "event_type": etype, "employee_email": e["email"],
                "employee_name": f"{e['first_name']} {e['last_name']}",
                "employee_id": e["employee_id"], "department": e["department"],
                "project_id": t["Parent_Project_ID"], "task_id": t["Task_ID"],
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

def read_config_key(key, default="false"):
    try:
        raw = notebookutils.fs.head(CONFIG_PATH, 10000)
        return json.loads(raw).get(key, default)
    except Exception:
        return default

def write_config_key(key, value):
    try:
        raw = notebookutils.fs.head(CONFIG_PATH, 10000)
        cfg = json.loads(raw)
        cfg[key] = value
        storage_token = notebookutils.credentials.getToken("https://storage.azure.com")
        sh = {"Authorization": f"Bearer {storage_token}"}
        url = CONFIG_PATH.replace(
            f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/",
            f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/")
        data = json.dumps(cfg, indent=2).encode("utf-8")
        requests.put(f"{url}?resource=file", headers=sh)
        requests.patch(f"{url}?action=append&position=0",
                      headers={**sh, "Content-Type": "application/octet-stream"}, data=data)
        requests.patch(f"{url}?action=flush&position={len(data)}", headers=sh)
    except Exception as e:
        print(f"  Warning: could not update config: {e}")

# --- Main loop ---
end_time = time.time() + DURATION_MIN * 60 if DURATION_MIN > 0 else 0
batch_count = 0
fault_active = False
fault_start_time = 0

try:
    while True:
        # Re-read FAULT_INJECTION from connections.json each iteration
        fault_requested = str(read_config_key("FAULT_INJECTION")).lower() == "true"

        if fault_requested and not fault_active:
            fault_active = True
            fault_start_time = time.time()
            print(f"\n>>> FAULT INJECTION on {FAULT_MACHINE} — {FAULT_DURATION_MIN} min window <<<\n")

        fault_elapsed_min = 0
        if fault_active:
            fault_elapsed_min = (time.time() - fault_start_time) / 60
            if fault_elapsed_min >= FAULT_DURATION_MIN:
                fault_active = False
                write_config_key("FAULT_INJECTION", "false")
                print(f"\n>>> FAULT COMPLETE — {FAULT_MACHINE} back to normal <<<\n")
                fault_elapsed_min = 0

        # Telemetry (with fault overlay if active)
        telemetry = generate_telemetry(sensor_defs, fault_active, fault_elapsed_min)
        send_batch(TELEMETRY_CONN, telemetry)

        # Clock-in events
        clockin = generate_clockin(workers, in_progress, proj_map)
        send_batch(CLOCKIN_CONN, clockin)

        batch_count += 1
        warn = sum(1 for e in telemetry if e["alert_level"] == "Warning")
        crit = sum(1 for e in telemetry if e["alert_level"] == "Critical")
        tag = f" FAULT[{fault_elapsed_min:.0f}m]" if fault_active else ""
        print(f"[{batch_count}] {len(telemetry)} telemetry (W:{warn} C:{crit}) + {len(clockin)} clock-in{tag}")

        if fault_active:
            for e in telemetry:
                if e["alert_level"] != "Normal":
                    print(f"    {e['machine_id']} {e['sensor_name']}: {e['value']} {e['unit']} [{e['alert_level']}]")

        if DURATION_MIN == 0:
            break
        if time.time() >= end_time:
            print(f"Duration reached ({DURATION_MIN} min). Stopping.")
            break
        time.sleep(INTERVAL_SEC)
except KeyboardInterrupt:
    print(f"Stopped after {batch_count} batches.")

if fault_active:
    write_config_key("FAULT_INJECTION", "false")
    print("Fault injection reset on exit.")

print(f"Done. {batch_count} batches sent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
