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

# # Clock-In Events - Single Batch
# 
# Generates ONE batch of workforce events (badge in/out, task start/complete)
# and sends them to the **ClockInEventStream** via its Event Hub-compatible
# custom endpoint. The EventStream routes data to the Eventhouse KQL Database.
# 
# **Designed to be called by ClockInPipeline** (or run manually for demos).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration
EVENTSTREAM_NAME = "ClockInEventStream"
CONFIG_KEY = "CLOCKIN_EVENTSTREAM_CONNECTION_STRING"
print(f"Target: {EVENTSTREAM_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, random, os, requests, base64
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
    raise RuntimeError("Lakehouse not found (needed to read staged CSVs)")

LH_ID = lh["id"]
BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}"

employees = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/hr/employees.csv", header=True).collect()]
tasks = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/plm/tasks.csv", header=True).collect()]
projects = [r.asDict() for r in spark.read.csv(f"{BASE}/Files/data/plm/projects.csv", header=True).collect()]

proj_map = {p["Project_ID"]: p for p in projects}
workers = [e for e in employees if e["employee_id"] != "EMP-050"]
in_progress = [t for t in tasks if t.get("Complete_Percentage") and 0 < int(t["Complete_Percentage"]) < 100]

print(f"{len(workers)} workers, {len(in_progress)} in-progress tasks")

# Read EventStream connection string from Lakehouse config file
CONFIG_PATH = f"{BASE}/config/connections.json"
try:
    config = json.loads(notebookutils.fs.head(CONFIG_PATH, 10000))
    EVENTSTREAM_CONNECTION_STRING = config.get(CONFIG_KEY, "")
    if EVENTSTREAM_CONNECTION_STRING:
        print(f"Loaded connection string from config ({CONFIG_KEY})")
    else:
        raise RuntimeError(
            f"{CONFIG_KEY} is empty in {CONFIG_PATH}. "
            f"Open '{EVENTSTREAM_NAME}' in Fabric UI → Custom Endpoint source → "
            f"copy the Event Hub connection string into the config file."
        )
except FileNotFoundError:
    raise RuntimeError(
        f"Config file not found: {CONFIG_PATH}. Run PostDeploymentConfig first."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def mk(etype, emp, **kw):
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": etype,
        "employee_email": emp["email"],
        "employee_name": f"{emp['first_name']} {emp['last_name']}",
        "employee_id": emp["employee_id"],
        "department": emp["department"],
        "project_id": kw.get("project_id", ""),
        "task_id": kw.get("task_id", ""),
        "simulator_id": kw.get("simulator_id", ""),
        "details": kw.get("details", ""),
    }

# Randomly pick a few events per batch (not a full day each time)
events = []

# 2-3 random badge events
for w in random.sample(workers, min(3, len(workers))):
    events.append(mk(random.choice(["badge_in", "badge_out"]), w, details="Shift event"))

# 1-2 task events for in-progress tasks
for t in random.sample(in_progress, min(2, len(in_progress))):
    e = next((w for w in workers if w["email"] == t["Resource_Login"]), None)
    p = proj_map.get(t["Parent_Project_ID"], {})
    if e:
        etype = random.choice(["task_start", "task_complete"])
        events.append(mk(etype, e,
            project_id=t["Parent_Project_ID"], task_id=t["Task_ID"],
            simulator_id=p.get("Simulator_ID", ""),
            details=f"{etype}: {t['Task_Name']}"))

# Send to EventStream via Event Hub SDK
from azure.eventhub import EventHubProducerClient, EventData

producer = EventHubProducerClient.from_connection_string(EVENTSTREAM_CONNECTION_STRING)
with producer:
    batch = producer.create_batch()
    for ev in events:
        batch.add(EventData(json.dumps(ev)))
    producer.send_batch(batch)

print(f"Sent {len(events)} clock-in events to {EVENTSTREAM_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
