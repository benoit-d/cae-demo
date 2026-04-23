# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Clock-In Events - Single Batch
# 
# Generates ONE batch of workforce events (badge in/out, task start/complete)
# and ingests directly into the KQL Database via the Kusto streaming ingestion API.
# 
# **Designed to be called by a Data Pipeline** (or run manually for demos).
# No EventHub or Eventstream needed.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

KQL_URI = ""  # Leave empty to auto-discover from Eventhouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, random, os, requests
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

# Send to KQL Database via streaming ingestion
TOKEN_KQL = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")

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

# Build CSV for streaming ingestion
def esc(v):
    s = str(v).replace('"', '""')
    return f'"{s}"' if ',' in s or '"' in s else s

csv_lines = []
for e in events:
    csv_lines.append(",".join([
        e["timestamp"], esc(e["event_type"]), esc(e["employee_email"]),
        esc(e["employee_name"]), e["employee_id"], esc(e["department"]),
        e["project_id"], e["task_id"], e["simulator_id"], esc(e["details"])
    ]))

csv_payload = "\n".join(csv_lines)

ingest_url = f"{KQL_URI}/v1/rest/ingest/{DB_NAME}/ClockInEvents?streamFormat=Csv"
ingest_headers = {
    "Authorization": f"Bearer {TOKEN_KQL}",
    "Content-Type": "text/csv",
}
ingest_resp = requests.post(ingest_url, headers=ingest_headers, data=csv_payload)

if ingest_resp.status_code == 200:
    print(f"Ingested {len(events)} clock-in events")
else:
    print(f"Ingestion failed ({ingest_resp.status_code}): {ingest_resp.text[:300]}")
    df = spark.createDataFrame(events)
    df.write.format("delta").mode("append").save(f"{BASE}/Tables/clockin_events_raw")
    print(f"Fallback: wrote {len(events)} events to Lakehouse staging")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
