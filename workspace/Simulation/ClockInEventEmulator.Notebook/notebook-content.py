# Fabric notebook source
# %% [markdown]
# # Clock-In / Task Event Emulator
#
# Generates workforce events and sends them to the `ClockInEventStream`:
# - **badge_in / badge_out** — shift start / end
# - **task_start / task_complete** — links to Project_ID, Task_ID
# - **break_start / break_end**
#
# Events carry employee email + project/task IDs so downstream agents
# can update Actual dates in the SQL Database.

# %%
import csv, json, random, time
from datetime import datetime, timezone

# Load reference data from staging Lakehouse
employees = [r.asDict() for r in spark.read.csv("Files/data/hr/employees.csv", header=True).collect()]
tasks     = [r.asDict() for r in spark.read.csv("Files/data/cosmosdb/tasks.csv", header=True).collect()]
projects  = [r.asDict() for r in spark.read.csv("Files/data/cosmosdb/projects.csv", header=True).collect()]

proj_map = {p["Project_ID"]: p for p in projects}
workers  = [e for e in employees if e["employee_id"] != "EMP-050"]

in_progress = [t for t in tasks if t.get("Complete_Percentage") and 0 < int(t["Complete_Percentage"]) < 100]

print(f"{len(workers)} workers, {len(in_progress)} in-progress tasks")

# %%
CONN = spark.conf.get("spark.cae.clockin.eventhub.connectionString", "")

def send(events):
    if CONN:
        from azure.eventhub import EventHubProducerClient, EventData
        p = EventHubProducerClient.from_connection_string(CONN)
        with p:
            b = p.create_batch()
            for e in events: b.add(EventData(json.dumps(e)))
            p.send_batch(b)
    else:
        for e in events:
            spark.createDataFrame([e]).write.format("delta").mode("append").save("Tables/clockin_events_raw")

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

# %%
# Generate a full-day scenario
events = []

# Badge in
for w in workers:
    events.append(mk("badge_in", w, details="Shift start"))

# Task starts for in-progress work
for t in in_progress:
    e = next((w for w in workers if w["email"] == t["Resource_Login"]), None)
    p = proj_map.get(t["Parent_Project_ID"], {})
    if e:
        events.append(mk("task_start", e,
            project_id=t["Parent_Project_ID"], task_id=t["Task_ID"],
            simulator_id=p.get("Simulator_ID", ""),
            details=f"Starting: {t['Task_Name']}"))

# Random breaks
for w in random.sample(workers, min(4, len(workers))):
    events.append(mk("break_start", w, details="Break"))
    events.append(mk("break_end", w, details="Back from break"))

# Task completions
for t in in_progress:
    e = next((w for w in workers if w["email"] == t["Resource_Login"]), None)
    p = proj_map.get(t["Parent_Project_ID"], {})
    if e:
        events.append(mk("task_complete", e,
            project_id=t["Parent_Project_ID"], task_id=t["Task_ID"],
            simulator_id=p.get("Simulator_ID", ""),
            details=f"Completed: {t['Task_Name']}"))

# Badge out
for w in workers:
    events.append(mk("badge_out", w, details="Shift end"))

print(f"Generated {len(events)} events")
send(events)
print("Events sent.")
