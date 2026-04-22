# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Capacity Management Agent
# 
# An agentic AI workflow that reasons across all data sources
# to optimise manufacturing capacity.
# 
# Data sources: Lakehouse tables (projects, tasks, employees, skills),
# telemetry Delta tables, inventory, and BOM.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import json
spark = SparkSession.builder.getOrCreate()

# Agent tool functions - query the Lakehouse Delta tables
def get_manufacturing_floor_status():
    rows = spark.sql("""
        SELECT p.Project_ID, p.Project_Name, p.Simulator_ID,
               m.customer, p.Modified_Planned_Start, p.Standard_Duration,
               p.Complete_Percentage, m.status AS sim_status
        FROM projects p
        LEFT JOIN machines m ON p.Simulator_ID = m.simulator_id
        ORDER BY p.Modified_Planned_Start
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def get_active_tasks(project_id=None):
    where = f"AND t.Parent_Project_ID = '{project_id}'" if project_id else ""
    rows = spark.sql(f"""
        SELECT t.Task_ID, t.Task_Name, t.Parent_Project_ID,
               t.Skill_Requirement, t.Modified_Planned_Start,
               t.Standard_Duration, t.Actual_Start, t.Actual_End,
               t.Complete_Percentage, t.Resource_Login
        FROM tasks t
        WHERE t.Complete_Percentage < 100 {where}
        ORDER BY t.Modified_Planned_Start
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def get_available_employees(skill=None):
    skill_filter = f"AND sc.skill_category = '{skill}'" if skill else ""
    rows = spark.sql(f"""
        SELECT e.employee_id, e.email, e.first_name, e.last_name,
               e.department, e.employee_type, e.shift_preference,
               sc.skill_category, sc.skill_name, sc.certification_level,
               pl.limitation_type, pl.description AS limitation_desc
        FROM employees e
        LEFT JOIN skills_certifications sc ON e.employee_id = sc.employee_id
            AND sc.is_current = 'Yes' {skill_filter}
        LEFT JOIN physical_limitations pl ON e.employee_id = pl.employee_id
        WHERE e.employment_status = 'Active' AND e.employee_id != 'EMP-050'
        ORDER BY e.employee_id
    """).collect()
    return json.dumps([r.asDict() for r in rows], default=str)

def get_telemetry_alerts():
    try:
        df = spark.read.format("delta").load("Tables/simulator_telemetry_raw")
        alerts = df.filter("alert_level != 'Normal'").orderBy("timestamp", ascending=False).limit(50)
        return json.dumps([r.asDict() for r in alerts.collect()], default=str)
    except Exception:
        return json.dumps({"message": "No telemetry data yet. Start the emulator."})

print("Agent tools loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Demo: Morning briefing
print("=== Manufacturing Floor Status ===\n")
status = json.loads(get_manufacturing_floor_status())
for p in status:
    print(f"  {p['Project_ID']} | {str(p.get('Project_Name',''))[:45]:45s} | {p['Complete_Percentage']:>3d}%")

print("\n=== In-Progress Tasks ===\n")
active = json.loads(get_active_tasks())
for t in active[:10]:
    print(f"  {t['Task_ID']:18s} {str(t['Task_Name'])[:35]:35s} {str(t['Resource_Login']):35s} {t['Complete_Percentage']:>3d}%")

print("\n=== Telemetry Alerts ===\n")
alerts = json.loads(get_telemetry_alerts())
if isinstance(alerts, list) and len(alerts) > 0:
    for a in alerts[:5]:
        print(f"  {a.get('sensor_name','')} = {a.get('value','')} [{a.get('alert_level','')}]")
else:
    msg = alerts.get("message", "No alerts") if isinstance(alerts, dict) else "No alerts"
    print(f"  {msg}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
