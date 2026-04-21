# Fabric notebook source
# %% [markdown]
# # Capacity Management Agent
#
# An **agentic AI workflow** using Azure AI Foundry that reasons across all
# data sources to optimise manufacturing capacity.
#
# ## Data sources the agent queries
# 1. **SQL Database** — projects, tasks, employees, skills, schedules, limitations
# 2. **Eventhouse** — real-time telemetry alerts, clock-in events
# 3. **Inventory / BOM** — parts availability for maintenance
#
# ## Prerequisites
# - All data loaded (PostDeploymentConfig completed)
# - Azure OpenAI endpoint configured (or runs in demo/offline mode)

# %%
%pip install -q azure-ai-projects azure-identity

# %%
from pyspark.sql import SparkSession
import json, os
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

# ── SQL Database connection setup ──
import notebookutils
ws_items = notebookutils.fabric.list_items()
sql_db = next((i for i in ws_items if 'SQLDB' in i.get('displayName', '') or 'SQLDatabase' in i.get('type', '')), None)
token = notebookutils.credentials.getToken("https://database.windows.net/")

jdbc_url = (
    f"jdbc:sqlserver://{sql_db['displayName']}.database.fabric.microsoft.com:1433;"
    f"database={sql_db['displayName']};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
) if sql_db else ""
jdbc_props = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "accessToken": token} if sql_db else {}

def sql_query(query):
    """Run a SQL query and return results as list of dicts."""
    return [r.asDict() for r in spark.read.jdbc(
        url=jdbc_url, table=f"({query}) q", properties=jdbc_props
    ).collect()]

print("SQL connection ready." if sql_db else "SQL Database not found.")

# %%
# ── Agent tool definitions ──

def get_manufacturing_floor_status():
    """Current status of all 8 projects with completion %."""
    rows = sql_query("""
        SELECT p.Project_ID, p.Project_Name, p.Simulator_ID, m.customer,
               p.Modified_Planned_Start, p.Standard_Duration, p.Complete_Percentage,
               m.status AS sim_status
        FROM dbo.projects p
        LEFT JOIN dbo.machines m ON p.Simulator_ID = m.simulator_id
        ORDER BY p.Modified_Planned_Start
    """)
    return json.dumps(rows, default=str)

def get_active_tasks(project_id=None):
    """Get tasks that are in-progress or next-up."""
    where = f"AND t.Parent_Project_ID = '{project_id}'" if project_id else ""
    rows = sql_query(f"""
        SELECT t.Task_ID, t.Task_Name, t.Parent_Project_ID, t.FS_Task_ID,
               t.Skill_Requirement, t.Modified_Planned_Start, t.Standard_Duration,
               t.Actual_Start, t.Actual_End, t.Complete_Percentage, t.Resource_Login,
               e.first_name + ' ' + e.last_name AS employee_name
        FROM dbo.tasks t
        LEFT JOIN dbo.employees e ON t.Resource_Login = e.email
        WHERE t.Complete_Percentage < 100 {where}
        ORDER BY t.Modified_Planned_Start
    """)
    return json.dumps(rows, default=str)

def get_available_employees(skill=None):
    """Employees with their skills, filtering on leave and limitations."""
    skill_filter = f"AND sc.skill_category = '{skill}'" if skill else ""
    rows = sql_query(f"""
        SELECT e.employee_id, e.email, e.first_name, e.last_name, e.department,
               e.employee_type, e.shift_preference,
               sc.skill_category, sc.skill_name, sc.certification_level,
               pl.limitation_type, pl.description AS limitation_desc
        FROM dbo.employees e
        LEFT JOIN dbo.skills_certifications sc ON e.employee_id = sc.employee_id
            AND sc.is_current = 'Yes' {skill_filter}
        LEFT JOIN dbo.physical_limitations pl ON e.employee_id = pl.employee_id
        WHERE e.employment_status = 'Active' AND e.employee_id != 'EMP-050'
        ORDER BY e.employee_id
    """)
    return json.dumps(rows, default=str)

def get_scheduling_constraints():
    """Union rules, agreements, contractor terms."""
    agreements = sql_query("SELECT * FROM dbo.employee_agreements")
    contractors = sql_query("SELECT * FROM dbo.contractual_workforce")
    return json.dumps({"agreements": agreements, "contractors": contractors}, default=str)

def get_telemetry_alerts():
    """Recent anomalous telemetry readings."""
    try:
        df = spark.read.format("delta").load("Tables/simulator_telemetry_raw")
        alerts = df.filter("alert_level != 'Normal'").orderBy("timestamp", ascending=False).limit(50)
        return alerts.toPandas().to_json(orient="records")
    except Exception:
        return json.dumps({"message": "No telemetry data yet. Start the emulator."})

def get_parts_availability(part_numbers):
    """Check inventory for specific parts."""
    pn_list = ",".join(f"'{p}'" for p in part_numbers)
    inv = sql_query(f"SELECT * FROM dbo.inventory WHERE part_number IN ({pn_list})")
    pos = sql_query(f"SELECT * FROM dbo.purchase_orders WHERE part_number IN ({pn_list})")
    return json.dumps({"inventory": inv, "purchase_orders": pos}, default=str)

TOOLS = {
    "get_manufacturing_floor_status": get_manufacturing_floor_status,
    "get_active_tasks": get_active_tasks,
    "get_available_employees": get_available_employees,
    "get_scheduling_constraints": get_scheduling_constraints,
    "get_telemetry_alerts": get_telemetry_alerts,
    "get_parts_availability": get_parts_availability,
}

print(f"{len(TOOLS)} agent tools registered.")

# %%
# ── Demo: Morning briefing ──
from datetime import date

print("=== Manufacturing Floor Status ===\n")
status = json.loads(get_manufacturing_floor_status())
for p in status:
    print(f"  {p['Project_ID']} | {p['Project_Name'][:45]:45s} | {p['Complete_Percentage']:>3d}% | {p.get('sim_status','')}")

print("\n=== In-Progress Tasks ===\n")
active = json.loads(get_active_tasks())
for t in active[:10]:
    print(f"  {t['Task_ID']:18s} {t['Task_Name'][:35]:35s} {t['Resource_Login']:35s} {t['Complete_Percentage']:>3d}%")

print("\n=== Telemetry Alerts ===\n")
alerts = json.loads(get_telemetry_alerts())
if isinstance(alerts, list):
    for a in alerts[:5]:
        print(f"  {a.get('sensor_name','')} = {a.get('value','')} [{a.get('alert_level','')}]")
else:
    print(f"  {alerts.get('message', 'No data')}")
