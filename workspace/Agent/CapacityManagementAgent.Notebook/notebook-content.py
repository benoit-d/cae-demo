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
# Data sources:
# - **SQL Database** (hr.* and erp.*) - employees, skills, projects, tasks, machines, inventory
# - **Eventhouse / KQL DB** - real-time telemetry alerts, clock-in events

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
# Paste the same JDBC string used in PostDeploymentConfig
SQL_JDBC_CONNECTION_STRING = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, re, os, struct, requests
import notebookutils
import pyodbc

TOKEN_SQL = notebookutils.credentials.getToken("https://database.windows.net/")

if not SQL_JDBC_CONNECTION_STRING:
    raise RuntimeError("Set SQL_JDBC_CONNECTION_STRING in the config cell above.")

sm = re.search(r'sqlserver://([^:;]+)', SQL_JDBC_CONNECTION_STRING)
dm = re.search(r'database=\{?([^};]+)\}?', SQL_JDBC_CONNECTION_STRING)
SQL_ENDPOINT = sm.group(1) if sm else ""
SQL_DBNAME = dm.group(1) if dm else ""

conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={SQL_ENDPOINT};Database={SQL_DBNAME};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)
token_bytes = TOKEN_SQL.encode("utf-16-le")
token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


def sql_query(query):
    """Run a SQL query and return results as list of dicts."""
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows

print(f"Connected to {SQL_DBNAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agent tool functions - query the SQL Database

def get_manufacturing_floor_status():
    rows = sql_query("""
        SELECT p.Project_ID, p.Project_Name, p.Simulator_ID, m.customer,
               p.Modified_Planned_Start, p.Standard_Duration,
               p.Complete_Percentage, m.status AS sim_status
        FROM erp.projects p
        LEFT JOIN erp.machines m ON p.Simulator_ID = m.simulator_id
        ORDER BY p.Modified_Planned_Start
    """)
    return json.dumps(rows, default=str)

def get_active_tasks(project_id=None):
    where = f"AND t.Parent_Project_ID = '{project_id}'" if project_id else ""
    rows = sql_query(f"""
        SELECT t.Task_ID, t.Task_Name, t.Parent_Project_ID,
               t.Skill_Requirement, t.Modified_Planned_Start,
               t.Standard_Duration, t.Actual_Start, t.Actual_End,
               t.Complete_Percentage, t.Resource_Login
        FROM erp.tasks t
        WHERE t.Complete_Percentage < 100 {where}
        ORDER BY t.Modified_Planned_Start
    """)
    return json.dumps(rows, default=str)

def get_available_employees(skill=None):
    skill_filter = f"AND sc.skill_category = '{skill}'" if skill else ""
    rows = sql_query(f"""
        SELECT e.employee_id, e.email, e.first_name, e.last_name,
               e.department, e.employee_type, e.shift_preference,
               sc.skill_category, sc.skill_name, sc.certification_level,
               pl.limitation_type, pl.description AS limitation_desc
        FROM hr.employees e
        LEFT JOIN hr.skills_certifications sc ON e.employee_id = sc.employee_id
            AND sc.is_current = 'Yes' {skill_filter}
        LEFT JOIN hr.physical_limitations pl ON e.employee_id = pl.employee_id
        WHERE e.employment_status = 'Active' AND e.employee_id != 'EMP-050'
        ORDER BY e.employee_id
    """)
    return json.dumps(rows, default=str)

def get_scheduling_constraints():
    agreements = sql_query("SELECT * FROM hr.employee_agreements")
    contractors = sql_query("SELECT * FROM hr.contractual_workforce")
    return json.dumps({"agreements": agreements, "contractors": contractors}, default=str)

def get_parts_availability(part_numbers):
    pn_list = ",".join(f"'{p}'" for p in part_numbers)
    inv = sql_query(f"SELECT * FROM erp.inventory WHERE part_number IN ({pn_list})")
    pos = sql_query(f"SELECT * FROM erp.purchase_orders WHERE part_number IN ({pn_list})")
    return json.dumps({"inventory": inv, "purchase_orders": pos}, default=str)

TOOLS = {
    "get_manufacturing_floor_status": get_manufacturing_floor_status,
    "get_active_tasks": get_active_tasks,
    "get_available_employees": get_available_employees,
    "get_scheduling_constraints": get_scheduling_constraints,
    "get_parts_availability": get_parts_availability,
}

print(f"{len(TOOLS)} agent tools registered (all querying SQL Database).")

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
