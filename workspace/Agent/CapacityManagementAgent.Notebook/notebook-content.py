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
# SQL connection is read from the Lakehouse config file (set by PostDeploymentConfig).
# Override here only if needed.
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

TOKEN_FABRIC = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
TOKEN_SQL = notebookutils.credentials.getToken("https://database.windows.net/")

# Read SQL connection from Lakehouse config file if not set
if not SQL_JDBC_CONNECTION_STRING:
    WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
    if not WORKSPACE_ID:
        try:
            ctx = notebookutils.runtime.context
            WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
        except Exception:
            pass
    fab_headers = {"Authorization": f"Bearer {TOKEN_FABRIC}"}
    items_resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=fab_headers)
    lh = next((i for i in items_resp.json().get("value", []) if i.get("displayName") == "CAEManufacturing_LH"), None)
    if lh:
        config_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh['id']}/Files/config/connections.json"
        try:
            config = json.loads(notebookutils.fs.head(config_path, 10000))
            SQL_JDBC_CONNECTION_STRING = config.get("SQL_JDBC_CONNECTION_STRING", "")
            if SQL_JDBC_CONNECTION_STRING:
                print(f"Loaded SQL connection from config file")
        except Exception:
            pass

if not SQL_JDBC_CONNECTION_STRING:
    raise RuntimeError("SQL_JDBC_CONNECTION_STRING not found. Run PostDeploymentConfig first (creates config file).")

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

# Agent tool functions - query the SQL Database and KQL Eventhouse

def get_manufacturing_floor_status():
    rows = sql_query("""
        SELECT p.Project_ID, p.Project_Name, p.Simulator_ID,
               s.customer, s.status AS sim_status,
               p.Modified_Planned_Start, p.Standard_Duration,
               p.Complete_Percentage, p.Hard_Deadline,
               p.Contract_Value_USD, p.Penalty_Per_Day_USD
        FROM plm.projects p
        LEFT JOIN plm.simulators s ON p.Simulator_ID = s.simulator_id
        ORDER BY p.Modified_Planned_Start
    """)
    return json.dumps(rows, default=str)

def get_active_tasks(project_id=None):
    where = f"AND t.Parent_Project_ID = '{project_id}'" if project_id else ""
    rows = sql_query(f"""
        SELECT t.Task_ID, t.Task_Name, t.Parent_Project_ID,
               t.Skill_Requirement, t.Modified_Planned_Start,
               t.Standard_Duration, t.Actual_Start, t.Actual_End,
               t.Complete_Percentage, t.Resource_Login, t.Machine_ID
        FROM plm.tasks t
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
               wr.limitation_type, wr.description AS limitation_desc
        FROM hr.employees e
        LEFT JOIN hr.skills_certifications sc ON e.employee_id = sc.employee_id
            AND sc.is_current = 'Yes' {skill_filter}
        LEFT JOIN hr.work_restrictions wr ON e.employee_id = wr.employee_id
        WHERE e.employment_status = 'Active' AND e.employee_id != 'EMP-050'
        ORDER BY e.employee_id
    """)
    return json.dumps(rows, default=str)

def get_scheduling_constraints():
    agreements = sql_query("SELECT * FROM hr.collective_agreements")
    contractors = sql_query("SELECT * FROM hr.contractor_agreements")
    return json.dumps({"agreements": agreements, "contractors": contractors}, default=str)

def get_parts_availability(part_numbers):
    pn_list = ",".join(f"'{p}'" for p in part_numbers)
    inv = sql_query(f"SELECT * FROM erp.inventory WHERE part_number IN ({pn_list})")
    pos = sql_query(f"SELECT * FROM erp.purchase_orders WHERE part_number IN ({pn_list})")
    return json.dumps({"inventory": inv, "purchase_orders": pos}, default=str)

def get_machine_health():
    """Query KQL Eventhouse for real-time machine health alerts."""
    TOKEN_KQL = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")
    WORKSPACE_ID_LOCAL = os.environ.get("TRIDENT_WORKSPACE_ID", "")
    if not WORKSPACE_ID_LOCAL:
        try:
            ctx = notebookutils.runtime.context
            WORKSPACE_ID_LOCAL = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
        except Exception:
            pass
    fab_token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    fab_headers = {"Authorization": f"Bearer {fab_token}"}
    resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID_LOCAL}/items", headers=fab_headers)
    items = resp.json().get("value", [])
    eh = next((i for i in items if i.get("displayName") == "CAEManufacturingEH"), None)
    if not eh:
        return json.dumps({"error": "Eventhouse not found"})
    eh_props = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID_LOCAL}/eventhouses/{eh['id']}",
        headers=fab_headers
    ).json()
    kql_uri = eh_props.get("properties", {}).get("queryServiceUri", "")
    query = "MachineHealthAlerts(30m) | take 50"
    kql_resp = requests.post(
        f"{kql_uri}/v1/rest/query",
        headers={"Authorization": f"Bearer {TOKEN_KQL}", "Content-Type": "application/json"},
        json={"db": "CAEManufacturingKQLDB", "csl": query}
    )
    return kql_resp.text

def get_machine_certifications(machine_id=None):
    where = f"WHERE mc.machine_id = '{machine_id}'" if machine_id else ""
    rows = sql_query(f"""
        SELECT mc.cert_id, mc.employee_id, mc.machine_id, mc.cert_level,
               mc.cert_date, mc.expiry_date, mc.is_current,
               e.first_name, e.last_name, e.email
        FROM hr.machine_certifications mc
        JOIN hr.employees e ON mc.employee_id = e.employee_id
        {where}
        ORDER BY mc.machine_id, mc.cert_level DESC
    """)
    return json.dumps(rows, default=str)

TOOLS = {
    "get_manufacturing_floor_status": get_manufacturing_floor_status,
    "get_active_tasks": get_active_tasks,
    "get_available_employees": get_available_employees,
    "get_scheduling_constraints": get_scheduling_constraints,
    "get_parts_availability": get_parts_availability,
    "get_machine_health": get_machine_health,
    "get_machine_certifications": get_machine_certifications,
}

print(f"{len(TOOLS)} agent tools registered.")

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

# Release Spark resources so the notebook (and its parent pipeline) can end
# instead of holding the session open until idle timeout.
try:
    spark.stop()
except Exception:
    pass
try:
    notebookutils.session.stop()
except Exception:
    try:
        import mssparkutils
        mssparkutils.session.stop()
    except Exception:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
