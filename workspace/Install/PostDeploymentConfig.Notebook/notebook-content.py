# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Post-Deployment Configuration
# 
# Loads all data into the **SQL Database** using two schemas:
# - **hr** schema: employees, skills, schedules, limitations, leave, contractors, agreements
# - **erp** schema: machines, projects, tasks, task types, BOM, inventory, POs, maintenance
# 
# Constraints (PKs, FKs) are added AFTER bulk insert to avoid insert-order issues.
# 
# ## Prerequisites
# 1. SolutionInstaller has run (Lakehouse has CSVs in Files/)
# 2. A **Fabric SQL Database** exists in the workspace
# 3. Paste the JDBC connection string in the config cell below
# 
# **Run All to configure.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
# Paste your SQL Database JDBC connection string below
# (SQL Database > Settings > Connection strings > JDBC)

SQL_JDBC_CONNECTION_STRING = ""
# Example: "jdbc:sqlserver://xxxxx.database.fabric.microsoft.com:1433;database={MyDB-guid};encrypt=true;trustServerCertificate=false;authentication=ActiveDirectoryInteractive"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 - Discover Lakehouse and parse SQL connection
import os, re, requests, struct
import notebookutils

TOKEN_FABRIC = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except Exception:
        pass

headers = {"Authorization": f"Bearer {TOKEN_FABRIC}"}
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=headers)
items = resp.json().get("value", [])

lh = next((i for i in items if i.get("displayName") == "CAEManufacturing_LH"), None)
if not lh:
    raise RuntimeError("CAEManufacturing_LH not found. Run SolutionInstaller first.")

LH_ID = lh["id"]
BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Files"
print(f"Lakehouse: {LH_ID}")

SQL_ENDPOINT = ""
SQL_DBNAME = ""
if SQL_JDBC_CONNECTION_STRING:
    sm = re.search(r'sqlserver://([^:;]+)', SQL_JDBC_CONNECTION_STRING)
    dm = re.search(r'database=\{?([^};]+)\}?', SQL_JDBC_CONNECTION_STRING)
    if sm and dm:
        SQL_ENDPOINT = sm.group(1)
        SQL_DBNAME = dm.group(1)
        print(f"SQL Server:   {SQL_ENDPOINT}")
        print(f"SQL Database: {SQL_DBNAME}")
    else:
        print("ERROR: Could not parse JDBC string.")
else:
    print("No SQL JDBC string. Paste it in the config cell above and re-run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 - Create schemas and tables (NO constraints yet)
if not SQL_ENDPOINT:
    raise RuntimeError("SQL connection not configured. Set SQL_JDBC_CONNECTION_STRING above.")

import pyodbc

TOKEN_SQL = notebookutils.credentials.getToken("https://database.windows.net/")
conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={SQL_ENDPOINT};"
    f"Database={SQL_DBNAME};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)
token_bytes = TOKEN_SQL.encode("utf-16-le")
token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
conn.autocommit = True
cursor = conn.cursor()

DDL = [
    # --- Drop in reverse dependency order ---
    "IF OBJECT_ID('erp.tasks') IS NOT NULL DROP TABLE erp.tasks",
    "IF OBJECT_ID('erp.projects') IS NOT NULL DROP TABLE erp.projects",
    "IF OBJECT_ID('erp.task_type_durations') IS NOT NULL DROP TABLE erp.task_type_durations",
    "IF OBJECT_ID('erp.maintenance_history') IS NOT NULL DROP TABLE erp.maintenance_history",
    "IF OBJECT_ID('erp.sensor_definitions') IS NOT NULL DROP TABLE erp.sensor_definitions",
    "IF OBJECT_ID('erp.purchase_orders') IS NOT NULL DROP TABLE erp.purchase_orders",
    "IF OBJECT_ID('erp.inventory') IS NOT NULL DROP TABLE erp.inventory",
    "IF OBJECT_ID('erp.bill_of_materials') IS NOT NULL DROP TABLE erp.bill_of_materials",
    "IF OBJECT_ID('erp.machines') IS NOT NULL DROP TABLE erp.machines",
    "IF OBJECT_ID('erp.simulators') IS NOT NULL DROP TABLE erp.simulators",
    "IF OBJECT_ID('hr.employee_agreements') IS NOT NULL DROP TABLE hr.employee_agreements",
    "IF OBJECT_ID('hr.contractual_workforce') IS NOT NULL DROP TABLE hr.contractual_workforce",
    "IF OBJECT_ID('hr.leave_of_absence') IS NOT NULL DROP TABLE hr.leave_of_absence",
    "IF OBJECT_ID('hr.physical_limitations') IS NOT NULL DROP TABLE hr.physical_limitations",
    "IF OBJECT_ID('hr.employee_schedules') IS NOT NULL DROP TABLE hr.employee_schedules",
    "IF OBJECT_ID('hr.skills_certifications') IS NOT NULL DROP TABLE hr.skills_certifications",
    "IF OBJECT_ID('hr.employees') IS NOT NULL DROP TABLE hr.employees",
    # --- Create schemas ---
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='hr') EXEC('CREATE SCHEMA hr')",
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='erp') EXEC('CREATE SCHEMA erp')",
    # --- HR tables (no constraints yet) ---
    """CREATE TABLE hr.employees (
        employee_id NVARCHAR(10), first_name NVARCHAR(50), last_name NVARCHAR(50),
        email NVARCHAR(100), teams_email NVARCHAR(100), role NVARCHAR(50),
        department NVARCHAR(50), employee_type NVARCHAR(20), hire_date DATE,
        shift_preference NVARCHAR(10), employment_status NVARCHAR(20),
        manager_email NVARCHAR(100), phone NVARCHAR(20), location NVARCHAR(50),
        badge_number NVARCHAR(20), union_member NVARCHAR(5))""",
    """CREATE TABLE hr.skills_certifications (
        employee_id NVARCHAR(10), skill_category NVARCHAR(50),
        skill_name NVARCHAR(100), certification_level NVARCHAR(20),
        certification_date DATE, expiry_date DATE,
        certifying_body NVARCHAR(50), is_current NVARCHAR(5))""",
    """CREATE TABLE hr.employee_schedules (
        schedule_id NVARCHAR(10), employee_id NVARCHAR(10), week_start DATE,
        shift_type NVARCHAR(10), shift_start_time NVARCHAR(10), shift_end_time NVARCHAR(10),
        monday NVARCHAR(10), tuesday NVARCHAR(10), wednesday NVARCHAR(10),
        thursday NVARCHAR(10), friday NVARCHAR(10), saturday NVARCHAR(10),
        sunday NVARCHAR(10), notes NVARCHAR(200))""",
    """CREATE TABLE hr.physical_limitations (
        limitation_id NVARCHAR(10), employee_id NVARCHAR(10),
        limitation_type NVARCHAR(30), description NVARCHAR(500),
        effective_date DATE, review_date DATE,
        accommodations_required NVARCHAR(500), certified_by NVARCHAR(100),
        impacts_assignments NVARCHAR(5))""",
    """CREATE TABLE hr.leave_of_absence (
        leave_id NVARCHAR(10), employee_id NVARCHAR(10), leave_type NVARCHAR(30),
        start_date DATE, end_date DATE, status NVARCHAR(20),
        approved_by NVARCHAR(100), reason NVARCHAR(200), days_count FLOAT)""",
    """CREATE TABLE hr.contractual_workforce (
        contract_id NVARCHAR(10), employee_id NVARCHAR(10),
        agency_name NVARCHAR(50), contract_start DATE, contract_end DATE,
        hourly_rate_usd FLOAT, max_weekly_hours INT, overtime_allowed NVARCHAR(5),
        shift_flexibility NVARCHAR(100), minimum_notice_hours INT,
        specialization NVARCHAR(50), performance_rating NVARCHAR(20),
        extension_option NVARCHAR(100))""",
    """CREATE TABLE hr.employee_agreements (
        agreement_id NVARCHAR(10), employee_type NVARCHAR(30),
        union_name NVARCHAR(50), provision_category NVARCHAR(30),
        provision_name NVARCHAR(50), description NVARCHAR(500),
        impacts_scheduling NVARCHAR(5))""",
    # --- ERP tables (no constraints yet) ---
    """CREATE TABLE erp.simulators (
        simulator_id NVARCHAR(10), simulator_model NVARCHAR(20),
        bay_id NVARCHAR(10), bay_name NVARCHAR(50), status NVARCHAR(20),
        customer NVARCHAR(50), aircraft_type NVARCHAR(50),
        serial_number NVARCHAR(20), build_start_date DATE,
        target_delivery_date DATE)""",
    """CREATE TABLE erp.machines (
        machine_id NVARCHAR(10), machine_type NVARCHAR(20),
        machine_name NVARCHAR(100), manufacturer NVARCHAR(50),
        model NVARCHAR(50), serial_number NVARCHAR(20),
        location NVARCHAR(20), zone NVARCHAR(30),
        install_date DATE, last_service_date DATE,
        status NVARCHAR(20), next_pm_date DATE)""",
    """CREATE TABLE erp.task_type_durations (
        Task_Type NVARCHAR(50), Task_Name NVARCHAR(100),
        Standard_Duration INT, Required_Skill NVARCHAR(50),
        Sequence_Order INT, Description NVARCHAR(500))""",
    """CREATE TABLE erp.bill_of_materials (
        bom_id NVARCHAR(10), simulator_model NVARCHAR(20),
        component_category NVARCHAR(30), component_name NVARCHAR(100),
        part_number NVARCHAR(20), quantity_required INT, unit_cost_usd FLOAT,
        supplier NVARCHAR(50), lead_time_days INT, critical_path NVARCHAR(5))""",
    """CREATE TABLE erp.inventory (
        part_number NVARCHAR(20), component_name NVARCHAR(100),
        warehouse_location NVARCHAR(20), quantity_on_hand INT,
        quantity_reserved INT, quantity_available INT,
        reorder_point INT, reorder_quantity INT,
        unit_cost_usd FLOAT, last_count_date DATE)""",
    """CREATE TABLE erp.purchase_orders (
        po_id NVARCHAR(10), part_number NVARCHAR(20), component_name NVARCHAR(100),
        supplier NVARCHAR(50), quantity_ordered INT, unit_cost_usd FLOAT,
        order_date DATE, expected_delivery DATE, actual_delivery DATE,
        status NVARCHAR(20), destination_simulator NVARCHAR(10),
        notes NVARCHAR(200))""",
    """CREATE TABLE erp.maintenance_history (
        maintenance_id NVARCHAR(10), machine_id NVARCHAR(10),
        maintenance_type NVARCHAR(20), system_affected NVARCHAR(30),
        description NVARCHAR(500), reported_date DATE, started_date DATE,
        completed_date DATE, downtime_hours FLOAT, root_cause NVARCHAR(200),
        technician_email NVARCHAR(100), parts_replaced NVARCHAR(100),
        cost_usd FLOAT)""",
    """CREATE TABLE erp.projects (
        Project_ID NVARCHAR(10), Project_Name NVARCHAR(100),
        Simulator_ID NVARCHAR(10), Initial_Planned_Start DATE,
        Modified_Planned_Start DATE, Standard_Duration INT,
        Actual_End DATE, Resource_Login NVARCHAR(100),
        Complete_Percentage INT, Last_Modified_By NVARCHAR(100),
        Last_Modified_On DATE)""",
    """CREATE TABLE erp.tasks (
        Task_ID NVARCHAR(20), Task_Name NVARCHAR(100),
        Parent_Project_ID NVARCHAR(10), FS_Task_ID NVARCHAR(20),
        Task_Type NVARCHAR(50), Milestone INT, Skill_Requirement NVARCHAR(50),
        Initial_Planned_Start DATE, Modified_Planned_Start DATE,
        Actual_Start DATE, Standard_Duration INT, Actual_End DATE,
        Resource_Login NVARCHAR(100), Complete_Percentage INT,
        Last_Modified_By NVARCHAR(100), Last_Modified_On DATE)""",
    """CREATE TABLE erp.sensor_definitions (
        sensor_id NVARCHAR(10), machine_id NVARCHAR(10),
        sensor_category NVARCHAR(30), sensor_name NVARCHAR(50),
        unit NVARCHAR(20), normal_min FLOAT, normal_max FLOAT,
        warning_min FLOAT, warning_max FLOAT,
        critical_min FLOAT, critical_max FLOAT)""",
]

print("Creating schemas and tables (no constraints)...\n")
for ddl in DDL:
    try:
        cursor.execute(ddl)
        if "CREATE TABLE" in ddl:
            tbl = ddl.split("CREATE TABLE ")[1].split(" ")[0].split("(")[0]
            print(f"  Created {tbl}")
        elif "CREATE SCHEMA" in ddl:
            schema = ddl.split("'")[1]
            print(f"  Schema {schema}")
    except Exception as e:
        print(f"  Warning: {e}")

cursor.close()
conn.close()
print("\nAll tables created (no constraints yet).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3 - Bulk insert all data via Spark JDBC
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_ENDPOINT}:1433;"
    f"database={SQL_DBNAME};"
    f"encrypt=true;trustServerCertificate=false;"
    f"loginTimeout=30;"
)
jdbc_props = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "accessToken": TOKEN_SQL,
}

# CSV path -> SQL table (insertion order does not matter - no constraints yet)
ALL_TABLES = [
    ("data/hr/employees.csv",              "hr.employees"),
    ("data/hr/skills_certifications.csv",  "hr.skills_certifications"),
    ("data/hr/employee_schedules.csv",     "hr.employee_schedules"),
    ("data/hr/physical_limitations.csv",   "hr.physical_limitations"),
    ("data/hr/leave_of_absence.csv",       "hr.leave_of_absence"),
    ("data/hr/contractual_workforce.csv",  "hr.contractual_workforce"),
    ("data/hr/employee_agreements.csv",    "hr.employee_agreements"),
    ("data/plm/simulators.csv",            "erp.simulators"),
    ("data/erp/machines.csv",              "erp.machines"),
    ("data/plm/task_type_durations.csv",   "erp.task_type_durations"),
    ("data/plm/bill_of_materials.csv",     "erp.bill_of_materials"),
    ("data/erp/inventory.csv",             "erp.inventory"),
    ("data/erp/purchase_orders.csv",       "erp.purchase_orders"),
    ("data/erp/maintenance_history.csv",   "erp.maintenance_history"),
    ("data/plm/projects.csv",            "erp.projects"),
    ("data/plm/tasks.csv",               "erp.tasks"),
    ("data/telemetry/sensor_definitions.csv", "erp.sensor_definitions"),
]

print("Bulk inserting data into SQL Database...\n")

for csv_rel, table_name in ALL_TABLES:
    csv_path = f"{BASE}/{csv_rel}"
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        df.write.jdbc(url=JDBC_URL, table=table_name, mode="append", properties=jdbc_props)
        print(f"  {table_name:35s} {df.count():>4d} rows")
    except Exception as e:
        print(f"  {table_name:35s} FAILED: {e}")

print("\nBulk insert complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4 - Add primary keys, unique constraints, and foreign keys
conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
conn.autocommit = True
cursor = conn.cursor()

CONSTRAINTS = [
    # --- Primary Keys ---
    "ALTER TABLE hr.employees ADD CONSTRAINT PK_employees PRIMARY KEY (employee_id)",
    "ALTER TABLE hr.employees ADD CONSTRAINT UQ_employees_email UNIQUE (email)",
    "ALTER TABLE hr.employee_schedules ADD CONSTRAINT PK_employee_schedules PRIMARY KEY (schedule_id)",
    "ALTER TABLE hr.physical_limitations ADD CONSTRAINT PK_physical_limitations PRIMARY KEY (limitation_id)",
    "ALTER TABLE hr.leave_of_absence ADD CONSTRAINT PK_leave_of_absence PRIMARY KEY (leave_id)",
    "ALTER TABLE hr.contractual_workforce ADD CONSTRAINT PK_contractual_workforce PRIMARY KEY (contract_id)",
    "ALTER TABLE hr.employee_agreements ADD CONSTRAINT PK_employee_agreements PRIMARY KEY (agreement_id)",
    "ALTER TABLE erp.simulators ADD CONSTRAINT PK_simulators PRIMARY KEY (simulator_id)",
    "ALTER TABLE erp.machines ADD CONSTRAINT PK_machines PRIMARY KEY (machine_id)",
    "ALTER TABLE erp.task_type_durations ADD CONSTRAINT PK_task_type_durations PRIMARY KEY (Task_Type)",
    "ALTER TABLE erp.bill_of_materials ADD CONSTRAINT PK_bill_of_materials PRIMARY KEY (bom_id)",
    "ALTER TABLE erp.inventory ADD CONSTRAINT PK_inventory PRIMARY KEY (part_number)",
    "ALTER TABLE erp.purchase_orders ADD CONSTRAINT PK_purchase_orders PRIMARY KEY (po_id)",
    "ALTER TABLE erp.maintenance_history ADD CONSTRAINT PK_maintenance_history PRIMARY KEY (maintenance_id)",
    "ALTER TABLE erp.projects ADD CONSTRAINT PK_projects PRIMARY KEY (Project_ID)",
    "ALTER TABLE erp.tasks ADD CONSTRAINT PK_tasks PRIMARY KEY (Task_ID)",
    "ALTER TABLE erp.sensor_definitions ADD CONSTRAINT PK_sensor_definitions PRIMARY KEY (sensor_id)",
    # --- Foreign Keys: HR ---
    "ALTER TABLE hr.skills_certifications ADD CONSTRAINT FK_skills_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.employee_schedules ADD CONSTRAINT FK_schedules_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.physical_limitations ADD CONSTRAINT FK_limitations_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.leave_of_absence ADD CONSTRAINT FK_leave_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.contractual_workforce ADD CONSTRAINT FK_contract_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    # --- Foreign Keys: ERP ---
    "ALTER TABLE erp.maintenance_history ADD CONSTRAINT FK_maint_machine FOREIGN KEY (machine_id) REFERENCES erp.machines(machine_id)",
    "ALTER TABLE erp.projects ADD CONSTRAINT FK_project_simulator FOREIGN KEY (Simulator_ID) REFERENCES erp.simulators(simulator_id)",
    "ALTER TABLE erp.tasks ADD CONSTRAINT FK_task_project FOREIGN KEY (Parent_Project_ID) REFERENCES erp.projects(Project_ID)",
    "ALTER TABLE erp.tasks ADD CONSTRAINT FK_task_type FOREIGN KEY (Task_Type) REFERENCES erp.task_type_durations(Task_Type)",
    "ALTER TABLE erp.sensor_definitions ADD CONSTRAINT FK_sensor_machine FOREIGN KEY (machine_id) REFERENCES erp.machines(machine_id)",
]

print("Adding primary keys and foreign keys...\n")
ok = 0
for c in CONSTRAINTS:
    try:
        cursor.execute(c)
        name = c.split("CONSTRAINT ")[1].split(" ")[0]
        print(f"  {name}")
        ok += 1
    except Exception as e:
        print(f"  FAILED: {e}")

cursor.close()
conn.close()
print(f"\n{ok}/{len(CONSTRAINTS)} constraints added.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5 - Verify row counts
conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
cursor = conn.cursor()

print("=== Verification ===\n")
for _, table_name in ALL_TABLES:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  {table_name:35s} {count:>4d} rows")
    except Exception as e:
        print(f"  {table_name:35s} ERROR: {e}")

cursor.close()
conn.close()

print("\n" + "=" * 50)
print("  POST-DEPLOYMENT COMPLETE")
print("=" * 50)
print(f"\nAll tables in SQL Database '{SQL_DBNAME}'")
print("  hr.*  - employee data (7 tables)")
print("  erp.* - machines, projects, tasks, BOM, etc. (9 tables)")
print("\nConstraints: PKs + FKs enforced.")
print("\nNext: Open the GetStarted notebook.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
