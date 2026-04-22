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
# Loads all data into the SQL Database using three schemas:
# - **hr** - employees, skills, schedules, limitations, leave, contractors, agreements
# - **erp** - production lines, machines, inventory, purchase orders, maintenance, sensor definitions
# - **plm** - simulators, bill of materials, projects, tasks, task type durations
# 
# ## Prerequisites
# 1. SolutionInstaller has run (Lakehouse has CSVs in Files/)
# 2. A Fabric SQL Database exists in the workspace
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
SQL_JDBC_CONNECTION_STRING = ""
# Example: "jdbc:sqlserver://xxxxx.database.fabric.microsoft.com:1433;database={MyDB-guid};encrypt=true;trustServerCertificate=false;authentication=ActiveDirectoryInteractive"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 - Parse config and discover Lakehouse
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
    raise RuntimeError("Set SQL_JDBC_CONNECTION_STRING in the config cell above.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 - Create KQL Database with schema via Fabric API
import base64, json, time

# Find the Eventhouse
eh = next((i for i in items if i.get("displayName") == "CAEManufacturingEH"), None)
if not eh:
    print("WARNING: CAEManufacturingEH Eventhouse not found. Skipping KQL setup.")
    KQL_SETUP_OK = False
else:
    eventhouse_id = eh["id"]
    print(f"Eventhouse: {eventhouse_id}")

    # KQL schema: table creation + streaming ingestion
    # (Materialized views must be created separately after tables have data)
    kql_schema = """
.create-merge table MachineTelemetry (
    timestamp: datetime, machine_id: string, sensor_id: string,
    sensor_category: string, sensor_name: string, value: real,
    unit: string, alert_level: string, is_anomaly: bool)

.create-merge table ClockInEvents (
    timestamp: datetime, event_type: string, employee_email: string,
    employee_name: string, employee_id: string, department: string,
    project_id: string, task_id: string, simulator_id: string, details: string)

.alter table MachineTelemetry policy streamingingestion enable
.alter table ClockInEvents policy streamingingestion enable
"""

    # Build the KQL Database definition
    db_name = "CAEManufacturingKQLDB"
    db_properties = json.dumps({
        "databaseType": "ReadWrite",
        "parentEventhouseItemId": eventhouse_id,
        "oneLakeCachingPeriod": "7d",
        "oneLakeStandardStoragePeriod": "30d"
    })
    db_props_b64 = base64.b64encode(db_properties.encode("utf-8")).decode("utf-8")
    db_schema_b64 = base64.b64encode(kql_schema.encode("utf-8")).decode("utf-8")

    # Check if KQL DB already exists
    existing_kqldb = next((i for i in items if i.get("displayName") == db_name), None)

    if existing_kqldb:
        print(f"KQL Database '{db_name}' already exists: {existing_kqldb['id']}")
        KQL_SETUP_OK = True
    else:
        print(f"Creating KQL Database '{db_name}'...")
        create_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/kqlDatabases"
        payload = {
            "displayName": db_name,
            "definition": {
                "parts": [
                    {"path": "DatabaseProperties.json", "payload": db_props_b64, "payloadType": "InlineBase64"},
                    {"path": "DatabaseSchema.kql", "payload": db_schema_b64, "payloadType": "InlineBase64"}
                ]
            }
        }
        resp = requests.post(create_url, json=payload, headers=headers)
        print(f"  Status: {resp.status_code}")

        if resp.status_code in (200, 201, 202):
            # Poll for completion
            if "Location" in resp.headers:
                poll_url = resp.headers["Location"]
                for attempt in range(20):
                    poll_resp = requests.get(poll_url, headers=headers)
                    status = poll_resp.json().get("status", "").lower()
                    print(f"  Polling: {status}")
                    if status != "running":
                        break
                    time.sleep(5)

                if status == "succeeded":
                    print(f"  KQL Database created with schema.")
                    KQL_SETUP_OK = True
                else:
                    print(f"  KQL Database creation ended with status: {status}")
                    KQL_SETUP_OK = False
            else:
                print("  Created (no polling needed).")
                KQL_SETUP_OK = True
        else:
            print(f"  Failed: {resp.text[:200]}")
            KQL_SETUP_OK = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3 - Drop ALL existing SQL tables and schemas, then recreate fresh
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

# Nuclear drop: drop ALL user tables in dependency-safe order (FKs first)
print("Dropping all existing tables and schemas...\n")
cursor.execute("""
    DECLARE @sql NVARCHAR(MAX) = '';
    -- Drop all foreign keys first
    SELECT @sql = @sql + 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
        + ' DROP CONSTRAINT ' + QUOTENAME(f.name) + '; '
    FROM sys.foreign_keys f
    JOIN sys.tables t ON f.parent_object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name IN ('hr', 'erp', 'plm');
    EXEC sp_executesql @sql;
""")
cursor.execute("""
    DECLARE @sql NVARCHAR(MAX) = '';
    -- Drop all tables
    SELECT @sql = @sql + 'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + '; '
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name IN ('hr', 'erp', 'plm');
    EXEC sp_executesql @sql;
""")
print("  All existing tables dropped.")

# Create schemas
for schema in ['hr', 'erp', 'plm']:
    try:
        cursor.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema}') EXEC('CREATE SCHEMA {schema}')")
        print(f"  Schema: {schema}")
    except Exception as e:
        print(f"  Schema {schema}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4 - Create SQL tables (PK columns are NOT NULL, everything else nullable)
DDL = [
    # --- hr schema ---
    """CREATE TABLE hr.employees (
        employee_id NVARCHAR(10) NOT NULL, first_name NVARCHAR(50), last_name NVARCHAR(50),
        email NVARCHAR(100) NOT NULL, teams_email NVARCHAR(100), role NVARCHAR(50),
        department NVARCHAR(50), employee_type NVARCHAR(20), hire_date DATE,
        shift_preference NVARCHAR(10), employment_status NVARCHAR(20),
        manager_email NVARCHAR(100), phone NVARCHAR(20), location NVARCHAR(50),
        badge_number NVARCHAR(20), union_member NVARCHAR(5),
        production_line_id NVARCHAR(10))""",
    """CREATE TABLE hr.skills_certifications (
        employee_id NVARCHAR(10) NOT NULL, skill_category NVARCHAR(50) NOT NULL,
        skill_name NVARCHAR(100) NOT NULL, certification_level NVARCHAR(20),
        certification_date DATE, expiry_date DATE,
        certifying_body NVARCHAR(50), is_current NVARCHAR(5))""",
    """CREATE TABLE hr.employee_schedules (
        schedule_id NVARCHAR(10) NOT NULL, employee_id NVARCHAR(10) NOT NULL, week_start DATE,
        shift_type NVARCHAR(10), shift_start_time NVARCHAR(10), shift_end_time NVARCHAR(10),
        monday NVARCHAR(10), tuesday NVARCHAR(10), wednesday NVARCHAR(10),
        thursday NVARCHAR(10), friday NVARCHAR(10), saturday NVARCHAR(10),
        sunday NVARCHAR(10), notes NVARCHAR(200))""",
    """CREATE TABLE hr.physical_limitations (
        limitation_id NVARCHAR(10) NOT NULL, employee_id NVARCHAR(10) NOT NULL,
        limitation_type NVARCHAR(30), description NVARCHAR(500),
        effective_date DATE, review_date DATE,
        accommodations_required NVARCHAR(500), certified_by NVARCHAR(100),
        impacts_assignments NVARCHAR(5))""",
    """CREATE TABLE hr.leave_of_absence (
        leave_id NVARCHAR(10) NOT NULL, employee_id NVARCHAR(10) NOT NULL, leave_type NVARCHAR(30),
        start_date DATE, end_date DATE, status NVARCHAR(20),
        approved_by NVARCHAR(100), reason NVARCHAR(200), days_count FLOAT)""",
    """CREATE TABLE hr.contractual_workforce (
        contract_id NVARCHAR(10) NOT NULL, employee_id NVARCHAR(10) NOT NULL,
        agency_name NVARCHAR(50), contract_start DATE, contract_end DATE,
        hourly_rate_usd FLOAT, max_weekly_hours INT, overtime_allowed NVARCHAR(5),
        shift_flexibility NVARCHAR(100), minimum_notice_hours INT,
        specialization NVARCHAR(50), performance_rating NVARCHAR(20),
        extension_option NVARCHAR(100))""",
    """CREATE TABLE hr.employee_agreements (
        agreement_id NVARCHAR(10) NOT NULL, employee_type NVARCHAR(30),
        union_name NVARCHAR(50), provision_category NVARCHAR(30),
        provision_name NVARCHAR(50), description NVARCHAR(500),
        impacts_scheduling NVARCHAR(5))""",
    # --- erp schema ---
    """CREATE TABLE erp.production_lines (
        production_line_id NVARCHAR(10) NOT NULL, line_name NVARCHAR(50),
        building NVARCHAR(20), description NVARCHAR(200),
        manager_email NVARCHAR(100))""",
    """CREATE TABLE erp.machines (
        machine_id NVARCHAR(10) NOT NULL, machine_type NVARCHAR(20),
        machine_name NVARCHAR(100), manufacturer NVARCHAR(50),
        model NVARCHAR(50), serial_number NVARCHAR(20),
        production_line_id NVARCHAR(10),
        location NVARCHAR(20), zone NVARCHAR(30),
        install_date DATE, last_service_date DATE,
        status NVARCHAR(20), next_pm_date DATE)""",
    """CREATE TABLE erp.inventory (
        part_number NVARCHAR(20) NOT NULL, component_name NVARCHAR(100),
        warehouse_location NVARCHAR(20), quantity_on_hand INT,
        quantity_reserved INT, quantity_available INT,
        reorder_point INT, reorder_quantity INT,
        unit_cost_usd FLOAT, last_count_date DATE)""",
    """CREATE TABLE erp.purchase_orders (
        po_id NVARCHAR(10) NOT NULL, part_number NVARCHAR(20), component_name NVARCHAR(100),
        supplier NVARCHAR(50), quantity_ordered INT, unit_cost_usd FLOAT,
        order_date DATE, expected_delivery DATE, actual_delivery DATE,
        status NVARCHAR(20), destination_simulator NVARCHAR(10),
        notes NVARCHAR(200))""",
    """CREATE TABLE erp.maintenance_history (
        maintenance_id NVARCHAR(10) NOT NULL, machine_id NVARCHAR(10),
        maintenance_type NVARCHAR(20), system_affected NVARCHAR(30),
        description NVARCHAR(500), reported_date DATE, started_date DATE,
        completed_date DATE, downtime_hours FLOAT, root_cause NVARCHAR(200),
        technician_email NVARCHAR(100), parts_replaced NVARCHAR(100),
        cost_usd FLOAT)""",
    """CREATE TABLE erp.sensor_definitions (
        sensor_id NVARCHAR(10) NOT NULL, machine_id NVARCHAR(10),
        sensor_category NVARCHAR(30), sensor_name NVARCHAR(50),
        unit NVARCHAR(20), normal_min FLOAT, normal_max FLOAT,
        warning_min FLOAT, warning_max FLOAT,
        critical_min FLOAT, critical_max FLOAT)""",
    # --- plm schema ---
    """CREATE TABLE plm.simulators (
        simulator_id NVARCHAR(10) NOT NULL, simulator_model NVARCHAR(20),
        bay_id NVARCHAR(10), bay_name NVARCHAR(50), status NVARCHAR(20),
        customer NVARCHAR(50), aircraft_type NVARCHAR(50),
        serial_number NVARCHAR(20), build_start_date DATE,
        target_delivery_date DATE)""",
    """CREATE TABLE plm.bill_of_materials (
        bom_id NVARCHAR(10) NOT NULL, simulator_model NVARCHAR(20),
        component_category NVARCHAR(30), component_name NVARCHAR(100),
        part_number NVARCHAR(20), quantity_required INT, unit_cost_usd FLOAT,
        supplier NVARCHAR(50), lead_time_days INT, critical_path NVARCHAR(5))""",
    """CREATE TABLE plm.task_type_durations (
        Task_Type NVARCHAR(50) NOT NULL, Task_Name NVARCHAR(100),
        Standard_Duration INT, Required_Skill NVARCHAR(50),
        Sequence_Order INT, Description NVARCHAR(500))""",
    """CREATE TABLE plm.projects (
        Project_ID NVARCHAR(10) NOT NULL, Project_Name NVARCHAR(100),
        Simulator_ID NVARCHAR(10), Initial_Planned_Start DATE,
        Modified_Planned_Start DATE, Standard_Duration INT,
        Actual_End DATE, Resource_Login NVARCHAR(100),
        Complete_Percentage INT, Last_Modified_By NVARCHAR(100),
        Last_Modified_On DATE)""",
    """CREATE TABLE plm.tasks (
        Task_ID NVARCHAR(20) NOT NULL, Task_Name NVARCHAR(100),
        Parent_Project_ID NVARCHAR(10) NOT NULL, FS_Task_ID NVARCHAR(20),
        Task_Type NVARCHAR(50), Milestone INT, Skill_Requirement NVARCHAR(50),
        Initial_Planned_Start DATE, Modified_Planned_Start DATE,
        Actual_Start DATE, Standard_Duration INT, Actual_End DATE,
        Resource_Login NVARCHAR(100), Complete_Percentage INT,
        Last_Modified_By NVARCHAR(100), Last_Modified_On DATE)""",
]

print("Creating tables...\n")
for ddl in DDL:
    try:
        cursor.execute(ddl)
        tbl = ddl.split("CREATE TABLE ")[1].split(" ")[0].split("(")[0]
        print(f"  {tbl}")
    except Exception as e:
        print(f"  Error: {e}")

cursor.close()
conn.close()
print("\nAll tables created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5 - Bulk insert all data
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

# CSV path -> SQL table (folder -> schema alignment)
ALL_TABLES = [
    # hr.*
    ("data/hr/employees.csv",              "hr.employees"),
    ("data/hr/skills_certifications.csv",  "hr.skills_certifications"),
    ("data/hr/employee_schedules.csv",     "hr.employee_schedules"),
    ("data/hr/physical_limitations.csv",   "hr.physical_limitations"),
    ("data/hr/leave_of_absence.csv",       "hr.leave_of_absence"),
    ("data/hr/contractual_workforce.csv",  "hr.contractual_workforce"),
    ("data/hr/employee_agreements.csv",    "hr.employee_agreements"),
    # erp.*
    ("data/erp/production_lines.csv",      "erp.production_lines"),
    ("data/erp/machines.csv",              "erp.machines"),
    ("data/erp/inventory.csv",             "erp.inventory"),
    ("data/erp/purchase_orders.csv",       "erp.purchase_orders"),
    ("data/erp/maintenance_history.csv",   "erp.maintenance_history"),
    ("data/telemetry/sensor_definitions.csv", "erp.sensor_definitions"),
    # plm.*
    ("data/plm/simulators.csv",            "plm.simulators"),
    ("data/plm/bill_of_materials.csv",     "plm.bill_of_materials"),
    ("data/plm/task_type_durations.csv",   "plm.task_type_durations"),
    ("data/plm/projects.csv",              "plm.projects"),
    ("data/plm/tasks.csv",                 "plm.tasks"),
]

print("Loading data...\n")
for csv_rel, table_name in ALL_TABLES:
    csv_path = f"{BASE}/{csv_rel}"
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        df.write.jdbc(url=JDBC_URL, table=table_name, mode="append", properties=jdbc_props)
        print(f"  {table_name:35s} {df.count():>4d} rows")
    except Exception as e:
        print(f"  {table_name:35s} FAILED: {e}")

print("\nAll data loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6 - Add primary keys and foreign keys
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
    "ALTER TABLE erp.production_lines ADD CONSTRAINT PK_production_lines PRIMARY KEY (production_line_id)",
    "ALTER TABLE erp.machines ADD CONSTRAINT PK_machines PRIMARY KEY (machine_id)",
    "ALTER TABLE erp.inventory ADD CONSTRAINT PK_inventory PRIMARY KEY (part_number)",
    "ALTER TABLE erp.purchase_orders ADD CONSTRAINT PK_purchase_orders PRIMARY KEY (po_id)",
    "ALTER TABLE erp.maintenance_history ADD CONSTRAINT PK_maintenance_history PRIMARY KEY (maintenance_id)",
    "ALTER TABLE erp.sensor_definitions ADD CONSTRAINT PK_sensor_definitions PRIMARY KEY (sensor_id)",
    "ALTER TABLE plm.simulators ADD CONSTRAINT PK_simulators PRIMARY KEY (simulator_id)",
    "ALTER TABLE plm.bill_of_materials ADD CONSTRAINT PK_bill_of_materials PRIMARY KEY (bom_id)",
    "ALTER TABLE plm.task_type_durations ADD CONSTRAINT PK_task_type_durations PRIMARY KEY (Task_Type)",
    "ALTER TABLE plm.projects ADD CONSTRAINT PK_projects PRIMARY KEY (Project_ID)",
    "ALTER TABLE plm.tasks ADD CONSTRAINT PK_tasks PRIMARY KEY (Task_ID)",
    # --- Foreign Keys: hr ---
    "ALTER TABLE hr.skills_certifications ADD CONSTRAINT FK_skills_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.employee_schedules ADD CONSTRAINT FK_schedules_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.physical_limitations ADD CONSTRAINT FK_limitations_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.leave_of_absence ADD CONSTRAINT FK_leave_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    "ALTER TABLE hr.contractual_workforce ADD CONSTRAINT FK_contract_employee FOREIGN KEY (employee_id) REFERENCES hr.employees(employee_id)",
    # --- Foreign Keys: erp ---
    "ALTER TABLE erp.machines ADD CONSTRAINT FK_machine_line FOREIGN KEY (production_line_id) REFERENCES erp.production_lines(production_line_id)",
    "ALTER TABLE erp.maintenance_history ADD CONSTRAINT FK_maint_machine FOREIGN KEY (machine_id) REFERENCES erp.machines(machine_id)",
    "ALTER TABLE erp.sensor_definitions ADD CONSTRAINT FK_sensor_machine FOREIGN KEY (machine_id) REFERENCES erp.machines(machine_id)",
    # --- Foreign Keys: plm ---
    "ALTER TABLE plm.projects ADD CONSTRAINT FK_project_simulator FOREIGN KEY (Simulator_ID) REFERENCES plm.simulators(simulator_id)",
    "ALTER TABLE plm.tasks ADD CONSTRAINT FK_task_project FOREIGN KEY (Parent_Project_ID) REFERENCES plm.projects(Project_ID)",
    "ALTER TABLE plm.tasks ADD CONSTRAINT FK_task_type FOREIGN KEY (Task_Type) REFERENCES plm.task_type_durations(Task_Type)",
]

print("Adding constraints...\n")
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

# Step 7 - Verify
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
print(f"\nSQL Database: {SQL_DBNAME}")
print("  hr.*  - 7 tables (employees, skills, schedules, ...)")
print("  erp.* - 6 tables (production lines, machines, inventory, ...)")
print("  plm.* - 5 tables (simulators, BOM, projects, tasks, task types)")
print("\nNext: Open GetStarted notebook.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

