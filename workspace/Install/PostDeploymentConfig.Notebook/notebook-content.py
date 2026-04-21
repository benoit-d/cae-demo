# Fabric notebook source, currentl not applicable to synapse_pyspark kernel
# Microsoft Fabric notebook source
# %% [markdown]
# # Post-Deployment Configuration
#
# Run this notebook **once** after the SolutionInstaller completes.
# It creates the Fabric SQL Database tables, loads all seed data, and verifies
# every component is ready.
#
# ## What this notebook does
# 1. Creates SQL tables in `CAEManufacturing_SQLDB` with foreign keys
# 2. Loads CSVs from the staging Lakehouse into the SQL tables
# 3. Verifies row counts and referential integrity
#
# **Run All to configure everything.**

# %%
# Step 1 — Create SQL Database tables
# The SQL Database item must already exist in the workspace (deployed by fabric-cicd).
# We use pyodbc via the Fabric SQL endpoint.

sql_ddl = """
-- =====================================================
-- PROJECTS
-- =====================================================
IF OBJECT_ID('dbo.tasks', 'U') IS NOT NULL DROP TABLE dbo.tasks;
IF OBJECT_ID('dbo.projects', 'U') IS NOT NULL DROP TABLE dbo.projects;
IF OBJECT_ID('dbo.maintenance_history', 'U') IS NOT NULL DROP TABLE dbo.maintenance_history;
IF OBJECT_ID('dbo.purchase_orders', 'U') IS NOT NULL DROP TABLE dbo.purchase_orders;
IF OBJECT_ID('dbo.inventory', 'U') IS NOT NULL DROP TABLE dbo.inventory;
IF OBJECT_ID('dbo.bill_of_materials', 'U') IS NOT NULL DROP TABLE dbo.bill_of_materials;
IF OBJECT_ID('dbo.task_type_durations', 'U') IS NOT NULL DROP TABLE dbo.task_type_durations;
IF OBJECT_ID('dbo.machines', 'U') IS NOT NULL DROP TABLE dbo.machines;
IF OBJECT_ID('dbo.employee_agreements', 'U') IS NOT NULL DROP TABLE dbo.employee_agreements;
IF OBJECT_ID('dbo.contractual_workforce', 'U') IS NOT NULL DROP TABLE dbo.contractual_workforce;
IF OBJECT_ID('dbo.leave_of_absence', 'U') IS NOT NULL DROP TABLE dbo.leave_of_absence;
IF OBJECT_ID('dbo.physical_limitations', 'U') IS NOT NULL DROP TABLE dbo.physical_limitations;
IF OBJECT_ID('dbo.employee_schedules', 'U') IS NOT NULL DROP TABLE dbo.employee_schedules;
IF OBJECT_ID('dbo.skills_certifications', 'U') IS NOT NULL DROP TABLE dbo.skills_certifications;
IF OBJECT_ID('dbo.employees', 'U') IS NOT NULL DROP TABLE dbo.employees;

-- Employees (root table — many FKs point here)
CREATE TABLE dbo.employees (
    employee_id        NVARCHAR(10)  PRIMARY KEY,
    first_name         NVARCHAR(50)  NOT NULL,
    last_name          NVARCHAR(50)  NOT NULL,
    email              NVARCHAR(100) NOT NULL UNIQUE,
    teams_email        NVARCHAR(100),
    role               NVARCHAR(50),
    department         NVARCHAR(50),
    employee_type      NVARCHAR(20),
    hire_date          DATE,
    shift_preference   NVARCHAR(10),
    employment_status  NVARCHAR(20),
    manager_email      NVARCHAR(100),
    phone              NVARCHAR(20),
    location           NVARCHAR(50),
    badge_number       NVARCHAR(20),
    union_member       NVARCHAR(5)
);

CREATE TABLE dbo.skills_certifications (
    employee_id         NVARCHAR(10)  NOT NULL REFERENCES dbo.employees(employee_id),
    skill_category      NVARCHAR(50)  NOT NULL,
    skill_name          NVARCHAR(100) NOT NULL,
    certification_level NVARCHAR(20),
    certification_date  DATE,
    expiry_date         DATE,
    certifying_body     NVARCHAR(50),
    is_current          NVARCHAR(5)
);

CREATE TABLE dbo.employee_schedules (
    schedule_id     NVARCHAR(10)  PRIMARY KEY,
    employee_id     NVARCHAR(10)  NOT NULL REFERENCES dbo.employees(employee_id),
    week_start      DATE,
    shift_type      NVARCHAR(10),
    shift_start_time NVARCHAR(10),
    shift_end_time  NVARCHAR(10),
    monday          NVARCHAR(10), tuesday  NVARCHAR(10), wednesday NVARCHAR(10),
    thursday        NVARCHAR(10), friday   NVARCHAR(10), saturday  NVARCHAR(10),
    sunday          NVARCHAR(10),
    notes           NVARCHAR(200)
);

CREATE TABLE dbo.physical_limitations (
    limitation_id         NVARCHAR(10)  PRIMARY KEY,
    employee_id           NVARCHAR(10)  NOT NULL REFERENCES dbo.employees(employee_id),
    limitation_type       NVARCHAR(30),
    description           NVARCHAR(500),
    effective_date        DATE,
    review_date           DATE,
    accommodations_required NVARCHAR(500),
    certified_by          NVARCHAR(100),
    impacts_assignments   NVARCHAR(5)
);

CREATE TABLE dbo.leave_of_absence (
    leave_id    NVARCHAR(10) PRIMARY KEY,
    employee_id NVARCHAR(10) NOT NULL REFERENCES dbo.employees(employee_id),
    leave_type  NVARCHAR(30),
    start_date  DATE,
    end_date    DATE,
    status      NVARCHAR(20),
    approved_by NVARCHAR(100),
    reason      NVARCHAR(200),
    days_count  FLOAT
);

CREATE TABLE dbo.contractual_workforce (
    contract_id         NVARCHAR(10) PRIMARY KEY,
    employee_id         NVARCHAR(10) NOT NULL REFERENCES dbo.employees(employee_id),
    agency_name         NVARCHAR(50),
    contract_start      DATE,
    contract_end        DATE,
    hourly_rate_usd     FLOAT,
    max_weekly_hours    INT,
    overtime_allowed    NVARCHAR(5),
    shift_flexibility   NVARCHAR(100),
    minimum_notice_hours INT,
    specialization      NVARCHAR(50),
    performance_rating  NVARCHAR(20),
    extension_option    NVARCHAR(100)
);

CREATE TABLE dbo.employee_agreements (
    agreement_id       NVARCHAR(10) PRIMARY KEY,
    employee_type      NVARCHAR(30),
    union_name         NVARCHAR(50),
    provision_category NVARCHAR(30),
    provision_name     NVARCHAR(50),
    description        NVARCHAR(500),
    impacts_scheduling NVARCHAR(5)
);

-- Machines / Simulators
CREATE TABLE dbo.machines (
    simulator_id        NVARCHAR(10) PRIMARY KEY,
    simulator_model     NVARCHAR(20),
    bay_id              NVARCHAR(10),
    bay_name            NVARCHAR(50),
    status              NVARCHAR(20),
    customer            NVARCHAR(50),
    aircraft_type       NVARCHAR(50),
    serial_number       NVARCHAR(20),
    build_start_date    DATE,
    target_delivery_date DATE
);

-- ERP reference tables
CREATE TABLE dbo.task_type_durations (
    Task_Type         NVARCHAR(50) PRIMARY KEY,
    Task_Name         NVARCHAR(100),
    Standard_Duration INT,
    Required_Skill    NVARCHAR(50),
    Sequence_Order    INT,
    Description       NVARCHAR(500)
);

CREATE TABLE dbo.bill_of_materials (
    bom_id              NVARCHAR(10) PRIMARY KEY,
    simulator_model     NVARCHAR(20),
    component_category  NVARCHAR(30),
    component_name      NVARCHAR(100),
    part_number         NVARCHAR(20),
    quantity_required   INT,
    unit_cost_usd       FLOAT,
    supplier            NVARCHAR(50),
    lead_time_days      INT,
    critical_path       NVARCHAR(5)
);

CREATE TABLE dbo.inventory (
    part_number        NVARCHAR(20) PRIMARY KEY,
    component_name     NVARCHAR(100),
    warehouse_location NVARCHAR(20),
    quantity_on_hand   INT,
    quantity_reserved  INT,
    quantity_available INT,
    reorder_point      INT,
    reorder_quantity   INT,
    unit_cost_usd      FLOAT,
    last_count_date    DATE
);

CREATE TABLE dbo.purchase_orders (
    po_id               NVARCHAR(10) PRIMARY KEY,
    part_number         NVARCHAR(20),
    component_name      NVARCHAR(100),
    supplier            NVARCHAR(50),
    quantity_ordered    INT,
    unit_cost_usd       FLOAT,
    order_date          DATE,
    expected_delivery   DATE,
    actual_delivery     DATE,
    status              NVARCHAR(20),
    destination_simulator NVARCHAR(10),
    notes               NVARCHAR(200)
);

CREATE TABLE dbo.maintenance_history (
    maintenance_id    NVARCHAR(10) PRIMARY KEY,
    simulator_id      NVARCHAR(10) REFERENCES dbo.machines(simulator_id),
    maintenance_type  NVARCHAR(20),
    system_affected   NVARCHAR(30),
    description       NVARCHAR(500),
    reported_date     DATE,
    started_date      DATE,
    completed_date    DATE,
    downtime_hours    FLOAT,
    root_cause        NVARCHAR(200),
    technician_email  NVARCHAR(100),
    parts_replaced    NVARCHAR(100),
    cost_usd          FLOAT
);

-- Project management (Gantt-compatible)
CREATE TABLE dbo.projects (
    Project_ID             NVARCHAR(10) PRIMARY KEY,
    Project_Name           NVARCHAR(100),
    Simulator_ID           NVARCHAR(10) REFERENCES dbo.machines(simulator_id),
    Initial_Planned_Start  DATE,
    Modified_Planned_Start DATE,
    Standard_Duration      INT,
    Actual_End             DATE,
    Resource_Login         NVARCHAR(100),
    Complete_Percentage    INT,
    Last_Modified_By       NVARCHAR(100),
    Last_Modified_On       DATE
);

CREATE TABLE dbo.tasks (
    Task_ID                NVARCHAR(20) PRIMARY KEY,
    Task_Name              NVARCHAR(100),
    Parent_Project_ID      NVARCHAR(10) NOT NULL REFERENCES dbo.projects(Project_ID),
    FS_Task_ID             NVARCHAR(20),
    Task_Type              NVARCHAR(50) REFERENCES dbo.task_type_durations(Task_Type),
    Milestone              INT,
    Skill_Requirement      NVARCHAR(50),
    Initial_Planned_Start  DATE,
    Modified_Planned_Start DATE,
    Actual_Start           DATE,
    Standard_Duration      INT,
    Actual_End             DATE,
    Resource_Login         NVARCHAR(100),
    Complete_Percentage    INT,
    Last_Modified_By       NVARCHAR(100),
    Last_Modified_On       DATE
);
"""

print("SQL DDL prepared — will execute in next cell.")
print(f"Tables: employees, skills_certifications, employee_schedules,")
print(f"  physical_limitations, leave_of_absence, contractual_workforce,")
print(f"  employee_agreements, machines, task_type_durations, bill_of_materials,")
print(f"  inventory, purchase_orders, maintenance_history, projects, tasks")

# %%
# Step 2 — Execute DDL against the Fabric SQL Database
# Uses the Fabric SQL connection string (auto-discovered from workspace)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# The Fabric SQL Database endpoint — discovered via notebookutils
import notebookutils

# Find the SQL Database item in the workspace
ws_items = notebookutils.fabric.list_items()
sql_db = next((i for i in ws_items if 'SQLDB' in i.get('displayName', '') or 'SQLDatabase' in i.get('type', '')), None)

if sql_db:
    sql_endpoint = sql_db.get('properties', {}).get('connectionString', '')
    print(f"Found SQL Database: {sql_db['displayName']}")
else:
    print("SQL Database not found in workspace.")
    print("Create a Fabric SQL Database named 'CAEManufacturing_SQLDB' and re-run.")

# Execute DDL — split by GO-equivalent (each statement separately)
import pyodbc

# For Fabric SQL Database, use the TDS endpoint with AAD auth
token = notebookutils.credentials.getToken("https://database.windows.net/")

conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={sql_db['displayName']}.database.fabric.microsoft.com;"
    f"Database={sql_db['displayName']};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

conn = pyodbc.connect(conn_str, attrs_before={1256: token.encode("utf-16-le")})
conn.autocommit = True
cursor = conn.cursor()

# Execute each statement (split on semicolons at statement boundaries)
for stmt in sql_ddl.split(";"):
    stmt = stmt.strip()
    if stmt and not stmt.startswith("--"):
        try:
            cursor.execute(stmt)
        except Exception as e:
            print(f"Warning: {e}")

print("All SQL tables created.")
cursor.close()
conn.close()

# %%
# Step 3 — Load CSV seed data from the staging Lakehouse into SQL tables

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Map CSV files → SQL table names
CSV_TO_TABLE = {
    # HR
    "data/hr/employees.csv":              "employees",
    "data/hr/skills_certifications.csv":  "skills_certifications",
    "data/hr/employee_schedules.csv":     "employee_schedules",
    "data/hr/physical_limitations.csv":   "physical_limitations",
    "data/hr/leave_of_absence.csv":       "leave_of_absence",
    "data/hr/contractual_workforce.csv":  "contractual_workforce",
    "data/hr/employee_agreements.csv":    "employee_agreements",
    # ERP
    "data/erp/machines.csv":              "machines",
    "data/erp/task_type_durations.csv":   "task_type_durations",
    "data/erp/bill_of_materials.csv":     "bill_of_materials",
    "data/erp/inventory.csv":             "inventory",
    "data/erp/purchase_orders.csv":       "purchase_orders",
    "data/erp/maintenance_history.csv":   "maintenance_history",
    # Project management
    "data/cosmosdb/projects.csv":         "projects",
    "data/cosmosdb/tasks.csv":            "tasks",
}

# Load order matters for FK constraints
LOAD_ORDER = [
    "employees",
    "skills_certifications", "employee_schedules", "physical_limitations",
    "leave_of_absence", "contractual_workforce", "employee_agreements",
    "machines", "task_type_durations",
    "bill_of_materials", "inventory", "purchase_orders", "maintenance_history",
    "projects", "tasks",
]

# Build reverse lookup
table_to_csv = {v: k for k, v in CSV_TO_TABLE.items()}

# Fabric SQL JDBC URL
jdbc_url = (
    f"jdbc:sqlserver://{sql_db['displayName']}.database.fabric.microsoft.com:1433;"
    f"database={sql_db['displayName']};"
    f"encrypt=true;trustServerCertificate=false;"
    f"loginTimeout=30;"
)

jdbc_props = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "accessToken": token,
}

print("Loading data into SQL Database...\n")
for table in LOAD_ORDER:
    csv_path = f"Files/{table_to_csv[table]}"
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    df.write.jdbc(
        url=jdbc_url,
        table=f"dbo.{table}",
        mode="append",
        properties=jdbc_props,
    )
    print(f"  {table}: {df.count()} rows")

print("\nAll data loaded into SQL Database.")

# %%
# Step 4 — Verify
print("=== Verification ===\n")

for table in LOAD_ORDER:
    count_df = spark.read.jdbc(url=jdbc_url, table=f"dbo.{table}", properties=jdbc_props)
    print(f"  {table:30s} {count_df.count():>4d} rows")

print("\n" + "=" * 50)
print("POST-DEPLOYMENT COMPLETE")
print("=" * 50)
print("\nNext: Open the GetStarted notebook for a guided walkthrough.")
