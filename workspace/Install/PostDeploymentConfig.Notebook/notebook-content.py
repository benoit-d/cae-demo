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
# This notebook sets up all data stores after the SolutionInstaller completes.
# 
# **Data goes to two places:**
# - **Lakehouse** (Delta tables) - HR, ERP, telemetry reference data (read-heavy)
# - **SQL Database** (T-SQL) - Projects, Tasks, Task Type Durations (write-back for scheduling)
# 
# ## Prerequisites
# 1. SolutionInstaller has run (Lakehouse exists with CSVs in Files/)
# 2. Create a **Fabric SQL Database** named **CAEManufacturing_SQLDB** in the workspace
# 
# **Run All to configure.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
# Paste your SQL Database JDBC connection string below (from SQL Database > Settings > Connection strings)
# Leave empty to skip SQL Database setup (project data goes to Lakehouse only)

SQL_JDBC_CONNECTION_STRING = ""
# Example: "jdbc:sqlserver://xxxxx.database.fabric.microsoft.com:1433;database={MyDB-guid};encrypt=true;trustServerCertificate=false;authentication=ActiveDirectoryInteractive"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 - Parse config and discover workspace items
import os, re, requests, notebookutils, struct

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

if lh:
    LH_ID = lh["id"]
    BASE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Files"
    TABLES_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables"
    print(f"Lakehouse: {LH_ID}")
else:
    raise RuntimeError("CAEManufacturing_LH not found. Run SolutionInstaller first.")

print(f"SQL Database JDBC: {'configured' if SQL_JDBC_CONNECTION_STRING else 'not configured (Lakehouse fallback)'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 - Load reference data into Lakehouse Delta tables (read-only)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

LAKEHOUSE_CSVS = [
    ("data/hr/employees.csv", "employees"),
    ("data/hr/skills_certifications.csv", "skills_certifications"),
    ("data/hr/employee_schedules.csv", "employee_schedules"),
    ("data/hr/physical_limitations.csv", "physical_limitations"),
    ("data/hr/leave_of_absence.csv", "leave_of_absence"),
    ("data/hr/contractual_workforce.csv", "contractual_workforce"),
    ("data/hr/employee_agreements.csv", "employee_agreements"),
    ("data/erp/machines.csv", "machines"),
    ("data/erp/bill_of_materials.csv", "bill_of_materials"),
    ("data/erp/inventory.csv", "inventory"),
    ("data/erp/purchase_orders.csv", "purchase_orders"),
    ("data/erp/maintenance_history.csv", "maintenance_history"),
    ("data/telemetry/sensor_definitions.csv", "sensor_definitions"),
]

print("Loading reference data into Lakehouse Delta tables...\n")

for csv_rel, table_name in LAKEHOUSE_CSVS:
    csv_path = f"{BASE_PATH}/{csv_rel}"
    table_path = f"{TABLES_PATH}/{table_name}"
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").save(table_path)
        print(f"  {table_name:30s} {df.count():>4d} rows")
    except Exception as e:
        print(f"  {table_name:30s} FAILED: {e}")

print("\nLakehouse tables loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3 - Create SQL Database tables for project management (write-back)
SQL_ENDPOINT = ""
SQL_DBNAME = ""

if SQL_JDBC_CONNECTION_STRING:
    import pyodbc

    # Parse server and database from the JDBC string
    server_match = re.search(r'sqlserver://([^:;]+)', SQL_JDBC_CONNECTION_STRING)
    db_match = re.search(r'database=\{?([^};]+)\}?', SQL_JDBC_CONNECTION_STRING)

    if server_match and db_match:
        SQL_ENDPOINT = server_match.group(1)
        SQL_DBNAME = db_match.group(1)
        print(f"SQL Server:   {SQL_ENDPOINT}")
        print(f"SQL Database: {SQL_DBNAME}")
    else:
        print("ERROR: Could not parse JDBC string. Expected format:")
        print("  jdbc:sqlserver://<server>:1433;database={<dbname>};...")

if SQL_ENDPOINT and SQL_DBNAME:
    TOKEN_SQL = notebookutils.credentials.getToken("https://database.windows.net/")

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={SQL_ENDPOINT};"
        f"Database={SQL_DBNAME};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    token_bytes = TOKEN_SQL.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    print("Connecting...")
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.autocommit = True
    cursor = conn.cursor()

    DDL_STATEMENTS = [
        "IF OBJECT_ID('dbo.tasks', 'U') IS NOT NULL DROP TABLE dbo.tasks",
        "IF OBJECT_ID('dbo.projects', 'U') IS NOT NULL DROP TABLE dbo.projects",
        "IF OBJECT_ID('dbo.task_type_durations', 'U') IS NOT NULL DROP TABLE dbo.task_type_durations",
        """CREATE TABLE dbo.task_type_durations (
            Task_Type NVARCHAR(50) PRIMARY KEY, Task_Name NVARCHAR(100),
            Standard_Duration INT, Required_Skill NVARCHAR(50),
            Sequence_Order INT, Description NVARCHAR(500))""",
        """CREATE TABLE dbo.projects (
            Project_ID NVARCHAR(10) PRIMARY KEY, Project_Name NVARCHAR(100),
            Simulator_ID NVARCHAR(10), Initial_Planned_Start DATE,
            Modified_Planned_Start DATE, Standard_Duration INT,
            Actual_End DATE, Resource_Login NVARCHAR(100),
            Complete_Percentage INT, Last_Modified_By NVARCHAR(100),
            Last_Modified_On DATE)""",
        """CREATE TABLE dbo.tasks (
            Task_ID NVARCHAR(20) PRIMARY KEY, Task_Name NVARCHAR(100),
            Parent_Project_ID NVARCHAR(10) NOT NULL REFERENCES dbo.projects(Project_ID),
            FS_Task_ID NVARCHAR(20),
            Task_Type NVARCHAR(50) REFERENCES dbo.task_type_durations(Task_Type),
            Milestone INT, Skill_Requirement NVARCHAR(50),
            Initial_Planned_Start DATE, Modified_Planned_Start DATE,
            Actual_Start DATE, Standard_Duration INT, Actual_End DATE,
            Resource_Login NVARCHAR(100), Complete_Percentage INT,
            Last_Modified_By NVARCHAR(100), Last_Modified_On DATE)""",
    ]

    print("Creating SQL tables...")
    for ddl in DDL_STATEMENTS:
        try:
            cursor.execute(ddl)
            if "CREATE" in ddl:
                tbl = ddl.split("dbo.")[1].split(" ")[0].split("(")[0]
                print(f"  Created dbo.{tbl}")
        except Exception as e:
            print(f"  Warning: {e}")

    cursor.close()
    conn.close()
    print("SQL tables ready.")
else:
    if SQL_JDBC_CONNECTION_STRING:
        print("Could not parse JDBC string. Check the format.")
    else:
        print("No SQL JDBC string provided. Project data will go to Lakehouse only.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4 - Load project data into SQL Database
if SQL_ENDPOINT and SQL_DBNAME:
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

    SQL_CSVS = [
        ("data/erp/task_type_durations.csv", "dbo.task_type_durations"),
        ("data/cosmosdb/projects.csv", "dbo.projects"),
        ("data/cosmosdb/tasks.csv", "dbo.tasks"),
    ]

    print("Loading project data into SQL Database...\n")

    for csv_rel, table_name in SQL_CSVS:
        csv_path = f"{BASE_PATH}/{csv_rel}"
        try:
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            df.write.jdbc(url=JDBC_URL, table=table_name, mode="append", properties=jdbc_props)
            print(f"  {table_name:35s} {df.count():>4d} rows")
        except Exception as e:
            print(f"  {table_name:35s} FAILED: {e}")

    print("\nSQL Database loaded.")
else:
    # Fallback: also load project data to Lakehouse
    print("Loading project data into Lakehouse (SQL Database fallback)...\n")
    FALLBACK_CSVS = [
        ("data/erp/task_type_durations.csv", "task_type_durations"),
        ("data/cosmosdb/projects.csv", "projects"),
        ("data/cosmosdb/tasks.csv", "tasks"),
    ]
    for csv_rel, table_name in FALLBACK_CSVS:
        csv_path = f"{BASE_PATH}/{csv_rel}"
        table_path = f"{TABLES_PATH}/{table_name}"
        try:
            df = spark.read.csv(csv_path, header=True, inferSchema=True)
            df.write.format("delta").mode("overwrite").save(table_path)
            print(f"  {table_name:30s} {df.count():>4d} rows")
        except Exception as e:
            print(f"  {table_name:30s} FAILED: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5 - Verify everything
print("=== Verification ===\n")

print("Lakehouse Delta tables:")
for _, table_name in LAKEHOUSE_CSVS:
    table_path = f"{TABLES_PATH}/{table_name}"
    try:
        count = spark.read.format("delta").load(table_path).count()
        print(f"  {table_name:30s} {count:>4d} rows")
    except Exception:
        print(f"  {table_name:30s} NOT FOUND")

if SQL_ENDPOINT and SQL_DBNAME:
    print(f"\nSQL Database ({SQL_DBNAME}):")
    for _, table_name in SQL_CSVS:
        try:
            count = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=jdbc_props).count()
            print(f"  {table_name:35s} {count:>4d} rows")
        except Exception as e:
            print(f"  {table_name:35s} ERROR: {e}")
else:
    print("\nProject tables (Lakehouse fallback):")
    for tbl in ["task_type_durations", "projects", "tasks"]:
        try:
            count = spark.read.format("delta").load(f"{TABLES_PATH}/{tbl}").count()
            print(f"  {tbl:30s} {count:>4d} rows")
        except Exception:
            print(f"  {tbl:30s} NOT FOUND")

print("\n" + "=" * 50)
print("  POST-DEPLOYMENT COMPLETE")
print("=" * 50)
if SQL_ENDPOINT:
    print(f"\nProject tables are in SQL Database '{SQL_DBNAME}' (write-back enabled).")
    print("Reference data is in Lakehouse Delta tables (read-only).")
else:
    print("\nAll data is in Lakehouse Delta tables.")
    print("To enable write-back, paste the SQL JDBC string in the config cell and re-run.")
print("\nNext: Open the GetStarted notebook.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
