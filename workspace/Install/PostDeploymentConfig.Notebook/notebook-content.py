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
# Run this notebook after the SolutionInstaller completes.
# It loads all CSV seed data from the Lakehouse into Delta tables.
# 
# **Run All to configure.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 - Discover the Lakehouse and build the base path
import os, requests, notebookutils

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

if lh:
    LH_ID = lh["id"]
    BASE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Files"
    print(f"Lakehouse found: {LH_ID}")
    print(f"Base path: {BASE_PATH}")
else:
    raise RuntimeError("CAEManufacturing_LH not found. Run the SolutionInstaller first.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 - Verify seed data files exist
test_files = [
    "data/hr/employees.csv",
    "data/cosmosdb/projects.csv",
    "data/erp/machines.csv",
    "data/telemetry/sensor_definitions.csv",
]
print("Checking seed data files...\n")
all_ok = True
for f in test_files:
    try:
        notebookutils.fs.ls(f"{BASE_PATH}/{f}")
        print(f"  OK   {f}")
    except Exception:
        print(f"  MISSING  {f}")
        all_ok = False

if not all_ok:
    print("\nSome files are missing. Re-run the SolutionInstaller Step 4 (upload seed data).")
else:
    print("\nAll seed data files present.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3 - Load CSVs into Lakehouse Delta tables
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

CSV_TABLES = [
    ("data/hr/employees.csv", "employees"),
    ("data/hr/skills_certifications.csv", "skills_certifications"),
    ("data/hr/employee_schedules.csv", "employee_schedules"),
    ("data/hr/physical_limitations.csv", "physical_limitations"),
    ("data/hr/leave_of_absence.csv", "leave_of_absence"),
    ("data/hr/contractual_workforce.csv", "contractual_workforce"),
    ("data/hr/employee_agreements.csv", "employee_agreements"),
    ("data/erp/machines.csv", "machines"),
    ("data/erp/task_type_durations.csv", "task_type_durations"),
    ("data/erp/bill_of_materials.csv", "bill_of_materials"),
    ("data/erp/inventory.csv", "inventory"),
    ("data/erp/purchase_orders.csv", "purchase_orders"),
    ("data/erp/maintenance_history.csv", "maintenance_history"),
    ("data/cosmosdb/projects.csv", "projects"),
    ("data/cosmosdb/tasks.csv", "tasks"),
]

# Delta table output path (Tables area of the same Lakehouse)
TABLES_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables"

print("Loading CSV data into Lakehouse Delta tables...\n")

for csv_rel, table_name in CSV_TABLES:
    csv_path = f"{BASE_PATH}/{csv_rel}"
    table_path = f"{TABLES_PATH}/{table_name}"
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").save(table_path)
        print(f"  {table_name:30s} {df.count():>4d} rows")
    except Exception as e:
        print(f"  {table_name:30s} FAILED: {e}")

print("\nAll data loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4 - Verify all tables
print("=== Verification ===\n")

for _, table_name in CSV_TABLES:
    table_path = f"{TABLES_PATH}/{table_name}"
    try:
        count = spark.read.format("delta").load(table_path).count()
        print(f"  {table_name:30s} {count:>4d} rows")
    except Exception:
        print(f"  {table_name:30s} NOT FOUND")

print("\n" + "=" * 50)
print("  POST-DEPLOYMENT COMPLETE")
print("=" * 50)
print("\nNext: Open the GetStarted notebook for a guided walkthrough.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
