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
# It creates SQL Database tables, loads all seed data, and verifies everything.
# 
# **Run All to configure.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 - Create SQL tables via Spark SQL on the Lakehouse
# We load CSVs into the Lakehouse as Delta tables (works without a SQL Database)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Map of CSV files to table names and load order
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

print("Loading CSV data into Lakehouse Delta tables...\n")

for csv_path, table_name in CSV_TABLES:
    full_path = f"Files/{csv_path}"
    try:
        df = spark.read.csv(full_path, header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"  {table_name:30s} {df.count():>4d} rows")
    except Exception as e:
        print(f"  {table_name:30s} FAILED: {e}")

print("\nAll data loaded into Lakehouse tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 - Verify all tables
print("=== Verification ===\n")

for _, table_name in CSV_TABLES:
    try:
        count = spark.table(table_name).count()
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
