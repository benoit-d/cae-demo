# Fabric notebook source
# %% [markdown]
# # CAE Flight Simulator Manufacturing — Get Started
#
# Welcome to the **CAE Flight Simulator Manufacturing** demo.
# This solution shows how **agentic AI workflows** can optimise capacity
# management in a flight-simulator factory using **Microsoft Fabric** and
# **Azure AI Foundry**.
#
# ## What's in the workspace
#
# | Category | Items |
# |---|---|
# | **SQL Database** | `CAEManufacturing_SQLDB` — 15 tables: projects, tasks, employees, skills, machines, inventory … |
# | **Lakehouse** | `CAEManufacturing_LH` — staging area (CSVs in Files/) |
# | **Eventhouse** | `CAEManufacturingEH` — real-time telemetry & clock-in events |
# | **Eventstreams** | `SimulatorTelemetryStream`, `ClockInEventStream` |
# | **Notebooks** | This guide + PostDeploymentConfig, LoadData, 3 simulators, Agent |
#
# ## The Story (April 21, 2026)
#
# You manage 8 flight-simulator builds for airlines worldwide:
#
# | Project | Simulator | Customer | Status |
# |---|---|---|---|
# | PRJ-003 | SIM-003 Boeing 777X | Emirates | ✅ 100 % — Delivered |
# | PRJ-001 | SIM-001 Boeing 737 MAX | Air Canada | 🔧 84 % — Qualification Testing |
# | PRJ-002 | SIM-002 Airbus A320neo | Lufthansa | 🔧 30 % — Cockpit Integration |
# | PRJ-006 | SIM-006 Boeing 737 MAX | WestJet | 🔧 15 % — Hydraulics |
# | PRJ-007 | SIM-007 Airbus A320neo | Air France | ⏳ 0 % — Planned |
# | PRJ-004 | SIM-004 Airbus A350 | Delta Airlines | ⏳ 0 % — Planned |
# | PRJ-005 | SIM-005 Boeing 787 | United Airlines | ⏳ 0 % — Planned |
# | PRJ-008 | SIM-008 Boeing 777X | Qatar Airways | ⏳ 0 % — Planned |
#
# ### Your 12-person team
# 10 FTEs + 2 contractors. 4 seniors have physical limitations.
# Skills span: Motion Systems, Hydraulics, Electrical, Avionics,
# Visual Systems, Structures, Test Engineering — with cross-coverage.
#
# ## Step-by-Step
#
# ### 1. Start the simulators
# Open **Simulation/SimulatorTelemetryEmulator** → Run All.
# It sends 60 sensor readings (3 sims × 20 sensors) every 30 s.
#
# ### 2. Inject a fault
# Open **Simulation/TelemetryFaultInjection** → Run All.
# SIM-001 hydraulic pressure will degrade over 10 minutes.
#
# ### 3. Generate clock-in events
# Open **Simulation/ClockInEventEmulator** → Run All.
# Produces badge-in/out and task-completion events.
#
# ### 4. Explore the data
# Query the SQL Database directly:
# ```sql
# SELECT p.Project_Name, t.Task_Name, t.Resource_Login,
#        t.Modified_Planned_Start, t.Standard_Duration,
#        t.Complete_Percentage
# FROM dbo.tasks t
# JOIN dbo.projects p ON t.Parent_Project_ID = p.Project_ID
# ORDER BY t.Modified_Planned_Start;
# ```
#
# ### 5. Build the Gantt chart
# In Power BI, use the **Gantt** visual (MAQ Software):
# - **Task** = Task_Name
# - **Parent** = Parent_Project_ID
# - **Start Date** = Modified_Planned_Start (or Actual_Start)
# - **Duration** = Standard_Duration
# - **% Complete** = Complete_Percentage
# - **Resource** = Resource_Login
#
# ### 6. Run the Capacity Management Agent
# Open **Agent/CapacityManagementAgent** → Run All.
# It reasons across telemetry, SQL tables, and the skills matrix.

# %%
# Quick data check — verify PostDeploymentConfig ran successfully
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import notebookutils
ws_items = notebookutils.fabric.list_items()
sql_db = next((i for i in ws_items if 'SQLDB' in i.get('displayName', '') or 'SQLDatabase' in i.get('type', '')), None)

if sql_db:
    token = notebookutils.credentials.getToken("https://database.windows.net/")
    jdbc_url = (
        f"jdbc:sqlserver://{sql_db['displayName']}.database.fabric.microsoft.com:1433;"
        f"database={sql_db['displayName']};encrypt=true;trustServerCertificate=false;loginTimeout=30;"
    )
    props = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "accessToken": token}

    tables = ["employees", "projects", "tasks", "machines", "skills_certifications"]
    for t in tables:
        count = spark.read.jdbc(url=jdbc_url, table=f"dbo.{t}", properties=props).count()
        print(f"  {t:30s} {count:>4d} rows")
else:
    print("SQL Database not found. Run PostDeploymentConfig first.")
