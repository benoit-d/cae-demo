# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # CAE Flight Simulator Manufacturing - Get Started
# 
# Welcome to the **CAE Flight Simulator Manufacturing** demo.
# This solution shows how agentic AI workflows can optimise capacity
# management in a flight-simulator factory using Microsoft Fabric and Azure AI Foundry.
# 
# ## What is in the workspace
# 
# | Category | Items |
# |---|---|
# | **SQL Database** | CAEManufacturing_SQLDB - 15 tables (projects, tasks, employees, ...) |
# | **Lakehouse** | CAEManufacturing_LH - staging area (CSVs in Files/) |
# | **Notebooks** | This guide + PostDeploymentConfig, 3 Simulators, Agent |

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## The Story (April 21, 2026)
# 
# You manage 8 flight-simulator builds for airlines worldwide:
# 
# | Project | Simulator | Customer | Status |
# |---|---|---|---|
# | PRJ-003 | SIM-003 Boeing 777X | Emirates | 100% Delivered |
# | PRJ-001 | SIM-001 Boeing 737 MAX | Air Canada | 84% Qualification Testing |
# | PRJ-002 | SIM-002 Airbus A320neo | Lufthansa | 30% Cockpit Integration |
# | PRJ-006 | SIM-006 Boeing 737 MAX | WestJet | 15% Hydraulics |
# | PRJ-004 to PRJ-008 | Various | Various | 0% Planned |
# 
# Your 12-person team: 10 FTEs + 2 contractors, 4 seniors with physical limitations.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step-by-Step Guide
# 
# 1. **Run PostDeploymentConfig** (if not done) - creates SQL tables, loads data
# 2. **Start Simulation/SimulatorTelemetryEmulator** - streams 60 sensor readings every 30s
# 3. **Start Simulation/TelemetryFaultInjection** - degrades SIM-001 hydraulics over 10 min
# 4. **Start Simulation/ClockInEventEmulator** - badge-in/out and task events
# 5. **Build Gantt chart** in Power BI (MAQ Software visual): Task=Task_Name, Start=Modified_Planned_Start, Duration=Standard_Duration, %Complete=Complete_Percentage, Resource=Resource_Login
# 6. **Run Agent/CapacityManagementAgent** - reasons across all data sources

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick data check - list items in the workspace
import os, requests, notebookutils

TOKEN = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except Exception:
        pass

if WORKSPACE_ID:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=headers)
    items = resp.json().get("value", [])
    print(f"Found {len(items)} items in workspace:\n")
    for i in sorted(items, key=lambda x: x.get("type", "")):
        print(f"  {i['type']:20s}  {i['displayName']}")
else:
    print("Could not detect workspace ID.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
