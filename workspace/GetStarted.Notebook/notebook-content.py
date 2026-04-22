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
# ## Architecture
# 
# | Store | Data | Purpose |
# |---|---|---|
# | **Lakehouse** (Delta) | HR, ERP, BOM, inventory, sensor defs | Read-heavy reference data |
# | **SQL Database** | Projects, Tasks, Task Type Durations | Write-back for scheduling (CRUD) |
# | **Eventhouse** (KQL DB) | Telemetry events, Clock-in events | Real-time event queries |
# | **Eventstreams** | SimulatorTelemetryStream, ClockInEventStream | Ingestion from pipelines |
# 
# ## Data Flow
# 
# | Source | Mechanism | Destination |
# |---|---|---|
# | Telemetry (normal) | Data Pipeline (every 1 min) calls notebook | Eventstream to KQL DB |
# | Telemetry (fault) | Manual notebook run during demo | Eventstream to KQL DB |
# | Clock-in events | Data Pipeline (every 1 min) calls notebook | Eventstream to KQL DB |
# | Schedule changes | Agent / manual | SQL Database UPDATE |

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup Steps
# 
# ### 1. PostDeploymentConfig (already done if you see data)
# Loads reference data into Lakehouse + project tables into SQL Database.
# 
# ### 2. Create Eventstreams (manual in Fabric UI)
# - Create **SimulatorTelemetryStream** Eventstream
# - Create **ClockInEventStream** Eventstream
# - For each: add a **Custom App** source and copy the connection string
# - Add a **KQL Database** destination routing to CAEManufacturingEH
# 
# ### 3. Create Data Pipelines (manual in Fabric UI)
# - **TelemetryPipeline**: Notebook activity pointing to SimulatorTelemetryEmulator, schedule every 1 minute
# - **ClockInPipeline**: Notebook activity pointing to ClockInEventEmulator, schedule every 1 minute
# 
# ### 4. Paste connection strings into the notebooks
# Open each simulator notebook, paste the Eventstream connection string in the config cell.
# 
# ### 5. Demo: Inject a fault
# Open **Simulation/TelemetryFaultInjection**, run manually. SIM-001 hydraulics degrade over 10 min.
# 
# ### 6. Power BI Gantt
# Connect to SQL Database. Use Gantt visual (MAQ Software):
# Task=Task_Name, Start=Modified_Planned_Start, Duration=Standard_Duration, %=Complete_Percentage, Resource=Resource_Login

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## The 8 Projects (as of April 21, 2026)
# 
# | Project | Simulator | Customer | Status |
# |---|---|---|---|
# | PRJ-003 | SIM-003 Boeing 777X | Emirates | 100% Delivered |
# | PRJ-001 | SIM-001 Boeing 737 MAX | Air Canada | 84% Qualification |
# | PRJ-002 | SIM-002 Airbus A320neo | Lufthansa | 30% Assembly |
# | PRJ-006 | SIM-006 Boeing 737 MAX | WestJet | 15% Hydraulics |
# | PRJ-004 to PRJ-008 | Various | Various | 0% Planned |

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick check - list workspace items
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
    print(f"Workspace has {len(items)} items:\n")
    for i in sorted(items, key=lambda x: (x.get("type", ""), x.get("displayName", ""))):
        print(f"  {i['type']:20s}  {i['displayName']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
