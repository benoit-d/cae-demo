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
# | Store | Schema | Data | Purpose |
# |---|---|---|---|
# | **SQL Database** | hr.* | employees, skills, schedules, limitations, leave, contractors, agreements | Workforce data (CRUD) |
# | **SQL Database** | erp.* | machines, projects, tasks, task types, BOM, inventory, POs, maintenance, sensors | Manufacturing + project mgmt (CRUD) |
# | **Eventhouse** (KQL DB) | - | Telemetry events, Clock-in events | Real-time event queries |
# | **Lakehouse** | - | CSV files in Files/ area | Staging only (used during deployment) |
# 
# ## Data Flow
# 
# | Source | Mechanism | Destination |
# |---|---|---|
# | Telemetry (normal) | Data Pipeline (1 min) calls notebook | Eventstream to KQL DB |
# | Telemetry (fault) | Manual notebook during demo | Eventstream to KQL DB |
# | Clock-in events | Data Pipeline (1 min) calls notebook | Eventstream to KQL DB |
# | Schedule changes | Agent / manual | SQL Database UPDATE |

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup Steps
# 
# 1. **PostDeploymentConfig** - creates hr.* and erp.* schemas + 16 tables with PKs/FKs in SQL DB
# 2. **Create Eventstreams** - SimulatorTelemetryStream + ClockInEventStream (manual in Fabric UI)
# 3. **Create Data Pipelines** - TelemetryPipeline + ClockInPipeline, 1-min schedule
# 4. **Paste connection strings** into the simulator notebook config cells
# 5. **Demo: Inject fault** - run TelemetryFaultInjection manually
# 6. **Power BI Gantt** - connect to SQL Database erp.tasks + erp.projects
# 
# ## The 8 Projects (April 21, 2026)
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
