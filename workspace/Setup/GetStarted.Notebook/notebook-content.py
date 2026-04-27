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
# | Telemetry (normal) | DataEmulator notebook (loop) | Eventstream to KQL DB |
# | Telemetry (fault) | Manual notebook during demo | Eventstream to KQL DB |
# | Clock-in events | DataEmulator notebook (loop) | Eventstream to KQL DB |
# | Schedule changes | Agent / manual | SQL Database UPDATE |
# | Anomaly detection | KQL materialized views | Composite scores per machine |
# | Alerts | Activator on MachineHealthAlerts() | Teams / Power Automate |

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup Steps (after running SolutionInstaller)
# 
# **Automated by fabric-cicd** (deployed automatically):
# - Lakehouse, Eventhouse, Eventstreams, Activator shell, all Notebooks
# 
# **Run once manually:**
# 1. **Configure `connections.json`** - Open Lakehouse > Files > config > connections.json, paste your SQL JDBC connection string
# 2. **PostDeploymentConfig** - Run All (creates KQL DB + hr/erp/plm schemas + 24 SQL tables + EventStreams)
# 3. **Paste EventStream connection strings** into `connections.json` (TELEMETRY + CLOCKIN keys)
# 
# **Configure in Fabric UI:**
# 4. **Configure Activator** - MachineHealthActivator monitors MachineHealthAlerts() function
# 5. **Create Real-Time Dashboard** - use queries from data/kql/dashboard_spec.json
# 
# **Demo:**
# 6. **Start DataEmulator** - Run All (loops: telemetry + clock-in every minute)
# 7. **Inject fault** - set FAULT_INJECTION=true in connections.json (auto-resets after 10 min)
# 7. **Power BI Gantt** - connect to SQL Database plm.tasks + plm.projects
# 
# ## Anomaly Detection (10 rules)
# 
# Multivariate composite scores detect patterns that single thresholds miss:
# 
# | Rule | Machines | What it detects |
# |---|---|---|
# | CNC Bearing Wear | MCH-001/002 | Temp + vibration up while speed normal |
# | CNC Coolant Failure | MCH-001/002/003 | Coolant flow down + temp rising |
# | Laser Nozzle Degradation | MCH-004 | Nozzle temp + alignment drift + power compensating |
# | Press Hydraulic Leak | MCH-005 | Pressure variance + oil temp + oil level drop |
# | Welder Gas Contamination | MCH-006/007 | Arc voltage unstable + interpass temp rising |
# | CMM Environmental Drift | MCH-008 | Enclosure temp off 20C + accuracy degrading |
# | Reflow Profile Drift | MCH-011 | Zone deltas outside IPC-7530 profile |
# | 3D Printer O2 Ingress | MCH-012 | O2 level rising in chamber |
# | Crane Brake Wear | MCH-014 | Brake pads thinning + motor temp |
# | Hydraulic Pump Cavitation | MCH-015 | Pump vibration + flow drop + pressure unstable |
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
