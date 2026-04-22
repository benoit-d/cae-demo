# CAE Flight Simulator Manufacturing Demo

An end-to-end **Microsoft Fabric** demo that simulates a CAE-style flight simulator manufacturing facility in Montreal. It combines real-time machine telemetry, workforce management, project scheduling, and agentic AI to demonstrate capacity optimisation across a factory floor.

## The Story

CAE builds full-flight simulators (FFS) for airlines worldwide. Each simulator is a multi-million-dollar machine assembled from precision-machined parts, hydraulic systems, avionics, visual projections, and control loading systems.

This demo models **8 concurrent simulator build projects** for customers including Air Canada, Lufthansa, Emirates, Delta, United, WestJet, Air France, and Qatar Airways. A team of **12 workers** (10 FTEs + 2 contractors) builds these simulators using **15 manufacturing machines** — CNC mills, lathes, laser cutters, welders, CMMs, and more.

The AI agent reasons across all data sources to:
- Detect machine health issues from telemetry and schedule preventive maintenance
- Reassign workers based on skills, certifications, physical limitations, and union rules
- Optimise the project schedule when tasks slip or resources become unavailable
- Explain every decision with full reasoning

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │         Microsoft Fabric Workspace       │
                        ├─────────────────────────────────────────┤
                        │                                         │
  Manufacturing         │   ┌─────────────┐    ┌──────────────┐  │
  Machines (15)    ────►│   │ Eventstream  │───►│  Eventhouse   │  │
  Telemetry             │   │             │    │  (KQL DB)    │  │
                        │   └─────────────┘    └──────────────┘  │
  Workforce             │   ┌─────────────┐           │          │
  Clock-in/out     ────►│   │ Eventstream  │───►       │          │
  Task events           │   └─────────────┘           ▼          │
                        │                     ┌──────────────┐   │
                        │                     │  Power BI     │   │
  Reference Data        │   ┌─────────────┐   │  Dashboards   │   │
  HR, BOM, Inventory───►│   │ SQL Database │──►│  Gantt Chart  │   │
  Projects, Tasks       │   │  hr.*        │   └──────────────┘   │
                        │   │  erp.*       │          │          │
                        │   └──────┬───────┘          │          │
                        │          │                  │          │
                        │          ▼                  ▼          │
                        │   ┌─────────────────────────────┐      │
                        │   │  AI Agent (Foundry)          │      │
                        │   │  Capacity Management         │      │
                        │   └─────────────────────────────┘      │
                        └─────────────────────────────────────────┘
```

## Data Stores

| Store | Schema | Tables | Purpose |
|---|---|---|---|
| **SQL Database** | `hr` | employees, skills_certifications, employee_schedules, physical_limitations, leave_of_absence, contractual_workforce, employee_agreements | Workforce data with CRUD |
| **SQL Database** | `erp` | simulators, machines, projects, tasks, task_type_durations, bill_of_materials, inventory, purchase_orders, maintenance_history, sensor_definitions | Manufacturing + project management |
| **Eventhouse** (KQL) | — | machine_telemetry, clockin_events | Real-time event data |
| **Lakehouse** | — | CSV files in Files/ | Staging only (deployment) |

## Manufacturing Machines (15)

| ID | Type | Machine | Manufacturer | Zone |
|---|---|---|---|---|
| MCH-001 | CNC Mill | 5-Axis CNC Milling Center | DMG MORI | Machining |
| MCH-002 | CNC Mill | 3-Axis CNC Milling Machine | Haas | Machining |
| MCH-003 | CNC Lathe | CNC Turning Center | Mazak | Machining |
| MCH-004 | Laser Cutter | Fiber Laser Cutting System | Trumpf | Sheet Metal |
| MCH-005 | Press Brake | CNC Hydraulic Press Brake | Amada | Sheet Metal |
| MCH-006 | TIG Welder | Automated TIG Welding Cell | Lincoln Electric | Welding |
| MCH-007 | MIG Welder | Robotic MIG Welding Cell | Fanuc | Welding |
| MCH-008 | CMM | Coordinate Measuring Machine | Zeiss | Quality |
| MCH-009 | Wire EDM | Wire EDM Machine | Sodick | Machining |
| MCH-010 | Electronics | Electronics Assembly Station | Juki | Electronics |
| MCH-011 | Reflow Oven | SMT Reflow Oven | Heller | Electronics |
| MCH-012 | 3D Printer | Metal Additive Manufacturing | EOS | Additive |
| MCH-013 | Paint Booth | Downdraft Paint Spray Booth | Global Finishing | Finishing |
| MCH-014 | Crane | 50-Ton Overhead Bridge Crane | Konecranes | Assembly |
| MCH-015 | Hydraulic Test | Hydraulic Test Bench | Parker Hannifin | Test |

**75 sensors** across all machines — spindle speed/temperature/vibration, coolant flow, laser power, arc voltage, welding current, probe deflection, reflow zone temperatures, and more.

## Simulator Projects (8)

| Project | Simulator | Customer | Status (Apr 21, 2026) |
|---|---|---|---|
| PRJ-003 | SIM-003 Boeing 777X | Emirates | 100% Delivered |
| PRJ-001 | SIM-001 Boeing 737 MAX | Air Canada | 84% Qualification Testing |
| PRJ-002 | SIM-002 Airbus A320neo | Lufthansa | 30% Cockpit Integration |
| PRJ-006 | SIM-006 Boeing 737 MAX | WestJet | 15% Hydraulics |
| PRJ-007 | SIM-007 Airbus A320neo | Air France | 0% Planned |
| PRJ-004 | SIM-004 Airbus A350 | Delta Airlines | 0% Planned |
| PRJ-005 | SIM-005 Boeing 787 | United Airlines | 0% Planned |
| PRJ-008 | SIM-008 Boeing 777X | Qatar Airways | 0% Planned |

Each project has **13 tasks** with finish-to-start dependencies, skill requirements, and a standard duration from the `task_type_durations` reference table. The Gantt structure is Power BI-compatible (Task Name, Start, Duration, % Complete, Resource).

## Workforce (12 workers + 1 PM)

| Employee | Type | Specialty | Limitation |
|---|---|---|---|
| Jean-Pierre Tremblay | FTE Senior | Motion Systems | Back — 25kg lift max |
| Marie-Claire Dubois | FTE | Hydraulics | — |
| Luc Bergeron | FTE Senior | Electrical | Knee — no ladders |
| Sophie Lavoie | FTE | Visual Systems | — |
| Philippe Gagnon | FTE Senior | Avionics | Hearing — noise restricted |
| Marc-André Pelletier | FTE Senior | Hydraulics (Night) | Respiratory — no chemicals |
| Catherine Morin | FTE | Electrical | — |
| François Côté | FTE | Motion Systems | — |
| David Chen | FTE | Test Engineering | — |
| Nathalie Bouchard | FTE | Structures | — |
| James Taylor | Contractor | Hydraulics | — |
| Maria Garcia | Contractor | Electrical | — |
| **Daniel Fortin** | **PM** | **Production Manager** | — |

## Repo Structure

```
cae-demo/
├── deploy/
│   └── SolutionInstaller.ipynb          # Import into Fabric → Run All
├── workspace/                           # Published by fabric-cicd
│   ├── CAEManufacturing_LH.Lakehouse/   # Staging Lakehouse
│   ├── CAEManufacturingEH.Eventhouse/   # Real-time telemetry store
│   ├── MachineHealthActivator.Reflex/   # Anomaly alert trigger
│   ├── GetStarted.Notebook/             # Guided walkthrough
│   ├── PostDeploymentConfig.Notebook/   # Creates SQL tables, loads data
│   ├── Simulation/
│   │   ├── SimulatorTelemetryEmulator.Notebook/  # Single-shot, pipeline-scheduled
│   │   ├── ClockInEventEmulator.Notebook/         # Single-shot, pipeline-scheduled
│   │   └── TelemetryFaultInjection.Notebook/      # Manual — CNC mill failure demo
│   └── Agent/
│       └── CapacityManagementAgent.Notebook/      # AI agent querying SQL DB
├── data/
│   ├── erp/          # machines, inventory, purchase orders, maintenance history
│   ├── hr/           # employees, skills, schedules, limitations, leave, contractors
│   ├── plm/          # simulators, BOMs, projects, tasks, task type durations
│   ├── kql/          # KQL queries, dashboard spec, anomaly scoring rules
│   └── telemetry/    # sensor_definitions.csv (75 sensors x 15 machines)
└── scripts/          # Local Python tools
    ├── generate_project_data.py    # Regenerate 8 projects with scheduling constraints
    ├── telemetry_normal.py         # Standalone telemetry generator
    ├── telemetry_fault_injection.py # CNC mill fault profile
    ├── clockin_events.py           # Workforce event generator
    └── validate_data.py            # Referential integrity checker
```

## Deployment

### 1. Run the SolutionInstaller

In a Fabric notebook, run these cells:

```python
# Cell 1
%pip install -q fabric-cicd azure-identity gitpython

# Cell 2
import os, shutil, tempfile, glob, requests
from git import Repo
from fabric_cicd import FabricWorkspace, publish_all_items
import notebookutils
from azure.core.credentials import AccessToken

clone_dir = os.path.join(tempfile.gettempdir(), "cae-demo-install")
if os.path.exists(clone_dir): shutil.rmtree(clone_dir)
Repo.clone_from("https://github.com/benoit-d/cae-demo.git", clone_dir, branch="master", depth=1)
workspace_dir = os.path.join(clone_dir, "workspace")
data_dir = os.path.join(clone_dir, "data")

WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except: pass

TOKEN = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
class _Cred:
    def get_token(self, *s, **k): return AccessToken(TOKEN, 0)

ws = FabricWorkspace(workspace_id=WORKSPACE_ID, repository_directory=workspace_dir,
    item_type_in_scope=[
        "Notebook", "Lakehouse", "Environment",
        "Eventhouse", "Eventstream",
        "KQLDatabase", "KQLDashboard", "KQLQueryset",
        "Reflex", "SemanticModel", "Report", "SQLDatabase",
    ], token_credential=_Cred())
publish_all_items(ws)

# Cell 3 — Upload seed data to Lakehouse
headers = {"Authorization": f"Bearer {TOKEN}"}
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=headers)
items = resp.json().get("value", [])
lh = next((i for i in items if i.get("displayName") == "CAEManufacturing_LH"), None)
if lh:
    for folder in ["erp", "hr", "telemetry", "plm"]:
        src = os.path.join(data_dir, folder)
        if not os.path.isdir(src): continue
        for f in sorted(glob.glob(os.path.join(src, "*"))):
            dest = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh['id']}/Files/data/{folder}/{os.path.basename(f)}"
            notebookutils.fs.cp(f"file://{f}", dest)

shutil.rmtree(clone_dir, ignore_errors=True)
```

### 2. Create a Fabric SQL Database

In the workspace, click **+ New item > SQL Database** and name it `CAEManufacturing_SQLDB`.

### 3. Run PostDeploymentConfig

Open the deployed `PostDeploymentConfig` notebook. Paste the JDBC connection string (from SQL Database > Settings > Connection strings) in the config cell. Run All.

This creates `hr.*` and `erp.*` schemas with 17 tables, bulk inserts all data, then adds primary keys and foreign keys.

### 4. Set Up Eventstreams and Pipelines

1. Create **SimulatorTelemetryStream** Eventstream with Custom App source → KQL Database destination
2. Create **ClockInEventStream** Eventstream with Custom App source → KQL Database destination
3. Paste connection strings into the simulator notebook config cells
4. Create Data Pipelines: Notebook activity → 1-minute schedule for each emulator

### 5. Demo

- Pipelines stream machine telemetry and clock-in events every minute
- Run **TelemetryFaultInjection** manually to simulate a CNC mill spindle bearing failure
- Open **CapacityManagementAgent** to see the AI reason across all sources
- Build a **Power BI Gantt chart** from `erp.projects` + `erp.tasks`

## Referential Integrity

All data has verified referential integrity:
- Employee emails link across: employees → tasks → projects → clock-in events → maintenance history
- Simulator IDs link: simulators → projects
- Machine IDs link: machines → sensor_definitions → maintenance_history → telemetry events
- Task dependencies: tasks.FS_Task_ID → tasks.Task_ID (self-referencing within project)
- Skill requirements: tasks.Skill_Requirement matches assigned employee's skills_certifications
- No employee is double-booked across concurrent tasks
- No actual dates are in the future (relative to April 21, 2026)

Run `python scripts/validate_data.py` to verify.

## Key Design Decisions

| Decision | Rationale |
|---|---|
| SQL Database for all reference/project tables | CRUD for write-back (schedule updates, task completions), DirectQuery for Power BI, agent-friendly |
| Eventhouse for telemetry + events | Sub-second queries on time-series data, native KQL |
| Lakehouse as staging only | CSVs upload there during deployment, then get loaded into SQL DB |
| Single-shot notebooks for data pipelines | No long-running Spark executors; pipeline calls notebook every 1 min |
| Constraints added after bulk insert | Avoids FK ordering issues during initial data load |
| Separate simulators (products) from machines (equipment) | Telemetry monitors manufacturing machines, not the simulators being built |

## License

This project is provided as-is for demonstration and educational purposes.
