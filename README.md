# CAE Flight Simulator Manufacturing Demo

An end-to-end **Microsoft Fabric** demo that simulates a CAE-style flight simulator manufacturing facility in Montreal. It combines real-time machine telemetry, workforce management, project scheduling, and agentic AI to demonstrate capacity optimisation across a factory floor.

## The Story

CAE builds full-flight simulators (FFS) for airlines worldwide. Each simulator is a multi-million-dollar machine assembled from precision-machined parts, hydraulic systems, avionics, visual projections, and control loading systems.

This demo models **9 concurrent simulator build projects** for customers including Air Canada, Lufthansa, Emirates, Delta, United, WestJet, Air France, Qatar Airways, and the Royal Canadian Air Force. A team of **28 technicians + 4 managers** (25 FTEs + 3 contractors) builds these simulators using **20 manufacturing machines** — CNC mills, lathes, laser cutters, welders, press brakes, CMMs, and more.

The AI agent reasons across all data sources to:
- Detect machine health issues from telemetry and schedule preventive maintenance
- Reassign workers based on skills, certifications, physical limitations, and union rules
- Optimise the project schedule when tasks slip or resources become unavailable
- Explain every decision with full reasoning

## Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │           Microsoft Fabric Workspace          │
                        ├──────────────────────────────────────────────┤
                        │                                              │
  Manufacturing         │   ┌────────────────────────────────┐         │
  Machines (20)    ────►│   │  TelemetryEventStream          │         │
  Telemetry             │   │  (Custom Endpoint source)      │         │
  (Event Hub SDK)       │   └───────┬───────────┬────────────┘         │
                        │           │           │                      │
  Workforce             │   ┌───────┴──────┐    │ ┌────────────────┐   │
  Clock-in/out     ────►│   │  Eventhouse   │   └►│  Activator     │   │
  Task events           │   │  (KQL DB)     │     │  (Reflex)      │   │
  (Event Hub SDK)       │   │  Machine      │     │  alert_level   │   │
                        │   │  Telemetry    │     │  = Critical    │   │
  ┌──────────────────┐  │   │  ClockIn      │     └───────┬────────┘   │
  │ClockInEventStream│──│──►│  Events       │             │            │
  │(Custom Endpoint) │  │   │  Anomaly      │     triggers│            │
  └──────────────────┘  │   │  Alerts       │             ▼            │
                        │   └──────┬────────┘     ┌────────────────┐   │
                        │          │              │ AnomalyDetect. │   │
                        │          │ 16 KQL       │ (ML Z-score)   │   │
                        │          │ health       └───────┬────────┘   │
                        │          │ functions            │            │
                        │          ▼                      ▼            │
                        │                         ┌────────────────┐   │
  Reference Data        │                         │ Foundry Agent  │   │
  HR, BOM, Inventory───►│                         │ (root cause +  │   │
  Projects, Tasks       │   ┌────────────┐        │  Teams notif.) │   │
                        │   │ Power BI   │        └───────┬────────┘   │
                        │   │ Dashboards │◄──┐            │            │
                        │   │ Gantt Chart│   │            ▼            │
                        │   └────────────┘   │     Microsoft Teams     │
                        │                    │                         │
                        │              ┌─────┴───────┐                 │
                        │              │ SQL Database │                 │
                        │              │ hr.* erp.*   │                 │
                        │              │ plm.* mes.*  │                 │
                        │              └──────────────┘                 │
                        └──────────────────────────────────────────────┘
```

## Data Stores

| Store | Schema | Tables | Purpose |
|---|---|---|---|
| **SQL Database** | `hr` | employees, skills_certifications, employee_schedules, work_restrictions, time_off, contractor_agreements, collective_agreements, machine_certifications | Workforce data with CRUD |
| **SQL Database** | `erp` | production_lines, production_line_dependencies, machines, inventory, purchase_orders, maintenance_history, contract_clauses | Production infrastructure |
| **SQL Database** | `plm` | simulators, bill_of_materials, projects, tasks, task_type_durations, part_specs, machine_capabilities | Product lifecycle management |
| **SQL Database** | `mes` | machine_jobs | Manufacturing execution |
| **SQL Database** | `telemetry` | sensor_definitions | Sensor metadata (107 sensors × 20 machines) |
| **Eventhouse** (KQL) | — | MachineTelemetry, ClockInEvents, AnomalyDetection | Real-time event data + ML anomaly alerts |
| **Eventhouse** (KQL) | — | 16 health scoring functions | Composite anomaly scores per machine type |
| **EventStream** | — | TelemetryEventStream | Routes telemetry → Eventhouse + Activator |
| **EventStream** | — | ClockInEventStream | Routes workforce events → Eventhouse |
| **Lakehouse** | — | CSV files in Files/ | Staging only (deployment) |

## Manufacturing Machines (20)

| ID | Type | Machine | Manufacturer | Line | Zone |
|---|---|---|---|---|---|
| CNC-001 | CNC Mill | 5-Axis CNC Milling Center | DMG MORI | PL-01 | Machining |
| CNC-002 | CNC Mill | 3-Axis CNC Milling Machine | Haas | PL-01 | Machining |
| CNC-003 | CNC Mill | 3-Axis Horizontal CNC Mill | Mazak | PL-01 | Machining |
| CNC-005 | CNC Mill | 5-Axis CNC Milling Center | DMG MORI | PL-01 | Machining |
| LTH-001 | CNC Lathe | 2-Axis CNC Turning Center | Mazak | PL-01 | Machining |
| LTH-002 | CNC Lathe | Multi-Axis CNC Turning Center | Okuma | PL-01 | Machining |
| LSR-001 | Laser Cutter | Fiber Laser Cutting System | Trumpf | PL-01 | Sheet Metal |
| LSR-002 | Laser Cutter | Fiber Laser Cutter 8kW | TRUMPF | PL-01 | Sheet Metal |
| PRB-001 | Press Brake | CNC Hydraulic Press Brake | Amada | PL-01 | Sheet Metal |
| EDM-001 | Wire EDM | Wire EDM Machine | Sodick | PL-01 | Machining |
| ADD-001 | 3D Printer | Metal Additive Manufacturing | EOS | PL-01 | Additive |
| WLD-001 | TIG Welder | Automated TIG Welding Cell | Lincoln Electric | PL-02 | Welding |
| WLD-002 | MIG Welder | Robotic MIG Welding Cell | Fanuc | PL-02 | Welding |
| CMM-001 | CMM | Coordinate Measuring Machine | Zeiss | PL-02 | Quality |
| PNT-001 | Paint Booth | Downdraft Paint Spray Booth | Global Finishing | PL-02 | Finishing |
| PNT-002 | Paint Booth | Automated Robotic Paint Booth | Durr | PL-02 | Finishing |
| CRN-001 | Overhead Crane | 50-Ton Overhead Bridge Crane | Konecranes | PL-02 | Assembly |
| HTB-001 | Hydraulic Test | Hydraulic Test Bench | Parker Hannifin | PL-02 | Test |
| ASM-001 | Electronics Assembly | Electronics Assembly Station | Juki | PL-03 | Electronics |
| RFL-001 | Reflow Oven | SMT Reflow Oven | Heller | PL-03 | Electronics |

**3 Production Lines:** PL-01 Precision Fabrication (Building A), PL-02 Assembly & Integration (Building B), PL-03 Electronics & Systems (Building C)

**107 sensors** across all machines — spindle speed/temperature/vibration, coolant flow, laser power, arc voltage, welding current, probe deflection, reflow zone temperatures, and more.

## Simulator Projects (9)

| Project | Simulator | Customer | Type | Status (Apr 22, 2026) |
|---|---|---|---|---|
| PRJ-003 | SIM-003 Boeing 777X | Emirates | Civilian | 100% Delivered |
| PRJ-001 | SIM-001 Boeing 737 MAX | Air Canada | Civilian | 84% Qualification Testing |
| PRJ-009 | SIM-009 CF-18 Hornet | Royal Canadian Air Force | Military | 45% Qualification Testing |
| PRJ-002 | SIM-002 Airbus A320neo | Lufthansa | Civilian | 30% Cockpit Integration |
| PRJ-006 | SIM-006 Boeing 737 MAX | WestJet | Civilian | 15% Hydraulics |
| PRJ-007 | SIM-007 Airbus A320neo | Air France | Civilian | 0% Planned |
| PRJ-004 | SIM-004 Airbus A350 | Delta Airlines | Civilian | 0% Planned |
| PRJ-005 | SIM-005 Boeing 787 | United Airlines | Civilian | 0% Planned |
| PRJ-008 | SIM-008 Boeing 777X | Qatar Airways | Civilian | 0% Planned |

Each project has **13 tasks** with finish-to-start dependencies, skill requirements, and a standard duration from the `task_type_durations` reference table. The Gantt structure is Power BI-compatible (Task Name, Start, Duration, % Complete, Resource).

## Workforce (28 technicians + 4 managers)

| Employee | Type | Specialty | Line | Limitation |
|---|---|---|---|---|
| Jean-Pierre Tremblay | FTE Senior | Motion Systems | PL-01 | Back — 25kg lift max |
| Marie-Claire Dubois | FTE | Hydraulics | PL-02 | — |
| Luc Bergeron | FTE Senior | Electrical | PL-03 | Knee — no ladders |
| Sophie Lavoie | FTE | Visual Systems | PL-02 | — |
| Philippe Gagnon | FTE Senior | Avionics | PL-03 | Hearing — noise restricted |
| Marc-André Pelletier | FTE Senior | Hydraulics (Night) | PL-02 | Respiratory — no chemicals |
| Catherine Morin | FTE | Electrical | PL-03 | — |
| François Côté | FTE | Motion Systems | PL-01 | — |
| David Chen | FTE | Test Engineering | PL-02 | — |
| Nathalie Bouchard | FTE | Structures | PL-02 | — |
| David Tremblay | FTE | CNC Machining | PL-01 | — |
| Sophie Martin | FTE Senior | CNC Machining | PL-01 | Vision — corrective lenses |
| Hassan Al-Farsi | FTE | Welding | PL-02 | — |
| Patrick O'Brien | FTE | Welding (Night) | PL-02 | — |
| Yuki Tanaka | FTE | Electronics | PL-03 | — |
| Samuel Martin | FTE Senior | Electronics | PL-03 | — |
| Priya Sharma | FTE | Avionics | PL-03 | — |
| Thomas Wilson | FTE | Sheet Metal | PL-01 | — |
| Mei Wong | FTE | Sheet Metal (Night) | PL-01 | — |
| Kevin Murphy | FTE | Painting | PL-02 | — |
| Aisha Mohammed | FTE | Quality | PL-02 | — |
| Roberto Silva | FTE Senior | Welding | PL-02 | Wrist — limited TIG |
| Luc Bouchard | FTE | CNC Machining | PL-01 | — |
| André Lefebvre | FTE | Additive Manufacturing | PL-01 | — |
| Isabelle Roy | FTE | Electronics | PL-03 | — |
| James Taylor | Contractor | Hydraulics | PL-02 | — |
| Maria Garcia | Contractor | Electrical | PL-03 | — |
| Wei Zhang | Contractor | CNC Machining | PL-01 | — |
| **Sylvie Raymond** | **Line Mgr** | **Precision Fabrication** | **PL-01** | — |
| **Robert Lapointe** | **Line Mgr** | **Assembly & Integration** | **PL-02** | — |
| **Claire Pelletier** | **Line Mgr** | **Electronics & Systems** | **PL-03** | — |
| **Marc Fortin** | **Prod Mgr** | **Manufacturing** | — | — |

## Repo Structure

```
cae-demo/
├── deploy/
│   ├── SolutionInstaller.ipynb          # Import into Fabric → Run All
│   └── solution_installer_cells.py      # Copy-paste version of installer cells
├── workspace/                           # Published by fabric-cicd
│   ├── Setup/
│   │   ├── GetStarted.Notebook/             # Guided walkthrough
│   │   ├── PostDeploymentConfig.Notebook/   # Creates SQL tables, loads data, KQL DB
│   │   └── CAEManufacturing_Env.Environment/ # Spark 2.0 + azure-eventhub
│   ├── Data/
│   │   └── CAEManufacturing_LH.Lakehouse/   # Staging Lakehouse
│   ├── RTI/                                  # Real-Time Intelligence
│   │   ├── CAEManufacturingEH.Eventhouse/   # Telemetry store
│   │   ├── DataEmulator.Notebook/           # Telemetry + clock-in + fault injection (loop)
│   │   ├── AnomalyDetection.Notebook/       # ML Z-score anomaly scoring
│   │   ├── AlertNotificationAgent.Notebook/ # Teams webhook + Foundry agent
│   │   ├── CapacityManagementAgent.Notebook/# AI agent querying SQL DB + KQL
│   │   ├── CreateOntology.Notebook/         # Fabric Ontology builder (preview)
│   │   ├── TelemetryEventStream*            # Created by PostDeploymentConfig (API)
│   │   └── ClockInEventStream*              # Created by PostDeploymentConfig (API)
│   └── ML/
│       └── TrainMVADModel.Notebook/         # Train multivariate anomaly detection model
├── data/
│   ├── erp/          # production lines, machines, inventory, purchase orders, maintenance
│   ├── hr/           # employees, skills, schedules, restrictions, time off, contractors
│   ├── plm/          # simulators, BOMs, projects, tasks, part specs, machine capabilities
│   ├── mes/          # machine_jobs (MES scheduling)
│   └── telemetry/    # sensor_definitions.csv (107 sensors × 20 machines)
└── scripts/          # Local Python tools + KQL scripts
    ├── kql/                        # KQL health scoring functions (16 functions)
    │   ├── machine_health_monitoring.kql   # All functions + table definitions
    │   ├── anomaly_scoring.kql             # Confidence + RUL estimation
    │   ├── mvad_prediction.kql             # MVAD prediction functions
    │   └── dashboard_spec.json             # Real-time dashboard spec
    ├── generate_project_data.py    # Regenerate 8 projects with scheduling constraints
    ├── telemetry_normal.py         # Standalone telemetry generator
    ├── telemetry_fault_injection.py # CNC mill fault profile
    ├── clockin_events.py           # Workforce event generator
    └── validate_data.py            # Referential integrity checker
```

## Deployment

### 1. Run the SolutionInstaller

**Option A — Import the notebook:** Download [`deploy/SolutionInstaller.ipynb`](deploy/SolutionInstaller.ipynb), import it into your Fabric workspace, and **Run All**.

**Option B — Copy-paste cells:** Open a new Fabric notebook and paste the 3 cells from [`deploy/solution_installer_cells.py`](deploy/solution_installer_cells.py). Run each cell in order.

This clones the repo, publishes all Fabric items via `fabric-cicd`, uploads seed data to the Lakehouse, and creates `config/connections.json`.

> **Note:** You will see a `Parameter file not found` warning during publishing — this is expected and harmless. No parameter file is needed.

> **Note:** EventStreams are **not** deployed by fabric-cicd. They are created by PostDeploymentConfig (Step 3) via the Fabric REST API, because their definitions reference the Eventhouse item ID which is only known at runtime.

### 2. Create a Fabric SQL Database

In the workspace, click **+ New item > SQL Database** and name it `CAEManufacturing_SQLDB`.

![Create SQL Database](docs/screenshots/01-create-sql-database.png)

Copy the **JDBC connection string** from SQL Database > Settings > Connection strings.

![JDBC Connection String](docs/screenshots/02-sql-jdbc-connection-string.png)

### 3. Configure connections.json

Open the Lakehouse (`CAEManufacturing_LH`) in the Fabric UI. Navigate to **Files > config > connections.json** and paste your JDBC connection string:

```json
{
  "SQL_JDBC_CONNECTION_STRING": "jdbc:sqlserver://your-server.database.fabric.microsoft.com:1433;database=...",
  "TELEMETRY_EVENTSTREAM_CONNECTION_STRING": "",
  "CLOCKIN_EVENTSTREAM_CONNECTION_STRING": "",
  "FOUNDRY_AGENT_PROJECT_ENDPOINT": "",
  "FOUNDRY_AGENT_ID": "",
  "TEAMS_WEBHOOK_URL": "",
  "CREATE_ONTOLOGY": "true",
  "FAULT_INJECTION": "false"
}
```

Then open the `PostDeploymentConfig` notebook and **Run All**. It reads the JDBC string from `connections.json` automatically.

![PostDeploymentConfig](docs/screenshots/03-postdeployment-run-all.png)

This creates 5 schemas (`hr`, `erp`, `plm`, `mes`, `telemetry`) with 24 tables, bulk inserts all data, then adds primary keys and foreign keys.

### 4. KQL Database & EventStreams Setup

The **PostDeploymentConfig** notebook automatically:
1. Creates the **KQL Database** inside the Eventhouse via the Fabric API with `MachineTelemetry`, `ClockInEvents`, and `AnomalyDetection` tables (streaming ingestion enabled)
2. Creates two **EventStreams** with Custom Endpoint source → Eventhouse destination routing:
   - **TelemetryEventStream** — routes sensor telemetry to `MachineTelemetry` table
   - **ClockInEventStream** — routes workforce events to `ClockInEvents` table

**After PostDeploymentConfig runs**, open each EventStream in the Fabric UI:
1. Click on the Custom Endpoint source node
2. Copy the **Event Hub connection string** from the Details pane (SAS Key Authentication tab)
3. Paste the connection strings into `config/connections.json` in the Lakehouse:
   - **TelemetryEventStream** → `TELEMETRY_EVENTSTREAM_CONNECTION_STRING`
   - **ClockInEventStream** → `CLOCKIN_EVENTSTREAM_CONNECTION_STRING`

The **DataEmulator** notebook reads connection strings from `connections.json` and sends events via the Azure Event Hub SDK to the EventStreams, which route them to the Eventhouse automatically.

After the KQL tables are created, deploy the **16 health scoring functions** by running the commands in `scripts/kql/machine_health_monitoring.kql` in the KQL Database query editor. These functions provide composite anomaly scores for every machine type:

| Function | Machines | Failure Mode |
|---|---|---|
| `CNC_BearingWearScore` | CNC-001/002/003/005 | Spindle bearing wear |
| `CNC_CoolantFailScore` | CNC-001/002/003/005 | Coolant system failure |
| `Laser_NozzleDegScore` | LSR-001/002 | Nozzle degradation |
| `Press_HydLeakScore` | PRB-001 | Hydraulic leak |
| `Weld_GasContamScore` | WLD-001/002 | Gas contamination |
| `CMM_EnvDriftScore` | CMM-001 | Environmental drift |
| `Reflow_ProfileScore` | RFL-001 | Thermal profile drift |
| `Printer_O2Score` | ADD-001 | O₂ ingress |
| `Crane_BrakeScore` | CRN-001 | Brake pad wear |
| `HydTest_CavitationScore` | HTB-001 | Pump cavitation |
| `EDM_WireHealthScore` | EDM-001 | Wire/dielectric degradation |
| `Lathe_SpindleScore` | LTH-001/002 | Spindle vibration |
| `PaintBooth_EnvScore` | PNT-001/002 | Booth environment |
| `Assembly_StationScore` | ASM-001 | ESD/soldering anomaly |
| `MachineHealthAlerts` | All 20 | Unified alert view |
| `CriticalAnomalyAlerts` | All 20 | ≥80% confidence alerts |

### 5. Semantic Model (DirectLake — automated)

The **PostDeploymentConfig** notebook automatically creates a `CAEManufacturing` semantic model using the Fabric REST API with TMDL format. It uses **DirectLake** mode pointing at the SQL Database via OneLake, and includes:

**8 tables:**
- `hr.employees`, `erp.production_lines`, `erp.machines`, `erp.maintenance_history`
- `plm.simulators`, `plm.projects`, `plm.tasks`, `mes.machine_jobs`

**8 relationships** (all with `relyOnReferentialIntegrity`):
- `employees.production_line_id` → `production_lines.production_line_id`
- `machines.production_line_id` → `production_lines.production_line_id`
- `projects.Simulator_ID` → `simulators.simulator_id`
- `tasks.Parent_Project_ID` → `projects.Project_ID`
- `tasks.Machine_ID` → `machines.machine_id`
- `maintenance_history.machine_id` → `machines.machine_id`
- `machine_jobs.machine_id` → `machines.machine_id`
- `machine_jobs.project_id` → `projects.Project_ID`

> **Note**: SQL FK constraints do NOT auto-propagate to DirectLake semantic models — relationships must be defined explicitly in TMDL. The PostDeploymentConfig handles this automatically.

### 5b. Fabric Ontology (preview — optional)

The **CreateOntology** notebook (in `workspace/RTI/`) builds a `CAEManufacturingOntology` Fabric Ontology item via the REST API. It is invoked automatically by PostDeploymentConfig as **Step 12** when `CREATE_ONTOLOGY` is `"true"` in `connections.json` — set it to `"false"` if your capacity does not have the Ontology preview enabled.

**Entity types (8):** `Machine`, `Employee`, `ProductionLine`, `Project`, `Simulator`, `Task`, `MaintenanceHistory`, `MachineJob` — all bound (NonTimeSeries) directly to the **Fabric SQL Database** (`CAEManufacturing_SQLDB`, `erp` schema). Verified: the Ontology data-binding API accepts a `SQLDatabase` item directly as `sourceType: "LakehouseTable"` with `sourceSchema: "erp"` — no intermediate lakehouse shortcut required.

**Relationships (8 — active verbs):**
- `EmployeeWorksOnProductionLine` (Employee → ProductionLine)
- `MachineOnProductionLine` (Machine → ProductionLine)
- `ProjectDeliversSimulator` (Project → Simulator)
- `TaskBelongsToProject` (Task → Project)
- `TaskRequiresMachine` (Task → Machine)
- `MaintenanceServicesMachine` (MaintenanceHistory → Machine)
- `JobRunsOnMachine` (MachineJob → Machine)
- `JobSupportsProject` (MachineJob → Project)

**Time-series bindings (3 — Eventhouse / KustoTable):**
- `MachineTelemetry` → `Machine` (by `machine_id`, timestamp `timestamp`)
- `ClockInEvents` → `Employee` (by `employee_id`, timestamp `timestamp`)
- `AnomalyDetection` → `Machine` (by `machine_id`, timestamp `alert_timestamp`)

The notebook is **idempotent** — re-running it deletes the existing ontology and recreates it. The Ontology API is in preview ([docs](https://learn.microsoft.com/en-us/rest/api/fabric/ontology)); if your workspace capacity does not support it, the notebook surfaces a 404 and PostDeploymentConfig treats the step as non-fatal.

### 6. Create Gantt Report

1. In the workspace, click **+ New item > Report** and connect it to the `CAEManufacturing` semantic model
2. **Page 1 — Project Overview**: Add a Card visual (# Projects) and a Bar chart (completion % by project)
3. **Page 2 — Project Timeline (Gantt)**:
   - Get the **Gantt** custom visual from AppSource (by MAQ Software)
   - Map the fields:
     | Gantt Field | Column |
     |---|---|
     | Task | Tasks → Task Name |
     | Start Date | Tasks → Planned Start (Modified_Planned_Start) |
     | Duration | Tasks → Standard Duration |
     | % Complete | Tasks → Complete % |
     | Resource | Tasks → Resource Login |
     | Legend | Tasks → Skill Requirement |
   - Add a slicer for `Projects → Project Name` to filter by project

### 7. Configure Activator

Set up the Activator to detect anomalies in real-time and trigger ML analysis:

1. Open the **TelemetryEventStream** in the Fabric UI
2. Click the default stream node → **Add destination** → select **Activator**
3. Create a new Activator (or select an existing one)
4. Set the **object ID** to `machine_id`
5. Create a rule: `alert_level` **Becomes** `"Critical"`
6. Set the **action** to **Run Notebook** → select `AnomalyDetection`

When a sensor reading arrives with `alert_level = Critical`, the Activator triggers the **AnomalyDetection** notebook which:
1. Computes ML baselines (24h) and Z-score anomaly confidence
2. Writes alerts to the `AnomalyDetection` KQL table
3. Calls the **Foundry agent** for AI root-cause analysis and recommendations
4. Sends a **Teams Adaptive Card** with alert details + AI analysis

To enable Teams notifications, set `TEAMS_WEBHOOK_URL` in `config/connections.json` in the Lakehouse.
To enable AI root-cause analysis, set `FOUNDRY_AGENT_PROJECT_ENDPOINT` and `FOUNDRY_AGENT_ID` in the same config file.

### 8. Multivariate Anomaly Detection (MVAD) — Optional

Adds cross-sensor correlation modeling to detect subtle multi-sensor anomalies that Z-score analysis misses (e.g., vibration ↑ + temperature ↑ + coolant ↓ + power ↑ simultaneously during a bearing failure).

**Architecture:** Spark notebook trains a `MultivariateAnomalyDetector` model → MLflow registry → KQL stored function loads model via Python plugin for real-time prediction.

#### What's automated

- **OneLake availability** — enabled automatically by PostDeploymentConfig (Step 4b)
- **Environment** — `time-series-anomaly-detector==0.3.9` is included in `CAEManufacturing_Env` and auto-published by PostDeploymentConfig (Step 2)
- **KQL function deployment** — the `TrainMVADModel` notebook automatically deploys all 3 KQL prediction functions with the trained model URI (no manual copy-paste)

#### Manual prerequisite (cannot be automated via API)

1. **Enable Python 3.11.7 DL plugin** on the Eventhouse:
   - Open `CAEManufacturingEH` → Plugins (from the ribbon) → Toggle **Python language extension** to **On** → Select **Python 3.11.7 DL (preview)** → Done

   ![Enable Python 3.11.7 DL Plugin](docs/screenshots/09-enable-python-plugin.png)

#### Train and deploy the model

1. Ensure several days of normal telemetry have accumulated (~400+ samples minimum, ideally 2+ days at 1-min intervals × 4 CNC machines)
2. Open the `TrainMVADModel` notebook (in `workspace/ML/`)
3. Attach the `CAEManufacturing_Env` environment
4. **Run All** — the notebook:
   - Reads `MachineTelemetry` from OneLake for CNC-001/002/003/005
   - Filters to normal data only, pivots to wide format (4 sensors)
   - Trains a `MultivariateAnomalyDetector` with sliding window = 200 (~3.3 hours)
   - Registers the model in MLflow as `cnc_bearing_mvad_model`
   - **Deploys 3 KQL functions automatically** (`predict_fabric_mvad_fl`, `predict_cnc_mvad`, `ingest_mvad_anomalies`)

#### KQL functions deployed

| Function | Purpose |
|---|---|
| `predict_fabric_mvad_fl()` | Generic MVAD prediction helper (Microsoft tutorial pattern) |
| `predict_cnc_mvad()` | CNC-specific wrapper — pivots telemetry, invokes trained model |
| `ingest_mvad_anomalies()` | Writes detected anomalies to `AnomalyDetection` table (`anomaly_type = "MVAD"`) |

> The `scripts/kql/mvad_prediction.kql` file contains the same functions for reference/manual deployment if needed.

#### Test it

```kql
// Run MVAD prediction on recent CNC data
predict_cnc_mvad(400)
| where is_anomaly == true
| order by timestamp desc
| take 50

// Ingest anomalies into AnomalyDetection table
.set-or-append AnomalyDetection <|
    ingest_mvad_anomalies()
```

#### Coexistence with Z-score detection

MVAD runs alongside the existing Z-score anomaly detection — they complement each other:
- **Z-score** (existing): Catches obvious single-sensor spikes (fast, rule-based)
- **MVAD** (new): Catches subtle multi-sensor correlations (ML-based, cross-sensor)

MVAD results are written to the same `AnomalyDetection` table with `anomaly_type = "MVAD"` to distinguish from Z-score alerts (`anomaly_type = "Z-Score"`). The existing Foundry agent and Teams notification pipeline works with both.

### 9. Demo

1. **Start telemetry**: Open `DataEmulator` and Run All — it loops for 8 hours, sending sensor data from all 20 machines + workforce clock-in events every minute
2. **Inject a fault**: Open `connections.json` in the Lakehouse and set `"FAULT_INJECTION": "true"` — the next emitter cycle starts a 10-minute CNC-003 spindle bearing failure (vibration ↑, temperature ↑, coolant ↓, power ↑). It auto-resets to `"false"` when done.
3. **Watch detection**: The 16 KQL health scoring functions produce composite scores in real-time. `CNC_BearingWearScore` will climb from ~0.2 to 0.99 as the fault progresses
4. **Activator fires**: When `alert_level` becomes `Critical`, the Activator triggers the `AnomalyDetection` notebook automatically
5. **ML scoring**: The notebook computes Z-score baselines and writes alerts to `AnomalyDetection` with confidence %, RUL estimate, and severity
6. **Foundry agent**: The notebook calls the Foundry agent for root-cause analysis, impact assessment, and recommended actions
7. **Teams notification**: An Adaptive Card is sent to Teams with machine ID, failure mode, confidence, AI analysis, and a link to the Fabric dashboard
8. **AI reasoning**: Open `CapacityManagementAgent` to see the agent query both SQL DB and KQL Eventhouse for scheduling impact, worker reassignment, and parts availability
9. **Power BI**: View the Gantt chart from `plm.projects` + `plm.tasks` and the real-time machine health dashboard

![Gantt Chart](docs/screenshots/07-gantt-powerbi.png)

![Real-Time Dashboard](docs/screenshots/08-rt-dashboard.png)

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
| Eventhouse for telemetry + events + anomaly alerts | Sub-second queries on time-series data, native KQL, real-time scoring functions |
| Lakehouse as staging only | CSVs upload there during deployment, then get loaded into SQL DB |
| Single long-running emulator notebook | One Spark session for both telemetry + clock-in; avoids 1-min pipeline overhead |
| EventStream → Activator → ML notebook | Activator detects threshold breach in real-time, triggers ML for deep analysis + Foundry agent for AI reasoning |
| Constraints added after bulk insert | Avoids FK ordering issues during initial data load |
| Semantic model created via REST API with TMDL | fabric-cicd doesn't pass `format=TMDL`; REST API supports full DirectLake TMDL definitions including relationships |
| Separate simulators (products) from machines (equipment) | Telemetry monitors manufacturing machines, not the simulators being built |

## License

This project is provided as-is for demonstration and educational purposes.
