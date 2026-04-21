# CAE Flight Simulator Manufacturing Demo

A comprehensive Fabric Jumpstart solution accelerator that demonstrates agentic capacity management for a **CAE-style flight simulator manufacturing facility** using Microsoft Fabric and Azure AI Foundry.

## Overview

This demo simulates a manufacturing floor where full-flight simulators (FFS) are built, tested, and maintained. It demonstrates how **agentic AI workflows** can optimize worker scheduling, predictive maintenance, and capacity management by reasoning across multiple real-time and batch data sources.

### Business Scenario

CAE is a global leader in training for civil aviation, defense, and healthcare. Their flight simulators are complex, high-value machines with hundreds of sensors. This demo models:

- **Manufacturing floor** with multiple simulator bays, each assembling/testing flight simulators
- **Real-time machine telemetry** monitoring simulator health during testing and burn-in
- **Workforce management** with skills-based scheduling, shift optimization, and proactive reassignment
- **ERP integration** for work orders, parts inventory, and production scheduling
- **Agentic AI** that reasons across all sources to make scheduling and maintenance decisions

### Key Capabilities

- **Proactive Maintenance Planning**: Detect anomalies in simulator telemetry → schedule maintenance → reassign workers
- **Capacity Optimization**: Maximize manufacturing bay utilization by intelligently scheduling work
- **Skills-Based Assignment**: Match workers to tasks based on certifications, physical limitations, shift preferences
- **Schedule Reasoning**: AI agent explains *why* it recommends specific changes (e.g., "Moved Jean-Pierre to Bay 3 because Bay 1 needs hydraulic maintenance and he's certified for Bay 3 electrical work")
- **Teams Integration**: Notify employees via Microsoft Teams when schedules change

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        External / On-Prem Sources                           │
├──────────────────┬──────────────────┬──────────────────┬─────────────────────┤
│ Simulator        │ Clock-In/        │ Oracle ERP       │ HR System           │
│ Telemetry (IoT)  │ Milestone Events │ (Work Orders,    │ (Skills, Schedules, │
│                  │                  │  BOM, Inventory)  │  Certifications)    │
└────────┬─────────┴────────┬─────────┴────────┬─────────┴──────────┬──────────┘
         │                  │                  │                    │
         ▼                  ▼                  ▼                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Microsoft Fabric Workspace                           │
│                                                                              │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────────────┐  │
│  │ SimulatorTelemetry│  │ ClockInEvents     │  │ ManufacturingERP           │  │
│  │ Eventstream       │  │ Eventstream       │  │ Lakehouse (Delta Tables)   │  │
│  └────────┬──────────┘  └────────┬──────────┘  │  - work_orders             │  │
│           │                      │              │  - bill_of_materials       │  │
│           ▼                      ▼              │  - purchase_orders         │  │
│  ┌─────────────────────────────────────┐       │  - inventory               │  │
│  │ ManufacturingEventhouse (KQL DB)    │       │  - production_schedule     │  │
│  │  - SimulatorTelemetry table         │       │  - machines                │  │
│  │  - ClockInEvents table              │       │  - maintenance_history     │  │
│  │  - MaintenanceAlerts table          │       └────────────────────────────┘  │
│  └────────┬──────────────────┬─────────┘                                      │
│           │                  │              ┌────────────────────────────────┐  │
│           ▼                  ▼              │ HRData Lakehouse              │  │
│  ┌──────────────┐  ┌──────────────────┐   │  - employees                  │  │
│  │ Maintenance   │  │ Capacity Mgmt    │   │  - skills_certifications      │  │
│  │ Activator     │  │ KQL Dashboard    │   │  - employee_schedules         │  │
│  └──────┬───────┘  └──────────────────┘   │  - leave_of_absence           │  │
│         │                                  │  - physical_limitations       │  │
│         ▼                                  │  - contractual_workforce      │  │
│  ┌──────────────────────────────────┐     └────────────────────────────────┘  │
│  │ Azure AI Foundry Agent           │                                         │
│  │  - Capacity Management Agent     │◄──── Reasons across ALL data sources    │
│  │  - Schedule Optimizer Agent      │                                         │
│  │  - Maintenance Planner Agent     │───── Sends Teams notifications          │
│  └──────────────────────────────────┘                                         │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │ Power BI Reports                                                         │  │
│  │  - Manufacturing Floor Overview    - Worker Schedule Heatmap             │  │
│  │  - Simulator Health Dashboard      - Capacity Utilization               │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Sources

### 1. Simulator Telemetry (Real-Time Eventstream)
Telemetry from flight simulators under test/burn-in. Each simulator has 30+ sensors:

| Sensor Category | Measures |
|---|---|
| **Motion Platform** | Pitch actuator position, Roll actuator position, Yaw actuator position, Heave displacement, Surge displacement, Sway displacement |
| **Hydraulics** | Hydraulic pressure (PSI), Hydraulic fluid temperature, Hydraulic flow rate |
| **Thermal** | Cockpit display temperature, Motion platform temperature, Projection room temperature, Power supply temperature, Hydraulic system temperature |
| **Electrical** | Main power voltage (AC), DC bus voltage (28V rail), UPS battery voltage, Power consumption (kW) |
| **Visual System** | Projector brightness (lumens), Image generator GPU temperature, Frame render time (ms), Visual-to-motion sync latency (ms) |
| **Vibration** | Motion platform vibration (X/Y/Z axes), Base frame vibration |
| **Control** | Control loading force feedback torque, Instructor station response time |
| **Environmental** | Cooling system flow rate, Ambient temperature, Humidity |

### 2. Oracle ERP Data (CSV → Lakehouse)
Simulated ERP tables representing manufacturing operations:
- **Work Orders**: Assembly, testing, burn-in, qualification tasks
- **Bill of Materials**: Components per simulator model (FFS Level D, etc.)
- **Purchase Orders**: Parts procurement and delivery status
- **Inventory**: Spare parts and components stock levels
- **Production Schedule**: Planned simulator builds and deliveries
- **Machines/Bays**: Manufacturing bay configuration and capabilities
- **Maintenance History**: Past maintenance records

### 3. Clock-In / Milestone Events (Real-Time Eventstream)
Real-time events from the manufacturing floor:
- Employee badge in/out (shift start/end)
- Task milestone completions (e.g., "hydraulic system installed in Bay 2")
- Maintenance task start/completion
- Quality inspection checkpoints
- Break start/end

### 4. HR Data (CSV → Lakehouse)
Workforce management data:
- **Employees**: Full roster with name, Teams email, role, employee type
- **Skills & Certifications**: Matrix of who can do what (hydraulics, electrical, avionics, projection, etc.)
- **Schedules**: Day shift / Night shift / Flex assignments
- **Leave of Absence**: Planned vacations, sick leave, personal days
- **Physical Limitations**: Restrictions on record (heavy lifting, confined spaces, etc.)
- **Contractual Workforce**: External contractors with different rules and availabilities
- **Employee Agreements**: Union/non-union conditions affecting scheduling

## Fabric Items Deployed

| Item Type | Name | Purpose |
|---|---|---|
| Lakehouse | ManufacturingERP_LH | ERP reference data (work orders, BOM, inventory) |
| Lakehouse | HRData_LH | HR data (employees, skills, schedules) |
| Eventhouse | CAEManufacturingEH | Real-time query engine for telemetry and events |
| KQL Database | CAEManufacturingKQLDB | Hot path for real-time queries |
| Eventstream | SimulatorTelemetryStream | Ingests simulator sensor data |
| Eventstream | ClockInEventStream | Ingests workforce clock-in/milestone events |
| Notebook | SimulatorTelemetryEmulator | Generates synthetic telemetry data |
| Notebook | ClockInEventEmulator | Generates synthetic clock-in events |
| Notebook | LoadERPData | Loads ERP CSVs into lakehouse Delta tables |
| Notebook | LoadHRData | Loads HR CSVs into lakehouse Delta tables |
| Notebook | CapacityManagementAgent | Foundry agent for schedule optimization |
| Notebook | PostDeploymentConfig | Post-install configuration |
| Notebook | GetStarted | Entry point with instructions |
| KQL Dashboard | ManufacturingFloorDashboard | Real-time manufacturing floor monitoring |
| KQL Queryset | ManufacturingQueries | Pre-built KQL queries |
| Reflex (Activator) | MaintenanceActivator | Triggers alerts on anomalous telemetry |
| Environment | CAEDemoRuntime | Spark runtime with dependencies |

## Installation

### One-Click Install (via Fabric Notebook)

1. Download [`deploy/SolutionInstaller.ipynb`](deploy/SolutionInstaller.ipynb)
2. In your Fabric workspace, click **+ New > Import notebook**
3. Upload `SolutionInstaller.ipynb` and open it
4. Update `REPO_URL` in the first code cell to point to **your** GitHub repo
5. Click **Run All**

The installer will:
- Clone the repo
- Deploy all Fabric items using [`fabric-cicd`](https://microsoft.github.io/fabric-cicd/)
- Upload CSV data files to the Lakehouses using `notebookutils`
- Clean up temp files

> **No dependency on Fabric Jumpstart.** This is a standalone repo that uses `fabric-cicd` (the same open-source library Jumpstart is built on) directly.

### How It Works

| Step | Tool | What it does |
|---|---|---|
| Clone repo | `gitpython` | Shallow-clones your GitHub repo into a temp folder |
| Deploy items | `fabric-cicd` (`FabricWorkspace` + `publish_all_items`) | Reads the `workspace/` folder and creates/updates Fabric items via REST API |
| Upload data | `notebookutils.fs.cp` | Copies CSV files from the cloned repo to Lakehouse Files via OneLake |
| Authenticate | `notebookutils.credentials` or `azure-identity` | Works in Fabric notebooks (auto) or locally (Azure CLI) |

### Reuse This Pattern

To package **any** Fabric demo the same way:

1. **Create a `workspace/` folder** with Fabric Git-integration format:
   - Each item is `<ItemName>.<ItemType>/` (e.g., `MyNotebook.Notebook/`)
   - Inside: `.platform` (JSON metadata) + `notebook-content.py` (code)
2. **Create a `data/` folder** with CSVs or Parquet files to load
3. **Copy `deploy/SolutionInstaller.ipynb`** and update `REPO_URL`
4. **Push to GitHub** with a tagged release (e.g., `v1.0.0`)

That's it — anyone can deploy your demo by importing one notebook.

## Prerequisites

- Microsoft Fabric capacity F16 or higher (for AI features)
- Fabric workspace with Contributor or Admin permissions
- Power BI Pro or PPU license
- Azure AI Foundry project (for the agentic workflow — optional, demo works without it)

## Post-Deployment Steps

1. Run the `PostDeploymentConfig` notebook
2. Start the `SimulatorTelemetryEmulator` notebook (runs continuously)
3. Start the `ClockInEventEmulator` notebook (runs continuously)
4. Explore the `GetStarted` notebook for a guided walkthrough
5. Open the `ManufacturingFloorDashboard` for real-time monitoring

## License

This project is provided as-is for demonstration and educational purposes.
