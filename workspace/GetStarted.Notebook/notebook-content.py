# Fabric notebook source
# %% [markdown]
# # CAE Flight Simulator Manufacturing - Get Started
#
# Welcome to the **CAE Flight Simulator Manufacturing** demo.
# This solution shows how agentic AI workflows can optimise capacity
# management in a flight-simulator factory using **Microsoft Fabric** and
# **Azure AI Foundry**.
#
# ## What is in the workspace
#
# | Category | Items |
# |---|---|
# | **SQL Database** | CAEManufacturing_SQLDB - 15 tables (projects, tasks, employees, skills, machines, ...) |
# | **Lakehouse** | CAEManufacturing_LH - staging area (CSVs in Files/) |
# | **Eventhouse** | CAEManufacturingEH - real-time telemetry and clock-in events |
# | **Eventstreams** | SimulatorTelemetryStream, ClockInEventStream |
# | **Notebooks** | This guide + PostDeploymentConfig, LoadData, 3 simulators, Agent |
# %% [markdown]
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
# | PRJ-007 | SIM-007 Airbus A320neo | Air France | 0% Planned |
# | PRJ-004 | SIM-004 Airbus A350 | Delta Airlines | 0% Planned |
# | PRJ-005 | SIM-005 Boeing 787 | United Airlines | 0% Planned |
# | PRJ-008 | SIM-008 Boeing 777X | Qatar Airways | 0% Planned |
#
# ### Your 12-person team
# 10 FTEs + 2 contractors. 4 seniors have physical limitations.
# Skills span: Motion Systems, Hydraulics, Electrical, Avionics,
# Visual Systems, Structures, Test Engineering - with cross-coverage.
# %% [markdown]
# ## Step-by-Step Guide
#
# ### 1. Run PostDeploymentConfig (if not done yet)
# Open **Install/PostDeploymentConfig** and Run All.
# It creates 15 SQL tables and loads all seed data.
#
# ### 2. Start the telemetry simulator
# Open **Simulation/SimulatorTelemetryEmulator** and Run All.
# It sends 60 sensor readings (3 sims x 20 sensors) every 30 seconds.
#
# ### 3. Inject a fault
# Open **Simulation/TelemetryFaultInjection** and Run All.
# SIM-001 hydraulic pressure will degrade over 10 minutes.
#
# ### 4. Generate clock-in events
# Open **Simulation/ClockInEventEmulator** and Run All.
# Produces badge-in/out and task-completion events.
#
# ### 5. Build the Gantt chart in Power BI
# Use the Gantt visual (MAQ Software) with these field mappings:
# - Task = Task_Name
# - Parent = Parent_Project_ID
# - Start Date = Modified_Planned_Start (or Actual_Start)
# - Duration = Standard_Duration
# - Percent Complete = Complete_Percentage
# - Resource = Resource_Login
#
# ### 6. Run the Capacity Management Agent
# Open **Agent/CapacityManagementAgent** and Run All.
# It reasons across telemetry, SQL tables, and the skills matrix.
# %% [markdown]
# ## Sample SQL Query
#
# You can query the SQL Database directly from any notebook:
#
#     SELECT p.Project_Name, t.Task_Name, t.Resource_Login,
#            t.Modified_Planned_Start, t.Standard_Duration,
#            t.Complete_Percentage
#     FROM dbo.tasks t
#     JOIN dbo.projects p ON t.Parent_Project_ID = p.Project_ID
#     ORDER BY t.Modified_Planned_Start

# %%
# Quick data check - verify PostDeploymentConfig ran successfully
print("Checking deployed items...")

import os, requests
import notebookutils

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
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items",
        headers=headers,
    )
    items = resp.json().get("value", [])
    print(f"Found {len(items)} items in workspace:\n")
    for i in sorted(items, key=lambda x: x.get("type", "")):
        print(f"  {i['type']:20s}  {i['displayName']}")
else:
    print("Could not detect workspace ID. Set TRIDENT_WORKSPACE_ID.")
