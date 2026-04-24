# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Create CAE Manufacturing Ontology
# 
# Builds a **Fabric Ontology** (preview) named `CAEManufacturingOntology` over the
# workspace's existing data sources using the Fabric REST API.
# 
# ## Entity Types (8)
# | Entity | Key | Display | Source |
# |---|---|---|---|
# | Machine | machine_id | machine_name | SQL `erp.machines` |
# | Employee | employee_id | last_name | SQL `hr.employees` |
# | ProductionLine | production_line_id | line_name | SQL `erp.production_lines` |
# | Project | Project_ID | Project_Name | SQL `plm.projects` |
# | Simulator | simulator_id | simulator_model | SQL `plm.simulators` |
# | Task | Task_ID | Task_Name | SQL `plm.tasks` |
# | MaintenanceHistory | maintenance_id | maintenance_type | SQL `erp.maintenance_history` |
# | MachineJob | job_id | job_id | SQL `mes.machine_jobs` |
# 
# ## Relationships (8 — active verbs)
# - `EmployeeWorksOnProductionLine` (Employee → ProductionLine)
# - `MachineOnProductionLine` (Machine → ProductionLine)
# - `ProjectDeliversSimulator` (Project → Simulator)
# - `TaskBelongsToProject` (Task → Project)
# - `TaskRequiresMachine` (Task → Machine)
# - `MaintenanceServicesMachine` (MaintenanceHistory → Machine)
# - `JobRunsOnMachine` (MachineJob → Machine)
# - `JobSupportsProject` (MachineJob → Project)
# 
# ## Time-series Bindings (3 — Eventhouse)
# - `MachineTelemetry` → Machine (by `machine_id`)
# - `ClockInEvents` → Employee (by `employee_id`)
# - `AnomalyDetection` → Machine (by `machine_id`)
# 
# **Idempotent:** existing ontology with the same name is deleted and recreated.
# 
# **Prerequisites:** PostDeploymentConfig has run (SQL Database populated, KQL DB
# with `MachineTelemetry` / `ClockInEvents` / `AnomalyDetection` tables).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
ONTOLOGY_NAME        = "CAEManufacturingOntology"
ONTOLOGY_DESCRIPTION = "CAE flight-simulator manufacturing ontology — machines, workforce, projects, tasks, and real-time telemetry."

SQL_DATABASE_NAME    = "CAEManufacturing_SQLDB"
LAKEHOUSE_NAME       = "CAEManufacturing_LH"
EVENTHOUSE_NAME      = "CAEManufacturingEH"
KQL_DATABASE_NAME    = "CAEManufacturingKQLDB"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 - Discover workspace items
import os, json, base64, uuid, random, time, requests
import notebookutils

TOKEN = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except Exception:
        pass
if not WORKSPACE_ID:
    raise RuntimeError("Could not resolve WORKSPACE_ID.")
print(f"Workspace: {WORKSPACE_ID}")

resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=HEADERS)
resp.raise_for_status()
items = resp.json().get("value", [])

def _find(name, kinds):
    return next((i for i in items if i.get("displayName") == name and i.get("type") in kinds), None)

sql_db = _find(SQL_DATABASE_NAME, ("SQLDatabase",))
lh     = _find(LAKEHOUSE_NAME, ("Lakehouse",))
eh     = _find(EVENTHOUSE_NAME, ("Eventhouse",))

if not sql_db:
    raise RuntimeError(f"SQL Database '{SQL_DATABASE_NAME}' not found. Run PostDeploymentConfig first.")
if not eh:
    raise RuntimeError(f"Eventhouse '{EVENTHOUSE_NAME}' not found.")

SQL_DB_ID = sql_db["id"]
EH_ID     = eh["id"]
LH_ID     = lh["id"] if lh else None
print(f"SQL Database : {SQL_DB_ID}")
print(f"Eventhouse   : {EH_ID}")
print(f"Lakehouse    : {LH_ID}")

# Resolve Eventhouse query service URI (needed for KustoTable bindings)
eh_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/eventhouses/{EH_ID}",
    headers=HEADERS,
)
eh_resp.raise_for_status()
eh_props = eh_resp.json().get("properties", {})
KUSTO_CLUSTER_URI = eh_props.get("queryServiceUri") or eh_props.get("ingestionServiceUri", "").replace("ingest-", "")
if not KUSTO_CLUSTER_URI:
    raise RuntimeError(f"Could not resolve Eventhouse queryServiceUri. Response: {eh_resp.text[:300]}")
print(f"Kusto URI    : {KUSTO_CLUSTER_URI}")

# Resolve RTI folder so the ontology lands there
RTI_FOLDER_ID = None
try:
    folders_resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/folders", headers=HEADERS)
    rti_folder = next((f for f in folders_resp.json().get("value", []) if f["displayName"] == "RTI"), None)
    if rti_folder:
        RTI_FOLDER_ID = rti_folder["id"]
        print(f"RTI folder   : {RTI_FOLDER_ID}")
except Exception as e:
    print(f"(folder discovery skipped: {e})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2 - Declare entity types, relationships, and timeseries bindings
ENTITIES = {
    # entity_name: (sql_schema, sql_table, key_column, display_column, properties[(col, valueType)])
    "Machine": {
        "schema": "erp", "table": "machines",
        "key": "machine_id", "display": "machine_name",
        "columns": [
            ("machine_id",          "String"),
            ("machine_name",        "String"),
            ("machine_type",        "String"),
            ("manufacturer",        "String"),
            ("model",               "String"),
            ("production_line_id",  "String"),
            ("zone",                "String"),
            ("status",              "String"),
            ("installation_date",   "DateTime"),
        ],
    },
    "Employee": {
        "schema": "hr", "table": "employees",
        "key": "employee_id", "display": "last_name",
        "columns": [
            ("employee_id",         "String"),
            ("first_name",          "String"),
            ("last_name",           "String"),
            ("email",               "String"),
            ("role",                "String"),
            ("specialty",           "String"),
            ("employment_type",     "String"),
            ("production_line_id",  "String"),
            ("shift",               "String"),
            ("hire_date",           "DateTime"),
        ],
    },
    "ProductionLine": {
        "schema": "erp", "table": "production_lines",
        "key": "production_line_id", "display": "line_name",
        "columns": [
            ("production_line_id",  "String"),
            ("line_name",           "String"),
            ("building",            "String"),
            ("description",         "String"),
        ],
    },
    "Project": {
        "schema": "plm", "table": "projects",
        "key": "Project_ID", "display": "Project_Name",
        "columns": [
            ("Project_ID",          "String"),
            ("Project_Name",        "String"),
            ("Customer",            "String"),
            ("Simulator_ID",        "String"),
            ("Start_Date",          "DateTime"),
            ("Target_End_Date",     "DateTime"),
            ("Status",              "String"),
            ("Completion_Pct",      "Double"),
        ],
    },
    "Simulator": {
        "schema": "plm", "table": "simulators",
        "key": "simulator_id", "display": "simulator_model",
        "columns": [
            ("simulator_id",        "String"),
            ("simulator_model",     "String"),
            ("aircraft_family",     "String"),
            ("simulator_type",      "String"),
        ],
    },
    "Task": {
        "schema": "plm", "table": "tasks",
        "key": "Task_ID", "display": "Task_Name",
        "columns": [
            ("Task_ID",               "String"),
            ("Parent_Project_ID",     "String"),
            ("Task_Name",             "String"),
            ("Skill_Requirement",     "String"),
            ("Machine_ID",            "String"),
            ("Planned_Start",         "DateTime"),
            ("Standard_Duration",     "Double"),
            ("Complete_Pct",          "Double"),
            ("Resource_Login",        "String"),
            ("FS_Task_ID",            "String"),
        ],
    },
    "MaintenanceHistory": {
        "schema": "erp", "table": "maintenance_history",
        "key": "maintenance_id", "display": "maintenance_type",
        "columns": [
            ("maintenance_id",      "String"),
            ("machine_id",          "String"),
            ("maintenance_type",    "String"),
            ("maintenance_date",    "DateTime"),
            ("technician_email",    "String"),
            ("duration_hours",      "Double"),
            ("notes",               "String"),
        ],
    },
    "MachineJob": {
        "schema": "mes", "table": "machine_jobs",
        "key": "job_id", "display": "job_id",
        "columns": [
            ("job_id",              "String"),
            ("machine_id",          "String"),
            ("project_id",          "String"),
            ("task_id",             "String"),
            ("job_status",          "String"),
            ("scheduled_start",     "DateTime"),
            ("scheduled_end",       "DateTime"),
            ("actual_start",        "DateTime"),
            ("actual_end",          "DateTime"),
        ],
    },
}

RELATIONSHIPS = [
    # name,                           source_entity,        target_entity,       source_table_ref,                 source_fk_col,         target_key_col
    ("EmployeeWorksOnProductionLine", "Employee",           "ProductionLine",    ("hr", "employees"),              "employee_id",         "production_line_id"),
    ("MachineOnProductionLine",       "Machine",            "ProductionLine",    ("erp", "machines"),              "machine_id",          "production_line_id"),
    ("ProjectDeliversSimulator",      "Project",            "Simulator",         ("plm", "projects"),              "Project_ID",          "Simulator_ID"),
    ("TaskBelongsToProject",          "Task",               "Project",           ("plm", "tasks"),                 "Task_ID",             "Parent_Project_ID"),
    ("TaskRequiresMachine",           "Task",               "Machine",           ("plm", "tasks"),                 "Task_ID",             "Machine_ID"),
    ("MaintenanceServicesMachine",    "MaintenanceHistory", "Machine",           ("erp", "maintenance_history"),   "maintenance_id",      "machine_id"),
    ("JobRunsOnMachine",              "MachineJob",         "Machine",           ("mes", "machine_jobs"),          "job_id",              "machine_id"),
    ("JobSupportsProject",            "MachineJob",         "Project",           ("mes", "machine_jobs"),          "job_id",              "project_id"),
]

# Timeseries bindings (Eventhouse KQL tables). For each binding we also declare
# the timeseries properties attached to the target entity.
TIMESERIES_BINDINGS = [
    {
        "entity": "Machine",
        "kql_table": "MachineTelemetry",
        "timestamp_column": "timestamp",
        "key_column": "machine_id",  # binds to Machine.machine_id
        "properties": [
            ("timestamp",        "DateTime"),
            ("machine_id",       "String"),
            ("sensor_id",        "String"),
            ("sensor_category",  "String"),
            ("sensor_name",      "String"),
            ("value",            "Double"),
            ("unit",             "String"),
            ("alert_level",      "String"),
            ("is_anomaly",       "Boolean"),
        ],
    },
    {
        "entity": "Employee",
        "kql_table": "ClockInEvents",
        "timestamp_column": "timestamp",
        "key_column": "employee_id",
        "properties": [
            ("timestamp",        "DateTime"),
            ("event_type",       "String"),
            ("employee_email",   "String"),
            ("employee_name",    "String"),
            ("employee_id",      "String"),
            ("department",       "String"),
            ("project_id",       "String"),
            ("task_id",          "String"),
            ("simulator_id",     "String"),
            ("details",          "String"),
        ],
    },
    {
        "entity": "Machine",
        "kql_table": "AnomalyDetection",
        "timestamp_column": "alert_timestamp",
        "key_column": "machine_id",
        "properties": [
            ("alert_timestamp",      "DateTime"),
            ("machine_id",           "String"),
            ("failure_mode",         "String"),
            ("confidence_pct",       "Double"),
            ("severity",             "String"),
            ("rul_hours",            "Double"),
            ("recommended_action",   "String"),
        ],
    },
]

print(f"Entities      : {len(ENTITIES)}")
print(f"Relationships : {len(RELATIONSHIPS)}")
print(f"Timeseries    : {len(TIMESERIES_BINDINGS)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3 - Build ontology definition parts
random.seed(20260423)
_used_ids = set()

def generate_id():
    while True:
        v = random.randint(10**12, 10**15)
        if v not in _used_ids:
            _used_ids.add(v)
            return str(v)

def to_b64(obj):
    return base64.b64encode(json.dumps(obj).encode("utf-8")).decode("utf-8")

entity_type_ids   = {}   # entity_name -> id
property_ids      = {}   # (entity_name, col_name) -> id
ts_property_ids   = {}   # (entity_name, kql_table, col_name) -> id (timeseries props live on the entity)
key_property_ids  = {}   # entity_name -> property id of the key column

parts = [
    {"path": "definition.json", "payload": to_b64({}), "payloadType": "InlineBase64"},
]

# ---- Entity types ----------------------------------------------------------
for entity_name, cfg in ENTITIES.items():
    etype_id = generate_id()
    entity_type_ids[entity_name] = etype_id

    properties = []
    for (col, vtype) in cfg["columns"]:
        pid = generate_id()
        property_ids[(entity_name, col)] = pid
        properties.append({
            "id": pid,
            "name": col,
            "redefines": None,
            "baseTypeNamespaceType": None,
            "valueType": vtype,
        })
    key_property_ids[entity_name] = property_ids[(entity_name, cfg["key"])]
    display_prop_id               = property_ids[(entity_name, cfg["display"])]

    # Attach timeseries properties for any timeseries binding targeting this entity
    ts_props = []
    for ts in TIMESERIES_BINDINGS:
        if ts["entity"] != entity_name:
            continue
        for (col, vtype) in ts["properties"]:
            pid = generate_id()
            ts_property_ids[(entity_name, ts["kql_table"], col)] = pid
            ts_props.append({
                "id": pid,
                "name": f"{ts['kql_table']}_{col}",
                "redefines": None,
                "baseTypeNamespaceType": None,
                "valueType": vtype,
            })

    entity_def = {
        "id": etype_id,
        "namespace": "usertypes",
        "baseEntityTypeId": None,
        "name": entity_name,
        "entityIdParts": [key_property_ids[entity_name]],
        "displayNamePropertyId": display_prop_id,
        "namespaceType": "Custom",
        "visibility": "Visible",
        "properties": properties,
        "timeseriesProperties": ts_props,
    }
    parts.append({
        "path": f"EntityTypes/{etype_id}/definition.json",
        "payload": to_b64(entity_def),
        "payloadType": "InlineBase64",
    })

    # Non-timeseries DataBinding to the SQL Database table (served via OneLake)
    binding_id = str(uuid.uuid4())
    data_binding = {
        "id": binding_id,
        "dataBindingConfiguration": {
            "dataBindingType": "NonTimeSeries",
            "propertyBindings": [
                {"sourceColumnName": col, "targetPropertyId": property_ids[(entity_name, col)]}
                for (col, _) in cfg["columns"]
            ],
            "sourceTableProperties": {
                "sourceType": "LakehouseTable",
                "workspaceId": WORKSPACE_ID,
                "itemId": SQL_DB_ID,
                "sourceTableName": cfg["table"],
                "sourceSchema": cfg["schema"],
            },
        },
    }
    parts.append({
        "path": f"EntityTypes/{etype_id}/DataBindings/{binding_id}.json",
        "payload": to_b64(data_binding),
        "payloadType": "InlineBase64",
    })

    print(f"  Entity: {entity_name:20s} ({len(properties)} props, {len(ts_props)} ts-props)")

# ---- Timeseries DataBindings (Eventhouse / KustoTable) --------------------
for ts in TIMESERIES_BINDINGS:
    entity_name = ts["entity"]
    etype_id    = entity_type_ids[entity_name]
    kql_table   = ts["kql_table"]

    property_bindings = []
    # Map timestamp column
    property_bindings.append({
        "sourceColumnName": ts["timestamp_column"],
        "targetPropertyId": ts_property_ids[(entity_name, kql_table, ts["timestamp_column"])],
    })
    # Map all ts columns
    for (col, _) in ts["properties"]:
        if col == ts["timestamp_column"]:
            continue
        property_bindings.append({
            "sourceColumnName": col,
            "targetPropertyId": ts_property_ids[(entity_name, kql_table, col)],
        })
    # Also map the key column to the entity's key (so events link to the entity)
    property_bindings.append({
        "sourceColumnName": ts["key_column"],
        "targetPropertyId": key_property_ids[entity_name],
    })

    binding_id = str(uuid.uuid4())
    ts_binding = {
        "id": binding_id,
        "dataBindingConfiguration": {
            "dataBindingType": "TimeSeries",
            "timestampColumnName": ts["timestamp_column"],
            "propertyBindings": property_bindings,
            "sourceTableProperties": {
                "sourceType": "KustoTable",
                "workspaceId": WORKSPACE_ID,
                "itemId": EH_ID,
                "clusterUri": KUSTO_CLUSTER_URI,
                "databaseName": KQL_DATABASE_NAME,
                "sourceTableName": kql_table,
            },
        },
    }
    parts.append({
        "path": f"EntityTypes/{etype_id}/DataBindings/{binding_id}.json",
        "payload": to_b64(ts_binding),
        "payloadType": "InlineBase64",
    })
    print(f"  Timeseries: {kql_table:20s} -> {entity_name}")

# ---- Relationship types + Contextualizations -------------------------------
for (rel_name, src_entity, tgt_entity, src_table_ref, src_key_col, tgt_fk_col) in RELATIONSHIPS:
    rel_id = generate_id()
    rel_def = {
        "namespace": "usertypes",
        "id": rel_id,
        "name": rel_name,
        "namespaceType": "Custom",
        "source": {"entityTypeId": entity_type_ids[src_entity]},
        "target": {"entityTypeId": entity_type_ids[tgt_entity]},
    }
    parts.append({
        "path": f"RelationshipTypes/{rel_id}/definition.json",
        "payload": to_b64(rel_def),
        "payloadType": "InlineBase64",
    })

    # Contextualization: links rows of src_table to source/target keys
    src_schema, src_table = src_table_ref
    ctx_id = str(uuid.uuid4())
    contextualization = {
        "id": ctx_id,
        "dataBindingTable": {
            "sourceType": "LakehouseTable",
            "workspaceId": WORKSPACE_ID,
            "itemId": SQL_DB_ID,
            "sourceTableName": src_table,
            "sourceSchema": src_schema,
        },
        "sourceKeyRefBindings": [
            {"sourceColumnName": src_key_col, "targetPropertyId": key_property_ids[src_entity]},
        ],
        "targetKeyRefBindings": [
            {"sourceColumnName": tgt_fk_col, "targetPropertyId": key_property_ids[tgt_entity]},
        ],
    }
    parts.append({
        "path": f"RelationshipTypes/{rel_id}/Contextualizations/{ctx_id}.json",
        "payload": to_b64(contextualization),
        "payloadType": "InlineBase64",
    })
    print(f"  Relationship: {rel_name:32s} {src_entity} -> {tgt_entity}")

print(f"\nTotal definition parts: {len(parts)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4 - Delete existing ontology (idempotent) and create new one
ONTOLOGY_BASE = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/ontologies"

list_resp = requests.get(ONTOLOGY_BASE, headers=HEADERS)
if list_resp.status_code == 404:
    print("Ontology API returned 404 - the Ontology preview may not be enabled on this capacity.")
    raise RuntimeError("Ontology API unavailable (HTTP 404). Ensure the workspace is on a supported Fabric capacity with Ontology preview enabled.")
list_resp.raise_for_status()

existing = next((o for o in list_resp.json().get("value", []) if o.get("displayName") == ONTOLOGY_NAME), None)
if existing:
    print(f"Deleting existing ontology '{ONTOLOGY_NAME}' (id={existing['id']})...")
    del_resp = requests.delete(f"{ONTOLOGY_BASE}/{existing['id']}", headers=HEADERS)
    if del_resp.status_code not in (200, 202, 204):
        print(f"  WARNING: delete returned {del_resp.status_code}: {del_resp.text[:200]}")
    else:
        print("  Deleted. Waiting 20s for cleanup...")
        time.sleep(20)

payload = {
    "displayName": ONTOLOGY_NAME,
    "description": ONTOLOGY_DESCRIPTION,
    "definition": {"parts": parts},
}
if RTI_FOLDER_ID:
    payload["folderId"] = RTI_FOLDER_ID

print(f"Creating ontology '{ONTOLOGY_NAME}' with {len(parts)} parts...")
create_resp = requests.post(ONTOLOGY_BASE, json=payload, headers=HEADERS)
print(f"  Status: {create_resp.status_code}")

if create_resp.status_code == 201:
    result = create_resp.json()
    print(f"Ontology created. ID: {result.get('id')}")
elif create_resp.status_code == 202:
    loc = create_resp.headers.get("Location")
    retry = int(create_resp.headers.get("Retry-After", "15"))
    print(f"  LRO accepted. Polling {loc} every {retry}s...")
    for _ in range(40):
        time.sleep(retry)
        poll = requests.get(loc, headers=HEADERS)
        status = (poll.json().get("status") or "").lower()
        print(f"  Status: {status}")
        if status == "succeeded":
            print("Ontology created (LRO succeeded).")
            break
        if status in ("failed", "cancelled"):
            print(f"LRO {status}: {poll.text[:500]}")
            break
    else:
        print("LRO still running after polling timeout.")
else:
    print(f"Create failed: {create_resp.text[:800]}")
    create_resp.raise_for_status()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5 - Summary
print("=" * 60)
print(f"Ontology : {ONTOLOGY_NAME}")
print(f"Entities : {len(ENTITIES)}")
for name, cfg in ENTITIES.items():
    print(f"  - {name:20s} key={cfg['key']:20s} src={cfg['schema']}.{cfg['table']}")
print(f"Relationships: {len(RELATIONSHIPS)}")
for r in RELATIONSHIPS:
    print(f"  - {r[0]:32s} {r[1]} -> {r[2]}")
print(f"Timeseries bindings: {len(TIMESERIES_BINDINGS)}")
for ts in TIMESERIES_BINDINGS:
    print(f"  - {ts['kql_table']:20s} -> {ts['entity']} (by {ts['key_column']})")
print("=" * 60)
print("Open the ontology in the Fabric workspace to explore entities, relationships,")
print("and live telemetry bindings.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
