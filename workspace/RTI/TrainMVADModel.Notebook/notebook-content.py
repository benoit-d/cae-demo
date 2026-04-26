# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Train Multivariate Anomaly Detection (MVAD) Model — CNC Mills
# 
# Trains a **MultivariateAnomalyDetector** model on normal CNC mill telemetry and
# registers it in Fabric MLflow for use by the KQL `predict_cnc_mvad` function.
# 
# ## Prerequisites
# 1. **OneLake availability** enabled on the Eventhouse (Settings → OneLake availability → On)
# 2. **`time-series-anomaly-detector==0.3.9`** in the attached Spark Environment
# 3. Several days of normal telemetry accumulated in `MachineTelemetry`
# 
# ## Flow
# 1. Read `MachineTelemetry` from OneLake (ABFSS) for CNC machines
# 2. Filter to `alert_level = "Normal"` data only
# 3. Pivot narrow table → wide format (one column per sensor)
# 4. Train `MultivariateAnomalyDetector` on 4 key sensors (Vibration, Temperature, Coolant, Power)
# 5. Register model in Fabric MLflow as `cnc_bearing_mvad_model`
# 6. Print the model ABFSS URI for the KQL prediction function

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===

# CNC machines sharing the same sensor profile (one model for all)
CNC_MACHINES = ["CNC-001", "CNC-002", "CNC-003", "CNC-005"]

# Sensors to include in the multivariate model — these are the 4 correlated
# sensors that shift together during a bearing failure cascade
FEATURE_SENSORS = [
    "Spindle Vibration",
    "Spindle Temperature",
    "Coolant Flow Rate",
    "Power Consumption",
]

# MVAD sliding window — number of consecutive samples the model looks at.
# At 1-min telemetry frequency, 200 samples ≈ 3.3 hours.
SLIDING_WINDOW = 200

# MLflow registered model name
MODEL_NAME = "cnc_bearing_mvad_model"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, os, requests
import numpy as np
import pandas as pd
import notebookutils

# --- Discover workspace & Eventhouse ---
TOKEN_FABRIC = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except Exception:
        pass

fab_headers = {"Authorization": f"Bearer {TOKEN_FABRIC}"}
resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items",
    headers=fab_headers,
)
items = resp.json().get("value", [])

# Find the Eventhouse query URI
eh = next((i for i in items if i.get("displayName") == "CAEManufacturingEH"), None)
if not eh:
    raise RuntimeError("CAEManufacturingEH not found in workspace")

eh_props = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/eventhouses/{eh['id']}",
    headers=fab_headers,
).json()
KQL_URI = eh_props.get("properties", {}).get("queryServiceUri", "")
print(f"Eventhouse query URI: {KQL_URI}")

# Find the KQL database ID for OneLake path
DB_NAME = "CAEManufacturingKQLDB"
kql_db = next(
    (i for i in items if i.get("displayName") == DB_NAME and i.get("type") == "KQLDatabase"),
    None,
)
if not kql_db:
    raise RuntimeError(f"{DB_NAME} not found in workspace")

DB_ID = kql_db["id"]
print(f"KQL Database ID: {DB_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Load telemetry from OneLake
# 
# OneLake availability must be enabled on the Eventhouse for this to work.
# The ABFSS path points to the Delta table backing `MachineTelemetry`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build ABFSS path to MachineTelemetry Delta table
onelake_abfss = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{DB_ID}/Tables/MachineTelemetry"
)
print(f"Reading from: {onelake_abfss}")

# Read via Spark (Delta format from OneLake)
raw_df = spark.read.format("delta").load(onelake_abfss)
total_rows = raw_df.count()
print(f"Total MachineTelemetry rows: {total_rows:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Filter & pivot to wide format
# 
# - Keep only CNC machines and the 4 target sensors
# - Keep only `alert_level = "Normal"` rows (training on healthy data)
# - Pivot from narrow (one row per sensor reading) to wide (one column per sensor)
# - Aggregate by `(timestamp, machine_id)` using mean to handle any duplicates

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Filter to CNC machines, target sensors, and normal data only
filtered_df = (
    raw_df
    .filter(F.col("machine_id").isin(CNC_MACHINES))
    .filter(F.col("sensor_name").isin(FEATURE_SENSORS))
    .filter(F.col("alert_level") == "Normal")
)

# Pivot: one column per sensor, grouped by (timestamp, machine_id)
pivoted_df = (
    filtered_df
    .groupBy("timestamp", "machine_id")
    .pivot("sensor_name", FEATURE_SENSORS)
    .agg(F.mean("value"))
)

# Drop rows with any null sensor values (incomplete readings)
pivoted_df = pivoted_df.dropna(subset=FEATURE_SENSORS)

# Sort by timestamp for time-series ordering
pivoted_df = pivoted_df.orderBy("timestamp", "machine_id")

pivoted_count = pivoted_df.count()
print(f"Pivoted rows (normal, CNC, 4 sensors): {pivoted_count:,}")
print(f"Per machine avg: ~{pivoted_count // max(len(CNC_MACHINES), 1):,} samples")

# Preview
pivoted_df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Prepare pandas DataFrame for MVAD training
# 
# The `MultivariateAnomalyDetector` expects a pandas DataFrame indexed by timestamp
# with numeric feature columns. We train one model across all CNC machines
# (pooled data — same sensor profile, same model).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert to pandas
pdf = pivoted_df.toPandas()

# Use timestamp as index (required by MVAD)
pdf["timestamp"] = pd.to_datetime(pdf["timestamp"])
pdf = pdf.sort_values(["timestamp", "machine_id"]).reset_index(drop=True)

# For training, we pool all CNC machines into a single time series.
# Group by timestamp and average across machines to get a representative signal.
train_df = (
    pdf
    .groupby("timestamp")[FEATURE_SENSORS]
    .mean()
    .sort_index()
)

print(f"Training samples: {len(train_df):,}")
print(f"Date range: {train_df.index.min()} → {train_df.index.max()}")
print(f"Columns: {list(train_df.columns)}")
train_df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sanity check: need at least sliding_window * 2 samples for meaningful training
min_samples = SLIDING_WINDOW * 2
if len(train_df) < min_samples:
    print(f"WARNING: Only {len(train_df)} samples available, need at least {min_samples}.")
    print(f"At 1-min intervals with 4 CNC machines, you need ~{min_samples} minutes "
          f"(~{min_samples / 60:.1f} hours) of normal telemetry.")
    print("The model will still train but quality may be poor. Accumulate more data and re-run.")
else:
    print(f"Sufficient data: {len(train_df)} samples (minimum {min_samples})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Train the MVAD model
# 
# Uses the `time-series-anomaly-detector` package from PyPI.
# The `MultivariateAnomalyDetector` learns normal cross-sensor correlations
# and can detect when the joint distribution shifts (e.g., bearing failure
# causes vibration ↑ + temperature ↑ + coolant ↓ + power ↑ simultaneously).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from anomaly_detector import MultivariateAnomalyDetector

model = MultivariateAnomalyDetector()
params = {"sliding_window": SLIDING_WINDOW}

print(f"Training MVAD model with sliding_window={SLIDING_WINDOW} on {len(train_df)} samples...")
model.fit(train_df, params=params)
print("Training complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Quick validation on training data
# 
# Run prediction on the training data to verify the model works.
# Anomalies on normal data should be rare (mostly near boundaries of the sliding window).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Predict on training data as a sanity check
predictions = model.predict(train_df)
pred_df = pd.DataFrame(predictions)
anomaly_count = pred_df["is_anomaly"].sum() if "is_anomaly" in pred_df.columns else 0
print(f"Anomalies detected on training data: {anomaly_count} / {len(pred_df)} "
      f"({anomaly_count / max(len(pred_df), 1) * 100:.1f}%)")
if anomaly_count > 0:
    print("(Small number of anomalies on training data is normal — edge effects near sliding window boundaries)")
pred_df.head(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6 — Register model in Fabric MLflow
# 
# The model is saved to the MLflow model registry so the KQL Python plugin
# can load it at prediction time via the ABFSS path.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
from mlflow.tracking import MlflowClient

with mlflow.start_run():
    mlflow.log_params(params)
    mlflow.log_param("feature_sensors", FEATURE_SENSORS)
    mlflow.log_param("cnc_machines", CNC_MACHINES)
    mlflow.log_param("training_samples", len(train_df))
    mlflow.log_param("date_range_start", str(train_df.index.min()))
    mlflow.log_param("date_range_end", str(train_df.index.max()))
    mlflow.set_tag("Training Info", "MVAD on CNC mill bearing failure sensors")
    mlflow.set_tag("machine_type", "CNC Mill")

    model_info = mlflow.pyfunc.log_model(
        python_model=model,
        artifact_path="mvad_artifacts",
        registered_model_name=MODEL_NAME,
    )

print(f"Model registered as: {MODEL_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the ABFSS URI of the registered model — this is what the KQL function needs
client = MlflowClient()
model_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest = max(model_versions, key=lambda v: v.creation_timestamp)
model_abfss = latest.source

print("=" * 80)
print("MODEL REGISTERED SUCCESSFULLY")
print("=" * 80)
print(f"Model name:    {MODEL_NAME}")
print(f"Version:       {latest.version}")
print(f"ABFSS URI:     {model_abfss}")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7 — Deploy KQL prediction functions
# 
# Automatically deploys the three MVAD KQL functions to the Eventhouse,
# substituting the trained model's ABFSS URI into `predict_cnc_mvad()`.
# 
# **Prerequisite:** Python 3.11.7 DL plugin must be enabled on the Eventhouse
# (Eventhouse → Plugins → Python language extension → On → Python 3.11.7 DL).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deploy KQL stored functions for MVAD prediction
TOKEN_KQL = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")

def kql_mgmt(command):
    """Run a KQL management command against the Eventhouse."""
    resp = requests.post(
        f"{KQL_URI}/v1/rest/mgmt",
        headers={"Authorization": f"Bearer {TOKEN_KQL}", "Content-Type": "application/json"},
        json={"db": DB_NAME, "csl": command}
    )
    return resp.status_code, resp.text[:300]

# Function 1: predict_fabric_mvad_fl — generic MVAD helper (from Microsoft tutorial)
fn1 = """
.create-or-alter function with (folder = "Packages\\\\ML", docstring = "Predict MVAD model in Microsoft Fabric")
predict_fabric_mvad_fl(samples:(*), features_cols:dynamic, artifacts_uri:string, trim_result:bool=false)
{
    let s = artifacts_uri;
    let artifacts = bag_pack(
        'MLmodel', strcat(s, '/MLmodel;impersonate'),
        'conda.yaml', strcat(s, '/conda.yaml;impersonate'),
        'requirements.txt', strcat(s, '/requirements.txt;impersonate'),
        'python_env.yaml', strcat(s, '/python_env.yaml;impersonate'),
        'python_model.pkl', strcat(s, '/python_model.pkl;impersonate')
    );
    let kwargs = bag_pack('features_cols', features_cols, 'trim_result', trim_result);
    let code = ```if 1:
        import os
        import shutil
        import mlflow
        model_dir = 'C:/Temp/mvad_model'
        model_data_dir = model_dir + '/data'
        os.mkdir(model_dir)
        shutil.move('C:/Temp/MLmodel', model_dir)
        shutil.move('C:/Temp/conda.yaml', model_dir)
        shutil.move('C:/Temp/requirements.txt', model_dir)
        shutil.move('C:/Temp/python_env.yaml', model_dir)
        shutil.move('C:/Temp/python_model.pkl', model_dir)
        features_cols = kargs["features_cols"]
        trim_result = kargs["trim_result"]
        test_data = df[features_cols]
        model = mlflow.pyfunc.load_model(model_dir)
        predictions = model.predict(test_data)
        predict_result = pd.DataFrame(predictions)
        samples_offset = len(df) - len(predict_result)
        if trim_result:
            result = df[samples_offset:]
            result.iloc[:,-4:] = predict_result.iloc[:, 1:]
        else:
            result = df
            result.iloc[samples_offset:,-4:] = predict_result.iloc[:, 1:]
        ```;
    samples
    | evaluate python(typeof(*), code, kwargs, external_artifacts=artifacts)
}
"""

s1, m1 = kql_mgmt(fn1)
print(f"predict_fabric_mvad_fl: {s1}")

# Function 2: predict_cnc_mvad — CNC-specific wrapper with model URI baked in
fn2 = f"""
.create-or-alter function with (folder = "Health\\\\MVAD", docstring = "Predict CNC mill multivariate anomalies using trained MVAD model")
predict_cnc_mvad(lookback_minutes:int = 400)
{{
    let sliding_window = 200;
    let prefix_score_len = sliding_window / 2 + min_of(sliding_window / 2, 200) - 1;
    let cnc_machines = dynamic(["CNC-001", "CNC-002", "CNC-003", "CNC-005"]);
    let feature_sensors = dynamic(["Spindle Vibration", "Spindle Temperature", "Coolant Flow Rate", "Power Consumption"]);
    let model_uri = "{model_abfss}";
    MachineTelemetry
    | where timestamp > ago(totimespan(strcat(tostring(lookback_minutes), "m")))
    | where machine_id in (cnc_machines)
    | where sensor_name in (feature_sensors)
    | summarize value = avg(value) by timestamp, machine_id, sensor_name
    | evaluate pivot(sensor_name, take_any(value))
    | project
        timestamp,
        machine_id,
        Spindle_Vibration = column_ifexists("Spindle Vibration", real(null)),
        Spindle_Temperature = column_ifexists("Spindle Temperature", real(null)),
        Coolant_Flow_Rate = column_ifexists("Coolant Flow Rate", real(null)),
        Power_Consumption = column_ifexists("Power Consumption", real(null))
    | where isnotnull(Spindle_Vibration) and isnotnull(Spindle_Temperature)
        and isnotnull(Coolant_Flow_Rate) and isnotnull(Power_Consumption)
    | order by machine_id asc, timestamp asc
    | extend is_anomaly = bool(false), score = real(null), severity = real(null), interpretation = dynamic(null)
    | invoke predict_fabric_mvad_fl(
        pack_array("Spindle_Vibration", "Spindle_Temperature", "Coolant_Flow_Rate", "Power_Consumption"),
        model_uri,
        trim_result=true
    )
}}
"""

s2, m2 = kql_mgmt(fn2)
print(f"predict_cnc_mvad: {s2}")

# Function 3: ingest_mvad_anomalies — writes MVAD results to AnomalyDetection table
fn3 = """
.create-or-alter function with (folder = "Health\\\\MVAD", docstring = "Insert MVAD anomalies into AnomalyDetection table")
ingest_mvad_anomalies()
{
    predict_cnc_mvad(400)
    | where is_anomaly == true
    | summarize
        top_sensors = strcat_delim(", ",
            iff(isnotnull(Spindle_Vibration), strcat("Spindle Vibration: ", round(Spindle_Vibration, 4)), ""),
            iff(isnotnull(Spindle_Temperature), strcat("Spindle Temperature: ", round(Spindle_Temperature, 1)), ""),
            iff(isnotnull(Coolant_Flow_Rate), strcat("Coolant Flow Rate: ", round(Coolant_Flow_Rate, 1)), ""),
            iff(isnotnull(Power_Consumption), strcat("Power Consumption: ", round(Power_Consumption, 1)), "")
        ),
        avg_score = avg(score),
        max_severity = max(severity)
        by timestamp, machine_id
    | extend
        anomaly_type = "MVAD",
        anomaly_confidence_pct = round(min_of(avg_score * 100.0, 99.9), 1),
        estimated_rul_hours = case(
            max_severity > 0.8, 4,
            max_severity > 0.6, 24,
            max_severity > 0.4, 72,
            max_severity > 0.2, 168,
            720
        ),
        composite_score = round(avg_score, 4),
        description = strcat("MVAD multivariate anomaly detected on ", machine_id),
        severity = case(
            max_severity > 0.8, "Critical",
            max_severity > 0.6, "High",
            max_severity > 0.4, "Medium",
            "Low"
        )
    | project
        timestamp,
        machine_id,
        anomaly_type,
        anomaly_confidence_pct,
        estimated_rul_hours,
        top_deviating_sensors = top_sensors,
        composite_score,
        description,
        severity
}
"""

s3, m3 = kql_mgmt(fn3)
print(f"ingest_mvad_anomalies: {s3}")

if all(s == 200 for s in [s1, s2, s3]):
    print("\nAll 3 MVAD KQL functions deployed successfully!")
    print("Test with: predict_cnc_mvad(400) | where is_anomaly == true | take 10")
else:
    print(f"\nSome functions failed. Check the Python 3.11.7 DL plugin is enabled on the Eventhouse.")
    if s1 != 200: print(f"  predict_fabric_mvad_fl error: {m1}")
    if s2 != 200: print(f"  predict_cnc_mvad error: {m2}")
    if s3 != 200: print(f"  ingest_mvad_anomalies error: {m3}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Done
# 
# The MVAD model is trained and KQL prediction functions are deployed.
# 
# **Remaining manual step:**
# - Ensure **Python 3.11.7 DL** plugin is enabled on the Eventhouse
#   (Eventhouse → Plugins → Python language extension → On → Python 3.11.7 DL)
# 
# **Test queries** (run in KQL Database query editor):
# ```kql
# // Detect anomalies on recent CNC data
# predict_cnc_mvad(400)
# | where is_anomaly == true
# | order by timestamp desc
# | take 50
# 
# // Ingest MVAD anomalies into AnomalyDetection table
# .set-or-append AnomalyDetection <|
#     ingest_mvad_anomalies()
# ```

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }
