# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # ML Anomaly Detection — Baseline, Scoring & Notification
# 
# Triggered by the **Fabric Activator** when sensor `alert_level` becomes `Critical`
# on the TelemetryEventStream. Can also be run manually for testing.
# 
# ## Flow
# 1. Query last 24h of telemetry from KQL to compute baseline stats (mean, stddev)
# 2. Query last 5min of telemetry and compute Z-scores per sensor
# 3. Combine Z-scores into a composite anomaly confidence per machine
# 4. Write rows with confidence >= 50% to `AnomalyDetection` table in KQL
# 5. Invoke the **Foundry agent** (`CAE-Manufacturing-Copilot`) via Azure AI Agent Service
#    — the agent performs root-cause analysis, assesses schedule impact, and sends Teams notifications
# 6. Fallback: send a **Teams Adaptive Card** if the agent is not configured

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
KQL_URI = ""  # Leave empty to auto-discover from Eventhouse
BASELINE_WINDOW = "24h"   # How far back to compute baseline stats
SCORING_WINDOW = "5m"     # Recent window to score
ALERT_THRESHOLD = 50.0    # Minimum confidence % to write to AnomalyDetection

# Azure AI Foundry Agent — root-cause analysis + Teams notification
AGENT_PROJECT_ENDPOINT = "https://demo-foundry-sweden.services.ai.azure.com/api/projects/proj-demo"
AGENT_ID = "CAE-Manufacturing-Copilot:1"

# Teams Incoming Webhook URL — fallback if agent is not configured
TEAMS_WEBHOOK_URL = ""  # e.g. "https://outlook.office.com/webhook/..."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, os, requests, math
from datetime import datetime, timezone
import notebookutils

# Discover workspace and Eventhouse
TOKEN_FABRIC = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
TOKEN_KQL = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")

WORKSPACE_ID = os.environ.get("TRIDENT_WORKSPACE_ID", "")
if not WORKSPACE_ID:
    try:
        ctx = notebookutils.runtime.context
        WORKSPACE_ID = ctx.get("currentWorkspaceId", "") or ctx.get("workspaceId", "")
    except Exception:
        pass

fab_headers = {"Authorization": f"Bearer {TOKEN_FABRIC}"}
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items", headers=fab_headers)
items = resp.json().get("value", [])

if not KQL_URI:
    eh = next((i for i in items if i.get("displayName") == "CAEManufacturingEH"), None)
    if eh:
        eh_props = requests.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/eventhouses/{eh['id']}",
            headers=fab_headers
        ).json()
        KQL_URI = eh_props.get("properties", {}).get("queryServiceUri", "")
        print(f"KQL URI: {KQL_URI}")
    else:
        raise RuntimeError("CAEManufacturingEH not found")

DB_NAME = "CAEManufacturingKQLDB"

def kql_query(query):
    """Run a KQL query and return results as list of dicts."""
    resp = requests.post(
        f"{KQL_URI}/v1/rest/query",
        headers={"Authorization": f"Bearer {TOKEN_KQL}", "Content-Type": "application/json"},
        json={"db": DB_NAME, "csl": query}
    )
    if resp.status_code != 200:
        print(f"KQL query failed: {resp.status_code} {resp.text[:200]}")
        return []
    data = resp.json()
    tables = data.get("Tables", [])
    if not tables:
        return []
    cols = [c["ColumnName"] for c in tables[0].get("Columns", [])]
    rows = tables[0].get("Rows", [])
    return [dict(zip(cols, row)) for row in rows]

def kql_mgmt(command):
    """Run a KQL management command."""
    resp = requests.post(
        f"{KQL_URI}/v1/rest/mgmt",
        headers={"Authorization": f"Bearer {TOKEN_KQL}", "Content-Type": "application/json"},
        json={"db": DB_NAME, "csl": command}
    )
    return resp.status_code, resp.text[:200]

print("Connected to KQL Database")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Ensure AnomalyDetection table exists with streaming ingestion
create_cmd = """
.create-merge table AnomalyDetection (
    timestamp: datetime,
    machine_id: string,
    anomaly_type: string,
    anomaly_confidence_pct: real,
    estimated_rul_hours: int,
    top_deviating_sensors: string,
    composite_score: real,
    description: string,
    severity: string
)
"""
status, msg = kql_mgmt(create_cmd)
print(f"AnomalyDetection table: {status}")

status2, _ = kql_mgmt(".alter table AnomalyDetection policy streamingingestion enable")
print(f"Streaming policy: {status2}")

# Retention: keep 30 days of anomaly alerts
status3, _ = kql_mgmt(".alter table AnomalyDetection policy retention softdelete = 30d recoverability = enabled")
print(f"Retention policy: {status3}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2: Compute baselines — mean and stddev per machine/sensor over baseline window
print(f"Computing baselines over last {BASELINE_WINDOW}...")

baselines = kql_query(f"""
    MachineTelemetry
    | where timestamp > ago({BASELINE_WINDOW})
    | summarize
        mean_val = avg(value),
        stddev_val = stdev(value),
        reading_count = count()
        by machine_id, sensor_name, unit
    | where reading_count >= 5
    | order by machine_id, sensor_name
""")

print(f"  {len(baselines)} machine-sensor baselines computed")

# Index baselines for fast lookup
baseline_map = {}
for b in baselines:
    key = f"{b['machine_id']}|{b['sensor_name']}"
    baseline_map[key] = {
        "mean": b["mean_val"],
        "stddev": b["stddev_val"] if b["stddev_val"] and b["stddev_val"] > 0 else 0.001,
        "count": b["reading_count"],
        "unit": b["unit"]
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Score recent readings with Z-scores
print(f"Scoring recent {SCORING_WINDOW} of telemetry...")

recent = kql_query(f"""
    MachineTelemetry
    | where timestamp > ago({SCORING_WINDOW})
    | summarize avg_val = avg(value), latest_alert = take_any(alert_level)
        by machine_id, sensor_name, unit
    | order by machine_id, sensor_name
""")

print(f"  {len(recent)} recent machine-sensor readings to score")

# Compute Z-scores and group by machine
machine_scores = {}
for r in recent:
    key = f"{r['machine_id']}|{r['sensor_name']}"
    bl = baseline_map.get(key)
    if not bl:
        continue

    z_score = abs(r["avg_val"] - bl["mean"]) / bl["stddev"]

    if r["machine_id"] not in machine_scores:
        machine_scores[r["machine_id"]] = []
    machine_scores[r["machine_id"]].append({
        "sensor_name": r["sensor_name"],
        "avg_val": r["avg_val"],
        "baseline_mean": bl["mean"],
        "baseline_stddev": bl["stddev"],
        "z_score": z_score,
        "unit": r["unit"],
        "latest_alert": r["latest_alert"]
    })

print(f"  {len(machine_scores)} machines scored")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4: Compute composite anomaly confidence per machine
# Map known failure modes to machines
FAILURE_MODES = {
    "CNC-001": "Spindle bearing wear",
    "CNC-002": "Spindle bearing wear",
    "CNC-003": "Spindle bearing wear",
    "CNC-005": "Spindle bearing wear",
    "LSR-001": "Laser nozzle degradation",
    "LSR-002": "Laser head overheating",
    "PRB-001": "Hydraulic leak / seal failure",
    "WLD-001": "Shielding gas contamination",
    "WLD-002": "Wire feed anomaly",
    "CMM-001": "Environmental drift / accuracy loss",
    "RFL-001": "Thermal profile drift",
    "ADD-001": "O2 ingress / chamber contamination",
    "CRN-001": "Brake pad wear",
    "HTB-001": "Pump cavitation",
    "EDM-001": "Wire tension / dielectric degradation",
    "LTH-001": "Spindle vibration anomaly",
    "LTH-002": "Spindle vibration anomaly",
    "PNT-001": "Booth environment deviation",
    "PNT-002": "Booth environment deviation",
    "ASM-001": "ESD / soldering anomaly",
}

anomaly_alerts = []
alert_ts = datetime.now(timezone.utc).isoformat()

for machine_id, sensors in machine_scores.items():
    # Composite: weighted average of Z-scores (higher Z = more anomalous)
    # Cap individual Z-scores at 5 to avoid one outlier dominating
    z_scores = [min(s["z_score"], 5.0) for s in sensors]
    if not z_scores:
        continue

    avg_z = sum(z_scores) / len(z_scores)
    max_z = max(z_scores)

    # Confidence: blend of avg and max Z-scores
    # Z=2 => ~50%, Z=3 => ~75%, Z=4 => ~90%, Z=5 => ~95%
    confidence = min(99.9, max(0.0, (avg_z * 0.4 + max_z * 0.6) * 20.0))

    # RUL estimate: rough inverse relationship to confidence
    if confidence >= 90:
        rul = 4
    elif confidence >= 80:
        rul = 24
    elif confidence >= 70:
        rul = 72
    elif confidence >= 50:
        rul = 168
    else:
        rul = 720

    # Top deviating sensors (sorted by Z-score)
    top_sensors = sorted(sensors, key=lambda s: s["z_score"], reverse=True)[:3]
    top_str = "; ".join(
        f"{s['sensor_name']}: {s['avg_val']:.2f} {s['unit']} (Z={s['z_score']:.1f})"
        for s in top_sensors
    )

    severity = "Critical" if confidence >= 85 else "High" if confidence >= 70 else "Medium" if confidence >= 50 else "Low"

    if confidence >= ALERT_THRESHOLD:
        anomaly_alerts.append({
            "timestamp": alert_ts,
            "machine_id": machine_id,
            "anomaly_type": FAILURE_MODES.get(machine_id, "Unknown"),
            "anomaly_confidence_pct": round(confidence, 1),
            "estimated_rul_hours": rul,
            "top_deviating_sensors": top_str,
            "composite_score": round(avg_z, 3),
            "description": f"{FAILURE_MODES.get(machine_id, 'Anomaly')} detected on {machine_id} with {confidence:.0f}% confidence. Top sensor: {top_sensors[0]['sensor_name']} (Z={top_sensors[0]['z_score']:.1f})",
            "severity": severity,
        })

print(f"\n{'='*60}")
print(f"  ANOMALY SCORING RESULTS")
print(f"{'='*60}")
print(f"  Machines scored:  {len(machine_scores)}")
print(f"  Alerts generated: {len(anomaly_alerts)}")
for a in sorted(anomaly_alerts, key=lambda x: x["anomaly_confidence_pct"], reverse=True):
    print(f"  {a['severity']:8s} {a['machine_id']:8s} {a['anomaly_confidence_pct']:5.1f}%  RUL:{a['estimated_rul_hours']:>4d}h  {a['anomaly_type']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 5: Write anomaly alerts to KQL via streaming ingestion
if anomaly_alerts:
    def esc_csv(v):
        s = str(v).replace('"', '""')
        return f'"{s}"' if ',' in s or '"' in s or ';' in s else s

    csv_lines = []
    for a in anomaly_alerts:
        csv_lines.append(",".join([
            a["timestamp"],
            a["machine_id"],
            esc_csv(a["anomaly_type"]),
            str(a["anomaly_confidence_pct"]),
            str(a["estimated_rul_hours"]),
            esc_csv(a["top_deviating_sensors"]),
            str(a["composite_score"]),
            esc_csv(a["description"]),
            a["severity"],
        ]))

    csv_payload = "\n".join(csv_lines)

    ingest_url = f"{KQL_URI}/v1/rest/ingest/{DB_NAME}/AnomalyDetection?streamFormat=Csv"
    ingest_headers = {
        "Authorization": f"Bearer {TOKEN_KQL}",
        "Content-Type": "text/csv",
    }
    ingest_resp = requests.post(ingest_url, headers=ingest_headers, data=csv_payload)

    if ingest_resp.status_code == 200:
        print(f"\nIngested {len(anomaly_alerts)} anomaly alerts to KQL")
    else:
        print(f"\nIngestion failed ({ingest_resp.status_code}): {ingest_resp.text[:300]}")
else:
    print("\nNo anomalies above threshold — all machines nominal")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 6: Invoke Foundry Agent for root-cause analysis + Teams notification
critical_alerts = [a for a in anomaly_alerts if a["anomaly_confidence_pct"] >= 70.0]
agent_analysis = None

if critical_alerts and AGENT_PROJECT_ENDPOINT and AGENT_ID:
    print("Invoking Foundry Agent for root-cause analysis...")
    print(f"  Agent: {AGENT_ID}")
    print(f"  Project: {AGENT_PROJECT_ENDPOINT}")

    # Build the alert message for the agent
    alert_summary = "\n".join(
        f"- {a['machine_id']} ({a['anomaly_type']}): {a['anomaly_confidence_pct']}% confidence, "
        f"RUL {a['estimated_rul_hours']}h, severity {a['severity']}. "
        f"Top sensors: {a['top_deviating_sensors']}"
        for a in critical_alerts
    )

    agent_message = (
        f"ANOMALY DETECTED — {len(critical_alerts)} machine(s) in critical state.\n\n"
        f"{alert_summary}\n\n"
        "Analyze these alerts. Provide:\n"
        "1. Likely root cause for each machine\n"
        "2. Recommended immediate action\n"
        "3. Impact on production schedule (queued jobs, revenue at risk)\n"
        "4. Recommended spare parts to have ready\n"
        "5. Send a summary notification to the production manager via Teams"
    )

    try:
        from azure.ai.projects import AIProjectClient
        from azure.core.credentials import AccessToken

        # Use Fabric token for Azure Cognitive Services
        TOKEN_AI = notebookutils.credentials.getToken("https://cognitiveservices.azure.com")

        class _FabricCredential:
            def get_token(self, *scopes, **kwargs):
                return AccessToken(TOKEN_AI, 0)

        client = AIProjectClient(
            endpoint=AGENT_PROJECT_ENDPOINT,
            credential=_FabricCredential(),
        )

        # Create a conversation thread and send the alert
        thread = client.agents.create_thread()
        client.agents.create_message(
            thread_id=thread.id,
            role="user",
            content=agent_message,
        )

        # Run the agent (synchronous — waits for completion)
        run = client.agents.create_and_process_run(
            thread_id=thread.id,
            agent_id=AGENT_ID,
        )

        if run.status == "completed":
            # Get the agent's response
            messages = client.agents.list_messages(thread_id=thread.id)
            for msg in messages.data:
                if msg.role == "assistant":
                    for block in msg.content:
                        if hasattr(block, "text"):
                            agent_analysis = block.text.value
                            break
                    break

            print("\n=== AI Root-Cause Analysis ===")
            print(agent_analysis or "(no response)")
        else:
            print(f"Agent run ended with status: {run.status}")
            if hasattr(run, "last_error") and run.last_error:
                print(f"  Error: {run.last_error}")

    except ImportError:
        print("azure-ai-projects SDK not available — install with: pip install azure-ai-projects")
    except Exception as e:
        print(f"Agent call failed: {e}")

elif critical_alerts:
    print("AGENT_PROJECT_ENDPOINT not configured — skipping AI analysis")
else:
    print("No critical alerts — skipping AI analysis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 7: Fallback Teams notification (if agent didn't handle it)
# The Foundry agent sends Teams notifications directly.
# This is a fallback for when the agent is not configured or fails.
def build_adaptive_card(alerts, analysis=None):
    """Build a Teams Adaptive Card for machine health alerts."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    severity_color = {
        "Critical": "attention", "High": "warning",
        "Medium": "accent", "Low": "good",
    }

    facts = []
    for a in alerts[:5]:
        color = severity_color.get(a["severity"], "default")
        rul_text = f" | RUL: {a['estimated_rul_hours']}h" if a["estimated_rul_hours"] > 0 else ""
        facts.append({
            "type": "FactSet",
            "facts": [
                {"title": "Machine", "value": a["machine_id"]},
                {"title": "Alert", "value": a["anomaly_type"]},
                {"title": "Confidence", "value": f"{a['anomaly_confidence_pct']}%{rul_text}"},
                {"title": "Severity", "value": a["severity"]},
            ]
        })
        if a.get("description"):
            facts.append({
                "type": "TextBlock", "text": a["description"],
                "wrap": True, "size": "small", "color": color,
            })
        facts.append({"type": "TextBlock", "text": "---", "spacing": "small"})

    body = [
        {
            "type": "TextBlock",
            "text": "⚠️ CAE Manufacturing — Machine Health Alert",
            "weight": "bolder", "size": "large",
            "color": "attention" if any(a["severity"] == "Critical" for a in alerts) else "warning",
        },
        {
            "type": "TextBlock",
            "text": f"{len(alerts)} machine(s) with anomalies detected at {now}",
            "wrap": True, "spacing": "small",
        },
    ] + facts

    if analysis:
        body.append({
            "type": "TextBlock",
            "text": "🤖 **AI Root-Cause Analysis**",
            "weight": "bolder", "spacing": "medium",
        })
        body.append({
            "type": "TextBlock",
            "text": json.dumps(analysis, indent=2, default=str)[:2000],
            "wrap": True, "fontType": "monospace", "size": "small",
        })

    body.append({
        "type": "ActionSet",
        "actions": [{
            "type": "Action.OpenUrl",
            "title": "Open Fabric Dashboard",
            "url": f"https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}",
        }]
    })

    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard", "version": "1.4", "body": body,
            }
        }]
    }

if critical_alerts and TEAMS_WEBHOOK_URL:
    card = build_adaptive_card(critical_alerts, agent_analysis)
    teams_resp = requests.post(
        TEAMS_WEBHOOK_URL, headers={"Content-Type": "application/json"},
        json=card, timeout=10,
    )
    if teams_resp.status_code in (200, 202):
        print(f"Teams notification sent ({len(critical_alerts)} alerts)")
    else:
        print(f"Teams notification failed: {teams_resp.status_code} {teams_resp.text[:200]}")
elif critical_alerts:
    print("TEAMS_WEBHOOK_URL not configured — skipping Teams notification")
    print("Alert summary:")
    for a in critical_alerts:
        print(f"  {a['severity']:8s} {a['machine_id']:8s} {a['anomaly_confidence_pct']:5.1f}%  {a['anomaly_type']}")
else:
    print("No critical alerts — nothing to send")

print(f"\n{'='*60}")
print(f"  ANOMALY DETECTION & NOTIFICATION COMPLETE")
print(f"{'='*60}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
