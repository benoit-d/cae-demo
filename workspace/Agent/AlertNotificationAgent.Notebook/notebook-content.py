# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Alert Notification Agent
# 
# Reads critical anomaly alerts from the KQL Database, invokes the
# **Foundry agent** for root-cause analysis, and sends notifications
# to Microsoft Teams.
# 
# **Trigger options:**
# 1. Called by the AnomalyDetection notebook after ML scoring
# 2. Run manually during a demo
# 
# ## Setup
# 1. The Foundry agent config is pre-set (CAE-Manufacturing-Copilot:1)
# 2. Optionally set TEAMS_WEBHOOK_URL for fallback Teams notifications
# 
# ## Flow
# 1. Query `CriticalAnomalyAlerts()` from KQL (confidence >= 80%)
# 2. Also query `MachineHealthAlerts()` for context
# 3. Invoke the **Foundry agent** for root-cause analysis + Teams notification
# 4. Fallback: send a Teams Adaptive Card via webhook if agent is not available

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
# All config read from Lakehouse config/connections.json (set once, persists across CI/CD).
AGENT_PROJECT_ENDPOINT = ""
AGENT_ID = ""
TEAMS_WEBHOOK_URL = ""

# KQL URI — leave empty to auto-discover
KQL_URI = ""

# Minimum confidence to send notification (avoid noise)
MIN_CONFIDENCE = 70.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, os, requests
from datetime import datetime, timezone
import notebookutils

# Discover workspace
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

# Read config from Lakehouse config file
lh = next((i for i in items if i.get("displayName") == "CAEManufacturing_LH" and i.get("type") == "Lakehouse"), None)
if lh and (not AGENT_PROJECT_ENDPOINT or not AGENT_ID):
    try:
        cfg_path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{lh['id']}/Files/config/connections.json"
        cfg = json.loads(notebookutils.fs.head(cfg_path, 10000))
        if not AGENT_PROJECT_ENDPOINT:
            AGENT_PROJECT_ENDPOINT = cfg.get("FOUNDRY_AGENT_PROJECT_ENDPOINT", "")
        if not AGENT_ID:
            AGENT_ID = cfg.get("FOUNDRY_AGENT_ID", "")
        if not TEAMS_WEBHOOK_URL:
            TEAMS_WEBHOOK_URL = cfg.get("TEAMS_WEBHOOK_URL", "")
        print(f"Agent: {'configured' if AGENT_ID else 'not configured'}")
        print(f"Teams: {'configured' if TEAMS_WEBHOOK_URL else 'not configured'}")
    except Exception as e:
        print(f"Config file not found — agent/Teams not configured: {e}")

def kql_query(query):
    """Run a KQL query and return results as list of dicts."""
    resp = requests.post(
        f"{KQL_URI}/v1/rest/query",
        headers={"Authorization": f"Bearer {TOKEN_KQL}", "Content-Type": "application/json"},
        json={"db": DB_NAME, "csl": query}
    )
    if resp.status_code != 200:
        return []
    data = resp.json()
    tables = data.get("Tables", [])
    if not tables:
        return []
    cols = [c["ColumnName"] for c in tables[0].get("Columns", [])]
    rows = tables[0].get("Rows", [])
    return [dict(zip(cols, row)) for row in rows]

print("Connected to KQL Database")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Query for critical anomaly alerts
print("Querying for anomaly alerts...\n")

# Check AnomalyDetection table (from ML scoring notebook)
anomaly_alerts = kql_query(f"""
    AnomalyDetection
    | where timestamp > ago(15m)
    | where anomaly_confidence_pct >= {MIN_CONFIDENCE}
    | summarize arg_max(timestamp, *) by machine_id
    | order by anomaly_confidence_pct desc
""")

# Also check the KQL function-based alerts
health_alerts = kql_query("""
    MachineHealthAlerts(15m)
    | summarize arg_max(timestamp, *) by machine_id, rule
    | order by composite_score desc
""")

print(f"  ML Anomaly Alerts: {len(anomaly_alerts)}")
print(f"  Health Rule Alerts: {len(health_alerts)}")

# Merge into a unified alert list
all_alerts = []

for a in anomaly_alerts:
    all_alerts.append({
        "machine_id": a["machine_id"],
        "alert_type": a.get("anomaly_type", "ML Anomaly"),
        "confidence": a["anomaly_confidence_pct"],
        "rul_hours": a.get("estimated_rul_hours", -1),
        "severity": a.get("severity", "High"),
        "description": a.get("description", ""),
        "sensors": a.get("top_deviating_sensors", ""),
        "source": "ML Model",
    })

for h in health_alerts:
    # Skip if already covered by ML alert for same machine
    if any(a["machine_id"] == h["machine_id"] for a in anomaly_alerts):
        continue
    all_alerts.append({
        "machine_id": h["machine_id"],
        "alert_type": h.get("rule", "Health Rule"),
        "confidence": round(h.get("composite_score", 0) * 100, 1),
        "rul_hours": -1,
        "severity": "Critical" if h.get("composite_score", 0) > 0.85 else "High",
        "description": h.get("description", ""),
        "sensors": "",
        "source": "KQL Rule",
    })

print(f"\n  Total unified alerts: {len(all_alerts)}")
for a in all_alerts:
    print(f"    {a['severity']:8s} {a['machine_id']:8s} {a['confidence']:5.1f}%  {a['alert_type']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 2: Build Teams Adaptive Card
def build_adaptive_card(alerts):
    """Build a Teams Adaptive Card for machine health alerts."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    severity_color = {
        "Critical": "attention",
        "High": "warning",
        "Medium": "accent",
        "Low": "good",
    }

    facts = []
    for a in alerts[:5]:  # Top 5 alerts
        color = severity_color.get(a["severity"], "default")
        rul_text = f" | RUL: {a['rul_hours']}h" if a["rul_hours"] > 0 else ""
        facts.append({
            "type": "FactSet",
            "facts": [
                {"title": "Machine", "value": a["machine_id"]},
                {"title": "Alert", "value": a["alert_type"]},
                {"title": "Confidence", "value": f"{a['confidence']}%{rul_text}"},
                {"title": "Severity", "value": a["severity"]},
            ]
        })
        if a["description"]:
            facts.append({
                "type": "TextBlock",
                "text": a["description"],
                "wrap": True,
                "size": "small",
                "color": color,
            })
        facts.append({"type": "TextBlock", "text": "---", "spacing": "small"})

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "⚠️ CAE Manufacturing — Machine Health Alert",
                        "weight": "bolder",
                        "size": "large",
                        "color": "attention" if any(a["severity"] == "Critical" for a in alerts) else "warning",
                    },
                    {
                        "type": "TextBlock",
                        "text": f"{len(alerts)} machine(s) with anomalies detected at {now}",
                        "wrap": True,
                        "spacing": "small",
                    },
                ] + facts + [
                    {
                        "type": "ActionSet",
                        "actions": [
                            {
                                "type": "Action.OpenUrl",
                                "title": "Open Fabric Dashboard",
                                "url": f"https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}",
                            }
                        ]
                    }
                ]
            }
        }]
    }
    return card

if all_alerts:
    card = build_adaptive_card(all_alerts)
    print("Adaptive Card built successfully")
    print(f"  Alerts included: {len(all_alerts)}")
else:
    print("No alerts to send — all machines nominal")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Send to Teams
if all_alerts and TEAMS_WEBHOOK_URL:
    card = build_adaptive_card(all_alerts)
    teams_resp = requests.post(
        TEAMS_WEBHOOK_URL,
        headers={"Content-Type": "application/json"},
        json=card,
        timeout=10,
    )
    if teams_resp.status_code in (200, 202):
        print(f"Teams notification sent ({len(all_alerts)} alerts)")
    else:
        print(f"Teams notification failed: {teams_resp.status_code} {teams_resp.text[:200]}")
elif all_alerts:
    print("TEAMS_WEBHOOK_URL not configured — skipping Teams notification")
    print("To enable: create an Incoming Webhook in your Teams channel")
    print("and paste the URL in the TEAMS_WEBHOOK_URL config cell above.")
    print("\nAlert summary that would have been sent:")
    print(json.dumps(all_alerts, indent=2, default=str))
else:
    print("No alerts — nothing to send")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 4: Invoke Foundry Agent for AI root-cause analysis
if all_alerts and AGENT_PROJECT_ENDPOINT and AGENT_ID:
    print("\nInvoking Foundry Agent for root-cause analysis...")
    print(f"  Agent: {AGENT_ID}")
    print(f"  Project: {AGENT_PROJECT_ENDPOINT}")

    alert_summary = "\n".join(
        f"- {a['machine_id']} ({a['alert_type']}): {a['confidence']}% confidence, "
        f"severity {a['severity']}. {a.get('description', '')}"
        for a in all_alerts
    )

    agent_message = (
        f"ANOMALY DETECTED — {len(all_alerts)} machine(s) in critical state.\n\n"
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

        TOKEN_AI = notebookutils.credentials.getToken("https://cognitiveservices.azure.com")

        class _FabricCredential:
            def get_token(self, *scopes, **kwargs):
                return AccessToken(TOKEN_AI, 0)

        client = AIProjectClient(
            endpoint=AGENT_PROJECT_ENDPOINT,
            credential=_FabricCredential(),
        )

        thread = client.agents.create_thread()
        client.agents.create_message(
            thread_id=thread.id,
            role="user",
            content=agent_message,
        )

        run = client.agents.create_and_process_run(
            thread_id=thread.id,
            agent_id=AGENT_ID,
        )

        if run.status == "completed":
            messages = client.agents.list_messages(thread_id=thread.id)
            for msg in messages.data:
                if msg.role == "assistant":
                    for block in msg.content:
                        if hasattr(block, "text"):
                            analysis = block.text.value
                            print("\n=== AI Root-Cause Analysis ===")
                            print(analysis)
                            break
                    break
        else:
            print(f"Agent run ended with status: {run.status}")
            if hasattr(run, "last_error") and run.last_error:
                print(f"  Error: {run.last_error}")

    except ImportError:
        print("azure-ai-projects SDK not available — install with: pip install azure-ai-projects")
    except Exception as e:
        print(f"Agent call failed: {e}")

elif all_alerts:
    print("\nAGENT_PROJECT_ENDPOINT not configured — skipping AI analysis")
else:
    print("\nNo alerts — skipping AI analysis")

print("\n" + "="*60)
print("  ALERT NOTIFICATION COMPLETE")
print("="*60)

# Release Spark resources so the notebook (and its parent pipeline) can end
# instead of holding the session open until idle timeout.
try:
    spark.stop()
except Exception:
    pass
try:
    notebookutils.session.stop()
except Exception:
    try:
        import mssparkutils
        mssparkutils.session.stop()
    except Exception:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
