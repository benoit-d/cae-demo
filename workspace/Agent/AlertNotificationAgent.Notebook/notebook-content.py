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
# Reads critical anomaly alerts from the KQL Database and sends
# notifications to Microsoft Teams via an Incoming Webhook.
# 
# **Trigger options:**
# 1. Called by the MachineHealthActivator (Reflex) when anomalies are detected
# 2. Scheduled via a Data Pipeline every 5 minutes
# 3. Run manually during a demo
# 
# ## Setup
# 1. Create a Teams Incoming Webhook in your target channel
# 2. Paste the webhook URL in the TEAMS_WEBHOOK_URL config cell
# 3. Optionally configure the Foundry agent endpoint for AI-powered analysis
# 
# ## Flow
# 1. Query `CriticalAnomalyAlerts()` from KQL (confidence >= 80%)
# 2. Also query `MachineHealthAlerts()` for context
# 3. Format an Adaptive Card with machine details, scores, RUL
# 4. POST to Teams webhook
# 5. Optionally invoke a Foundry agent for root-cause analysis

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === CONFIGURATION ===
# Teams Incoming Webhook URL — create one in your Teams channel
TEAMS_WEBHOOK_URL = ""  # e.g. "https://outlook.office.com/webhook/..."

# Foundry Agent endpoint (optional — for AI root-cause analysis)
FOUNDRY_AGENT_ENDPOINT = ""  # e.g. "https://your-foundry-endpoint.azurewebsites.net"

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

# Check AnomalyAlerts table (from ML scoring notebook)
anomaly_alerts = kql_query(f"""
    AnomalyAlerts
    | where scored_at > ago(15m)
    | where anomaly_confidence_pct >= {MIN_CONFIDENCE}
    | summarize arg_max(scored_at, *) by machine_id
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

# Step 4: (Optional) Invoke Foundry Agent for AI root-cause analysis
if all_alerts and FOUNDRY_AGENT_ENDPOINT:
    print("\nInvoking Foundry Agent for root-cause analysis...")

    # Build context for the agent
    agent_context = {
        "alerts": all_alerts,
        "question": (
            "Analyze these machine health alerts for CAE flight simulator manufacturing. "
            "For each alert, provide: (1) likely root cause, (2) recommended immediate action, "
            "(3) impact on production schedule, (4) recommended spare parts to have ready."
        ),
    }

    try:
        agent_resp = requests.post(
            f"{FOUNDRY_AGENT_ENDPOINT}/api/agent/analyze",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN_FABRIC}",
            },
            json=agent_context,
            timeout=60,
        )
        if agent_resp.status_code == 200:
            analysis = agent_resp.json()
            print("\n=== AI Root-Cause Analysis ===")
            print(json.dumps(analysis, indent=2))

            # Send analysis to Teams too
            if TEAMS_WEBHOOK_URL:
                analysis_card = {
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
                                    "text": "🤖 AI Root-Cause Analysis",
                                    "weight": "bolder",
                                    "size": "large",
                                },
                                {
                                    "type": "TextBlock",
                                    "text": json.dumps(analysis, indent=2)[:2000],
                                    "wrap": True,
                                    "fontType": "monospace",
                                }
                            ]
                        }
                    }]
                }
                requests.post(TEAMS_WEBHOOK_URL, json=analysis_card, timeout=10)
                print("Analysis sent to Teams")
        else:
            print(f"Agent response: {agent_resp.status_code} {agent_resp.text[:200]}")
    except Exception as e:
        print(f"Agent call failed: {e}")
elif all_alerts:
    print("\nFOUNDRY_AGENT_ENDPOINT not configured — skipping AI analysis")
    print("To enable: deploy a Foundry agent and set the endpoint URL above.")
else:
    print("\nNo alerts — skipping AI analysis")

print("\n" + "="*60)
print("  ALERT NOTIFICATION COMPLETE")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
