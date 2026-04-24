#!/usr/bin/env pwsh
# Creates a rich multi-page Real-Time Dashboard for the CAE manufacturing demo
param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
)
$ErrorActionPreference = "Stop"

$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

$items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value
$eh = $items | Where-Object { $_.displayName -eq "CAEManufacturingEH" }
$ehProps = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventhouses/$($eh.id)" -Headers $headers
$kqlUri = $ehProps.properties.queryServiceUri
Write-Host "KQL URI: $kqlUri"

$existing = $items | Where-Object { $_.displayName -eq "MachineHealthDashboard" -and $_.type -eq "KQLDashboard" }

# ── Dashboard definition ──────────────────────────────────────────────
$dashDef = @{
    autoRefresh = @{ enabled = $true; defaultRefreshRate = @{ type = "seconds"; value = 30 } }
    dataSources = @(
        @{ id = "ds"; scopeId = "fabricCluster"; name = "CAEManufacturingKQLDB"; clusterUri = $kqlUri; database = "CAEManufacturingKQLDB"; kind = "manual-kusto" }
    )
    pages = @(
        # ════════════════════════════════════════════════════════════
        # PAGE 1 — Fleet Overview
        # ════════════════════════════════════════════════════════════
        @{
            id = "p1"; name = "Factory Floor Overview"
            tiles = @(
                # Row 0 — KPI cards
                @{
                    id = "t01"; title = "Active Machines"; layout = @{ x = 0; y = 0; width = 4; height = 3 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry | where timestamp > ago(5m) | summarize machines = dcount(machine_id) | project strcat(machines, ' / 20')"
                    visualType = "stat"
                },
                @{
                    id = "t02"; title = "Readings (Last Hour)"; layout = @{ x = 4; y = 0; width = 4; height = 3 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry | where timestamp > ago(1h) | count"
                    visualType = "stat"
                },
                @{
                    id = "t03"; title = "Critical Readings (Last Hour)"; layout = @{ x = 8; y = 0; width = 4; height = 3 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry | where timestamp > ago(1h) | where alert_level == 'Critical' | count"
                    visualType = "stat"
                },
                @{
                    id = "t04"; title = "Warning Readings (Last Hour)"; layout = @{ x = 12; y = 0; width = 4; height = 3 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry | where timestamp > ago(1h) | where alert_level == 'Warning' | count"
                    visualType = "stat"
                },
                @{
                    id = "t05"; title = "Sensors Reporting"; layout = @{ x = 16; y = 0; width = 4; height = 3 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry | where timestamp > ago(5m) | summarize dcount(sensor_id)"
                    visualType = "stat"
                },
                @{
                    id = "t06"; title = "Anomaly Detections (1h)"; layout = @{ x = 20; y = 0; width = 4; height = 3 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "AnomalyDetection | where timestamp > ago(1h) | count"
                    visualType = "stat"
                },

                # Row 1 — Ingestion rate + alert distribution
                @{
                    id = "t07"; title = "Ingestion Rate (events/min)"; layout = @{ x = 0; y = 3; width = 12; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(1h)`n| summarize events = count() by bin(timestamp, 1m)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t08"; title = "Alert Level Distribution (1h)"; layout = @{ x = 12; y = 3; width = 6; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(1h)`n| summarize count() by alert_level`n| render piechart"
                    visualType = "pie"
                },
                @{
                    id = "t09"; title = "Readings by Machine Type (1h)"; layout = @{ x = 18; y = 3; width = 6; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(1h)`n| summarize readings = count() by sensor_category`n| order by readings desc`n| render piechart"
                    visualType = "pie"
                },

                # Row 2 — Machine heatmap + alert timeline
                @{
                    id = "t10"; title = "Alert Heatmap — Machines × Time (1h)"; layout = @{ x = 0; y = 8; width = 16; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(1h)`n| where alert_level in ('Warning', 'Critical')`n| summarize alerts = count() by machine_id, bin(timestamp, 5m)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t11"; title = "Machines with Alerts (1h)"; layout = @{ x = 16; y = 8; width = 8; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(1h)`n| where alert_level in ('Warning', 'Critical')`n| summarize critical = countif(alert_level == 'Critical'), warning = countif(alert_level == 'Warning') by machine_id`n| order by critical desc`n| render barchart"
                    visualType = "bar"
                },

                # Row 3 — Latest readings table
                @{
                    id = "t12"; title = "Latest Readings — All Machines (Live)"; layout = @{ x = 0; y = 14; width = 24; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(5m)`n| summarize arg_max(timestamp, *) by machine_id, sensor_name`n| project timestamp, machine_id, sensor_category, sensor_name, value = round(value, 3), unit, alert_level`n| order by machine_id, sensor_name"
                    visualType = "table"
                }
            )
        },

        # ════════════════════════════════════════════════════════════
        # PAGE 2 — CNC Deep Dive (where faults happen)
        # ════════════════════════════════════════════════════════════
        @{
            id = "p2"; name = "CNC Mills — Sensor Detail"
            tiles = @(
                @{
                    id = "t20"; title = "Spindle Temperature — CNC Machines (30 min)"; layout = @{ x = 0; y = 0; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where sensor_name == 'Spindle Temperature'`n| project timestamp, value, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t21"; title = "Spindle Vibration — CNC Machines (30 min)"; layout = @{ x = 12; y = 0; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where sensor_name == 'Spindle Vibration'`n| project timestamp, value, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t22"; title = "Power Consumption — CNC Machines (30 min)"; layout = @{ x = 0; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where sensor_name == 'Power Consumption'`n| project timestamp, value, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t23"; title = "Coolant Flow Rate — CNC Machines (30 min)"; layout = @{ x = 12; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where sensor_name == 'Coolant Flow Rate'`n| project timestamp, value, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t24"; title = "Axis Position Accuracy — CNC Machines (30 min)"; layout = @{ x = 0; y = 12; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where sensor_name == 'Axis Position Accuracy'`n| project timestamp, value, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t25"; title = "Spindle Speed — CNC Machines (30 min)"; layout = @{ x = 12; y = 12; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where sensor_name == 'Spindle Speed'`n| project timestamp, value, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t26"; title = "CNC-003 All Sensors — Current Values"; layout = @{ x = 0; y = 18; width = 12; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(5m)`n| where machine_id == 'CNC-003'`n| summarize arg_max(timestamp, *) by sensor_name`n| project sensor_name, value = round(value, 4), unit, alert_level`n| order by sensor_name"
                    visualType = "table"
                },
                @{
                    id = "t27"; title = "CNC Alert Level Over Time (30 min)"; layout = @{ x = 12; y = 18; width = 12; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'CNC'`n| where alert_level != 'Normal'`n| summarize alerts = count() by machine_id, bin(timestamp, 1m), alert_level`n| render timechart"
                    visualType = "timechart"
                }
            )
        },

        # ════════════════════════════════════════════════════════════
        # PAGE 3 — All Machine Types
        # ════════════════════════════════════════════════════════════
        @{
            id = "p3"; name = "All Machine Types"
            tiles = @(
                @{
                    id = "t30"; title = "Laser Cutters — Power & Temperature (30 min)"; layout = @{ x = 0; y = 0; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'LSR'`n| where sensor_name in ('Laser Power', 'Laser Temperature', 'Nozzle Distance')`n| project timestamp, value, strcat(machine_id, ' - ', sensor_name)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t31"; title = "Welders — Arc Voltage & Wire Feed (30 min)"; layout = @{ x = 12; y = 0; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'WLD'`n| where sensor_name in ('Arc Voltage', 'Welding Current', 'Wire Feed Speed', 'Gas Flow Rate')`n| project timestamp, value, strcat(machine_id, ' - ', sensor_name)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t32"; title = "Lathes — Spindle & Power (30 min)"; layout = @{ x = 0; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'LTH'`n| where sensor_name in ('Spindle Speed', 'Spindle Vibration', 'Power Consumption')`n| project timestamp, value, strcat(machine_id, ' - ', sensor_name)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t33"; title = "Paint Booths — Environment (30 min)"; layout = @{ x = 12; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id startswith 'PNT'`n| where sensor_name in ('Booth Temperature', 'Humidity', 'Airflow Velocity')`n| project timestamp, value, strcat(machine_id, ' - ', sensor_name)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t34"; title = "Electronics — Reflow & Assembly (30 min)"; layout = @{ x = 0; y = 12; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id in ('RFL-001', 'ASM-001')`n| project timestamp, value, strcat(machine_id, ' - ', sensor_name)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t35"; title = "Hydraulic Test & Crane (30 min)"; layout = @{ x = 12; y = 12; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(30m)`n| where machine_id in ('HTB-001', 'CRN-001')`n| project timestamp, value, strcat(machine_id, ' - ', sensor_name)`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t36"; title = "Sensor Value Distribution — All Machines (1h)"; layout = @{ x = 0; y = 18; width = 24; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "MachineTelemetry`n| where timestamp > ago(1h)`n| summarize avg_val = round(avg(value), 2), min_val = round(min(value), 2), max_val = round(max(value), 2), readings = count() by machine_id, sensor_name, unit`n| order by machine_id, sensor_name"
                    visualType = "table"
                }
            )
        },

        # ════════════════════════════════════════════════════════════
        # PAGE 4 — Anomaly Detection & ML
        # ════════════════════════════════════════════════════════════
        @{
            id = "p4"; name = "Anomaly Detection"
            tiles = @(
                @{
                    id = "t40"; title = "ML Anomaly Alerts (Last 24h)"; layout = @{ x = 0; y = 0; width = 24; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "AnomalyDetection`n| where timestamp > ago(24h)`n| project timestamp, machine_id, anomaly_type, confidence = anomaly_confidence_pct, severity, rul_hours = estimated_rul_hours, description`n| order by timestamp desc"
                    visualType = "table"
                },
                @{
                    id = "t41"; title = "Anomaly Confidence Over Time (24h)"; layout = @{ x = 0; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "AnomalyDetection`n| where timestamp > ago(24h)`n| project timestamp, anomaly_confidence_pct, machine_id`n| render timechart"
                    visualType = "timechart"
                },
                @{
                    id = "t42"; title = "Anomalies by Machine (24h)"; layout = @{ x = 12; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "AnomalyDetection`n| where timestamp > ago(24h)`n| summarize alerts = count(), max_confidence = max(anomaly_confidence_pct) by machine_id`n| order by max_confidence desc`n| render barchart"
                    visualType = "bar"
                },
                @{
                    id = "t43"; title = "Severity Breakdown (24h)"; layout = @{ x = 0; y = 12; width = 8; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "AnomalyDetection`n| where timestamp > ago(24h)`n| summarize count() by severity`n| render piechart"
                    visualType = "pie"
                },
                @{
                    id = "t44"; title = "RUL Estimates (hours remaining)"; layout = @{ x = 8; y = 12; width = 16; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds" }
                    query = "AnomalyDetection`n| where timestamp > ago(24h)`n| summarize arg_max(timestamp, *) by machine_id`n| project machine_id, anomaly_type, anomaly_confidence_pct, estimated_rul_hours, severity`n| order by estimated_rul_hours asc"
                    visualType = "table"
                }
            )
        }
    )
} | ConvertTo-Json -Depth 20

$dashDefB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashDef))

# Deploy
if ($existing) {
    Write-Host "Updating dashboard $($existing.id)..."
    $body = @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashDefB64; payloadType = "InlineBase64" } ) } }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/kqlDashboards/$($existing.id)/updateDefinition" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
} else {
    Write-Host "Creating dashboard..."
    $folders = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Headers $headers).value
    $rtiFolder = $folders | Where-Object { $_.displayName -eq "RTI" }
    $body = @{
        displayName = "MachineHealthDashboard"
        description = "CAE Manufacturing — Real-time machine health monitoring (4 pages, 30s auto-refresh)"
        definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashDefB64; payloadType = "InlineBase64" } ) }
    }
    if ($rtiFolder) { $body.folderId = $rtiFolder.id }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/kqlDashboards" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
}

Write-Host "Status: $($resp.StatusCode)"
$dashId = if ($existing) { $existing.id } else { ($resp.Content | ConvertFrom-Json).id }
if ($resp.StatusCode -in 200, 201) {
    Write-Host "Done! Open: https://app.fabric.microsoft.com/groups/$WorkspaceId/kqlDashboards/$dashId"
} elseif ($resp.StatusCode -eq 202) {
    $opId = $resp.Headers['x-ms-operation-id'] | Select-Object -First 1
    for ($i=0; $i -lt 20; $i++) { Start-Sleep 3; $p = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $headers; Write-Host "  $($p.status)"; if ($p.status -notin 'Running','NotStarted') { break } }
    if ($p.status -eq 'Succeeded') { Write-Host "Done! Open: https://app.fabric.microsoft.com/groups/$WorkspaceId/kqlDashboards/$dashId" }
    else { $p | ConvertTo-Json -Depth 5 }
} else { $resp.Content.Substring(0, [Math]::Min(500, $resp.Content.Length)) }
