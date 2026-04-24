#!/usr/bin/env pwsh
# Creates a Real-Time Dashboard in the Fabric workspace via API
param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
)

$ErrorActionPreference = "Stop"

$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

# Discover Eventhouse KQL URI
$items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value
$eh = $items | Where-Object { $_.displayName -eq "CAEManufacturingEH" }
$ehProps = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventhouses/$($eh.id)" -Headers $headers
$kqlUri = $ehProps.properties.queryServiceUri
Write-Host "KQL URI: $kqlUri"

# Check if already exists
$existing = $items | Where-Object { $_.displayName -eq "MachineHealthDashboard" -and $_.type -eq "KQLDashboard" }
if ($existing) {
    Write-Host "Dashboard exists: $($existing.id) — will update definition"
    $dashboardId = $existing.id
    $isUpdate = $true
} else {
    $isUpdate = $false
}

# Build dashboard definition
$dashDef = @{
    autoRefresh = @{
        enabled = $true
        defaultRefreshRate = @{ type = "seconds"; value = 30 }
    }
    dataSources = @(
        @{
            id = "ds-kql"
            scopeId = "fabricCluster"
            name = "CAEManufacturingKQLDB"
            clusterUri = $kqlUri
            database = "CAEManufacturingKQLDB"
            kind = "manual-kusto"
        }
    )
    parameters = @(
        @{
            id = "param-machine"
            displayName = "Machine"
            type = "string"
            defaultValue = @{ kind = "scalar"; value = "CNC-003" }
            showOnPages = @{ kind = "all" }
        }
    )
    pages = @(
        @{
            id = "page-overview"
            name = "Machine Health Overview"
            tiles = @(
                # Tile 1 — Alert Level pie chart
                @{
                    id = "tile-alert-pie"
                    title = "Alert Distribution (30 min)"
                    layout = @{ x = 0; y = 0; width = 6; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(30m)
| summarize count() by alert_level
| render piechart
"@
                    visualType = "pie"
                },
                # Tile 2 — Machines with Critical alerts
                @{
                    id = "tile-critical-machines"
                    title = "Machines with Critical Readings (30 min)"
                    layout = @{ x = 6; y = 0; width = 6; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(30m)
| where alert_level in ('Warning', 'Critical')
| summarize
    critical = countif(alert_level == 'Critical'),
    warning = countif(alert_level == 'Warning')
    by machine_id
| order by critical desc
| render barchart
"@
                    visualType = "bar"
                },
                # Tile 3 — Latest readings table
                @{
                    id = "tile-latest-readings"
                    title = "Latest Readings — All Machines"
                    layout = @{ x = 12; y = 0; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(5m)
| summarize arg_max(timestamp, *) by machine_id, sensor_name
| project timestamp, machine_id, sensor_name, value, unit, alert_level
| order by machine_id, sensor_name
"@
                    visualType = "table"
                },
                # Tile 4 — Spindle Temperature time chart (CNC machines)
                @{
                    id = "tile-spindle-temp"
                    title = "Spindle Temperature — CNC Machines"
                    layout = @{ x = 0; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(30m)
| where machine_id startswith 'CNC'
| where sensor_name == 'Spindle Temperature'
| project timestamp, value, machine_id
| render timechart
"@
                    visualType = "timechart"
                },
                # Tile 5 — Spindle Vibration time chart (CNC machines)
                @{
                    id = "tile-spindle-vib"
                    title = "Spindle Vibration — CNC Machines"
                    layout = @{ x = 12; y = 6; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(30m)
| where machine_id startswith 'CNC'
| where sensor_name == 'Spindle Vibration'
| project timestamp, value, machine_id
| render timechart
"@
                    visualType = "timechart"
                },
                # Tile 6 — Power Consumption time chart (CNC machines)
                @{
                    id = "tile-power"
                    title = "Power Consumption — CNC Machines"
                    layout = @{ x = 0; y = 12; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(30m)
| where machine_id startswith 'CNC'
| where sensor_name == 'Power Consumption'
| project timestamp, value, machine_id
| render timechart
"@
                    visualType = "timechart"
                },
                # Tile 7 — Coolant Flow Rate time chart (CNC machines)
                @{
                    id = "tile-coolant"
                    title = "Coolant Flow Rate — CNC Machines"
                    layout = @{ x = 12; y = 12; width = 12; height = 6 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
MachineTelemetry
| where timestamp > ago(30m)
| where machine_id startswith 'CNC'
| where sensor_name == 'Coolant Flow Rate'
| project timestamp, value, machine_id
| render timechart
"@
                    visualType = "timechart"
                },
                # Tile 8 — Anomaly Detections
                @{
                    id = "tile-anomalies"
                    title = "Latest Anomaly Detections"
                    layout = @{ x = 0; y = 18; width = 24; height = 5 }
                    queryRef = @{ kind = "inline"; dataSourceId = "ds-kql" }
                    query = @"
AnomalyDetection
| where timestamp > ago(1h)
| project timestamp, machine_id, anomaly_type, anomaly_confidence_pct, severity, estimated_rul_hours, description
| order by timestamp desc
| take 20
"@
                    visualType = "table"
                }
            )
        }
    )
} | ConvertTo-Json -Depth 20

$dashDefB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashDef))

# Find RTI folder
$folders = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/folders" -Headers $headers).value
$rtiFolder = $folders | Where-Object { $_.displayName -eq "RTI" }

$body = @{
    displayName = "MachineHealthDashboard"
    description = "Real-time machine health monitoring — 30s auto-refresh. Sensor readings, alert distribution, CNC spindle/vibration/coolant/power time charts, anomaly detections."
    definition = @{
        parts = @(
            @{ path = "RealTimeDashboard.json"; payload = $dashDefB64; payloadType = "InlineBase64" }
        )
    }
}
if ($rtiFolder) {
    $body.folderId = $rtiFolder.id
    Write-Host "RTI folder: $($rtiFolder.id)"
}

Write-Host "Creating MachineHealthDashboard..."
if ($isUpdate) {
    $updateBody = @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashDefB64; payloadType = "InlineBase64" } ) } }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/kqlDashboards/$dashboardId/updateDefinition" -Headers $headers -Body ($updateBody | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
} else {
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/kqlDashboards" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
}
Write-Host "Status: $($resp.StatusCode)"

if ($resp.StatusCode -in 200, 201) {
    $result = $resp.Content | ConvertFrom-Json
    $id = if ($isUpdate) { $dashboardId } else { $result.id }
    Write-Host "Dashboard ready: $id"
    Write-Host "Open: https://app.fabric.microsoft.com/groups/$WorkspaceId/kqlDashboards/$id"
}
elseif ($resp.StatusCode -eq 202) {
    $opId = $resp.Headers['x-ms-operation-id'] | Select-Object -First 1
    Write-Host "Polling operation: $opId"
    for ($i = 0; $i -lt 20; $i++) {
        Start-Sleep 3
        $p = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $headers
        Write-Host "  $($p.status)"
        if ($p.status -notin 'Running', 'NotStarted') { break }
    }
    if ($p.status -eq 'Succeeded') {
        $r2 = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId/result" -Headers $headers
        Write-Host "`nDashboard created: $($r2.id)"
        Write-Host "Open: https://app.fabric.microsoft.com/groups/$WorkspaceId/kqlDashboards/$($r2.id)"
    }
    else {
        Write-Host "Failed:"
        $p | ConvertTo-Json -Depth 5
    }
}
else {
    Write-Host "Failed: $($resp.Content.Substring(0, [Math]::Min(500, $resp.Content.Length)))"
}
