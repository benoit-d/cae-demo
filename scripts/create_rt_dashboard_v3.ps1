$ErrorActionPreference = "Stop"
$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"

$items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items" -Headers $headers).value
$eh = $items | Where-Object { $_.displayName -eq "CAEManufacturingEH" }
$ehProps = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/eventhouses/$($eh.id)" -Headers $headers
$kqlUri = $ehProps.properties.queryServiceUri
$kqlDb = ($items | Where-Object { $_.displayName -eq "CAEManufacturingKQLDB" -and $_.type -eq "KQLDatabase" }).id
Write-Host "KQL URI: $kqlUri | DB: $kqlDb"

$existing = $items | Where-Object { $_.displayName -eq "MachineHealthDashboard" -and $_.type -eq "KQLDashboard" }

$dsId = "ds-cae-kql"
$p1 = "page-floor"
$p2 = "page-cnc"
$p3 = "page-allm"
$p4 = "page-anomaly"

# Colors: Fabric Teal #0078D4, Magenta #E81123
$teal = "#0078D4"
$magenta = "#E81123"
$amber = "#FF8C00"

function Q($id, $kql) { @{ id = $id; dataSource = @{ kind = "inline"; dataSourceId = $dsId }; text = $kql; usedVariables = @("_startTime","_endTime") } }
function T($id, $title, $page, $vtype, $qid, $x, $y, $w, $h, $vopts) {
    $t = @{ id = $id; title = $title; visualType = $vtype; pageId = $page; layout = @{ x=$x; y=$y; width=$w; height=$h }; queryRef = @{ kind = "query"; queryId = $qid }; visualOptions = @{} }
    if ($vopts) { $t.visualOptions = $vopts }
    $t
}

$queries = @(
    # Page 1 queries
    (Q "q01" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize machines=dcount(machine_id) | project strcat(machines, ' / 20')")
    (Q "q02" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | count")
    (Q "q03" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where alert_level == 'Critical' | count")
    (Q "q04" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where alert_level == 'Warning' | count")
    (Q "q05" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize events=count() by bin(timestamp, 1m) | render timechart with (ytitle='Events/min')")
    (Q "q06" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize count() by alert_level | render piechart")
    (Q "q07" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where alert_level in ('Warning','Critical') | summarize critical=countif(alert_level=='Critical'), warning=countif(alert_level=='Warning') by machine_id | order by critical desc | render barchart")
    (Q "q08" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize countLevel=count() by bin(timestamp,5m), alert_level | render timechart with (kind=stacked, ytitle='Readings')")
    (Q "q09" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize arg_max(timestamp, *) by machine_id, sensor_name | project timestamp, machine_id, sensor_category, sensor_name, value=round(value,3), unit, alert_level | order by machine_id, sensor_name")
    # Page 2 queries — CNC detail
    (Q "q20" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Spindle Temperature' | project timestamp, value, machine_id | render timechart with (ytitle='°C')")
    (Q "q21" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Spindle Vibration' | project timestamp, value, machine_id | render timechart with (ytitle='g')")
    (Q "q22" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Power Consumption' | project timestamp, value, machine_id | render timechart with (ytitle='kW')")
    (Q "q23" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Coolant Flow Rate' | project timestamp, value, machine_id | render timechart with (ytitle='LPM')")
    (Q "q24" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Axis Position Accuracy' | project timestamp, value, machine_id | render timechart with (ytitle='mm')")
    (Q "q25" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Spindle Speed' | project timestamp, value, machine_id | render timechart with (ytitle='RPM')")
    (Q "q26" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id == 'CNC-003' | summarize arg_max(timestamp, *) by sensor_name | project sensor_name, value=round(value,4), unit, alert_level | order by sensor_name")
    (Q "q27" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where alert_level != 'Normal' | summarize alerts=count() by machine_id, bin(timestamp,1m), alert_level | render timechart with (kind=stacked)")
    # Page 3 queries — all machines
    (Q "q30" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'LSR' | where sensor_name in ('Laser Power','Laser Temperature','Nozzle Distance') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q31" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'WLD' | where sensor_name in ('Arc Voltage','Welding Current','Wire Feed Speed','Gas Flow Rate') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q32" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'LTH' | where sensor_name in ('Spindle Speed','Spindle Vibration','Power Consumption') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q33" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'PNT' | where sensor_name in ('Booth Temperature','Humidity','Airflow Velocity') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q34" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id in ('RFL-001','ASM-001') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q35" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize avg_v=round(avg(value),2), min_v=round(min(value),2), max_v=round(max(value),2), readings=count() by machine_id, sensor_name, unit | order by machine_id, sensor_name")
    # Page 4 queries — anomaly
    (Q "q40" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | project timestamp, machine_id, anomaly_type, confidence=anomaly_confidence_pct, severity, rul_hours=estimated_rul_hours, description | order by timestamp desc")
    (Q "q41" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | project timestamp, anomaly_confidence_pct, machine_id | render timechart with (ytitle='Confidence %')")
    (Q "q42" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | summarize alerts=count(), max_conf=max(anomaly_confidence_pct) by machine_id | order by max_conf desc | render barchart")
    (Q "q43" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | summarize count() by severity | render piechart")
)

$tcOpts = @{
    multipleYAxes = @{ additional = @(); showMultiplePanels = $false; base = @{ horizontalLines = @(); yAxisScale = "linear"; columns = @(); id = "-1" } }
    seriesColors = @{ "CNC-003" = $magenta; "CNC-001" = $teal; "CNC-002" = "#50E6FF"; "CNC-005" = "#00B294" }
    legendLocation = "bottom"
    crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @()
}
$pieOpts = @{
    seriesColors = @{ "Normal" = $teal; "Warning" = $amber; "Critical" = $magenta }
    crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @()
}
$barOpts = @{
    seriesColors = @{ "critical" = $magenta; "warning" = $amber }
    legendLocation = "bottom"
    crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @()
}
$stackOpts = @{
    seriesColors = @{ "Normal" = $teal; "Warning" = $amber; "Critical" = $magenta }
    legendLocation = "bottom"
    crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @()
}
$tblOpts = @{ table__enableRenderLinks = $true; colorRulesDisabled = $false; crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @(); colorRules = @(
    @{ column = "alert_level"; rules = @( @{ type = "text"; text = "Critical"; color = $magenta }, @{ type = "text"; text = "Warning"; color = $amber }, @{ type = "text"; text = "Normal"; color = $teal } ) }
) }
$anomTblOpts = @{ table__enableRenderLinks = $true; colorRulesDisabled = $false; crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @(); colorRules = @(
    @{ column = "severity"; rules = @( @{ type = "text"; text = "Critical"; color = $magenta }, @{ type = "text"; text = "High"; color = $amber } ) }
) }

$tiles = @(
    # Page 1 — Factory Floor
    (T "t01" "Active Machines" $p1 "stat" "q01" 0 0 4 3 @{})
    (T "t02" "Total Readings" $p1 "stat" "q02" 4 0 4 3 @{})
    (T "t03" "⚠ Critical" $p1 "stat" "q03" 8 0 4 3 @{})
    (T "t04" "⚠ Warnings" $p1 "stat" "q04" 12 0 4 3 @{})
    (T "t05" "Ingestion Rate" $p1 "timechart" "q05" 0 3 12 5 $tcOpts)
    (T "t06" "Alert Distribution" $p1 "pie" "q06" 12 3 6 5 $pieOpts)
    (T "t07" "Machines with Alerts" $p1 "bar" "q07" 18 3 6 5 $barOpts)
    (T "t08" "Alert Level Over Time" $p1 "timechart" "q08" 0 8 12 5 $stackOpts)
    (T "t09" "Latest Readings" $p1 "table" "q09" 12 8 12 5 $tblOpts)
    # Page 2 — CNC Detail
    (T "t20" "Spindle Temperature (°C)" $p2 "timechart" "q20" 0 0 12 6 $tcOpts)
    (T "t21" "Spindle Vibration (g)" $p2 "timechart" "q21" 12 0 12 6 $tcOpts)
    (T "t22" "Power Consumption (kW)" $p2 "timechart" "q22" 0 6 12 6 $tcOpts)
    (T "t23" "Coolant Flow Rate (LPM)" $p2 "timechart" "q23" 12 6 12 6 $tcOpts)
    (T "t24" "Axis Accuracy (mm)" $p2 "timechart" "q24" 0 12 12 6 $tcOpts)
    (T "t25" "Spindle Speed (RPM)" $p2 "timechart" "q25" 12 12 12 6 $tcOpts)
    (T "t26" "CNC-003 Current Values" $p2 "table" "q26" 0 18 12 5 $tblOpts)
    (T "t27" "CNC Alert Timeline" $p2 "timechart" "q27" 12 18 12 5 $stackOpts)
    # Page 3 — All Machines
    (T "t30" "Laser Cutters" $p3 "timechart" "q30" 0 0 12 6 @{})
    (T "t31" "Welders" $p3 "timechart" "q31" 12 0 12 6 @{})
    (T "t32" "Lathes" $p3 "timechart" "q32" 0 6 12 6 @{})
    (T "t33" "Paint Booths" $p3 "timechart" "q33" 12 6 12 6 @{})
    (T "t34" "Electronics" $p3 "timechart" "q34" 0 12 12 6 @{})
    (T "t35" "All Sensors — Stats" $p3 "table" "q35" 12 12 12 6 @{})
    # Page 4 — Anomaly
    (T "t40" "ML Anomaly Alerts" $p4 "table" "q40" 0 0 24 7 $anomTblOpts)
    (T "t41" "Confidence Over Time" $p4 "timechart" "q41" 0 7 12 6 @{})
    (T "t42" "Anomalies by Machine" $p4 "bar" "q42" 12 7 12 6 @{})
    (T "t43" "Severity Breakdown" $p4 "pie" "q43" 0 13 12 5 $pieOpts)
)

$dashDef = @{
    schema_version = 74
    tiles = $tiles
    baseQueries = @()
    parameters = @(
        @{
            kind = "duration"
            id = "param-time"
            displayName = "Time range"
            description = ""
            beginVariableName = "_startTime"
            endVariableName = "_endTime"
            defaultValue = @{ kind = "dynamic"; count = 1; unit = "hours" }
            showOnPages = @{ kind = "all" }
        }
    )
    dataSources = @(
        @{
            kind = "kusto-trident"
            name = "CAEManufacturingKQLDB"
            clusterUri = $kqlUri
            databaseArtifactId = $kqlDb
            database = $kqlDb
            workspace = "00000000-0000-0000-0000-000000000000"
            id = $dsId
        }
    )
    pages = @(
        @{ name = "Factory Floor"; id = $p1 }
        @{ name = "CNC Mills — Sensor Detail"; id = $p2 }
        @{ name = "All Machine Types"; id = $p3 }
        @{ name = "Anomaly Detection"; id = $p4 }
    )
    queries = $queries
} | ConvertTo-Json -Depth 20

$dashDefB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashDef))

if ($existing) {
    Write-Host "Updating MachineHealthDashboard ($($existing.id))..."
    $body = @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashDefB64; payloadType = "InlineBase64" } ) } }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/kqlDashboards/$($existing.id)/updateDefinition" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
    $dashId = $existing.id
} else {
    Write-Host "Creating MachineHealthDashboard..."
    $folders = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/folders" -Headers $headers).value
    $rtiFolder = $folders | Where-Object { $_.displayName -eq "RTI" }
    $body = @{ displayName = "MachineHealthDashboard"; description = "CAE Manufacturing — 4-page real-time monitoring"; definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashDefB64; payloadType = "InlineBase64" } ) } }
    if ($rtiFolder) { $body.folderId = $rtiFolder.id }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/kqlDashboards" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
    $dashId = ($resp.Content | ConvertFrom-Json).id
}
Write-Host "Status: $($resp.StatusCode)"
if ($resp.StatusCode -in 200,201) { Write-Host "Done! Open: https://app.fabric.microsoft.com/groups/$ws/kqlDashboards/$dashId" }
elseif ($resp.StatusCode -eq 202) {
    $opId = $resp.Headers['x-ms-operation-id'] | Select-Object -First 1
    for ($i=0;$i -lt 20;$i++) { Start-Sleep 3; $p = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $headers; Write-Host "  $($p.status)"; if ($p.status -notin 'Running','NotStarted') { break } }
    if ($p.status -eq 'Succeeded') { Write-Host "Done! Open: https://app.fabric.microsoft.com/groups/$ws/kqlDashboards/$dashId" } else { $p | ConvertTo-Json -Depth 5 }
} else { Write-Host "Error: $($resp.Content.Substring(0,[Math]::Min(500,$resp.Content.Length)))" }
