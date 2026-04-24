$ErrorActionPreference = "Stop"
$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"

$items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items" -Headers $headers).value
$eh = $items | Where-Object { $_.displayName -eq "CAEManufacturingEH" }
$ehProps = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/eventhouses/$($eh.id)" -Headers $headers
$kqlUri = $ehProps.properties.queryServiceUri
$kqlDb = ($items | Where-Object { $_.displayName -eq "CAEManufacturingKQLDB" -and $_.type -eq "KQLDatabase" }).id
Write-Host "KQL: $kqlUri | DB: $kqlDb"

$existing = $items | Where-Object { $_.displayName -eq "MachineHealthDashboard" -and $_.type -eq "KQLDashboard" }

# Generate proper UUIDs
function U { [guid]::NewGuid().ToString() }

$dsId = U
$p1 = U; $p2 = U; $p3 = U; $p4 = U
$paramId = U

# Colors in RGBA (dashboard requires this format)
$teal = "rgba(0,120,212,1)"
$magenta = "rgba(232,17,35,1)"
$amber = "rgba(255,140,0,1)"
$cyan = "rgba(80,230,255,1)"
$green = "rgba(0,178,148,1)"

# Generate query IDs and tile IDs
$qIds = @{}; $tIds = @{}
1..27 | ForEach-Object { $qIds["q$_"] = U; $tIds["t$_"] = U }

function Q($key, $kql) {
    @{ id = $qIds[$key]; text = $kql; usedVariables = @("_startTime","_endTime")
       dataSource = @{ kind = "inline"; dataSourceId = $dsId } }
}

function T($key, $title, $page, $vtype, $qkey, $x, $y, $w, $h, $vopts) {
    $t = @{ id = $tIds[$key]; title = $title; visualType = $vtype; pageId = $page
            layout = @{ x=$x; y=$y; width=$w; height=$h }
            queryRef = @{ kind = "query"; queryId = $qIds[$qkey] }
            visualOptions = if ($vopts) { $vopts } else { @{} } }
    $t
}

$tcCNC = @{
    multipleYAxes = @{ additional = @(); showMultiplePanels = $false
        base = @{ id = "-1"; columns = @(); label = ""; yAxisMinimumValue = $null; yAxisMaximumValue = $null; yAxisScale = "linear"; horizontalLines = @() } }
    seriesColors = @{ "CNC-003" = $magenta; "CNC-001" = $teal; "CNC-002" = $cyan; "CNC-005" = $green }
    legendLocation = "bottom"; crossFilter = @(); crossFilterDisabled = $false; drillthroughDisabled = $false; drillthrough = @()
}

$queries = @(
    (Q "q5" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize events=count() by bin(timestamp, 1m) | render timechart with (ytitle='Events/min')")
    (Q "q6" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize count() by alert_level | render piechart")
    (Q "q7" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where alert_level in ('Warning','Critical') | summarize critical=countif(alert_level=='Critical'), warning=countif(alert_level=='Warning') by machine_id | order by critical desc | render barchart")
    (Q "q8" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize countLevel=count() by bin(timestamp,5m), alert_level | render timechart with (kind=stacked, ytitle='Readings')")
    (Q "q9" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize arg_max(timestamp, *) by machine_id, sensor_name | project timestamp, machine_id, sensor_category, sensor_name, value=round(value,3), unit, alert_level | order by machine_id, sensor_name")
    (Q "q10" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Spindle Temperature' | project timestamp, value, machine_id | render timechart with (ytitle='C')")
    (Q "q11" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Spindle Vibration' | project timestamp, value, machine_id | render timechart with (ytitle='g')")
    (Q "q12" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Power Consumption' | project timestamp, value, machine_id | render timechart with (ytitle='kW')")
    (Q "q13" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Coolant Flow Rate' | project timestamp, value, machine_id | render timechart with (ytitle='LPM')")
    (Q "q14" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Axis Position Accuracy' | project timestamp, value, machine_id | render timechart with (ytitle='mm')")
    (Q "q15" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where sensor_name == 'Spindle Speed' | project timestamp, value, machine_id | render timechart with (ytitle='RPM')")
    (Q "q16" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id == 'CNC-003' | summarize arg_max(timestamp, *) by sensor_name | project sensor_name, value=round(value,4), unit, alert_level | order by sensor_name")
    (Q "q17" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'CNC' | where alert_level != 'Normal' | summarize alerts=count() by machine_id, bin(timestamp,1m), alert_level | render timechart with (kind=stacked)")
    (Q "q18" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'LSR' | where sensor_name in ('Laser Power','Laser Temperature','Nozzle Distance') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q19" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'WLD' | where sensor_name in ('Arc Voltage','Welding Current','Wire Feed Speed','Gas Flow Rate') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q20" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'LTH' | where sensor_name in ('Spindle Speed','Spindle Vibration','Power Consumption') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q21" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id startswith 'PNT' | where sensor_name in ('Booth Temperature','Humidity','Airflow Velocity') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q22" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | where machine_id in ('RFL-001','ASM-001') | project timestamp, value, series=strcat(machine_id,' ',sensor_name) | render timechart")
    (Q "q23" "MachineTelemetry | where timestamp between (_startTime .. _endTime) | summarize avg_v=round(avg(value),2), min_v=round(min(value),2), max_v=round(max(value),2), readings=count() by machine_id, sensor_name, unit | order by machine_id, sensor_name")
    (Q "q24" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | project timestamp, machine_id, anomaly_type, confidence=anomaly_confidence_pct, severity, rul_hours=estimated_rul_hours, description | order by timestamp desc")
    (Q "q25" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | project timestamp, anomaly_confidence_pct, machine_id | render timechart with (ytitle='Confidence %')")
    (Q "q26" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | summarize alerts=count(), max_conf=max(anomaly_confidence_pct) by machine_id | order by max_conf desc | render barchart")
    (Q "q27" "AnomalyDetection | where timestamp between (_startTime .. _endTime) | summarize count() by severity | render piechart")
)

$tiles = @(
    (T "t1" "Ingestion Rate" $p1 "timechart" "q5" 0 0 12 7 @{})
    (T "t2" "Alert Distribution" $p1 "pie" "q6" 12 0 12 7 @{})
    (T "t3" "Machines with Alerts" $p1 "bar" "q7" 0 7 12 7 @{})
    (T "t4" "Alert Level Over Time" $p1 "timechart" "q8" 12 7 12 7 @{})
    (T "t9" "Latest Readings" $p1 "table" "q9" 0 14 24 7 @{})
    (T "t10" "Spindle Temperature" $p2 "timechart" "q10" 0 0 12 6 $tcCNC)
    (T "t11" "Spindle Vibration" $p2 "timechart" "q11" 12 0 12 6 $tcCNC)
    (T "t12" "Power Consumption" $p2 "timechart" "q12" 0 6 12 6 $tcCNC)
    (T "t13" "Coolant Flow Rate" $p2 "timechart" "q13" 12 6 12 6 $tcCNC)
    (T "t14" "Axis Accuracy" $p2 "timechart" "q14" 0 12 12 6 $tcCNC)
    (T "t15" "Spindle Speed" $p2 "timechart" "q15" 12 12 12 6 $tcCNC)
    (T "t16" "CNC-003 Current Values" $p2 "table" "q16" 0 18 12 5 @{})
    (T "t17" "CNC Alert Timeline" $p2 "timechart" "q17" 12 18 12 5 @{})
    (T "t18" "Laser Cutters" $p3 "timechart" "q18" 0 0 12 6 @{})
    (T "t19" "Welders" $p3 "timechart" "q19" 12 0 12 6 @{})
    (T "t20" "Lathes" $p3 "timechart" "q20" 0 6 12 6 @{})
    (T "t21" "Paint Booths" $p3 "timechart" "q21" 12 6 12 6 @{})
    (T "t22" "Electronics" $p3 "timechart" "q22" 0 12 12 6 @{})
    (T "t23" "All Sensors Stats" $p3 "table" "q23" 12 12 12 6 @{})
    (T "t24" "ML Anomaly Alerts" $p4 "table" "q24" 0 0 24 7 @{})
    (T "t25" "Confidence Over Time" $p4 "timechart" "q25" 0 7 12 6 @{})
    (T "t26" "Anomalies by Machine" $p4 "bar" "q26" 12 7 12 6 @{})
    (T "t27" "Severity Breakdown" $p4 "pie" "q27" 0 13 12 5 @{})
)

$dashDef = @{
    schema_version = 74
    tiles = $tiles
    baseQueries = @()
    parameters = @(
        @{ kind = "duration"; id = $paramId; displayName = "Time range"; description = ""
           beginVariableName = "_startTime"; endVariableName = "_endTime"
           defaultValue = @{ kind = "dynamic"; count = 1; unit = "hours" }
           showOnPages = @{ kind = "all" } }
    )
    dataSources = @(
        @{ kind = "kusto-trident"; name = "CAEManufacturingKQLDB"; clusterUri = $kqlUri
           databaseArtifactId = $kqlDb; database = $kqlDb
           workspace = "00000000-0000-0000-0000-000000000000"; id = $dsId }
    )
    pages = @(
        @{ name = "Factory Floor"; id = $p1 }
        @{ name = "CNC Mills Detail"; id = $p2 }
        @{ name = "All Machine Types"; id = $p3 }
        @{ name = "Anomaly Detection"; id = $p4 }
    )
    queries = $queries
} | ConvertTo-Json -Depth 20

$b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashDef))

if ($existing) {
    Write-Host "Updating $($existing.id)..."
    $body = @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $b64; payloadType = "InlineBase64" } ) } }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/kqlDashboards/$($existing.id)/updateDefinition" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
    $dashId = $existing.id
} else {
    Write-Host "Creating..."
    $folders = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/folders" -Headers $headers).value
    $rti = $folders | Where-Object { $_.displayName -eq "RTI" }
    $body = @{ displayName = "MachineHealthDashboard"; definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $b64; payloadType = "InlineBase64" } ) } }
    if ($rti) { $body.folderId = $rti.id }
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/kqlDashboards" -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -SkipHttpErrorCheck
    $dashId = ($resp.Content | ConvertFrom-Json).id
}
Write-Host "Status: $($resp.StatusCode)"
if ($resp.StatusCode -in 200,201) { Write-Host "Done! https://app.fabric.microsoft.com/groups/$ws/kqlDashboards/$dashId" }
elseif ($resp.StatusCode -eq 202) {
    $opId = $resp.Headers['x-ms-operation-id'] | Select-Object -First 1
    for ($i=0;$i -lt 20;$i++) { Start-Sleep 3; $p = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $headers; Write-Host "  $($p.status)"; if ($p.status -notin 'Running','NotStarted') { break } }
    if ($p.status -eq 'Succeeded') { Write-Host "Done! https://app.fabric.microsoft.com/groups/$ws/kqlDashboards/$dashId" } else { $p | ConvertTo-Json }
} else { Write-Host "Error: $($resp.Content.Substring(0,[Math]::Min(500,$resp.Content.Length)))" }
