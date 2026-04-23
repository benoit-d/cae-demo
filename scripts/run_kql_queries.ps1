param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$DatabaseName = "CAEManufacturingKQLDB"
)

$ErrorActionPreference = "Stop"

Write-Host "=== KQL Query Tester for Anomaly Detection Pipeline ===" -ForegroundColor Cyan
Write-Host ""

# Get Fabric token
Write-Host "Getting Fabric access token..." -ForegroundColor Yellow
$fabricToken = (az account get-access-token --resource "https://api.fabric.microsoft.com" | ConvertFrom-Json).accessToken

# Get Kusto token (uses different resource)
Write-Host "Getting Kusto access token..." -ForegroundColor Yellow  
$kustoToken = (az account get-access-token --resource "https://api.kusto.windows.net" | ConvertFrom-Json).accessToken

$fabricHeaders = @{ "Authorization" = "Bearer $fabricToken" }

# Get Eventhouse details
Write-Host "Fetching Eventhouse details..." -ForegroundColor Yellow
$ehResp = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventhouses" -Headers $fabricHeaders -Method Get
$eh = $ehResp.value | Select-Object -First 1

if (-not $eh) {
    Write-Error "No Eventhouse found in workspace"
    exit 1
}

$queryUri = $eh.properties.queryServiceUri
Write-Host "Eventhouse: $($eh.displayName)" -ForegroundColor Green
Write-Host "Query URI: $queryUri" -ForegroundColor Green
Write-Host ""

# Function to run KQL query
function Invoke-KQLQuery {
    param(
        [string]$Query,
        [string]$QueryName
    )
    
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "QUERY: $QueryName" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host $Query -ForegroundColor DarkGray
    Write-Host ""
    
    $body = @{
        db = $DatabaseName
        csl = $Query
    } | ConvertTo-Json
    
    $kustoHeaders = @{
        "Authorization" = "Bearer $kustoToken"
        "Content-Type" = "application/json"
    }
    
    try {
        $response = Invoke-RestMethod -Uri "$queryUri/v1/rest/query" -Method Post -Headers $kustoHeaders -Body $body
        
        if ($response.Tables -and $response.Tables.Count -gt 0) {
            $table = $response.Tables[0]
            $columns = $table.Columns | ForEach-Object { $_.ColumnName }
            $rows = $table.Rows
            
            Write-Host "Results ($($rows.Count) rows):" -ForegroundColor Green
            Write-Host ""
            
            if ($rows.Count -eq 0) {
                Write-Host "(No data returned)" -ForegroundColor Yellow
            }
            else {
                # Create objects from rows
                $results = foreach ($row in $rows) {
                    $obj = [ordered]@{}
                    for ($i = 0; $i -lt $columns.Count; $i++) {
                        $obj[$columns[$i]] = $row[$i]
                    }
                    [PSCustomObject]$obj
                }
                $results | Format-Table -AutoSize -Wrap
            }
        }
        else {
            Write-Host "(Query returned no tables)" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
        if ($_.ErrorDetails) {
            Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Red
        }
    }
    Write-Host ""
}

# Query 1 - Data volume and recency
$query1 = @"
MachineTelemetry 
| summarize total_records = count(), 
    latest = max(timestamp), 
    earliest = min(timestamp),
    machines = dcount(machine_id)
"@
Invoke-KQLQuery -Query $query1 -QueryName "1 - Data Volume and Recency"

# Query 2 - Baselines
$query2 = @"
MachineTelemetry
| summarize 
    mean_val = avg(value), 
    stddev_val = stdev(value), 
    reading_count = count()
    by machine_id, sensor_name
| where reading_count >= 5
| order by machine_id, sensor_name
| take 30
"@
Invoke-KQLQuery -Query $query2 -QueryName "2 - Compute Baselines (mean + stddev)"

# Query 3 - Health scoring function
$query3 = @"
CNC_BearingWearScore(30d) | take 10
"@
Invoke-KQLQuery -Query $query3 -QueryName "3 - CNC Bearing Wear Score Function"

# Query 4 - Unified alerts
$query4 = @"
MachineHealthAlerts(30d) | take 20
"@
Invoke-KQLQuery -Query $query4 -QueryName "4 - Machine Health Alerts"

# Query 5 - Z-score deviation
$query5 = @"
let baselines = MachineTelemetry
    | summarize mean_val = avg(value), stddev_val = stdev(value) by machine_id, sensor_name;
MachineTelemetry
| where timestamp > ago(30d)
| join kind=inner baselines on machine_id, sensor_name
| extend z_score = abs(value - mean_val) / iff(stddev_val > 0, stddev_val, 0.001)
| where z_score > 2.0
| summarize high_z_count = count(), avg_z = avg(z_score), max_z = max(z_score) by machine_id, sensor_name
| order by max_z desc
| take 20
"@
Invoke-KQLQuery -Query $query5 -QueryName "5 - High Z-Score Deviations"

# Query 6 - List functions
$query6 = @"
.show functions | project Name, DocString, Folder | order by Folder, Name
"@
Invoke-KQLQuery -Query $query6 -QueryName "6 - List All Functions"

Write-Host "=== All queries completed ===" -ForegroundColor Cyan
