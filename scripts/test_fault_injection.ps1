param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$DatabaseName = "CAEManufacturingKQLDB",
    [string]$QueryUri = "https://trd-d7uc0kt9eex2bc7e1q.z9.kusto.fabric.microsoft.com"
)

$ErrorActionPreference = "Stop"

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  Fault Injection & Anomaly Detection Test   " -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Workspace ID: $WorkspaceId" -ForegroundColor DarkGray
Write-Host "Database:     $DatabaseName" -ForegroundColor DarkGray
Write-Host "Query URI:    $QueryUri" -ForegroundColor DarkGray
Write-Host ""

# Get Kusto token
Write-Host "Getting Kusto access token..." -ForegroundColor Yellow
$kustoToken = (az account get-access-token --resource "https://api.kusto.windows.net" --query accessToken -o tsv)

if (-not $kustoToken) {
    Write-Error "Failed to get Kusto token. Ensure you're logged in with 'az login'"
    exit 1
}
Write-Host "Token acquired successfully" -ForegroundColor Green
Write-Host ""

# Headers
$kustoHeaders = @{
    "Authorization" = "Bearer $kustoToken"
    "Content-Type" = "application/json"
}

# Function to run KQL management command (for .ingest inline)
function Invoke-KQLMgmt {
    param([string]$Command, [string]$Name)
    
    Write-Host "========================================" -ForegroundColor Magenta
    Write-Host "MGMT: $Name" -ForegroundColor Magenta
    Write-Host "========================================" -ForegroundColor Magenta
    Write-Host $Command -ForegroundColor DarkGray
    Write-Host ""
    
    $body = @{ 
        db = $DatabaseName
        csl = $Command 
    } | ConvertTo-Json -Depth 10
    
    try {
        $response = Invoke-RestMethod -Uri "$QueryUri/v1/rest/mgmt" -Method Post -Headers $kustoHeaders -Body $body
        if ($response.Tables -and $response.Tables[0].Rows) {
            Write-Host "SUCCESS: Ingested $($response.Tables[0].Rows.Count) records" -ForegroundColor Green
        } else {
            Write-Host "SUCCESS: Command executed" -ForegroundColor Green
        }
    } catch {
        Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
        if ($_.ErrorDetails) { 
            Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Red 
        }
    }
    Write-Host ""
}

# Function to run KQL query
function Invoke-KQLQuery {
    param([string]$Query, [string]$Name)
    
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "QUERY: $Name" -ForegroundColor Cyan  
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host $Query -ForegroundColor DarkGray
    Write-Host ""
    
    $body = @{ 
        db = $DatabaseName
        csl = $Query 
    } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Uri "$QueryUri/v1/rest/query" -Method Post -Headers $kustoHeaders -Body $body
        if ($response.Tables -and $response.Tables.Count -gt 0) {
            $table = $response.Tables[0]
            $columns = $table.Columns | ForEach-Object { $_.ColumnName }
            $rows = $table.Rows
            
            Write-Host "Results ($($rows.Count) rows):" -ForegroundColor Green
            
            if ($rows.Count -eq 0) {
                Write-Host "(No data returned)" -ForegroundColor Yellow
            } else {
                $results = foreach ($row in $rows) {
                    $obj = [ordered]@{}
                    for ($i = 0; $i -lt $columns.Count; $i++) { 
                        $obj[$columns[$i]] = $row[$i] 
                    }
                    [PSCustomObject]$obj
                }
                $results | Format-Table -AutoSize -Wrap
            }
        } else {
            Write-Host "(No tables returned)" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
        if ($_.ErrorDetails) { 
            Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Red 
        }
    }
    Write-Host ""
}

# ============================================
# STEP 1: Inject first batch of anomalous telemetry
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 1: Inject First Batch of Fault Data" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$ingest1 = @"
.ingest inline into table MachineTelemetry <|
2026-04-23T12:35:00Z,CNC-001,SENS-001,Spindle,Spindle Speed,5500.0,RPM,Warning,true
2026-04-23T12:35:00Z,CNC-001,SENS-002,Spindle,Spindle Temperature,68.5,Celsius,Warning,true
2026-04-23T12:35:00Z,CNC-001,SENS-003,Vibration,Spindle Vibration,0.19,g,Warning,true
2026-04-23T12:35:00Z,CNC-001,SENS-004,Coolant,Coolant Flow Rate,5.2,LPM,Warning,true
2026-04-23T12:35:00Z,CNC-001,SENS-005,Coolant,Coolant Temperature,32.0,Celsius,Normal,false
2026-04-23T12:35:00Z,CNC-001,SENS-006,Power,Power Consumption,31.5,kW,Warning,true
2026-04-23T12:35:00Z,CNC-001,SENS-007,Axis,Axis Position Accuracy,0.012,mm,Warning,true
2026-04-23T12:36:00Z,CNC-001,SENS-001,Spindle,Spindle Speed,5200.0,RPM,Warning,true
2026-04-23T12:36:00Z,CNC-001,SENS-002,Spindle,Spindle Temperature,72.0,Celsius,Critical,true
2026-04-23T12:36:00Z,CNC-001,SENS-003,Vibration,Spindle Vibration,0.22,g,Critical,true
2026-04-23T12:36:00Z,CNC-001,SENS-004,Coolant,Coolant Flow Rate,4.0,LPM,Critical,true
2026-04-23T12:36:00Z,CNC-001,SENS-005,Coolant,Coolant Temperature,35.0,Celsius,Warning,true
2026-04-23T12:36:00Z,CNC-001,SENS-006,Power,Power Consumption,34.0,kW,Critical,true
2026-04-23T12:36:00Z,CNC-001,SENS-007,Axis,Axis Position Accuracy,0.018,mm,Critical,true
"@
Invoke-KQLMgmt -Command $ingest1 -Name "Inject Batch 1 - CNC-001 Warning/Critical Spindle Failure"

# ============================================
# STEP 2: Inject second batch (1 minute later)
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 2: Inject Second Batch (Critical)" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$ingest2 = @"
.ingest inline into table MachineTelemetry <|
2026-04-23T12:37:00Z,CNC-001,SENS-001,Spindle,Spindle Speed,4800.0,RPM,Critical,true
2026-04-23T12:37:00Z,CNC-001,SENS-002,Spindle,Spindle Temperature,74.5,Celsius,Critical,true
2026-04-23T12:37:00Z,CNC-001,SENS-003,Vibration,Spindle Vibration,0.24,g,Critical,true
2026-04-23T12:37:00Z,CNC-001,SENS-004,Coolant,Coolant Flow Rate,3.5,LPM,Critical,true
2026-04-23T12:37:00Z,CNC-001,SENS-006,Power,Power Consumption,35.0,kW,Critical,true
2026-04-23T12:37:00Z,CNC-001,SENS-007,Axis,Axis Position Accuracy,0.022,mm,Critical,true
"@
Invoke-KQLMgmt -Command $ingest2 -Name "Inject Batch 2 - Critical Escalation"

# ============================================
# STEP 3: Check CNC Bearing Wear Score
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 3: Check CNC Bearing Wear Score" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$query3 = @"
CNC_BearingWearScore(30d) 
| where machine_id == "CNC-001" 
| where timestamp >= datetime(2026-04-23T12:35:00Z)
| order by timestamp desc
"@
Invoke-KQLQuery -Query $query3 -Name "CNC Bearing Wear Score for CNC-001"

# ============================================
# STEP 4: Check CNC Coolant Failure Score
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 4: Check CNC Coolant Failure Score" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$query4 = @"
CNC_CoolantFailScore(30d) 
| where machine_id == "CNC-001" 
| where timestamp >= datetime(2026-04-23T12:35:00Z)
| order by timestamp desc
"@
Invoke-KQLQuery -Query $query4 -Name "CNC Coolant Failure Score for CNC-001"

# ============================================
# STEP 5: Check MachineHealthAlerts
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 5: Check MachineHealthAlerts" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$query5 = @"
MachineHealthAlerts(30d) 
| where machine_id == "CNC-001"
| order by timestamp desc
| take 10
"@
Invoke-KQLQuery -Query $query5 -Name "Machine Health Alerts for CNC-001"

# ============================================
# STEP 6: Inject AnomalyAlert record
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 6: Inject AnomalyAlert Record" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$ingest3 = @"
.ingest inline into table AnomalyAlerts <|
2026-04-23T12:38:00Z,CNC-001,Spindle bearing wear,92.5,24,"Spindle Vibration: 0.24g (Z=4.2); Spindle Temperature: 74.5C (Z=3.8); Power: 35kW (Z=3.1)",4.2,"Spindle bearing wear detected on CNC-001 with 92% confidence. Top sensor: Spindle Vibration (Z=4.2)",Critical
"@
Invoke-KQLMgmt -Command $ingest3 -Name "Inject AnomalyAlert for Notification Path Test"

# ============================================
# STEP 7: Verify AnomalyAlerts
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 7: Verify AnomalyAlerts Table" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$query7 = @"
AnomalyAlerts 
| order by scored_at desc 
| take 5
"@
Invoke-KQLQuery -Query $query7 -Name "AnomalyAlerts (Latest 5)"

# ============================================
# STEP 8: Test CriticalAnomalyAlerts function
# ============================================
Write-Host "`n#############################################" -ForegroundColor Yellow
Write-Host "### STEP 8: Test CriticalAnomalyAlerts()" -ForegroundColor Yellow
Write-Host "#############################################`n" -ForegroundColor Yellow

$query8 = @"
CriticalAnomalyAlerts()
"@
Invoke-KQLQuery -Query $query8 -Name "Critical Anomaly Alerts Function"

# ============================================
# Summary
# ============================================
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  ALL STEPS COMPLETED" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Summary of injected data:" -ForegroundColor Green
Write-Host "  - MachineTelemetry: 20 fault records for CNC-001" -ForegroundColor White
Write-Host "  - AnomalyAlerts: 1 critical alert record" -ForegroundColor White
Write-Host ""
Write-Host "The anomaly detection pipeline should now show:" -ForegroundColor Green
Write-Host "  - High bearing wear scores (Step 3)" -ForegroundColor White
Write-Host "  - Elevated coolant failure scores (Step 4)" -ForegroundColor White
Write-Host "  - Machine health alerts (Step 5)" -ForegroundColor White
Write-Host "  - Critical anomaly alert for notification (Steps 7-8)" -ForegroundColor White
Write-Host ""
