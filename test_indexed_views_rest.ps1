# Test Indexed/Materialized Views using Fabric REST API
$ErrorActionPreference = "Stop"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$databaseName = "CAEManufacturing_SQLDB"

Write-Host "Getting Fabric API token..." -ForegroundColor Yellow
$token = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv).Trim()
Write-Host "Token acquired`n" -ForegroundColor Green

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

# Get database item ID
Write-Host "Looking up database item..." -ForegroundColor Yellow
$itemsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items?type=SQLDatabase"
$items = Invoke-RestMethod -Uri $itemsUrl -Headers $headers -Method Get
$dbItem = $items.value | Where-Object { $_.displayName -eq $databaseName }

if (-not $dbItem) {
    Write-Host "Database not found: $databaseName" -ForegroundColor Red
    exit 1
}

$dbItemId = $dbItem.id
Write-Host "Database Item ID: $dbItemId`n" -ForegroundColor Green

# Function to execute SQL via REST API
function Invoke-FabricSql {
    param([string]$Query, [string]$Description)
    
    Write-Host "========================================" -ForegroundColor Magenta
    Write-Host $Description -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Magenta
    Write-Host "SQL: $($Query.Substring(0, [Math]::Min($Query.Length, 80)))..." -ForegroundColor Gray
    
    $body = @{
        queries = @(@{ query = $Query })
    } | ConvertTo-Json -Depth 5
    
    # Try executeQueries endpoint
    $endpoint = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$dbItemId/executeQueries"
    
    try {
        $response = Invoke-RestMethod -Uri $endpoint -Headers $headers -Method Post -Body $body
        Write-Host "RESULT:" -ForegroundColor Green
        
        if ($response.results -and $response.results.Count -gt 0) {
            $result = $response.results[0]
            if ($result.tabularResults -and $result.tabularResults.Count -gt 0) {
                $rows = $result.tabularResults[0].rows
                $rows | ForEach-Object { $_ | ConvertTo-Json -Compress }
            }
            elseif ($result.rowsAffected -ne $null) {
                Write-Host "Rows affected: $($result.rowsAffected)" -ForegroundColor Green
            }
            else {
                Write-Host "Command completed successfully" -ForegroundColor Green
            }
        }
        else {
            Write-Host "Command completed (no result set)" -ForegroundColor Green
        }
        return $true
    }
    catch {
        $errBody = $_.ErrorDetails.Message
        Write-Host "ERROR:" -ForegroundColor Red
        try {
            $errJson = $errBody | ConvertFrom-Json
            Write-Host $errJson.error.message -ForegroundColor Red
            if ($errJson.error.details) {
                $errJson.error.details | ForEach-Object { Write-Host "  - $($_.message)" -ForegroundColor Red }
            }
        } catch {
            Write-Host $errBody -ForegroundColor Red
        }
        return $false
    }
}

# Query 1
Invoke-FabricSql -Query "SELECT @@VERSION AS sql_version, DB_NAME() AS db_name, SERVERPROPERTY('Edition') AS edition" -Description "QUERY 1: Check SQL Database edition"

# Query 2
$q2 = @"
CREATE OR ALTER VIEW plm.v_tasks_gantt AS
SELECT 
    t.Task_ID, t.Task_Name, t.Parent_Project_ID, t.Task_Type,
    t.Milestone, t.Skill_Requirement, t.Standard_Duration,
    t.Resource_Login, t.Complete_Percentage, t.Machine_ID,
    COALESCE(t.Actual_Start, t.Modified_Planned_Start, t.Initial_Planned_Start) AS Calculated_Start_Date,
    COALESCE(t.Actual_End, DATEADD(day, ISNULL(t.Standard_Duration, 0), COALESCE(t.Actual_Start, t.Modified_Planned_Start, t.Initial_Planned_Start))) AS Calculated_End_Date,
    p.Project_Name, p.Customer, p.Simulator_ID
FROM plm.tasks t
JOIN plm.projects p ON t.Parent_Project_ID = p.Project_ID
"@
Invoke-FabricSql -Query $q2 -Description "QUERY 2: Create simple view plm.v_tasks_gantt"

# Query 3
Invoke-FabricSql -Query "SELECT TOP 5 Task_Name, Calculated_Start_Date, Calculated_End_Date, Project_Name FROM plm.v_tasks_gantt ORDER BY Task_ID" -Description "QUERY 3: Test the view"

# Query 4
$q4 = @"
CREATE OR ALTER VIEW plm.v_tasks_gantt_mat WITH SCHEMABINDING AS
SELECT 
    t.Task_ID, t.Task_Name, t.Parent_Project_ID, t.Task_Type,
    t.Milestone, t.Skill_Requirement, t.Standard_Duration,
    t.Resource_Login, t.Complete_Percentage, t.Machine_ID,
    COALESCE(t.Actual_Start, t.Modified_Planned_Start, t.Initial_Planned_Start) AS Calculated_Start_Date,
    COALESCE(t.Actual_End, DATEADD(day, ISNULL(t.Standard_Duration, 0), COALESCE(t.Actual_Start, t.Modified_Planned_Start, t.Initial_Planned_Start))) AS Calculated_End_Date
FROM plm.tasks t
"@
$q4Success = Invoke-FabricSql -Query $q4 -Description "QUERY 4: Create indexed view with SCHEMABINDING"

# Query 5 - only if Query 4 succeeded
if ($q4Success) {
    Invoke-FabricSql -Query "CREATE UNIQUE CLUSTERED INDEX IX_v_tasks_gantt_mat ON plm.v_tasks_gantt_mat (Task_ID)" -Description "QUERY 5: Create clustered index to materialize the view"
} else {
    Write-Host "`n========================================" -ForegroundColor Magenta
    Write-Host "QUERY 5: SKIPPED (Query 4 failed)" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Magenta
}

Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "ALL QUERIES COMPLETED" -ForegroundColor Magenta
Write-Host "========================================" -ForegroundColor Magenta
