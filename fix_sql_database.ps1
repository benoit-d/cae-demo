# Fix SQL Database columns
$ErrorActionPreference = "Stop"

$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$DatabaseName = "CAEManufacturing_SQLDB"

Write-Host "=== PART 1: SQL Database Fixes ===" -ForegroundColor Cyan
Write-Host "Workspace: $WorkspaceId"
Write-Host "Database: $DatabaseName"

# Get token
$token = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv).Trim()
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

# Get database item ID
$itemsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=SQLDatabase"
$items = Invoke-RestMethod -Uri $itemsUrl -Headers $headers -Method Get
$dbItem = $items.value | Where-Object { $_.displayName -eq $DatabaseName }

if (-not $dbItem) {
    Write-Host "ERROR: Database not found: $DatabaseName" -ForegroundColor Red
    exit 1
}

$dbItemId = $dbItem.id
Write-Host "Database Item ID: $dbItemId" -ForegroundColor Green

$sqlEndpoint = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/SqlDatabases/$dbItemId/executeQueries"

# Helper function
function Invoke-SqlQuery {
    param([string]$Query, [string]$Description)
    
    Write-Host "`n--- $Description ---" -ForegroundColor Yellow
    Write-Host "Query: $Query" -ForegroundColor Gray
    
    $body = @{ queries = @(@{ query = $Query }) } | ConvertTo-Json -Depth 5
    
    try {
        $response = Invoke-RestMethod -Uri $sqlEndpoint -Headers $headers -Method Post -Body $body
        if ($response.results -and $response.results[0].tabularData) {
            Write-Host "Result:"
            $response.results[0].tabularData | Format-Table -AutoSize
            return $response.results[0].tabularData
        } elseif ($response.results -and $response.results[0].rowCount -ne $null) {
            Write-Host "Rows affected: $($response.results[0].rowCount)" -ForegroundColor Green
            return @{ rowCount = $response.results[0].rowCount }
        } else {
            Write-Host "Query executed successfully (no data returned)" -ForegroundColor Green
            return $null
        }
    } catch {
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

# Step 1: Check current state
Write-Host "`n=== STEP 1: Check current column state ===" -ForegroundColor Cyan
$checkQuery = @"
SELECT name, type_name(user_type_id) as type_name, is_computed 
FROM sys.columns 
WHERE object_id = OBJECT_ID('plm.tasks') 
AND name IN ('Calculated_Start_Date', 'Calculated_End_Date', 'Is_Milestone')
"@
$currentState = Invoke-SqlQuery -Query $checkQuery -Description "Current column state"

# Step 2: Drop computed columns if they exist
Write-Host "`n=== STEP 2: Drop computed columns if they exist ===" -ForegroundColor Cyan

$dropQueries = @(
    "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') ALTER TABLE plm.tasks DROP COLUMN Calculated_End_Date",
    "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') ALTER TABLE plm.tasks DROP COLUMN Calculated_Start_Date",
    "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') ALTER TABLE plm.tasks DROP COLUMN Is_Milestone"
)

foreach ($q in $dropQueries) {
    Invoke-SqlQuery -Query $q -Description "Drop computed column"
}

# Step 3: Add as regular columns
Write-Host "`n=== STEP 3: Add regular columns ===" -ForegroundColor Cyan

$addQueries = @(
    "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') ALTER TABLE plm.tasks ADD Calculated_Start_Date DATE",
    "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') ALTER TABLE plm.tasks ADD Calculated_End_Date DATE",
    "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') ALTER TABLE plm.tasks ADD Is_Milestone BIT"
)

foreach ($q in $addQueries) {
    Invoke-SqlQuery -Query $q -Description "Add column"
}

# Step 4: Populate columns
Write-Host "`n=== STEP 4: Populate columns ===" -ForegroundColor Cyan
$updateQuery = @"
UPDATE plm.tasks SET
    Calculated_Start_Date = COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start),
    Calculated_End_Date = COALESCE(Actual_End, DATEADD(day, ISNULL(Standard_Duration, 0), COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start))),
    Is_Milestone = CASE WHEN Milestone = 1 THEN 1 ELSE 0 END
"@
Invoke-SqlQuery -Query $updateQuery -Description "Populate columns"

# Step 5: Verify
Write-Host "`n=== STEP 5: Verify ===" -ForegroundColor Cyan
$verifyQuery = "SELECT TOP 5 Task_ID, Calculated_Start_Date, Calculated_End_Date, Is_Milestone, Milestone FROM plm.tasks ORDER BY Task_ID"
$verifyResult = Invoke-SqlQuery -Query $verifyQuery -Description "Verify columns"

# Final state check
Write-Host "`n=== Final column state ===" -ForegroundColor Cyan
$finalState = Invoke-SqlQuery -Query $checkQuery -Description "Final column check"

Write-Host "`n=== SQL Database fixes completed ===" -ForegroundColor Green
