# Fix SQL Database columns for CAEManufacturing
$ErrorActionPreference = "Stop"

$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$DatabaseName = "CAEManufacturing_SQLDB"

Write-Host "=== SQL Database Fix Script ===" -ForegroundColor Cyan
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
$dbItemId = $dbItem.id
Write-Host "Database Item ID: $dbItemId" -ForegroundColor Green

$sqlEndpoint = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/sqlDatabases/$dbItemId/executeQueries"

function Invoke-SqlQuery {
    param([string]$Query, [string]$Description)
    
    Write-Host "`n--- $Description ---" -ForegroundColor Yellow
    Write-Host "Query: $Query" -ForegroundColor Gray
    
    $body = @{ queries = @(@{ query = $Query }) } | ConvertTo-Json -Depth 5
    
    # Make the request
    $response = Invoke-WebRequest -Uri $sqlEndpoint -Headers $headers -Method Post -Body $body -UseBasicParsing
    
    # Check if it's an LRO (202)
    if ($response.StatusCode -eq 202) {
        $lro = $response.Headers["Location"]
        if ($lro -is [array]) { $lro = $lro[0] }
        Write-Host "Waiting for result..." -ForegroundColor Gray
        Start-Sleep -Seconds 5
        
        $resultResponse = Invoke-WebRequest -Uri "$lro/result" -Headers $headers -Method Get -UseBasicParsing
        $result = $resultResponse.Content | ConvertFrom-Json
        
        if ($result.results -and $result.results[0].tabularData) {
            Write-Host "Result:" -ForegroundColor Green
            $result.results[0].tabularData | Format-Table -AutoSize
            return $result.results[0].tabularData
        } elseif ($result.results -and $result.results[0].rowCount -ne $null) {
            Write-Host "Rows affected: $($result.results[0].rowCount)" -ForegroundColor Green
            return @{ rowCount = $result.results[0].rowCount }
        } else {
            Write-Host "Query executed successfully" -ForegroundColor Green
            return $null
        }
    } else {
        $result = $response.Content | ConvertFrom-Json
        if ($result.results -and $result.results[0].tabularData) {
            Write-Host "Result:" -ForegroundColor Green
            $result.results[0].tabularData | Format-Table -AutoSize
            return $result.results[0].tabularData
        }
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
Invoke-SqlQuery -Query $checkQuery -Description "Check columns"

# Step 2: Drop computed columns if they exist
Write-Host "`n=== STEP 2: Drop computed columns ===" -ForegroundColor Cyan
Invoke-SqlQuery -Query "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') ALTER TABLE plm.tasks DROP COLUMN Calculated_End_Date" -Description "Drop Calculated_End_Date"
Invoke-SqlQuery -Query "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') ALTER TABLE plm.tasks DROP COLUMN Calculated_Start_Date" -Description "Drop Calculated_Start_Date"
Invoke-SqlQuery -Query "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') ALTER TABLE plm.tasks DROP COLUMN Is_Milestone" -Description "Drop Is_Milestone"

# Step 3: Add regular columns
Write-Host "`n=== STEP 3: Add regular columns ===" -ForegroundColor Cyan
Invoke-SqlQuery -Query "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') ALTER TABLE plm.tasks ADD Calculated_Start_Date DATE" -Description "Add Calculated_Start_Date"
Invoke-SqlQuery -Query "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') ALTER TABLE plm.tasks ADD Calculated_End_Date DATE" -Description "Add Calculated_End_Date"
Invoke-SqlQuery -Query "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') ALTER TABLE plm.tasks ADD Is_Milestone BIT" -Description "Add Is_Milestone"

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
Invoke-SqlQuery -Query "SELECT TOP 5 Task_ID, Calculated_Start_Date, Calculated_End_Date, Is_Milestone, Milestone FROM plm.tasks ORDER BY Task_ID" -Description "Verify data"

# Final check
Write-Host "`n=== Final Column State ===" -ForegroundColor Cyan
Invoke-SqlQuery -Query $checkQuery -Description "Final column check"

Write-Host "`n=== SQL Database fix completed ===" -ForegroundColor Green
