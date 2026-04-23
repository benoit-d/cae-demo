# Fix SQL Database columns using Invoke-Sqlcmd
$ErrorActionPreference = "Stop"

$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$DatabaseName = "CAEManufacturing_SQLDB"
$ServerName = "$WorkspaceId.datawarehouse.fabric.microsoft.com"

Write-Host "=== SQL Database Fix (using Invoke-Sqlcmd) ===" -ForegroundColor Cyan
Write-Host "Server: $ServerName"
Write-Host "Database: $DatabaseName"

# Get token
Write-Host "`nGetting access token..." -ForegroundColor Yellow
$token = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv
if ($LASTEXITCODE -ne 0) { 
    Write-Error "Failed to get token"
    exit 1
}
Write-Host "Token acquired" -ForegroundColor Green

function Run-SqlQuery {
    param([string]$Query, [string]$Description, [switch]$NonQuery)
    
    Write-Host "`n--- $Description ---" -ForegroundColor Yellow
    Write-Host "Query: $Query" -ForegroundColor Gray
    
    try {
        if ($NonQuery) {
            Invoke-Sqlcmd -ServerInstance $ServerName -Database $DatabaseName -AccessToken $token -Query $Query -TrustServerCertificate
            Write-Host "Executed successfully" -ForegroundColor Green
        } else {
            $result = Invoke-Sqlcmd -ServerInstance $ServerName -Database $DatabaseName -AccessToken $token -Query $Query -TrustServerCertificate
            if ($result) {
                $result | Format-Table -AutoSize
            } else {
                Write-Host "No results (or executed successfully)" -ForegroundColor Green
            }
            return $result
        }
    } catch {
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
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
Run-SqlQuery -Query $checkQuery -Description "Check columns"

# Step 2: Drop computed columns
Write-Host "`n=== STEP 2: Drop computed columns if they exist ===" -ForegroundColor Cyan
Run-SqlQuery -Query "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') ALTER TABLE plm.tasks DROP COLUMN Calculated_End_Date" -Description "Drop Calculated_End_Date" -NonQuery
Run-SqlQuery -Query "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') ALTER TABLE plm.tasks DROP COLUMN Calculated_Start_Date" -Description "Drop Calculated_Start_Date" -NonQuery
Run-SqlQuery -Query "IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') ALTER TABLE plm.tasks DROP COLUMN Is_Milestone" -Description "Drop Is_Milestone" -NonQuery

# Step 3: Add regular columns
Write-Host "`n=== STEP 3: Add regular columns ===" -ForegroundColor Cyan
Run-SqlQuery -Query "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') ALTER TABLE plm.tasks ADD Calculated_Start_Date DATE" -Description "Add Calculated_Start_Date" -NonQuery
Run-SqlQuery -Query "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') ALTER TABLE plm.tasks ADD Calculated_End_Date DATE" -Description "Add Calculated_End_Date" -NonQuery
Run-SqlQuery -Query "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') ALTER TABLE plm.tasks ADD Is_Milestone BIT" -Description "Add Is_Milestone" -NonQuery

# Step 4: Populate
Write-Host "`n=== STEP 4: Populate columns ===" -ForegroundColor Cyan
$updateQuery = @"
UPDATE plm.tasks SET
    Calculated_Start_Date = COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start),
    Calculated_End_Date = COALESCE(Actual_End, DATEADD(day, ISNULL(Standard_Duration, 0), COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start))),
    Is_Milestone = CASE WHEN Milestone = 1 THEN 1 ELSE 0 END
"@
Run-SqlQuery -Query $updateQuery -Description "Populate columns" -NonQuery

# Step 5: Verify
Write-Host "`n=== STEP 5: Verify ===" -ForegroundColor Cyan
Run-SqlQuery -Query "SELECT TOP 5 Task_ID, Calculated_Start_Date, Calculated_End_Date, Is_Milestone, Milestone FROM plm.tasks ORDER BY Task_ID" -Description "Verify data"

# Final check
Write-Host "`n=== Final Column State ===" -ForegroundColor Cyan
Run-SqlQuery -Query $checkQuery -Description "Final column check"

Write-Host "`n=== SQL Database fix completed ===" -ForegroundColor Green
