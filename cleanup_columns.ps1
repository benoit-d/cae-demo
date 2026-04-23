# SQL cleanup script for computed columns and views
$ErrorActionPreference = "Stop"
# Correct connection details from API
$serverName = "glhdjewslwruzpuscihr6nmsre-urbryfqunkhuxapla4hqtangbe.database.fabric.microsoft.com,1433"
$databaseName = "CAEManufacturing_SQLDB-6c31cad3-74a3-4eae-91f3-e2a4ed845e7e"

Write-Host "Getting access token..." -ForegroundColor Yellow
$token = (az account get-access-token --resource https://database.windows.net --query accessToken -o tsv).Trim()
Write-Host "Token acquired" -ForegroundColor Green

# Query 1: Check if columns exist
Write-Host "`n=== Query 1: Check if computed columns exist ===" -ForegroundColor Cyan
$q1 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "SELECT name, is_computed, is_persisted FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name IN ('Calculated_Start_Date', 'Calculated_End_Date')" -TrustServerCertificate
if ($q1) { 
    $q1 | Format-Table -AutoSize
    $columnsExist = $true
} else { 
    Write-Host "No computed columns found" -ForegroundColor Yellow 
    $columnsExist = $false
}

# Query 2: Drop columns if they exist
if ($columnsExist) {
    Write-Host "`n=== Query 2: Drop Calculated_End_Date ===" -ForegroundColor Cyan
    try {
        Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "ALTER TABLE plm.tasks DROP COLUMN Calculated_End_Date" -TrustServerCertificate
        Write-Host "Dropped Calculated_End_Date" -ForegroundColor Green
    } catch {
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    }

    Write-Host "`n=== Query 3: Drop Calculated_Start_Date ===" -ForegroundColor Cyan
    try {
        Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "ALTER TABLE plm.tasks DROP COLUMN Calculated_Start_Date" -TrustServerCertificate
        Write-Host "Dropped Calculated_Start_Date" -ForegroundColor Green
    } catch {
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Query 4: Drop indexed view if exists
Write-Host "`n=== Query 4: Drop indexed view plm.v_tasks_gantt_mat ===" -ForegroundColor Cyan
try {
    Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "IF OBJECT_ID('plm.v_tasks_gantt_mat', 'V') IS NOT NULL DROP VIEW plm.v_tasks_gantt_mat" -TrustServerCertificate
    Write-Host "Drop indexed view completed" -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 5: Drop regular view if exists
Write-Host "`n=== Query 5: Drop regular view plm.v_tasks_gantt ===" -ForegroundColor Cyan
try {
    Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "IF OBJECT_ID('plm.v_tasks_gantt', 'V') IS NOT NULL DROP VIEW plm.v_tasks_gantt" -TrustServerCertificate
    Write-Host "Drop regular view completed" -ForegroundColor Green
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 6: Verify columns are gone
Write-Host "`n=== Query 6: Verify - List all columns in plm.tasks ===" -ForegroundColor Cyan
$q6 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') ORDER BY column_id" -TrustServerCertificate
$q6 | Format-Table -AutoSize

Write-Host "`n=== All queries completed ===" -ForegroundColor Green
