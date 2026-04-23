$ErrorActionPreference = "Stop"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$databaseName = "CAEManufacturing_SQLDB"
$serverName = "$workspaceId.datawarehouse.fabric.microsoft.com"

# Get fresh token
Write-Host "Getting access token..." -ForegroundColor Yellow
$token = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv
if ($LASTEXITCODE -ne 0) { 
    Write-Error "Failed to get token"
    exit 1
}
Write-Host "Token acquired successfully" -ForegroundColor Green

# Query 1: Check current milestone count
Write-Host "`n=== Query 1: Current milestone count ===" -ForegroundColor Cyan
$q1 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -TrustServerCertificate
$q1 | Format-Table -AutoSize

# Query 2: Update the 3 additional milestone task types
Write-Host "`n=== Query 2: Updating milestone task types ===" -ForegroundColor Cyan
$updateQuery = @"
UPDATE plm.tasks SET Milestone = 1 
WHERE Task_ID LIKE '%-TSK-001' OR Task_ID LIKE '%-TSK-010' OR Task_ID LIKE '%-TSK-012'
"@
Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query $updateQuery -TrustServerCertificate
Write-Host "Update completed" -ForegroundColor Green

# Query 3: Verify new count
Write-Host "`n=== Query 3: New milestone count ===" -ForegroundColor Cyan
$q3 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -TrustServerCertificate
$q3 | Format-Table -AutoSize

# Query 4: Verify all 4 types
Write-Host "`n=== Query 4: Milestone tasks by type ===" -ForegroundColor Cyan
$q4 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -Query "SELECT Task_Name, COUNT(*) as cnt FROM plm.tasks WHERE Milestone = 1 GROUP BY Task_Name ORDER BY Task_Name" -TrustServerCertificate
$q4 | Format-Table -AutoSize

Write-Host "`nAll queries completed successfully!" -ForegroundColor Green
