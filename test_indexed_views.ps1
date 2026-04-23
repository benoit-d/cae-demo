# Test Indexed/Materialized Views in Fabric SQL Database
$ErrorActionPreference = "Continue"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$databaseName = "CAEManufacturing_SQLDB"
$serverName = "$workspaceId.datawarehouse.fabric.microsoft.com"

Write-Host "Getting access token..." -ForegroundColor Yellow
$token = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv
if ($LASTEXITCODE -ne 0) { 
    Write-Error "Failed to get token"
    exit 1
}
Write-Host "Token acquired`n" -ForegroundColor Green

# Query 1: Check SQL Database edition
Write-Host "========================================" -ForegroundColor Magenta
Write-Host "QUERY 1: Check SQL Database edition" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
try {
    $q1 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -TrustServerCertificate -Query "SELECT @@VERSION AS sql_version, DB_NAME() AS db_name, SERVERPROPERTY('Edition') AS edition"
    Write-Host "RESULT:" -ForegroundColor Green
    $q1 | Format-List
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 2: Create simple view
Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "QUERY 2: Create simple view plm.v_tasks_gantt" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
$viewQuery = @"
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
try {
    Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -TrustServerCertificate -Query $viewQuery
    Write-Host "RESULT: SUCCESS - View plm.v_tasks_gantt created" -ForegroundColor Green
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 3: Test the view
Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "QUERY 3: Test the view" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
try {
    $q3 = Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -TrustServerCertificate -Query "SELECT TOP 5 Task_Name, Calculated_Start_Date, Calculated_End_Date, Project_Name FROM plm.v_tasks_gantt ORDER BY Task_ID"
    Write-Host "RESULT:" -ForegroundColor Green
    $q3 | Format-Table -AutoSize
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 4: Try indexed view with SCHEMABINDING
Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "QUERY 4: Create indexed view with SCHEMABINDING" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
$matViewQuery = @"
CREATE OR ALTER VIEW plm.v_tasks_gantt_mat WITH SCHEMABINDING AS
SELECT 
    t.Task_ID, t.Task_Name, t.Parent_Project_ID, t.Task_Type,
    t.Milestone, t.Skill_Requirement, t.Standard_Duration,
    t.Resource_Login, t.Complete_Percentage, t.Machine_ID,
    COALESCE(t.Actual_Start, t.Modified_Planned_Start, t.Initial_Planned_Start) AS Calculated_Start_Date,
    COALESCE(t.Actual_End, DATEADD(day, ISNULL(t.Standard_Duration, 0), COALESCE(t.Actual_Start, t.Modified_Planned_Start, t.Initial_Planned_Start))) AS Calculated_End_Date
FROM plm.tasks t
"@
try {
    Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -TrustServerCertificate -Query $matViewQuery
    Write-Host "RESULT: SUCCESS - View plm.v_tasks_gantt_mat created with SCHEMABINDING" -ForegroundColor Green
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 5: Create clustered index to materialize
Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "QUERY 5: Create clustered index to materialize the view" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
try {
    Invoke-Sqlcmd -ServerInstance $serverName -Database $databaseName -AccessToken $token -TrustServerCertificate -Query "CREATE UNIQUE CLUSTERED INDEX IX_v_tasks_gantt_mat ON plm.v_tasks_gantt_mat (Task_ID)"
    Write-Host "RESULT: SUCCESS - Clustered index IX_v_tasks_gantt_mat created - view is now materialized!" -ForegroundColor Green
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "ALL QUERIES COMPLETED" -ForegroundColor Magenta
Write-Host "========================================" -ForegroundColor Magenta
