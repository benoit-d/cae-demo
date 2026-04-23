# Test Indexed Views with CORRECT Fabric SQL Database connection
$ErrorActionPreference = "Continue"

# Correct connection details from API
$serverFqdn = "glhdjewslwruzpuscihr6nmsre-urbryfqunkhuxapla4hqtangbe.database.fabric.microsoft.com,1433"
$databaseName = "CAEManufacturing_SQLDB-6c31cad3-74a3-4eae-91f3-e2a4ed845e7e"

Write-Host "Getting SQL token..." -ForegroundColor Yellow
$token = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv
Write-Host "Token acquired" -ForegroundColor Green

Write-Host "Server: $serverFqdn"
Write-Host "Database: $databaseName`n"

# Query 1: Check SQL Database edition
Write-Host "========================================" -ForegroundColor Magenta
Write-Host "QUERY 1: Check SQL Database edition" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
try {
    $q1 = Invoke-Sqlcmd -ServerInstance $serverFqdn -Database $databaseName -AccessToken $token -TrustServerCertificate -Query "SELECT @@VERSION AS sql_version, DB_NAME() AS db_name, SERVERPROPERTY('Edition') AS edition" -ErrorAction Stop
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
    Invoke-Sqlcmd -ServerInstance $serverFqdn -Database $databaseName -AccessToken $token -TrustServerCertificate -Query $viewQuery -ErrorAction Stop
    Write-Host "RESULT: SUCCESS - View plm.v_tasks_gantt created" -ForegroundColor Green
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 3: Test the view
Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "QUERY 3: Test the view" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
try {
    $q3 = Invoke-Sqlcmd -ServerInstance $serverFqdn -Database $databaseName -AccessToken $token -TrustServerCertificate -Query "SELECT TOP 5 Task_Name, Calculated_Start_Date, Calculated_End_Date, Project_Name FROM plm.v_tasks_gantt ORDER BY Task_ID" -ErrorAction Stop
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
$q4Success = $false
try {
    Invoke-Sqlcmd -ServerInstance $serverFqdn -Database $databaseName -AccessToken $token -TrustServerCertificate -Query $matViewQuery -ErrorAction Stop
    Write-Host "RESULT: SUCCESS - View plm.v_tasks_gantt_mat created with SCHEMABINDING" -ForegroundColor Green
    $q4Success = $true
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# Query 5: Create clustered index to materialize (only if Q4 succeeded)
Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "QUERY 5: Create clustered index to materialize the view" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Magenta
if ($q4Success) {
    try {
        Invoke-Sqlcmd -ServerInstance $serverFqdn -Database $databaseName -AccessToken $token -TrustServerCertificate -Query "CREATE UNIQUE CLUSTERED INDEX IX_v_tasks_gantt_mat ON plm.v_tasks_gantt_mat (Task_ID)" -ErrorAction Stop
        Write-Host "RESULT: SUCCESS - Clustered index IX_v_tasks_gantt_mat created" -ForegroundColor Green
    } catch {
        Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
    }
} else {
    Write-Host "SKIPPED: Query 4 failed, cannot create index" -ForegroundColor Yellow
}

Write-Host "`n========================================" -ForegroundColor Magenta
Write-Host "ALL QUERIES COMPLETED" -ForegroundColor Magenta
Write-Host "========================================" -ForegroundColor Magenta
