# SQL Query Script for Fabric SQL Database using correct TDS endpoint
param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$DatabaseName = "CAEManufacturing_SQLDB"
)

$ErrorActionPreference = "Stop"

Write-Host "Connecting to Fabric SQL Database..." -ForegroundColor Yellow

# Get database access token (for SQL authentication)
$sqlToken = (az account get-access-token --resource https://database.windows.net --query accessToken -o tsv).Trim()

# Get Fabric API token to look up database metadata
$fabricToken = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv).Trim()

$headers = @{
    "Authorization" = "Bearer $fabricToken"
    "Content-Type" = "application/json"
}

# Get database item to find the server endpoint
Write-Host "Looking up SQL Database connection info..." -ForegroundColor Yellow
$itemsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=SQLDatabase"
$items = Invoke-RestMethod -Uri $itemsUrl -Headers $headers -Method Get
$dbItem = $items.value | Where-Object { $_.displayName -eq $DatabaseName }

if (-not $dbItem) {
    Write-Host "Database not found: $DatabaseName" -ForegroundColor Red
    exit 1
}

$dbItemId = $dbItem.id
Write-Host "Database Item ID: $dbItemId" -ForegroundColor Green

# The TDS endpoint for Fabric SQL Database
# Format: {server-name}.database.fabric.microsoft.com where server-name is from the item properties
$serverName = "$($dbItemId.Replace('-', '')).database.fabric.microsoft.com"
Write-Host "Trying server: $serverName`n" -ForegroundColor Gray

# Also try the workspace-based format
$altServerName = "$WorkspaceId.datawarehouse.fabric.microsoft.com"
Write-Host "Alt server: $altServerName`n" -ForegroundColor Gray

# Load SqlClient assembly
Add-Type -AssemblyName "System.Data"

# Function to execute SQL using ADO.NET with token
function Invoke-FabricSqlQuery {
    param(
        [string]$ServerName,
        [string]$Database,
        [string]$Query, 
        [string]$Token,
        [switch]$NonQuery
    )
    
    # Connection string for Azure SQL with token auth
    $connString = "Data Source=$ServerName;Initial Catalog=$Database;Connect Timeout=60;Encrypt=True;TrustServerCertificate=False"
    
    $conn = New-Object System.Data.SqlClient.SqlConnection($connString)
    $conn.AccessToken = $Token
    
    try {
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = 120
        
        if ($NonQuery) {
            $result = $cmd.ExecuteNonQuery()
            return @{ RowsAffected = $result }
        } else {
            $reader = $cmd.ExecuteReader()
            $table = New-Object System.Data.DataTable
            $table.Load($reader)
            return $table
        }
    } finally {
        if ($conn.State -eq 'Open') { $conn.Close() }
    }
}

# Try different server formats
$servers = @(
    "$WorkspaceId-$dbItemId.datawarehouse.fabric.microsoft.com",
    "$WorkspaceId.datawarehouse.fabric.microsoft.com",
    "$(($dbItemId).Replace('-','')).database.fabric.microsoft.com",
    "$($WorkspaceId.Replace('-',''))$($dbItemId.Substring(0,8)).datawarehouse.fabric.microsoft.com"
)

$connected = $false
foreach ($server in $servers) {
    Write-Host "Trying: $server" -ForegroundColor Yellow
    try {
        $testResult = Invoke-FabricSqlQuery -ServerName $server -Database $DatabaseName -Query "SELECT 1 as test" -Token $sqlToken
        Write-Host "Connected successfully to: $server" -ForegroundColor Green
        $connectedServer = $server
        $connected = $true
        break
    } catch {
        Write-Host "  Failed: $($_.Exception.Message)" -ForegroundColor Red
    }
}

if (-not $connected) {
    Write-Host "`nCould not connect to any server endpoint." -ForegroundColor Red
    Write-Host "Please check if the database exists and you have access." -ForegroundColor Yellow
    exit 1
}

Write-Host "`n=== Query 1: Current milestone count ===" -ForegroundColor Cyan
$q1 = Invoke-FabricSqlQuery -ServerName $connectedServer -Database $DatabaseName -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -Token $sqlToken
$q1 | Format-Table -AutoSize

Write-Host "`n=== Query 2: Updating milestone task types ===" -ForegroundColor Cyan
$q2 = Invoke-FabricSqlQuery -ServerName $connectedServer -Database $DatabaseName -Query "UPDATE plm.tasks SET Milestone = 1 WHERE Task_ID LIKE '%-TSK-001' OR Task_ID LIKE '%-TSK-010' OR Task_ID LIKE '%-TSK-012'" -Token $sqlToken -NonQuery
Write-Host "Rows affected: $($q2.RowsAffected)" -ForegroundColor Green

Write-Host "`n=== Query 3: New milestone count ===" -ForegroundColor Cyan
$q3 = Invoke-FabricSqlQuery -ServerName $connectedServer -Database $DatabaseName -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -Token $sqlToken
$q3 | Format-Table -AutoSize

Write-Host "`n=== Query 4: Milestone tasks by type ===" -ForegroundColor Cyan
$q4 = Invoke-FabricSqlQuery -ServerName $connectedServer -Database $DatabaseName -Query "SELECT Task_Name, COUNT(*) as cnt FROM plm.tasks WHERE Milestone = 1 GROUP BY Task_Name ORDER BY Task_Name" -Token $sqlToken
$q4 | Format-Table -AutoSize

Write-Host "`nAll queries completed." -ForegroundColor Green
