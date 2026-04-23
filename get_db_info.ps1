# Get SQL Database connection details
$ErrorActionPreference = "Continue"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$dbItemId = "6c31cad3-74a3-4eae-91f3-e2a4ed845e7e"
$token = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv).Trim()
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

# Get DB item details
Write-Host "Getting item details..."
$itemUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$dbItemId"
$item = Invoke-RestMethod -Uri $itemUrl -Headers $headers
$item | ConvertTo-Json -Depth 5

# Get SQL Database properties
Write-Host "`nGetting SQL Database properties..."
$sqlDbUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/sqldatabases/$dbItemId"
try {
    $sqlDb = Invoke-RestMethod -Uri $sqlDbUrl -Headers $headers
    $sqlDb | ConvertTo-Json -Depth 5
} catch {
    Write-Host "SQLDatabase endpoint error: $($_.Exception.Message)"
}

# Now try TDS with correct token
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Trying TDS connection..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Use SQL token
$sqlToken = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv

# Server is workspace-specific
$server = "$workspaceId.database.fabric.microsoft.com"
$database = "CAEManufacturing_SQLDB"

Write-Host "Server: $server"
Write-Host "Database: $database"

# Try simple query
Write-Host "`nQuery 1: SQL Version..."
try {
    $result = Invoke-Sqlcmd -ServerInstance $server -Database $database -AccessToken $sqlToken -TrustServerCertificate -Query "SELECT @@VERSION AS sql_version, DB_NAME() AS db_name, SERVERPROPERTY('Edition') AS edition" -ErrorAction Stop
    Write-Host "SUCCESS:" -ForegroundColor Green
    $result | Format-List
} catch {
    Write-Host "ERROR: $($_.Exception.Message)" -ForegroundColor Red
}
