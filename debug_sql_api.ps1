# Debug Fabric SQL Database API Endpoints
$ErrorActionPreference = "Continue"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$databaseName = "CAEManufacturing_SQLDB"

Write-Host "Getting Fabric API token..." -ForegroundColor Yellow
$token = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv).Trim()
Write-Host "Token acquired" -ForegroundColor Green

$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

# Get database item
Write-Host "Looking up database..." -ForegroundColor Yellow
$itemsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items?type=SQLDatabase"
$items = Invoke-RestMethod -Uri $itemsUrl -Headers $headers -Method Get
$dbItem = $items.value | Where-Object { $_.displayName -eq $databaseName }
$dbItemId = $dbItem.id
Write-Host "DB Item ID: $dbItemId`n" -ForegroundColor Green

# Query 1 SQL
$query1 = "SELECT @@VERSION AS sql_version, DB_NAME() AS db_name, SERVERPROPERTY('Edition') AS edition"
$body = @{ queries = @(@{ query = $query1 }) } | ConvertTo-Json -Depth 5

# Endpoint - using the sqldatabase API pattern
$endpoint = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/sqldatabases/$dbItemId/executeQueries"
Write-Host "Endpoint: $endpoint" -ForegroundColor Cyan
Write-Host "Query: $query1`n" -ForegroundColor Gray

try {
    $response = Invoke-WebRequest -Uri $endpoint -Headers $headers -Method Post -Body $body -UseBasicParsing
    Write-Host "SUCCESS (Status: $($response.StatusCode)):" -ForegroundColor Green
    $content = $response.Content | ConvertFrom-Json
    $content | ConvertTo-Json -Depth 10
} catch {
    $statusCode = $_.Exception.Response.StatusCode.Value__
    Write-Host "FAILED (Status: $statusCode):" -ForegroundColor Red
    
    try {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $errBody = $reader.ReadToEnd()
        Write-Host $errBody
    } catch {
        Write-Host $_.Exception.Message -ForegroundColor Red
    }
}
