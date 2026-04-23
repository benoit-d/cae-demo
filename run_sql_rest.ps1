# SQL Query Script for Fabric SQL Database using REST API
param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$DatabaseName = "CAEManufacturing_SQLDB"
)

$ErrorActionPreference = "Stop"

Write-Host "Connecting to Fabric SQL Database via REST API..." -ForegroundColor Yellow
Write-Host "Workspace: $WorkspaceId" -ForegroundColor Gray
Write-Host "Database: $DatabaseName" -ForegroundColor Gray

# Get Fabric access token
$token = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv).Trim()
Write-Host "Token acquired`n" -ForegroundColor Green

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

# Get database item ID
Write-Host "Looking up database item..." -ForegroundColor Yellow
$itemsUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items?type=SQLDatabase"
$items = Invoke-RestMethod -Uri $itemsUrl -Headers $headers -Method Get
$dbItem = $items.value | Where-Object { $_.displayName -eq $DatabaseName }

if (-not $dbItem) {
    Write-Host "Database not found: $DatabaseName" -ForegroundColor Red
    exit 1
}

$dbItemId = $dbItem.id
Write-Host "Database Item ID: $dbItemId`n" -ForegroundColor Green

# Function to execute SQL query via Fabric API
function Invoke-FabricSql {
    param(
        [string]$Query, 
        [string]$Description
    )
    
    Write-Host "=== $Description ===" -ForegroundColor Cyan
    Write-Host "Query: $Query" -ForegroundColor Gray
    
    # Use the SQL API endpoint for Fabric
    $sqlEndpoint = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/SqlDatabases/$dbItemId/executeQueries"
    
    $body = @{
        queries = @(
            @{
                query = $Query
            }
        )
    } | ConvertTo-Json -Depth 5
    
    try {
        $response = Invoke-RestMethod -Uri $sqlEndpoint -Headers $headers -Method Post -Body $body
        return $response
    } catch {
        $errBody = $_.ErrorDetails.Message
        Write-Host "Query failed: $errBody" -ForegroundColor Red
        
        # Try alternative endpoint
        $altEndpoint = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$dbItemId/executeQueries"
        try {
            $response = Invoke-RestMethod -Uri $altEndpoint -Headers $headers -Method Post -Body $body
            return $response
        } catch {
            Write-Host "Alt query failed: $($_.ErrorDetails.Message)" -ForegroundColor Red
            return $null
        }
    }
}

# Query 1: Check current milestone count
$q1 = Invoke-FabricSql -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -Description "Query 1: Current milestone count"
if ($q1) { $q1 | ConvertTo-Json -Depth 10 }

# Query 2: Update milestone task types
$q2 = Invoke-FabricSql -Query "UPDATE plm.tasks SET Milestone = 1 WHERE Task_ID LIKE '%-TSK-001' OR Task_ID LIKE '%-TSK-010' OR Task_ID LIKE '%-TSK-012'" -Description "Query 2: Update milestone task types"
if ($q2) { $q2 | ConvertTo-Json -Depth 10 }

# Query 3: Verify new count  
$q3 = Invoke-FabricSql -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -Description "Query 3: New milestone count"
if ($q3) { $q3 | ConvertTo-Json -Depth 10 }

# Query 4: Verify all 4 types
$q4 = Invoke-FabricSql -Query "SELECT Task_Name, COUNT(*) as cnt FROM plm.tasks WHERE Milestone = 1 GROUP BY Task_Name ORDER BY Task_Name" -Description "Query 4: Milestone tasks by type"
if ($q4) { $q4 | ConvertTo-Json -Depth 10 }

Write-Host "`nScript completed." -ForegroundColor Green
