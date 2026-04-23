# SQL Query Script for Fabric SQL Database using .NET SqlClient
# Run with: powershell -File run_milestone_queries.ps1

param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$DatabaseName = "CAEManufacturing_SQLDB"
)

$ErrorActionPreference = "Continue"
$serverName = "$WorkspaceId.datawarehouse.fabric.microsoft.com"

Write-Host "Connecting to Fabric SQL Database..." -ForegroundColor Yellow
Write-Host "Server: $serverName" -ForegroundColor Gray
Write-Host "Database: $DatabaseName" -ForegroundColor Gray

# Get access token
try {
    $tokenOutput = az account get-access-token --resource https://database.windows.net 2>&1
    $tokenJson = $tokenOutput | ConvertFrom-Json
    $token = $tokenJson.accessToken
    Write-Host "Token acquired successfully`n" -ForegroundColor Green
} catch {
    Write-Host "Failed to get token: $_" -ForegroundColor Red
    exit 1
}

# Function to run SQL query using SqlClient
function Invoke-FabricSql {
    param(
        [string]$Query, 
        [string]$Description,
        [switch]$NonQuery
    )
    
    Write-Host "=== $Description ===" -ForegroundColor Cyan
    
    $connString = "Server=tcp:$serverName,1433;Initial Catalog=$DatabaseName;Encrypt=True;TrustServerCertificate=True;Connection Timeout=30"
    $conn = New-Object Microsoft.Data.SqlClient.SqlConnection($connString)
    $conn.AccessToken = $token
    
    try {
        $conn.Open()
        $cmd = New-Object Microsoft.Data.SqlClient.SqlCommand($Query, $conn)
        $cmd.CommandTimeout = 120
        
        if ($NonQuery) {
            $rowsAffected = $cmd.ExecuteNonQuery()
            Write-Host "Rows affected: $rowsAffected" -ForegroundColor Green
            return $rowsAffected
        } else {
            $adapter = New-Object Microsoft.Data.SqlClient.SqlDataAdapter($cmd)
            $dataset = New-Object System.Data.DataSet
            [void]$adapter.Fill($dataset)
            return $dataset.Tables[0]
        }
    } catch {
        Write-Host "Query failed: $_" -ForegroundColor Red
        return $null
    } finally {
        if ($conn.State -eq 'Open') { $conn.Close() }
    }
}

# Load Microsoft.Data.SqlClient
try {
    Add-Type -Path "$env:USERPROFILE\.nuget\packages\microsoft.data.sqlclient\5.2.2\lib\net8.0\Microsoft.Data.SqlClient.dll" -ErrorAction SilentlyContinue
} catch {
    # Try loading from SqlServer module path or fall back to System.Data.SqlClient
    try {
        [void][System.Reflection.Assembly]::LoadWithPartialName("System.Data.SqlClient")
        
        # Redefine function using System.Data.SqlClient
        function Invoke-FabricSql {
            param(
                [string]$Query, 
                [string]$Description,
                [switch]$NonQuery
            )
            
            Write-Host "=== $Description ===" -ForegroundColor Cyan
            
            $connString = "Server=tcp:$serverName,1433;Initial Catalog=$DatabaseName;Encrypt=True;TrustServerCertificate=True;Connection Timeout=30"
            $conn = New-Object System.Data.SqlClient.SqlConnection($connString)
            $conn.AccessToken = $token
            
            try {
                $conn.Open()
                $cmd = New-Object System.Data.SqlClient.SqlCommand($Query, $conn)
                $cmd.CommandTimeout = 120
                
                if ($NonQuery) {
                    $rowsAffected = $cmd.ExecuteNonQuery()
                    Write-Host "Rows affected: $rowsAffected" -ForegroundColor Green
                    return $rowsAffected
                } else {
                    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
                    $dataset = New-Object System.Data.DataSet
                    [void]$adapter.Fill($dataset)
                    return $dataset.Tables[0]
                }
            } catch {
                Write-Host "Query failed: $_" -ForegroundColor Red
                return $null
            } finally {
                if ($conn.State -eq 'Open') { $conn.Close() }
            }
        }
    } catch {
        Write-Host "Could not load SQL client libraries" -ForegroundColor Red
    }
}

# Query 1: Check current milestone count
$q1 = Invoke-FabricSql -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -Description "Query 1: Current milestone count"
if ($q1) { $q1 | Format-Table -AutoSize }

# Query 2: Update milestone task types
$updateQuery = "UPDATE plm.tasks SET Milestone = 1 WHERE Task_ID LIKE '%-TSK-001' OR Task_ID LIKE '%-TSK-010' OR Task_ID LIKE '%-TSK-012'"
Invoke-FabricSql -Query $updateQuery -Description "Query 2: Updating milestone task types" -NonQuery

# Query 3: Verify new count
$q3 = Invoke-FabricSql -Query "SELECT COUNT(*) as milestone_count FROM plm.tasks WHERE Milestone = 1" -Description "Query 3: New milestone count"
if ($q3) { $q3 | Format-Table -AutoSize }

# Query 4: Verify all 4 types
$q4 = Invoke-FabricSql -Query "SELECT Task_Name, COUNT(*) as cnt FROM plm.tasks WHERE Milestone = 1 GROUP BY Task_Name ORDER BY Task_Name" -Description "Query 4: Milestone tasks by type"
if ($q4) { $q4 | Format-Table -AutoSize }

Write-Host "`nScript completed." -ForegroundColor Green
