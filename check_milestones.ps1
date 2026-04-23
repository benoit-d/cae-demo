# SQL Query Script for Task 1
$ErrorActionPreference = "Continue"
Write-Host "=== TASK 1: SQL Query for Milestones ===" -ForegroundColor Cyan

try {
    Write-Host "Getting access token..."
    $token = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv
    
    if (-not $token) {
        throw "Failed to get access token"
    }
    Write-Host "Token obtained successfully"
    
    $server = "x6eps4xrq2xudenlfcykddpgry-t6qmqwlnb25ernnilbqhxqxqbe.datawarehouse.fabric.microsoft.com"
    $database = "CAEManufacturing_LH"
    $query = "SELECT Task_ID, Task_Name, Milestone FROM plm.tasks WHERE Milestone = 1 ORDER BY Task_ID"
    
    Write-Host "Connecting to: $server"
    Write-Host "Database: $database"
    Write-Host "Query: $query"
    Write-Host ""
    
    $results = Invoke-Sqlcmd -ServerInstance $server -Database $database -AccessToken $token -Query $query -TrustServerCertificate -ConnectionTimeout 120 -QueryTimeout 120
    
    Write-Host "=== Query Results ===" -ForegroundColor Green
    $results | Format-Table Task_ID, Task_Name, Milestone -AutoSize
    
    $count = ($results | Measure-Object).Count
    Write-Host ""
    Write-Host "Total milestone count: $count" -ForegroundColor Yellow
    
    if ($count -eq 36) {
        Write-Host "✓ EXPECTED: 36 milestones (4 per project × 9 projects)" -ForegroundColor Green
    } elseif ($count -eq 9) {
        Write-Host "✗ Data NOT reloaded yet - still shows 9 milestones (1 per project)" -ForegroundColor Red
    } else {
        Write-Host "? Unexpected count: $count" -ForegroundColor Yellow
    }
    
    # Save results
    $results | Export-Csv -Path "milestone_results.csv" -NoTypeInformation
    Write-Host "Results saved to milestone_results.csv"
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
}
