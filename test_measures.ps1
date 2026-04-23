$ErrorActionPreference = "Continue"
$env:AZURE_CORE_COLLECT_TELEMETRY = "false"

$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"
$daxUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$itemId/executeQueries"
$headers = @{ "Authorization" = "Bearer $pbiToken"; "Content-Type" = "application/json" }

$output = @()
$output += "=== Semantic Model Measure Verification ==="
$output += "Date: $(Get-Date)"
$output += ""

# Query available measures using DMV
$output += "=== Available Measures (via DMV) ==="
$dmvQuery = @{
    queries = @(
        @{
            query = "SELECT [MEASURE_UNIQUE_NAME], [MEASURE_NAME], [EXPRESSION] FROM `$SYSTEM.MDSCHEMA_MEASURES WHERE [CUBE_NAME] = 'Model'"
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $response = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $headers -Body $dmvQuery
    $output += "SUCCESS!"
    $output += ($response | ConvertTo-Json -Depth 10)
} catch {
    $output += "DMV Query failed: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) {
        $output += $_.ErrorDetails.Message
    }
}

$output += ""
$output += "=== Testing Individual Measures ==="

# Test Calculated Start Date
$output += "`n--- Calculated Start Date ---"
$testQuery1 = @{
    queries = @(
        @{
            query = "EVALUATE ROW(`"Value`", CALCULATE([Calculated Start Date], TOPN(1, tasks)))"
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $r = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $headers -Body $testQuery1
    $output += "SUCCESS: $($r | ConvertTo-Json -Depth 5)"
} catch {
    $output += "FAILED: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) { $output += $_.ErrorDetails.Message }
}

# Test Is Milestone
$output += "`n--- Is Milestone ---"
$testQuery2 = @{
    queries = @(
        @{
            query = "EVALUATE ROW(`"Value`", CALCULATE([Is Milestone], TOPN(1, tasks)))"
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $r = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $headers -Body $testQuery2
    $output += "SUCCESS: $($r | ConvertTo-Json -Depth 5)"
} catch {
    $output += "FAILED: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) { $output += $_.ErrorDetails.Message }
}

$output += ""
$output += "=== Full Query Test ==="
$fullQuery = @{
    queries = @(
        @{
            query = "EVALUATE TOPN(5, ADDCOLUMNS(tasks, `"CalcStart`", [Calculated Start Date], `"CalcEnd`", [Calculated End Date], `"IsMilestone`", [Is Milestone]), tasks[Task_ID], ASC)"
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $r = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $headers -Body $fullQuery
    $output += "SUCCESS!"
    $output += ($r | ConvertTo-Json -Depth 10)
} catch {
    $output += "FAILED: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) { $output += $_.ErrorDetails.Message }
}

# Write output to file
$output | Out-File "c:\Repo\cae-demo\measure_test_results.txt" -Encoding utf8
Write-Host "Results written to measure_test_results.txt"
