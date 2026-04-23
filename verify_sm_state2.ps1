$ErrorActionPreference = "Stop"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

$env:AZURE_CORE_COLLECT_TELEMETRY = "false"
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv

$output = @()
$output += "=== STEP 1: Getting Semantic Model Definition ==="

$defUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$itemId/getDefinition"
$headers = @{ "Authorization" = "Bearer $fabricToken" }

$response = Invoke-WebRequest -Uri $defUrl -Method POST -Headers $headers -ContentType "application/json"
$output += "Initial response status: $($response.StatusCode)"

if ($response.StatusCode -eq 202) {
    $location = $response.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    $output += "Polling location: $location"
    
    $maxAttempts = 30
    $attempt = 0
    do {
        Start-Sleep -Seconds 2
        $attempt++
        $pollResponse = Invoke-WebRequest -Uri $location -Headers $headers
        $output += "Poll attempt $attempt - status: $($pollResponse.StatusCode)"
    } while ($pollResponse.StatusCode -eq 202 -and $attempt -lt $maxAttempts)
    
    $definition = $pollResponse.Content | ConvertFrom-Json
} else {
    $definition = $response.Content | ConvertFrom-Json
}

$output += ""
$output += "=== DEFINITION STRUCTURE ==="
$output += "Parts count: $($definition.definition.parts.Count)"
$output += ""
$output += "All parts:"
foreach ($part in $definition.definition.parts) {
    $output += "  - $($part.path)"
}

$output += ""
$output += "=== STEP 2: Looking for tasks.tmdl ==="

# Try different possible paths
$searchPaths = @(
    "definition/tables/tasks.tmdl",
    "tables/tasks.tmdl", 
    "tasks.tmdl"
)

$tasksTmdl = $null
foreach ($path in $searchPaths) {
    $found = $definition.definition.parts | Where-Object { $_.path -eq $path }
    if ($found) {
        $tasksTmdl = $found
        $output += "Found at path: $path"
        break
    }
}

# Also try partial match
if (-not $tasksTmdl) {
    $tasksTmdl = $definition.definition.parts | Where-Object { $_.path -like "*tasks*" }
    if ($tasksTmdl) {
        $output += "Found partial match: $($tasksTmdl.path)"
    }
}

if ($tasksTmdl) {
    $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksTmdl.payload))
    $output += ""
    $output += "--- FULL tasks.tmdl Content ---"
    $output += $decoded
    $output += "--- End of tasks.tmdl ---"
    
    $output += ""
    $output += "=== Analysis ==="
    $output += "Calculated_Start_Date column: $(if ($decoded -match 'Calculated_Start_Date') { 'FOUND' } else { 'NOT FOUND' })"
    $output += "Calculated_End_Date column: $(if ($decoded -match 'Calculated_End_Date') { 'FOUND' } else { 'NOT FOUND' })"
    $output += "'Calculated Start Date' measure: $(if ($decoded -match "measure 'Calculated Start Date'") { 'FOUND' } else { 'NOT FOUND' })"
    $output += "'Calculated End Date' measure: $(if ($decoded -match "measure 'Calculated End Date'") { 'FOUND' } else { 'NOT FOUND' })"
    $output += "'Is Milestone' measure: $(if ($decoded -match "measure 'Is Milestone'") { 'FOUND' } else { 'NOT FOUND' })"
} else {
    $output += "tasks.tmdl NOT FOUND!"
}

$output += ""
$output += "=== STEP 3: Executing DAX Query ==="

$daxUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$itemId/executeQueries"
$pbiHeaders = @{ "Authorization" = "Bearer $pbiToken"; "Content-Type" = "application/json" }

$daxBody = @{
    queries = @(
        @{
            query = "EVALUATE TOPN(5, ADDCOLUMNS(tasks, `"CalcStart`", [Calculated Start Date], `"CalcEnd`", [Calculated End Date], `"IsMilestone`", [Is Milestone]), tasks[Task_ID], ASC)"
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $daxResponse = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $daxBody
    $output += "DAX Query SUCCESS!"
    $output += ($daxResponse | ConvertTo-Json -Depth 10)
} catch {
    $output += "DAX Query ERROR: $($_.Exception.Message)"
    if ($_.ErrorDetails) {
        $output += "Error Details: $($_.ErrorDetails.Message)"
    }
}

# Write all output to file
$output | Out-File -FilePath "c:\Repo\cae-demo\sm_verify_result.txt" -Encoding utf8
Write-Host "Results written to sm_verify_result.txt"
$output | ForEach-Object { Write-Host $_ }
