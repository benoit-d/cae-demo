$ErrorActionPreference = "Stop"
$env:AZURE_CORE_COLLECT_TELEMETRY = "false"

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

Write-Host "Getting tokens..."
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv

# Use format=tmdl to get TMDL parts
$defUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$itemId/getDefinition?format=tmdl"
$headers = @{ "Authorization" = "Bearer $fabricToken" }

Write-Host "Step 1: POST getDefinition with format=tmdl"
$response = Invoke-WebRequest -Uri $defUrl -Method POST -Headers $headers -ContentType "application/json"
Write-Host "  Initial status: $($response.StatusCode)"

$definition = $null
if ($response.StatusCode -eq 202) {
    $location = $response.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    Write-Host "  Polling: $location"
    
    for ($i = 1; $i -le 20; $i++) {
        Start-Sleep -Seconds 2
        try {
            $pollResponse = Invoke-WebRequest -Uri $location -Headers $headers
            Write-Host "  Poll $i`: $($pollResponse.StatusCode)"
            if ($pollResponse.StatusCode -eq 200) {
                $definition = $pollResponse.Content | ConvertFrom-Json
                break
            }
        } catch {
            Write-Host "  Poll $i`: Error - $($_.Exception.Message)"
        }
    }
} else {
    $definition = $response.Content | ConvertFrom-Json
}

if (-not $definition) {
    Write-Host "ERROR: Could not get definition!"
    exit 1
}

Write-Host "`nParts found: $($definition.definition.parts.Count)"

# List all parts
Write-Host "`nAll definition parts:"
foreach ($part in $definition.definition.parts) {
    Write-Host "  - $($part.path)"
}

# Find tasks.tmdl
Write-Host "`nStep 2: Looking for tasks.tmdl..."
$tasksPart = $definition.definition.parts | Where-Object { $_.path -like "*tasks.tmdl" }

if ($tasksPart) {
    Write-Host "Found: $($tasksPart.path)"
    $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksPart.payload))
    
    # Save to file
    $decoded | Out-File "c:\Repo\cae-demo\tasks_tmdl_server_current.txt" -Encoding utf8
    Write-Host "Content saved to tasks_tmdl_server_current.txt"
    
    Write-Host "`n=== FULL tasks.tmdl Content ==="
    Write-Host $decoded
    Write-Host "=== END ===" 
    
    Write-Host "`n=== Column/Measure Check ==="
    Write-Host "Calculated_Start_Date column: $(if ($decoded -match 'column Calculated_Start_Date') { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "Calculated_End_Date column: $(if ($decoded -match 'column Calculated_End_Date') { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "'Calculated Start Date' measure: $(if ($decoded -match "measure 'Calculated Start Date'") { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "'Calculated End Date' measure: $(if ($decoded -match "measure 'Calculated End Date'") { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "'Is Milestone' measure: $(if ($decoded -match "measure 'Is Milestone'") { 'FOUND' } else { 'NOT FOUND' })"
} else {
    Write-Host "tasks.tmdl NOT FOUND in any path!"
}

# Step 3: DAX Query
Write-Host "`nStep 3: Executing DAX Query..."
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
    Write-Host "DAX Query SUCCESS!"
    $daxResponse | ConvertTo-Json -Depth 10
} catch {
    Write-Host "DAX Query ERROR: $($_.Exception.Message)"
    if ($_.ErrorDetails) {
        $errorInfo = $_.ErrorDetails.Message | ConvertFrom-Json
        Write-Host "Details: $($errorInfo.error.pbi.error.details | ConvertTo-Json -Depth 5)"
    }
}
