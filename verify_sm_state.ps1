$ErrorActionPreference = "Continue"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

# Get tokens with telemetry disabled
$env:AZURE_CORE_COLLECT_TELEMETRY = "false"
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv

Write-Host "=== STEP 1: Getting Semantic Model Definition ===" -ForegroundColor Cyan
$defUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$itemId/getDefinition"
$headers = @{ "Authorization" = "Bearer $fabricToken" }

$response = Invoke-WebRequest -Uri $defUrl -Method POST -Headers $headers -ContentType "application/json"
Write-Host "Initial response status: $($response.StatusCode)"

if ($response.StatusCode -eq 202) {
    $location = $response.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    Write-Host "Polling location: $location"
    
    $maxAttempts = 30
    $attempt = 0
    do {
        Start-Sleep -Seconds 2
        $attempt++
        Write-Host "Poll attempt $attempt..."
        try {
            $pollResponse = Invoke-WebRequest -Uri $location -Headers $headers
            Write-Host "Poll status: $($pollResponse.StatusCode)"
        } catch {
            if ($_.Exception.Response.StatusCode -eq 202) {
                Write-Host "Still processing..."
                continue
            }
            throw
        }
    } while ($pollResponse.StatusCode -eq 202 -and $attempt -lt $maxAttempts)
    
    $definition = $pollResponse.Content | ConvertFrom-Json
} else {
    $definition = $response.Content | ConvertFrom-Json
}

# Step 2: Find and decode tasks.tmdl
Write-Host "`n=== STEP 2: Decoding definition/tables/tasks.tmdl ===" -ForegroundColor Cyan
$tasksTmdl = $definition.definition.parts | Where-Object { $_.path -eq "definition/tables/tasks.tmdl" }

if ($tasksTmdl) {
    Write-Host "Found tasks.tmdl, decoding..."
    $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksTmdl.payload))
    Write-Host "`n--- FULL tasks.tmdl Content ---" -ForegroundColor Yellow
    Write-Host $decoded
    Write-Host "--- End of tasks.tmdl ---`n" -ForegroundColor Yellow
    
    # Save to file for reference
    $decoded | Out-File -FilePath "c:\Repo\cae-demo\tasks_tmdl_current.txt" -Encoding utf8
    Write-Host "Saved to tasks_tmdl_current.txt"
    
    # Check for specific columns and measures
    Write-Host "`n=== Analysis ===" -ForegroundColor Cyan
    Write-Host "Checking for Calculated_Start_Date column: $(if ($decoded -match 'Calculated_Start_Date') { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "Checking for Calculated_End_Date column: $(if ($decoded -match 'Calculated_End_Date') { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "Checking for 'Calculated Start Date' measure: $(if ($decoded -match "measure 'Calculated Start Date'") { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "Checking for 'Calculated End Date' measure: $(if ($decoded -match "measure 'Calculated End Date'") { 'FOUND' } else { 'NOT FOUND' })"
    Write-Host "Checking for 'Is Milestone' measure: $(if ($decoded -match "measure 'Is Milestone'") { 'FOUND' } else { 'NOT FOUND' })"
} else {
    Write-Host "tasks.tmdl NOT FOUND in definition parts!"
    Write-Host "Available parts:"
    $definition.definition.parts | ForEach-Object { Write-Host "  - $($_.path)" }
}

# Step 3: Execute DAX Query
Write-Host "`n=== STEP 3: Executing DAX Query ===" -ForegroundColor Cyan
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
    Write-Host "DAX Query Result:" -ForegroundColor Green
    $daxResponse | ConvertTo-Json -Depth 10
} catch {
    Write-Host "DAX Query Error:" -ForegroundColor Red
    Write-Host $_.Exception.Message
    try {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $reader.BaseStream.Position = 0
        Write-Host $reader.ReadToEnd()
    } catch {
        Write-Host "Could not read error details"
    }
}
