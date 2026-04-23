param()
$ErrorActionPreference = "Continue"
$env:AZURE_CORE_COLLECT_TELEMETRY = "false"

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"
$outputFile = "c:\Repo\cae-demo\verification_results.txt"

# Clear output file
"" | Out-File $outputFile

function Log($msg) {
    $msg | Out-File $outputFile -Append
    Write-Host $msg
}

Log "=== Semantic Model Verification ==="
Log "Workspace: $workspaceId"
Log "Item: $itemId"
Log "Time: $(Get-Date)"
Log ""

# Get tokens
Log "Getting access tokens..."
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv 2>$null
$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv 2>$null

if (-not $fabricToken -or -not $pbiToken) {
    Log "ERROR: Failed to get tokens"
    exit 1
}
Log "Tokens acquired successfully"

# Step 1: Get Definition
Log ""
Log "=== STEP 1: Getting Semantic Model Definition ==="
$defUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$itemId/getDefinition"
$headers = @{ "Authorization" = "Bearer $fabricToken" }

try {
    $response = Invoke-WebRequest -Uri $defUrl -Method POST -Headers $headers -ContentType "application/json"
    Log "Initial response: $($response.StatusCode)"
    
    if ($response.StatusCode -eq 202) {
        $location = $response.Headers["Location"]
        if ($location -is [array]) { $location = $location[0] }
        Log "Polling location: $location"
        
        $definition = $null
        for ($i = 1; $i -le 20; $i++) {
            Start-Sleep -Seconds 2
            try {
                $pollResponse = Invoke-WebRequest -Uri $location -Headers $headers -ErrorAction Stop
                Log "Poll $i : Status $($pollResponse.StatusCode)"
                if ($pollResponse.StatusCode -eq 200) {
                    $definition = $pollResponse.Content | ConvertFrom-Json
                    break
                }
            } catch {
                if ($_.Exception.Response.StatusCode.Value__ -eq 202) {
                    Log "Poll $i : Still processing..."
                } else {
                    throw
                }
            }
        }
    } else {
        $definition = $response.Content | ConvertFrom-Json
    }
} catch {
    Log "ERROR getting definition: $($_.Exception.Message)"
    exit 1
}

if (-not $definition) {
    Log "ERROR: Definition is null"
    exit 1
}

# Save full definition
$definition | ConvertTo-Json -Depth 10 | Out-File "c:\Repo\cae-demo\sm_definition.json" -Encoding utf8
Log "Full definition saved to sm_definition.json"

Log ""
Log "Parts count: $($definition.definition.parts.Count)"

if ($definition.definition.parts.Count -eq 0) {
    Log "WARNING: No parts in definition!"
    Log "Checking if parts are at different location..."
    Log "Definition keys: $($definition | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name)"
    Log "Definition.definition keys: $($definition.definition | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name)"
}

Log ""
Log "All parts:"
foreach ($part in $definition.definition.parts) {
    Log "  - $($part.path)"
}

# Step 2: Find and decode tasks.tmdl
Log ""
Log "=== STEP 2: Finding tasks.tmdl ==="

$tasksPart = $definition.definition.parts | Where-Object { $_.path -like "*tasks*" }

if ($tasksPart) {
    Log "Found: $($tasksPart.path)"
    $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksPart.payload))
    
    # Save decoded content
    $decoded | Out-File "c:\Repo\cae-demo\tasks_tmdl_current.txt" -Encoding utf8
    Log "Saved to tasks_tmdl_current.txt"
    
    Log ""
    Log "--- FULL tasks.tmdl Content ---"
    Log $decoded
    Log "--- END ---"
    
    Log ""
    Log "=== Analysis ==="
    Log "Calculated_Start_Date column: $(if ($decoded -match 'column Calculated_Start_Date') { 'FOUND' } else { 'NOT FOUND' })"
    Log "Calculated_End_Date column: $(if ($decoded -match 'column Calculated_End_Date') { 'FOUND' } else { 'NOT FOUND' })"
    Log "'Calculated Start Date' measure: $(if ($decoded -match "measure 'Calculated Start Date'") { 'FOUND' } else { 'NOT FOUND' })"
    Log "'Calculated End Date' measure: $(if ($decoded -match "measure 'Calculated End Date'") { 'FOUND' } else { 'NOT FOUND' })"
    Log "'Is Milestone' measure: $(if ($decoded -match "measure 'Is Milestone'") { 'FOUND' } else { 'NOT FOUND' })"
} else {
    Log "tasks.tmdl NOT FOUND!"
    Log "Searching in all parts for any table definition..."
    
    foreach ($part in $definition.definition.parts) {
        if ($part.path -like "*.tmdl") {
            Log "Found TMDL file: $($part.path)"
        }
    }
}

# Step 3: Execute DAX Query
Log ""
Log "=== STEP 3: Executing DAX Query ==="

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
    Log "DAX Query SUCCESS!"
    Log ($daxResponse | ConvertTo-Json -Depth 10)
} catch {
    Log "DAX Query ERROR: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) {
        $errorJson = $_.ErrorDetails.Message | ConvertFrom-Json
        Log "Error Code: $($errorJson.error.code)"
        if ($errorJson.error.'pbi.error'.details) {
            foreach ($detail in $errorJson.error.'pbi.error'.details) {
                Log "  $($detail.code): $($detail.detail.value)"
            }
        }
    }
}

Log ""
Log "=== Verification Complete ==="
