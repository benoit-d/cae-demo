$ErrorActionPreference = "Stop"

Write-Host "=== Step 4: Update Semantic Model Definition ===" -ForegroundColor Cyan

# Get token
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv).Trim()
Write-Host "Token obtained"

$h = @{"Authorization"="Bearer $token";"Content-Type"="application/json"}
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$modelId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

# Read original definition
$def = Get-Content "sm_def_current.json" -Raw | ConvertFrom-Json

# Read and encode modified files
$modifiedTasksContent = Get-Content "tasks_tmdl_modified.txt" -Raw
$modifiedTasksB64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modifiedTasksContent))

$modifiedModelContent = Get-Content "model_tmdl_modified.txt" -Raw
$modifiedModelB64 = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modifiedModelContent))

# Update parts
foreach ($part in $def.definition.parts) {
    if ($part.path -eq "definition/tables/tasks.tmdl") { 
        $part.payload = $modifiedTasksB64 
        Write-Host "Updated: tasks.tmdl"
    }
    if ($part.path -eq "definition/model.tmdl") { 
        $part.payload = $modifiedModelB64 
        Write-Host "Updated: model.tmdl"
    }
}

# Create payload
$updatePayload = @{ definition = @{ parts = $def.definition.parts } } | ConvertTo-Json -Depth 10
Write-Host "Payload size: $($updatePayload.Length) bytes"

# POST update
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$modelId/updateDefinition"
Write-Host "POSTing to: $uri"

try {
    $r = Invoke-WebRequest -Uri $uri -Method POST -Headers $h -Body $updatePayload -UseBasicParsing
    Write-Host "Response Status: $($r.StatusCode)" -ForegroundColor Green
    $lro = $r.Headers["Location"]; if ($lro -is [array]) { $lro = $lro[0] }
    Write-Host "LRO Location: $lro"
    
    # Poll for result
    Write-Host "Waiting 15 seconds for operation to complete..."
    Start-Sleep -Seconds 15
    
    $resultUri = "$lro/result"
    Write-Host "Polling: $resultUri"
    $result = Invoke-WebRequest -Uri $resultUri -Method GET -Headers $h -UseBasicParsing
    Write-Host "Poll Status: $($result.StatusCode)" -ForegroundColor Green
    Write-Host "Update Result: $($result.Content)"
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $body = $reader.ReadToEnd()
        Write-Host "Response Body: $body" -ForegroundColor Red
        $body | Out-File "sm_update_error.json" -Encoding UTF8
    }
    exit 1
}

Write-Host ""
Write-Host "=== Step 5: Trigger Refresh ===" -ForegroundColor Cyan

# Get Power BI token
$pbiToken = (az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv).Trim()
$pbiH = @{"Authorization"="Bearer $pbiToken";"Content-Type"="application/json"}

$refreshUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$modelId/refreshes"
$refreshBody = '{"type":"full"}'

Write-Host "POSTing refresh to: $refreshUri"
try {
    $refreshResult = Invoke-WebRequest -Uri $refreshUri -Method POST -Headers $pbiH -Body $refreshBody -UseBasicParsing
    Write-Host "Refresh triggered: $($refreshResult.StatusCode)" -ForegroundColor Green
} catch {
    Write-Host "Refresh Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Step 6: Check Refresh Status ===" -ForegroundColor Cyan
Write-Host "Waiting 10 seconds..."
Start-Sleep -Seconds 10

$statusUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$modelId/refreshes?`$top=3"
Write-Host "Getting status from: $statusUri"
try {
    $statusResult = Invoke-WebRequest -Uri $statusUri -Method GET -Headers $pbiH -UseBasicParsing
    Write-Host "Status Response:" -ForegroundColor Cyan
    $statusResult.Content | ConvertFrom-Json | ConvertTo-Json -Depth 10
    $statusResult.Content | Out-File "sm_refresh_status.json" -Encoding UTF8
} catch {
    Write-Host "Status Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Green
