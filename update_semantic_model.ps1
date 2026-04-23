# Script to update semantic model with new columns

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$semanticModelId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

# Get token
$token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

# Read the existing definition from file
$defJson = Get-Content "sm_def_fresh.json" -Raw | ConvertFrom-Json

# Decode tasks.tmdl
$tasksPart = $defJson.definition.parts | Where-Object { $_.path -eq "definition/tables/tasks.tmdl" }
$tasksContent = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksPart.payload))

Write-Host "=== Current tasks.tmdl content ==="
Write-Host $tasksContent
Write-Host "==================================="

# Add the new columns BEFORE the partition definition
$newColumns = @"

	column Calculated_Start_Date
		dataType: dateTime
		formatString: General Date
		summarizeBy: none
		sourceColumn: Calculated_Start_Date

	column Calculated_End_Date
		dataType: dateTime
		formatString: General Date
		summarizeBy: none
		sourceColumn: Calculated_End_Date

"@

# Find the partition line and insert before it
$updatedTasksContent = $tasksContent -replace "([\t]partition tasks = entity)", "$newColumns`$1"

Write-Host "=== Updated tasks.tmdl content ==="
Write-Host $updatedTasksContent
Write-Host "==================================="

# Encode the updated content back to base64
$updatedPayload = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($updatedTasksContent))

# Build the update payload with all parts
$updateParts = @()
foreach ($part in $defJson.definition.parts) {
    if ($part.path -eq "definition/tables/tasks.tmdl") {
        $updateParts += @{
            path = $part.path
            payload = $updatedPayload
            payloadType = "InlineBase64"
        }
    } else {
        $updateParts += @{
            path = $part.path
            payload = $part.payload
            payloadType = $part.payloadType
        }
    }
}

$updateBody = @{
    definition = @{
        format = "TMDL"
        parts = $updateParts
    }
} | ConvertTo-Json -Depth 10

# Save the update payload for inspection
$updateBody | Out-File "update_sm_payload.json" -Encoding utf8
Write-Host "Update payload saved to update_sm_payload.json"

# POST the update
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$semanticModelId/updateDefinition"
Write-Host "Posting update to: $uri"

try {
    $response = Invoke-WebRequest -Uri $uri -Method POST -Headers $headers -Body $updateBody -UseBasicParsing
    Write-Host "Update response status: $($response.StatusCode)"
    if ($response.Headers['Location']) {
        Write-Host "Operation Location: $($response.Headers['Location'])"
        # Poll for completion
        Start-Sleep -Seconds 5
        $opResult = Invoke-RestMethod -Uri "$($response.Headers['Location'])/result" -Method GET -Headers $headers -ErrorAction SilentlyContinue
        Write-Host "Operation result: $($opResult | ConvertTo-Json -Depth 5)"
    }
} catch {
    Write-Host "Error: $_"
    Write-Host "Response: $($_.Exception.Response)"
}
