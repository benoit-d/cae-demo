# Update Semantic Model: Remove computed columns, add DAX measures
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$datasetId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

# Get token for Fabric API
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{ 
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

Write-Host "=== Step 1: Get Current Definition ===" -ForegroundColor Cyan

# POST getDefinition using Fabric API
$getDefUri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$datasetId/getDefinition"
$resp = Invoke-WebRequest -Uri $getDefUri -Method POST -Headers $headers -UseBasicParsing

$definitionJson = $null

if ($resp.StatusCode -eq 202) {
    Write-Host "Got 202, polling for completion..."
    $locationUri = $resp.Headers["Location"]
    if ($locationUri -is [array]) { $locationUri = $locationUri[0] }
    Write-Host "Poll URI: $locationUri"
    
    $maxAttempts = 30
    $attempt = 0
    while ($attempt -lt $maxAttempts) {
        Start-Sleep -Seconds 2
        $attempt++
        Write-Host "Polling attempt $attempt..."
        
        $pollResp = Invoke-WebRequest -Uri $locationUri -Method GET -Headers $headers -UseBasicParsing
        $pollBody = $pollResp.Content | ConvertFrom-Json
        Write-Host "Status: $($pollBody.status)"
        
        if ($pollBody.status -eq "Succeeded") {
            # Get the NEW Location header which points to the /result endpoint
            $resultUri = $pollResp.Headers["Location"]
            if ($resultUri -is [array]) { $resultUri = $resultUri[0] }
            Write-Host "Result URI: $resultUri"
            
            # GET the actual definition from the result endpoint
            $resultResp = Invoke-WebRequest -Uri $resultUri -Method GET -Headers $headers -UseBasicParsing
            $definitionJson = $resultResp.Content
            Write-Host "Definition retrieved successfully!"
            break
        } elseif ($pollBody.status -eq "Failed") {
            Write-Host "Operation failed: $($pollBody.error)" -ForegroundColor Red
            exit 1
        }
    }
} elseif ($resp.StatusCode -eq 200) {
    Write-Host "Definition retrieved immediately!"
    $definitionJson = $resp.Content
}

if (-not $definitionJson) {
    Write-Host "ERROR: Could not retrieve definition" -ForegroundColor Red
    exit 1
}

# Parse the definition
$definition = $definitionJson | ConvertFrom-Json
Write-Host "Found $($definition.definition.parts.Count) parts in definition"

# Save original for reference
$definitionJson | Out-File -FilePath "sm_definition_original.json" -Encoding utf8
Write-Host "Saved original definition to sm_definition_original.json"

Write-Host "`n=== Step 2: Modify tasks.tmdl ===" -ForegroundColor Cyan

# Find and modify tasks.tmdl
$modifiedParts = @()
foreach ($part in $definition.definition.parts) {
    if ($part.path -eq "definition/tables/tasks.tmdl") {
        Write-Host "Found tasks.tmdl, modifying..."
        
        # Decode base64 content
        $originalContent = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($part.payload))
        Write-Host "Original content length: $($originalContent.Length)"
        
        # Save original tasks.tmdl for reference
        $originalContent | Out-File -FilePath "tasks_tmdl_original.txt" -Encoding utf8
        
        # Remove Calculated_Start_Date column (simple format without lineageTag/annotations)
        $modifiedContent = $originalContent -replace "(?ms)\tcolumn Calculated_Start_Date\r?\n\t\tdataType: dateTime\r?\n\t\tformatString: General Date\r?\n\t\tsummarizeBy: none\r?\n\t\tsourceColumn: Calculated_Start_Date\r?\n\r?\n", ""
        
        # Remove Calculated_End_Date column (simple format without lineageTag/annotations)
        $modifiedContent = $modifiedContent -replace "(?ms)\tcolumn Calculated_End_Date\r?\n\t\tdataType: dateTime\r?\n\t\tformatString: General Date\r?\n\t\tsummarizeBy: none\r?\n\t\tsourceColumn: Calculated_End_Date\r?\n\r?\n", ""
        
        # DAX measures to add (using tabs for indentation)
        $measuresToAdd = @"

	measure 'Calculated Start Date' = 
		VAR _start = COALESCE(tasks[Actual_Start], tasks[Modified_Planned_Start], tasks[Initial_Planned_Start])
		RETURN _start
		formatString: General Date

	measure 'Calculated End Date' = 
		VAR _start = COALESCE(tasks[Actual_Start], tasks[Modified_Planned_Start], tasks[Initial_Planned_Start])
		VAR _end = COALESCE(tasks[Actual_End], _start + tasks[Standard_Duration])
		RETURN _end
		formatString: General Date

	measure 'Is Milestone' = IF(tasks[Milestone] = 1, TRUE(), FALSE())

"@
        
        # Insert measures before the partition definition
        $modifiedContent = $modifiedContent -replace "(\r?\n\tpartition tasks)", "$measuresToAdd`$1"
        
        Write-Host "Modified content length: $($modifiedContent.Length)"
        
        # Save modified tasks.tmdl for reference
        $modifiedContent | Out-File -FilePath "tasks_tmdl_modified.txt" -Encoding utf8
        
        # Encode back to base64
        $encodedContent = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modifiedContent))
        
        $modifiedParts += @{
            path = $part.path
            payload = $encodedContent
            payloadType = $part.payloadType
        }
    } else {
        # Keep other parts unchanged
        $modifiedParts += @{
            path = $part.path
            payload = $part.payload
            payloadType = $part.payloadType
        }
    }
}

Write-Host "`n=== Step 3: Update Definition ===" -ForegroundColor Cyan

# Build update payload
$updatePayload = @{
    definition = @{
        parts = $modifiedParts
    }
} | ConvertTo-Json -Depth 10

# Save the update payload
$updatePayload | Out-File -FilePath "sm_update_payload.json" -Encoding utf8
Write-Host "Saved update payload to sm_update_payload.json"

# POST updateDefinition using Fabric API
$updateUri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$datasetId/updateDefinition"
try {
    $updateResponse = Invoke-WebRequest -Uri $updateUri -Method POST -Headers $headers -Body $updatePayload -UseBasicParsing
    Write-Host "Update response status: $($updateResponse.StatusCode)"
    
    if ($updateResponse.StatusCode -eq 202) {
        Write-Host "Update accepted (202), polling for completion..."
        $locationHeader = $updateResponse.Headers["Location"]
        if ($locationHeader) {
            $pollUri = $locationHeader
            if ($pollUri -is [array]) { $pollUri = $pollUri[0] }
            Write-Host "Update poll URI: $pollUri"
            
            $maxAttempts = 60
            $attempt = 0
            while ($attempt -lt $maxAttempts) {
                Start-Sleep -Seconds 2
                $attempt++
                Write-Host "Polling update attempt $attempt..."
                
                $pollResponse = Invoke-WebRequest -Uri $pollUri -Method GET -Headers $headers -UseBasicParsing
                $pollBody = $pollResponse.Content | ConvertFrom-Json
                Write-Host "Status: $($pollBody.status)"
                
                if ($pollBody.status -eq "Succeeded") {
                    Write-Host "Update completed successfully!" -ForegroundColor Green
                    break
                } elseif ($pollBody.status -eq "Failed") {
                    Write-Host "Update failed: $($pollBody.error | ConvertTo-Json -Depth 5)" -ForegroundColor Red
                    exit 1
                }
            }
        }
    } elseif ($updateResponse.StatusCode -eq 200) {
        Write-Host "Update completed immediately!" -ForegroundColor Green
    }
    
    $updateResponse.Content | Out-File -FilePath "sm_update_result.json" -Encoding utf8
} catch {
    Write-Host "Update error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $errorBody = $reader.ReadToEnd()
        Write-Host "Error details: $errorBody"
        $errorBody | Out-File -FilePath "sm_update_error.json" -Encoding utf8
    }
    exit 1
}

Write-Host "`n=== Step 4: Trigger Refresh ===" -ForegroundColor Cyan

# Get Power BI token for refresh endpoint
$pbiToken = (az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
$pbiHeaders = @{ 
    "Authorization" = "Bearer $pbiToken"
    "Content-Type" = "application/json"
}

$refreshUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/refreshes"
$refreshBody = '{"type":"full"}'

try {
    $refreshResponse = Invoke-WebRequest -Uri $refreshUri -Method POST -Headers $pbiHeaders -Body $refreshBody -UseBasicParsing
    Write-Host "Refresh triggered! Status: $($refreshResponse.StatusCode)" -ForegroundColor Green
    
    # Get refresh status
    Start-Sleep -Seconds 3
    $statusUri = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$datasetId/refreshes?`$top=1"
    $statusResponse = Invoke-RestMethod -Uri $statusUri -Method GET -Headers $pbiHeaders
    $statusResponse.value[0] | ConvertTo-Json -Depth 3 | Out-File -FilePath "sm_refresh_status.json" -Encoding utf8
    Write-Host "Refresh status:"
    $statusResponse.value[0] | Format-List
} catch {
    Write-Host "Refresh error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $errorBody = $reader.ReadToEnd()
        Write-Host "Error details: $errorBody"
    }
}

Write-Host "`n=== Complete ===" -ForegroundColor Green
Write-Host "Files created:"
Write-Host "  - sm_definition_original.json (original definition)"
Write-Host "  - tasks_tmdl_original.txt (original tasks.tmdl)"
Write-Host "  - tasks_tmdl_modified.txt (modified tasks.tmdl)"
Write-Host "  - sm_update_payload.json (update request payload)"
Write-Host "  - sm_update_result.json (update response)"
Write-Host "  - sm_refresh_status.json (refresh status)"
