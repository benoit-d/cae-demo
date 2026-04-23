$ErrorActionPreference = "Stop"
$env:AZURE_CORE_COLLECT_TELEMETRY = "false"

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv
$headers = @{ "Authorization" = "Bearer $fabricToken" }

Write-Host "=== Step 1: Get Definition ===" -ForegroundColor Cyan

# POST to initiate getDefinition
$defUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$itemId/getDefinition"
$response = Invoke-WebRequest -Uri $defUrl -Method POST -Headers $headers -ContentType "application/json" -UseBasicParsing
Write-Host "Initial: Status=$($response.StatusCode)"

if ($response.StatusCode -eq 202) {
    $location = $response.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    Write-Host "Location: $location"
    
    # Extract operation ID from location
    if ($location -match "/operations/([^/]+)") {
        $operationId = $matches[1]
        Write-Host "Operation ID: $operationId"
    }
    
    # Poll until complete with result
    $maxAttempts = 20
    $definition = $null
    
    for ($i = 1; $i -le $maxAttempts; $i++) {
        Start-Sleep -Seconds 2
        
        try {
            # Try getting result from location URL
            $pollResponse = Invoke-WebRequest -Uri $location -Headers $headers -UseBasicParsing
            Write-Host "Poll $i : Status=$($pollResponse.StatusCode)"
            
            $pollContent = $pollResponse.Content | ConvertFrom-Json
            Write-Host "  Keys: $($pollContent | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name)"
            
            if ($pollContent.status -eq "Succeeded") {
                # Check for definition in various places
                if ($pollContent.definition) {
                    $definition = $pollContent
                    Write-Host "  Found definition at root!"
                    break
                }
                
                # Try getting result with /result suffix
                $resultUrl = "$location/result"
                Write-Host "  Trying result URL: $resultUrl"
                try {
                    $resultResponse = Invoke-WebRequest -Uri $resultUrl -Headers $headers -UseBasicParsing
                    $definition = $resultResponse.Content | ConvertFrom-Json
                    Write-Host "  Got result from /result endpoint!"
                    break
                } catch {
                    Write-Host "  /result endpoint failed: $($_.Exception.Message)"
                }
                
                # The definition might be embedded in the response
                if ($pollContent.result) {
                    $definition = $pollContent.result
                    Write-Host "  Found definition in result property!"
                    break
                }
                
                Write-Host "  Operation succeeded but no definition found"
                Write-Host "  Full response:"
                $pollResponse.Content
                break
            }
        } catch {
            Write-Host "Poll $i : Error - $($_.Exception.Message)"
        }
    }
} else {
    Write-Host "Got immediate response"
    $definition = $response.Content | ConvertFrom-Json
}

# Save whatever we got
if ($definition) {
    $definition | ConvertTo-Json -Depth 15 | Out-File "c:\Repo\cae-demo\sm_definition_debug.json" -Encoding utf8
    Write-Host "`nDefinition saved to sm_definition_debug.json"
    
    # Check for parts
    if ($definition.definition -and $definition.definition.parts) {
        Write-Host "Parts count: $($definition.definition.parts.Count)"
        foreach ($part in $definition.definition.parts) {
            Write-Host "  - $($part.path)"
            
            if ($part.path -like "*tasks*") {
                Write-Host "    Found tasks! Decoding..."
                $decoded = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($part.payload))
                $decoded | Out-File "c:\Repo\cae-demo\tasks_tmdl_final.txt" -Encoding utf8
                Write-Host $decoded
            }
        }
    } else {
        Write-Host "No parts found in definition"
    }
}

Write-Host "`n=== Step 2: Execute DAX Query ===" -ForegroundColor Cyan

$daxUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$itemId/executeQueries"
$pbiHeaders = @{ "Authorization" = "Bearer $pbiToken"; "Content-Type" = "application/json" }

# Try a simple query first to see what columns exist
$simpleQuery = @{
    queries = @(
        @{
            query = "EVALUATE TOPN(5, tasks)"
        }
    )
} | ConvertTo-Json -Depth 5

Write-Host "Executing simple DAX query to see table structure..."
try {
    $daxResponse = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $simpleQuery
    Write-Host "Success! Results:" -ForegroundColor Green
    $daxResponse | ConvertTo-Json -Depth 10
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message
    }
}

# Also try the measure query
Write-Host "`nExecuting measure query..."
$measureQuery = @{
    queries = @(
        @{
            query = "EVALUATE TOPN(5, ADDCOLUMNS(tasks, `"CalcStart`", [Calculated Start Date], `"CalcEnd`", [Calculated End Date], `"IsMilestone`", [Is Milestone]), tasks[Task_ID], ASC)"
        }
    )
} | ConvertTo-Json -Depth 5

try {
    $daxResponse2 = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $measureQuery
    Write-Host "Success! Results:" -ForegroundColor Green
    $daxResponse2 | ConvertTo-Json -Depth 10
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        $errObj = $_.ErrorDetails.Message | ConvertFrom-Json
        Write-Host "Code: $($errObj.error.code)"
        foreach ($detail in $errObj.error.'pbi.error'.details) {
            Write-Host "  $($detail.code): $($detail.detail.value)"
        }
    }
}
