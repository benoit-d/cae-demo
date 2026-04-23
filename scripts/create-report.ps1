# Create PBIR Report in Fabric
$ErrorActionPreference = 'Stop'

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$semanticModelId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

# Get token
$token = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
$headers = @{
    Authorization = "Bearer $token"
    "Content-Type" = "application/json"
}

# Definition files
$definitionPbir = @"
{
  "version": "4.0",
  "datasetReference": {
    "byConnection": {
      "connectionString": null,
      "pbiServiceModelId": null,
      "pbiModelVirtualServerName": "sobe_wowvirtualserver",
      "pbiModelDatabaseName": "$semanticModelId",
      "name": "EntityDataSource",
      "connectionType": "pbiServiceXmlaStyleLive"
    }
  }
}
"@

$reportJson = @'
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
  "themeCollection": {
    "baseTheme": {
      "name": "CY24SU06",
      "reportVersionAtImport": "5.55",
      "type": "SharedResources"
    }
  }
}
'@

$pageJson = @'
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
  "name": "ReportSection1",
  "displayName": "Project Overview",
  "displayOption": "FitToPage",
  "height": 720,
  "width": 1280
}
'@

# Base64 encode
$pbirBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($definitionPbir))
$reportBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($reportJson))
$pageBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pageJson))

# Build payload
$payload = @{
    displayName = "CAEManufacturing_Report"
    type = "Report"
    definition = @{
        parts = @(
            @{path = "definition.pbir"; payload = $pbirBase64; payloadType = "InlineBase64"}
            @{path = "definition/report.json"; payload = $reportBase64; payloadType = "InlineBase64"}
            @{path = "definition/pages/ReportSection1/page.json"; payload = $pageBase64; payloadType = "InlineBase64"}
        )
    }
} | ConvertTo-Json -Depth 10

Write-Host "=== REQUEST PAYLOAD ===" -ForegroundColor Cyan
Write-Host $payload

try {
    $response = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items" `
        -Method POST `
        -Headers $headers `
        -Body $payload `
        -UseBasicParsing
    
    Write-Host "`n=== RESPONSE ===" -ForegroundColor Green
    Write-Host "Status Code: $($response.StatusCode)"
    
    Write-Host "`n=== HEADERS ===" -ForegroundColor Cyan
    $response.Headers | ConvertTo-Json -Depth 5
    
    # Check for LRO
    $location = $response.Headers["Location"]
    $retryAfter = $response.Headers["Retry-After"]
    $operationId = $response.Headers["x-ms-operation-id"]
    
    Write-Host "Location: $location"
    Write-Host "Retry-After: $retryAfter"
    Write-Host "Operation-Id: $operationId"
    
    if ($response.StatusCode -eq 202 -and $location) {
        Write-Host "`n=== POLLING LRO ===" -ForegroundColor Yellow
        $pollHeaders = @{Authorization = "Bearer $token"}
        
        $maxAttempts = 30
        $attempt = 0
        do {
            Start-Sleep -Seconds 2
            $attempt++
            Write-Host "Poll attempt $attempt..."
            
            try {
                $pollResponse = Invoke-WebRequest -Uri $location -Headers $pollHeaders -UseBasicParsing
                Write-Host "Poll Status: $($pollResponse.StatusCode)"
                
                if ($pollResponse.StatusCode -eq 200) {
                    Write-Host "Operation completed!"
                    $pollResponse.Content | Out-File "c:\Repo\cae-demo\lro-result.json"
                    Write-Host $pollResponse.Content
                    break
                }
            } catch {
                $pollStatus = $_.Exception.Response.StatusCode.value__
                Write-Host "Poll returned: $pollStatus"
                if ($pollStatus -eq 200) { break }
            }
        } while ($attempt -lt $maxAttempts)
    }
    
    if ($response.Content) {
        Write-Host "`nResponse Body:"
        $response.Content
    }
    
} catch {
    Write-Host "`n=== ERROR ===" -ForegroundColor Red
    Write-Host "Status Code: $($_.Exception.Response.StatusCode.value__)"
    
    Write-Host "`n=== ERROR HEADERS ===" -ForegroundColor Cyan
    $_.Exception.Response.Headers | ForEach-Object { Write-Host "$($_.Key): $($_.Value)" }
    
    $stream = $_.Exception.Response.GetResponseStream()
    $reader = [System.IO.StreamReader]::new($stream)
    $errorBody = $reader.ReadToEnd()
    
    Write-Host "`nError Body:"
    Write-Host $errorBody
    $errorBody | Out-File "c:\Repo\cae-demo\error-body.json"
}
