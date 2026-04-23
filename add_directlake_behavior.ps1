# Add directLakeBehavior: Automatic to model.tmdl
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$semanticModelId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv).Trim()
$h = @{"Authorization"="Bearer $token";"Content-Type"="application/json"}

# Get current definition
$def = Get-Content "c:\Repo\cae-demo\sm_def_verified.json" -Raw | ConvertFrom-Json
$parts = $def.definition.parts

# Create new model.tmdl with directLakeBehavior
$newModelTmdl = @"
model Model
	culture: en-US
	defaultPowerBIDataSourceVersion: powerBI_V3
	sourceQueryCulture: en-US
	directLakeBehavior: Automatic
	dataAccessOptions
		legacyRedirects
		returnErrorValuesAsNull

annotation PBI_QueryOrder = ["DirectLake - CAEManufacturing_SQLDB"]

annotation __PBI_TimeIntelligenceEnabled = 1

annotation PBI_ProTooling = ["DirectLakeOnOneLakeInWeb","WebModelingEdit"]

ref table employees
ref table production_lines
ref table machines
ref table machine_jobs
ref table simulators
ref table maintenance_history
ref table projects
ref table tasks

"@

$modelBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($newModelTmdl))
Write-Host "Model base64 length: $($modelBase64.Length)"

# Update model.tmdl in parts
foreach ($part in $parts) {
    if ($part.path -eq "definition/model.tmdl") {
        $part.payload = $modelBase64
        Write-Host "Updated model.tmdl payload" -ForegroundColor Green
    }
}

# POST update
$updatePayload = @{ definition = @{ parts = $parts } } | ConvertTo-Json -Depth 10 -Compress
$updateUri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$semanticModelId/updateDefinition"
Write-Host "Posting update..."
$r = Invoke-WebRequest -Uri $updateUri -Method POST -Headers $h -Body $updatePayload -UseBasicParsing
Write-Host "Status: $($r.StatusCode)"

if ($r.StatusCode -eq 202) {
    $lro = $r.Headers["Location"]
    if ($lro -is [array]) { $lro = $lro[0] }
    Write-Host "LRO: $lro"
    
    for ($i = 1; $i -le 20; $i++) {
        Start-Sleep -Seconds 2
        $poll = Invoke-WebRequest -Uri $lro -Method GET -Headers $h -UseBasicParsing
        $result = $poll.Content | ConvertFrom-Json
        Write-Host "  $i. $($result.status)"
        if ($result.status -eq "Succeeded") { 
            Write-Host "SUCCESS!" -ForegroundColor Green
            break 
        }
        if ($result.status -eq "Failed") { 
            Write-Host "FAILED: $($result.error.message)" -ForegroundColor Red
            break 
        }
    }
}
