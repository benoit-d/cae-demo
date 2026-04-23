# Fix DAX Measures - Using existing definition and fixing tasks.tmdl
# This script properly handles the Fabric API for semantic model definitions

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$semanticModelId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "STEP 1: Get Tokens" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$env:AZURE_CORE_COLLECT_TELEMETRY = "false"
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv

$fabricHeaders = @{
    "Authorization" = "Bearer $fabricToken"
    "Content-Type" = "application/json"
}

$pbiHeaders = @{
    "Authorization" = "Bearer $pbiToken"
    "Content-Type" = "application/json"
}

Write-Host "Tokens obtained" -ForegroundColor Green

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 2: Get Current Definition" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$getDefUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$semanticModelId/getDefinition"
Write-Host "POST $getDefUrl"

$response = Invoke-WebRequest -Uri $getDefUrl -Method POST -Headers $fabricHeaders -Body "{}" -UseBasicParsing
Write-Host "Initial status: $($response.StatusCode)"

$definition = $null
if ($response.StatusCode -eq 202) {
    $location = $response.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    Write-Host "LRO Location: $location"
    
    # Poll for completion - when 200, the body contains the definition
    for ($i = 1; $i -le 30; $i++) {
        Start-Sleep -Seconds 2
        $pollResponse = Invoke-WebRequest -Uri $location -Method GET -Headers $fabricHeaders -UseBasicParsing
        Write-Host "Poll $i - Status: $($pollResponse.StatusCode)"
        
        if ($pollResponse.StatusCode -eq 200) {
            $pollContent = $pollResponse.Content | ConvertFrom-Json
            
            # Check if this is the operation status or the actual definition
            if ($pollContent.definition -and $pollContent.definition.parts) {
                Write-Host "Definition retrieved directly from poll response!" -ForegroundColor Green
                $definition = $pollContent
            } else {
                # Operation completed, but definition might be returned on the next call
                # For semantic models, try calling getDefinition again
                Write-Host "Operation completed, fetching definition result..."
                
                # Try the result endpoint
                $resultUrl = "$location/result"
                try {
                    $resultResponse = Invoke-WebRequest -Uri $resultUrl -Method GET -Headers $fabricHeaders -UseBasicParsing
                    $definition = $resultResponse.Content | ConvertFrom-Json
                    Write-Host "Got definition from result endpoint" -ForegroundColor Green
                } catch {
                    Write-Host "Result endpoint didn't work, checking response..."
                }
            }
            break
        }
    }
}

# If we still don't have the definition, check if it was returned directly
if (-not $definition -and $response.StatusCode -eq 200) {
    $definition = $response.Content | ConvertFrom-Json
}

# Verify we have the definition
if (-not $definition -or -not $definition.definition -or -not $definition.definition.parts) {
    Write-Host "Failed to get definition with parts. Checking structure..." -ForegroundColor Yellow
    Write-Host "Response type: $($definition.GetType().Name)"
    Write-Host "Has definition: $($null -ne $definition.definition)"
    if ($definition.definition) {
        Write-Host "Has parts: $($null -ne $definition.definition.parts)"
        Write-Host "Parts count: $($definition.definition.parts.Count)"
    }
    
    # Try using the known-good definition from file as fallback
    Write-Host "`nUsing known-good definition from sm_def_success.json..." -ForegroundColor Yellow
    $definition = Get-Content "c:\Repo\cae-demo\sm_def_success.json" -Raw | ConvertFrom-Json
}

Write-Host "`nDefinition parts count: $($definition.definition.parts.Count)"
$definition.definition.parts | ForEach-Object { Write-Host "  - $($_.path)" }

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 3: Decode and Fix tasks.tmdl" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$tasksPart = $definition.definition.parts | Where-Object { $_.path -eq "definition/tables/tasks.tmdl" }

if (-not $tasksPart) {
    Write-Host "ERROR: tasks.tmdl not found!" -ForegroundColor Red
    exit 1
}

$tasksContent = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksPart.payload))
Write-Host "Current tasks.tmdl:" -ForegroundColor Yellow
Write-Host $tasksContent
Write-Host "`n--- End of current content ---"

# Save original for reference
$tasksContent | Out-File -FilePath "c:\Repo\cae-demo\tasks_tmdl_before_fix2.txt" -Encoding UTF8

# Check if measures already exist or need to be added
$hasCalcStart = $tasksContent -match "measure 'Calculated Start Date'"
$hasCalcEnd = $tasksContent -match "measure 'Calculated End Date'"
$hasIsMilestone = $tasksContent -match "measure 'Is Milestone'"

Write-Host "`nCurrent measures presence:"
Write-Host "  Calculated Start Date: $hasCalcStart"
Write-Host "  Calculated End Date: $hasCalcEnd"
Write-Host "  Is Milestone: $hasIsMilestone"

# Build the correct measure definitions with proper TMDL syntax
# Using actual tab characters for TMDL compliance
$tab = "`t"

# Correct measures - expression on same line as measure, formatString on next line with 2 tabs
$correctMeasures = @"

${tab}measure 'Calculated Start Date' = COALESCE([Actual_Start], [Modified_Planned_Start], [Initial_Planned_Start])
${tab}${tab}formatString: General Date

${tab}measure 'Calculated End Date' = COALESCE([Actual_End], [Actual_Start] + [Standard_Duration], [Modified_Planned_Start] + [Standard_Duration], [Initial_Planned_Start] + [Standard_Duration])
${tab}${tab}formatString: General Date

${tab}measure 'Is Milestone' = IF([Milestone] = 1, TRUE(), FALSE())

"@

# Strategy: Remove any existing broken measures and add correct ones before the partition section
$newContent = $tasksContent

# Remove existing measure definitions (they might be broken)
# This regex handles various broken measure formats
$measurePatterns = @(
    "(?ms)\s*measure\s+'Calculated Start Date'.*?(?=\s*measure|\s*partition|\s*column|\z)",
    "(?ms)\s*measure\s+'Calculated End Date'.*?(?=\s*measure|\s*partition|\s*column|\z)",
    "(?ms)\s*measure\s+'Is Milestone'.*?(?=\s*measure|\s*partition|\s*column|\z)"
)

foreach ($pattern in $measurePatterns) {
    if ($newContent -match $pattern) {
        Write-Host "Removing existing measure definition..." -ForegroundColor Yellow
        $newContent = $newContent -replace $pattern, ""
    }
}

# Clean up extra blank lines that might result
$newContent = $newContent -replace "(`n\s*){3,}", "`n`n"

# Insert correct measures before the partition section
if ($newContent -match "(?m)^\s*partition\s+tasks") {
    Write-Host "Inserting correct measures before partition section..." -ForegroundColor Green
    $newContent = $newContent -replace "(?m)(^\s*partition\s+tasks)", "$correctMeasures`$1"
} else {
    Write-Host "Adding measures at the end of table definition..." -ForegroundColor Green
    $newContent = $newContent.TrimEnd() + "`n" + $correctMeasures
}

Write-Host "`nNew tasks.tmdl content:" -ForegroundColor Green
Write-Host $newContent
Write-Host "`n--- End of new content ---"

# Save new content
$newContent | Out-File -FilePath "c:\Repo\cae-demo\tasks_tmdl_after_fix2.txt" -Encoding UTF8

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 4: Update Definition" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Encode the new content
$newTasksPayload = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($newContent))

# Build updated parts array
$updatedParts = @()
foreach ($part in $definition.definition.parts) {
    if ($part.path -eq "definition/tables/tasks.tmdl") {
        $updatedParts += @{
            path = $part.path
            payload = $newTasksPayload
            payloadType = "InlineBase64"
        }
    } else {
        $updatedParts += @{
            path = $part.path
            payload = $part.payload
            payloadType = "InlineBase64"
        }
    }
}

$updatePayload = @{
    definition = @{
        parts = $updatedParts
    }
} | ConvertTo-Json -Depth 10

Write-Host "Updating definition with $($updatedParts.Count) parts..."

$updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$semanticModelId/updateDefinition"
Write-Host "POST $updateUrl"

try {
    $updateResponse = Invoke-WebRequest -Uri $updateUrl -Method POST -Headers $fabricHeaders -Body $updatePayload -UseBasicParsing
    Write-Host "Update response status: $($updateResponse.StatusCode)" -ForegroundColor Green
    
    if ($updateResponse.StatusCode -eq 202) {
        $location = $updateResponse.Headers["Location"]
        if ($location -is [array]) { $location = $location[0] }
        Write-Host "Update LRO: $location"
        
        for ($i = 1; $i -le 30; $i++) {
            Start-Sleep -Seconds 2
            $pollResponse = Invoke-WebRequest -Uri $location -Method GET -Headers $fabricHeaders -UseBasicParsing
            $pollContent = $pollResponse.Content | ConvertFrom-Json
            Write-Host "Update poll $i - Status: $($pollResponse.StatusCode), Operation: $($pollContent.status)"
            
            if ($pollContent.status -eq "Succeeded") {
                Write-Host "Update completed successfully!" -ForegroundColor Green
                break
            } elseif ($pollContent.status -eq "Failed") {
                Write-Host "Update FAILED!" -ForegroundColor Red
                Write-Host ($pollContent | ConvertTo-Json -Depth 5)
                exit 1
            }
        }
    }
} catch {
    Write-Host "Update Error: $_" -ForegroundColor Red
    if ($_.Exception.Response) {
        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $errorBody = $reader.ReadToEnd()
        Write-Host "Error details: $errorBody" -ForegroundColor Red
    }
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 5: Trigger Refresh" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$refreshUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$semanticModelId/refreshes"
$refreshBody = '{"type":"full"}'

Write-Host "POST $refreshUrl"

try {
    $refreshResponse = Invoke-WebRequest -Uri $refreshUrl -Method POST -Headers $pbiHeaders -Body $refreshBody -UseBasicParsing
    Write-Host "Refresh triggered: $($refreshResponse.StatusCode)" -ForegroundColor Green
    
    # Wait for refresh
    $refreshStatusUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$semanticModelId/refreshes?`$top=1"
    
    for ($i = 1; $i -le 60; $i++) {
        Start-Sleep -Seconds 5
        $statusResponse = Invoke-RestMethod -Uri $refreshStatusUrl -Method GET -Headers $pbiHeaders
        $status = $statusResponse.value[0].status
        Write-Host "Refresh poll $i - Status: $status"
        
        if ($status -eq "Completed") {
            Write-Host "Refresh completed!" -ForegroundColor Green
            break
        } elseif ($status -eq "Failed") {
            Write-Host "Refresh FAILED!" -ForegroundColor Red
            Write-Host ($statusResponse.value[0] | ConvertTo-Json -Depth 5)
            break
        }
    }
} catch {
    Write-Host "Refresh Error: $_" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 6: Test DAX Query" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$daxUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$semanticModelId/executeQueries"

# Test each measure individually
$testQueries = @(
    @{ name = "Calculated Start Date"; query = "EVALUATE ROW(`"test`", [Calculated Start Date])" },
    @{ name = "Calculated End Date"; query = "EVALUATE ROW(`"test`", [Calculated End Date])" },
    @{ name = "Is Milestone"; query = "EVALUATE ROW(`"test`", [Is Milestone])" }
)

foreach ($test in $testQueries) {
    Write-Host "`nTesting: $($test.name)" -ForegroundColor Yellow
    Write-Host "Query: $($test.query)"
    
    $body = @{
        queries = @(@{ query = $test.query })
    } | ConvertTo-Json -Depth 5
    
    try {
        $result = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $body
        Write-Host "Result:" -ForegroundColor Green
        Write-Host ($result | ConvertTo-Json -Depth 10)
    } catch {
        Write-Host "Error: $_" -ForegroundColor Red
    }
}

# Test all measures together
Write-Host "`n--- Full test with all measures ---" -ForegroundColor Cyan
$fullQuery = "EVALUATE TOPN(5, SELECTCOLUMNS(tasks, `"TaskID`", [Task_ID], `"CalcStart`", [Calculated Start Date], `"CalcEnd`", [Calculated End Date], `"IsMilestone`", [Is Milestone]))"
Write-Host "Query: $fullQuery"

$body = @{
    queries = @(@{ query = $fullQuery })
} | ConvertTo-Json -Depth 5

try {
    $result = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $body
    Write-Host "Result:" -ForegroundColor Green
    Write-Host ($result | ConvertTo-Json -Depth 10)
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "WORKFLOW COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
