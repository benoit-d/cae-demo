# Fix DAX Measures - Full Workflow
# Step 1: Get definition, Step 2: Fix TMDL, Step 3: Update, Step 4: Refresh, Step 5: Test

$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$semanticModelId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"

# Get tokens
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

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "STEP 1: Get Current Definition" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# POST getDefinition
$getDefUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$semanticModelId/getDefinition"
Write-Host "POST $getDefUrl"

$getDefResponse = Invoke-WebRequest -Uri $getDefUrl -Method POST -Headers $fabricHeaders -Body "{}"
Write-Host "Status: $($getDefResponse.StatusCode)"

if ($getDefResponse.StatusCode -eq 202) {
    $location = $getDefResponse.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    $retryAfter = $getDefResponse.Headers["Retry-After"]
    if ($retryAfter -is [array]) { $retryAfter = $retryAfter[0] }
    Write-Host "LRO started, polling location: $location"
    
    # Poll until complete
    $maxAttempts = 30
    $attempt = 0
    $definition = $null
    
    while ($attempt -lt $maxAttempts) {
        $attempt++
        Start-Sleep -Seconds ([int]$retryAfter)
        Write-Host "Poll attempt $attempt..."
        
        $pollResponse = Invoke-WebRequest -Uri $location -Method GET -Headers $fabricHeaders
        Write-Host "Poll status: $($pollResponse.StatusCode)"
        
        if ($pollResponse.StatusCode -eq 200) {
            $definition = $pollResponse.Content | ConvertFrom-Json
            Write-Host "Definition retrieved successfully!" -ForegroundColor Green
            break
        }
    }
} elseif ($getDefResponse.StatusCode -eq 200) {
    $definition = $getDefResponse.Content | ConvertFrom-Json
    Write-Host "Definition retrieved immediately!" -ForegroundColor Green
}

if (-not $definition) {
    Write-Host "Failed to get definition!" -ForegroundColor Red
    exit 1
}

# Show current parts
Write-Host "`nCurrent definition parts:" -ForegroundColor Yellow
foreach ($part in $definition.definition.parts) {
    Write-Host "  - $($part.path)"
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 2: Fix TMDL Syntax in tasks.tmdl" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Find and decode tasks.tmdl
$tasksPart = $definition.definition.parts | Where-Object { $_.path -eq "definition/tables/tasks.tmdl" }

if (-not $tasksPart) {
    Write-Host "tasks.tmdl not found!" -ForegroundColor Red
    exit 1
}

$tasksContent = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($tasksPart.payload))
Write-Host "`nCurrent tasks.tmdl content:" -ForegroundColor Yellow
Write-Host $tasksContent

# Save original for reference
$tasksContent | Out-File -FilePath "tasks_tmdl_before_fix.txt" -Encoding UTF8

# The corrected measures with proper TMDL syntax (tabs for indentation)
# Using actual tab characters
$tab = "`t"

# Build the corrected measure definitions
$correctMeasure1 = "${tab}measure 'Calculated Start Date' = COALESCE([Actual_Start], [Modified_Planned_Start], [Initial_Planned_Start])`n${tab}${tab}formatString: General Date"
$correctMeasure2 = "${tab}measure 'Calculated End Date' = COALESCE([Actual_End], [Actual_Start] + [Standard_Duration], [Modified_Planned_Start] + [Standard_Duration], [Initial_Planned_Start] + [Standard_Duration])`n${tab}${tab}formatString: General Date"
$correctMeasure3 = "${tab}measure 'Is Milestone' = IF([Milestone] = 1, TRUE(), FALSE())"

# We need to find and replace the existing broken measures
# First, let's see what patterns exist

# Strategy: Find lines containing "measure 'Calculated Start Date'" etc and replace the whole measure block
# Since measures might span multiple lines or have wrong syntax, we'll rebuild them

# Split content into lines for analysis
$lines = $tasksContent -split "`n"
Write-Host "`nAnalyzing lines for measure definitions..."

$newLines = @()
$skipUntilNextMeasureOrColumn = $false
$measureNames = @("Calculated Start Date", "Calculated End Date", "Is Milestone")
$measuresInserted = $false

for ($i = 0; $i -lt $lines.Count; $i++) {
    $line = $lines[$i]
    
    # Check if this line starts a measure we need to replace
    $isBrokenMeasure = $false
    foreach ($mName in $measureNames) {
        if ($line -match "measure\s+'$mName'") {
            $isBrokenMeasure = $true
            Write-Host "Found measure to replace at line $i : $line" -ForegroundColor Yellow
            break
        }
    }
    
    if ($isBrokenMeasure) {
        # Skip this line and any continuation lines (formatString on wrong place, etc)
        $skipUntilNextMeasureOrColumn = $true
        
        # Insert all corrected measures once (when we hit the first broken measure)
        if (-not $measuresInserted) {
            $newLines += ""
            $newLines += $correctMeasure1
            $newLines += ""
            $newLines += $correctMeasure2
            $newLines += ""
            $newLines += $correctMeasure3
            $measuresInserted = $true
            Write-Host "Inserted corrected measures" -ForegroundColor Green
        }
        continue
    }
    
    # If we're skipping, check if we hit a new non-measure definition
    if ($skipUntilNextMeasureOrColumn) {
        # Check if this is a new structural element (column, measure for different name, partition, etc)
        if ($line -match "^\s*(column|partition|measure\s+'(?!Calculated|Is Milestone))" -or 
            $line -match "^table\s+" -or
            ($line.Trim() -eq "" -and $i+1 -lt $lines.Count -and $lines[$i+1] -match "^\s*(column|partition)")) {
            $skipUntilNextMeasureOrColumn = $false
            # Check if blank line before column/partition
            if ($line.Trim() -eq "") {
                $newLines += $line
                continue
            }
        } else {
            # Still in the broken measure block, skip
            continue
        }
    }
    
    $newLines += $line
}

$newTasksContent = $newLines -join "`n"

# Clean up any multiple consecutive blank lines
$newTasksContent = $newTasksContent -replace "`n`n`n+", "`n`n"

Write-Host "`nNew tasks.tmdl content:" -ForegroundColor Yellow
Write-Host $newTasksContent

# Save for reference
$newTasksContent | Out-File -FilePath "tasks_tmdl_after_fix.txt" -Encoding UTF8

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 3: Update Definition" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Encode the new content
$newTasksPayload = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($newTasksContent))

# Update the parts array
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

Write-Host "Updating with $($updatedParts.Count) parts..."

$updateUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/semanticModels/$semanticModelId/updateDefinition"
Write-Host "POST $updateUrl"

try {
    $updateResponse = Invoke-WebRequest -Uri $updateUrl -Method POST -Headers $fabricHeaders -Body $updatePayload
    Write-Host "Update Status: $($updateResponse.StatusCode)" -ForegroundColor Green
    
    if ($updateResponse.StatusCode -eq 202) {
        $location = $updateResponse.Headers["Location"]
        if ($location -is [array]) { $location = $location[0] }
        $retryAfter = $updateResponse.Headers["Retry-After"]
        if ($retryAfter -is [array]) { $retryAfter = $retryAfter[0] }
        if (-not $retryAfter) { $retryAfter = 2 }
        
        Write-Host "LRO started for update, polling..."
        
        $maxAttempts = 30
        $attempt = 0
        while ($attempt -lt $maxAttempts) {
            $attempt++
            Start-Sleep -Seconds ([int]$retryAfter)
            
            $pollResponse = Invoke-WebRequest -Uri $location -Method GET -Headers $fabricHeaders
            Write-Host "Update poll $attempt : $($pollResponse.StatusCode)"
            
            if ($pollResponse.StatusCode -eq 200) {
                Write-Host "Update completed successfully!" -ForegroundColor Green
                break
            }
        }
    }
} catch {
    Write-Host "Update Error: $_" -ForegroundColor Red
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $errorBody = $reader.ReadToEnd()
        Write-Host "Error details: $errorBody" -ForegroundColor Red
    }
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 4: Trigger Refresh" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$refreshUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$semanticModelId/refreshes"
$refreshBody = '{"type":"full"}'

Write-Host "POST $refreshUrl"

try {
    $refreshResponse = Invoke-WebRequest -Uri $refreshUrl -Method POST -Headers $pbiHeaders -Body $refreshBody
    Write-Host "Refresh Status: $($refreshResponse.StatusCode)" -ForegroundColor Green
    
    # Wait for refresh to complete
    Write-Host "Waiting for refresh to complete..."
    $refreshStatusUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$semanticModelId/refreshes?`$top=1"
    
    $maxAttempts = 60
    $attempt = 0
    while ($attempt -lt $maxAttempts) {
        $attempt++
        Start-Sleep -Seconds 5
        
        $statusResponse = Invoke-RestMethod -Uri $refreshStatusUrl -Method GET -Headers $pbiHeaders
        $latestRefresh = $statusResponse.value[0]
        $status = $latestRefresh.status
        
        Write-Host "Refresh status ($attempt): $status"
        
        if ($status -eq "Completed") {
            Write-Host "Refresh completed successfully!" -ForegroundColor Green
            break
        } elseif ($status -eq "Failed") {
            Write-Host "Refresh failed!" -ForegroundColor Red
            Write-Host ($latestRefresh | ConvertTo-Json -Depth 5)
            break
        }
    }
} catch {
    Write-Host "Refresh Error: $_" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "STEP 5: Test DAX Query" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$daxUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$semanticModelId/executeQueries"
$daxBody = @{
    queries = @(
        @{ query = "EVALUATE ROW(`"test`", [Calculated Start Date])" }
    )
} | ConvertTo-Json -Depth 5

Write-Host "POST $daxUrl"
Write-Host "Query: EVALUATE ROW(`"test`", [Calculated Start Date])"

try {
    $daxResponse = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $daxBody
    Write-Host "`nDAX Query Result:" -ForegroundColor Green
    Write-Host ($daxResponse | ConvertTo-Json -Depth 10)
} catch {
    Write-Host "DAX Query Error: $_" -ForegroundColor Red
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $errorBody = $reader.ReadToEnd()
        Write-Host "Error details: $errorBody" -ForegroundColor Red
    }
}

# Test all three measures
Write-Host "`n--- Testing all measures ---" -ForegroundColor Cyan

$testQueries = @(
    "EVALUATE TOPN(3, SELECTCOLUMNS(tasks, `"ID`", [Task_ID], `"CalcStart`", [Calculated Start Date], `"CalcEnd`", [Calculated End Date], `"IsMilestone`", [Is Milestone]))"
)

foreach ($q in $testQueries) {
    Write-Host "`nQuery: $q"
    $testBody = @{
        queries = @(@{ query = $q })
    } | ConvertTo-Json -Depth 5
    
    try {
        $testResponse = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $pbiHeaders -Body $testBody
        Write-Host "Result:" -ForegroundColor Green
        Write-Host ($testResponse | ConvertTo-Json -Depth 10)
    } catch {
        Write-Host "Error: $_" -ForegroundColor Red
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "WORKFLOW COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
