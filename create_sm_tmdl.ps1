# Create Semantic Model with TMDL Definition
# Workspace: 161c43a4-6a14-4b8f-81eb-070f0981a609
# SQL Database: 6c31cad3-74a3-4eae-91f3-e2a4ed845e7e
# Data Folder: 645a5345-2ccb-4926-9453-0a03184c936b

$ErrorActionPreference = "Stop"
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$smName = "CAEManufacturing"
$folderId = "645a5345-2ccb-4926-9453-0a03184c936b"

# Get token
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

Write-Host "=== Step 1: Check if semantic model '$smName' exists ===" -ForegroundColor Cyan

# List semantic models in workspace
$listUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items?type=SemanticModel"
try {
    $response = Invoke-RestMethod -Uri $listUrl -Headers $headers -Method Get
    $existingSM = $response.value | Where-Object { $_.displayName -eq $smName }
    
    if ($existingSM) {
        Write-Host "Found existing semantic model: $($existingSM.id)" -ForegroundColor Yellow
        
        # Delete it
        Write-Host "=== Step 2: Deleting existing semantic model ===" -ForegroundColor Cyan
        $deleteUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items/$($existingSM.id)"
        try {
            $deleteResponse = Invoke-WebRequest -Uri $deleteUrl -Headers $headers -Method Delete
            Write-Host "Delete HTTP Status: $($deleteResponse.StatusCode)" -ForegroundColor Green
            Start-Sleep -Seconds 3
        } catch {
            Write-Host "Delete Error: $($_.Exception.Message)" -ForegroundColor Red
            if ($_.Exception.Response) {
                $reader = [System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream())
                $errorBody = $reader.ReadToEnd()
                Write-Host "Delete Error Body: $errorBody" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "No existing semantic model found with name '$smName'" -ForegroundColor Green
    }
} catch {
    Write-Host "Error listing items: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Step 3: Create semantic model with TMDL definition ===" -ForegroundColor Cyan

# Build TMDL content with TABS for indentation
# Using @' '@ here-strings to preserve literal tabs

$definitionPbism = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json","version":"4.2","settings":{}}'

$databaseTmdl = @'
database
	compatibilityLevel: 1604
'@

$modelTmdl = @'
model Model
	culture: en-US
	defaultPowerBIDataSourceVersion: powerBI_V3
	sourceQueryCulture: en-US
	dataAccessOptions
		legacyRedirects
		returnErrorValuesAsNull

annotation PBI_QueryOrder = ["DirectLake - CAEManufacturing_SQLDB"]

annotation __PBI_TimeIntelligenceEnabled = 1

ref table employees
ref table production_lines
ref table machines
ref table machine_jobs
ref table simulators
ref table maintenance_history
ref table projects
ref table tasks
'@

$expressionsTmdl = @'
expression 'DirectLake - CAEManufacturing_SQLDB' =
		let
			Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/161c43a4-6a14-4b8f-81eb-070f0981a609/6c31cad3-74a3-4eae-91f3-e2a4ed845e7e", [HierarchicalNavigation=true])
		in
			Source
'@

$relationshipsTmdl = @'
relationship 'Tasks to Projects'
	relyOnReferentialIntegrity
	fromColumn: tasks.Parent_Project_ID
	toColumn: projects.Project_ID
'@

$employeesTmdl = @'
table employees
	sourceLineageTag: [hr].[employees]

	column employee_id
		dataType: string
		summarizeBy: none
		sourceColumn: employee_id

	partition employees = entity
		mode: directLake
		source
			entityName: employees
			schemaName: hr
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$productionLinesTmdl = @'
table production_lines
	sourceLineageTag: [erp].[production_lines]

	column production_line_id
		dataType: string
		summarizeBy: none
		sourceColumn: production_line_id

	partition production_lines = entity
		mode: directLake
		source
			entityName: production_lines
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$machinesTmdl = @'
table machines
	sourceLineageTag: [erp].[machines]

	column Machine_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Machine_ID

	column Machine_Name
		dataType: string
		summarizeBy: none
		sourceColumn: Machine_Name

	partition machines = entity
		mode: directLake
		source
			entityName: machines
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$machineJobsTmdl = @'
table machine_jobs
	sourceLineageTag: [erp].[machine_jobs]

	column Job_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Job_ID

	partition machine_jobs = entity
		mode: directLake
		source
			entityName: machine_jobs
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$simulatorsTmdl = @'
table simulators
	sourceLineageTag: [erp].[simulators]

	column Simulator_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Simulator_ID

	partition simulators = entity
		mode: directLake
		source
			entityName: simulators
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$maintenanceHistoryTmdl = @'
table maintenance_history
	sourceLineageTag: [erp].[maintenance_history]

	column Maintenance_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Maintenance_ID

	partition maintenance_history = entity
		mode: directLake
		source
			entityName: maintenance_history
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$projectsTmdl = @'
table projects
	sourceLineageTag: [erp].[projects]

	column Project_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Project_ID

	column Project_Name
		dataType: string
		summarizeBy: none
		sourceColumn: Project_Name

	partition projects = entity
		mode: directLake
		source
			entityName: projects
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

$tasksTmdl = @'
table tasks
	sourceLineageTag: [erp].[tasks]

	column Task_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Task_ID

	column Task_Name
		dataType: string
		summarizeBy: none
		sourceColumn: Task_Name

	column Parent_Project_ID
		dataType: string
		summarizeBy: none
		sourceColumn: Parent_Project_ID

	column Standard_Duration
		dataType: int64
		formatString: 0
		summarizeBy: sum
		sourceColumn: Standard_Duration

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

	column Is_Milestone
		dataType: boolean
		formatString: """TRUE"";""TRUE"";""FALSE"""
		summarizeBy: none
		sourceColumn: Is_Milestone

	partition tasks = entity
		mode: directLake
		source
			entityName: tasks
			schemaName: erp
			expressionSource: 'DirectLake - CAEManufacturing_SQLDB'
'@

# Build parts array
$parts = @(
    @{
        path = "definition.pbism"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($definitionPbism))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/database.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($databaseTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/model.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($modelTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/expressions.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($expressionsTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/relationships.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($relationshipsTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/employees.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($employeesTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/production_lines.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($productionLinesTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/machines.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($machinesTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/machine_jobs.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($machineJobsTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/simulators.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($simulatorsTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/maintenance_history.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($maintenanceHistoryTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/projects.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($projectsTmdl))
        payloadType = "InlineBase64"
    },
    @{
        path = "definition/tables/tasks.tmdl"
        payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($tasksTmdl))
        payloadType = "InlineBase64"
    }
)

# Build request body
$body = @{
    displayName = $smName
    type = "SemanticModel"
    definition = @{
        format = "TMDL"
        parts = $parts
    }
    folderId = $folderId
} | ConvertTo-Json -Depth 10

# Save payload for debugging
$body | Out-File -FilePath "c:\Repo\cae-demo\sm_create_payload.json" -Encoding UTF8
Write-Host "Payload saved to sm_create_payload.json" -ForegroundColor Gray

# Create semantic model
$createUrl = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items"

try {
    $createResponse = Invoke-WebRequest -Uri $createUrl -Headers $headers -Method Post -Body $body
    Write-Host "Create HTTP Status: $($createResponse.StatusCode)" -ForegroundColor Green
    
    # Check for LRO
    $locationHeader = $createResponse.Headers["Location"]
    $retryAfter = $createResponse.Headers["Retry-After"]
    $operationId = $createResponse.Headers["x-ms-operation-id"]
    
    Write-Host "Location Header: $locationHeader"
    Write-Host "Retry-After: $retryAfter"
    Write-Host "Operation ID: $operationId"
    
    if ($createResponse.StatusCode -eq 202 -and $locationHeader) {
        Write-Host ""
        Write-Host "=== Step 4: Polling LRO for completion ===" -ForegroundColor Cyan
        
        $maxRetries = 30
        $retryCount = 0
        $pollInterval = if ($retryAfter) { [int]$retryAfter } else { 5 }
        
        while ($retryCount -lt $maxRetries) {
            Start-Sleep -Seconds $pollInterval
            $retryCount++
            
            try {
                $pollResponse = Invoke-WebRequest -Uri $locationHeader -Headers $headers -Method Get
                Write-Host "Poll $retryCount - HTTP Status: $($pollResponse.StatusCode)"
                
                $pollContent = $pollResponse.Content | ConvertFrom-Json
                Write-Host "LRO Status: $($pollContent.status)"
                
                if ($pollContent.status -eq "Succeeded") {
                    Write-Host ""
                    Write-Host "=== SUCCESS ===" -ForegroundColor Green
                    Write-Host "Semantic model created successfully!" -ForegroundColor Green
                    $pollContent | ConvertTo-Json -Depth 5
                    break
                } elseif ($pollContent.status -eq "Failed") {
                    Write-Host ""
                    Write-Host "=== FAILED ===" -ForegroundColor Red
                    Write-Host "LRO Failed!" -ForegroundColor Red
                    $pollContent | ConvertTo-Json -Depth 10
                    break
                }
            } catch {
                Write-Host "Poll Error: $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }
    } else {
        # Synchronous success
        Write-Host ""
        Write-Host "=== SUCCESS (Synchronous) ===" -ForegroundColor Green
        $createResponse.Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
    }
} catch {
    $statusCode = $_.Exception.Response.StatusCode.value__
    Write-Host ""
    Write-Host "=== CREATE ERROR ===" -ForegroundColor Red
    Write-Host "HTTP Status Code: $statusCode" -ForegroundColor Red
    Write-Host "Error Message: $($_.Exception.Message)" -ForegroundColor Red
    
    if ($_.Exception.Response) {
        try {
            $reader = [System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream())
            $errorBody = $reader.ReadToEnd()
            Write-Host ""
            Write-Host "Full Error Body:" -ForegroundColor Red
            Write-Host $errorBody -ForegroundColor Red
            
            # Try to parse as JSON for better formatting
            try {
                $errorJson = $errorBody | ConvertFrom-Json
                Write-Host ""
                Write-Host "Parsed Error:" -ForegroundColor Red
                $errorJson | ConvertTo-Json -Depth 10
            } catch {}
        } catch {
            Write-Host "Could not read error response stream" -ForegroundColor Red
        }
    }
}
