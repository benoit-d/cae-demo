# Standalone test: attempt to create a minimal Fabric Ontology with
# one entity bound to a SQL DB table. Variants test different sourceType/itemId
# combinations to see which one the API accepts.

param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$SqlDbItemId = "6c31cad3-74a3-4eae-91f3-e2a4ed845e7e",
    [string]$SqlEndpointItemId = "2578ea28-0f61-49f3-bab4-b6dfdef1d49a",
    [string]$LakehouseItemId = "d9589856-ea47-4ecd-9057-878ea59da3c0",
    [string]$EventhouseItemId = "8f255656-5bd5-49dd-a4aa-0a7b7d597ee1",
    [string]$KqlDbName = "CAEManufacturingKQLDB",
    [string]$Variant = "SQLDB"   # SQLDB | SQLEndpoint | Lakehouse
)

$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

# Resolve Kusto cluster URI for timeseries binding
try {
    $eh = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventhouses/$EventhouseItemId" -Headers $headers
    $kustoUri = $eh.properties.queryServiceUri
    Write-Host "Kusto URI: $kustoUri"
} catch {
    Write-Host "Eventhouse detail error: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) { Write-Host $_.ErrorDetails.Message }
    $kustoUri = ""
}

function To-B64 { param($obj) [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($obj | ConvertTo-Json -Depth 20 -Compress))) }

# Pick item/id/type based on variant
switch ($Variant) {
    "SQLDB"       { $targetItemId = $SqlDbItemId;       $sourceType = "LakehouseTable"; $schema = "erp" }
    "SQLEndpoint" { $targetItemId = $SqlEndpointItemId; $sourceType = "LakehouseTable"; $schema = "erp" }
    "Lakehouse"   { $targetItemId = $LakehouseItemId;   $sourceType = "LakehouseTable"; $schema = "dbo" }
}

Write-Host "Variant: $Variant  itemId=$targetItemId  sourceType=$sourceType  schema=$schema"

# Build IDs
$rand = New-Object Random 42
function Get-Id { $script:rand.NextInt64(1000000000000, 999999999999999).ToString() }
# .NET Random on Windows ps may not have NextInt64 under older versions — fallback:
function Get-Id { ([int64](Get-Random -Minimum 1000000000000 -Maximum 999999999999999)).ToString() }

$entityId      = Get-Id
$pMachineId    = Get-Id
$pMachineName  = Get-Id
$pTsTimestamp  = Get-Id
$pTsValue      = Get-Id

$entityDef = @{
    id = $entityId
    namespace = "usertypes"
    baseEntityTypeId = $null
    name = "Machine"
    entityIdParts = @($pMachineId)
    displayNamePropertyId = $pMachineName
    namespaceType = "Custom"
    visibility = "Visible"
    properties = @(
        @{ id = $pMachineId;   name = "machine_id";   redefines = $null; baseTypeNamespaceType = $null; valueType = "String" },
        @{ id = $pMachineName; name = "machine_name"; redefines = $null; baseTypeNamespaceType = $null; valueType = "String" }
    )
    timeseriesProperties = @(
        @{ id = $pTsTimestamp; name = "MachineTelemetry_timestamp"; redefines = $null; baseTypeNamespaceType = $null; valueType = "DateTime" },
        @{ id = $pTsValue;     name = "MachineTelemetry_value";     redefines = $null; baseTypeNamespaceType = $null; valueType = "Double" }
    )
}

$nonTsBinding = @{
    id = ([guid]::NewGuid().ToString())
    dataBindingConfiguration = @{
        dataBindingType = "NonTimeSeries"
        propertyBindings = @(
            @{ sourceColumnName = "machine_id";   targetPropertyId = $pMachineId },
            @{ sourceColumnName = "machine_name"; targetPropertyId = $pMachineName }
        )
        sourceTableProperties = @{
            sourceType = $sourceType
            workspaceId = $WorkspaceId
            itemId = $targetItemId
            sourceTableName = "machines"
            sourceSchema = $schema
        }
    }
}

$tsBinding = @{
    id = ([guid]::NewGuid().ToString())
    dataBindingConfiguration = @{
        dataBindingType = "TimeSeries"
        timestampColumnName = "timestamp"
        propertyBindings = @(
            @{ sourceColumnName = "timestamp";  targetPropertyId = $pTsTimestamp },
            @{ sourceColumnName = "value";      targetPropertyId = $pTsValue },
            @{ sourceColumnName = "machine_id"; targetPropertyId = $pMachineId }
        )
        sourceTableProperties = @{
            sourceType = "KustoTable"
            workspaceId = $WorkspaceId
            itemId = $EventhouseItemId
            clusterUri = $kustoUri
            databaseName = $KqlDbName
            sourceTableName = "MachineTelemetry"
        }
    }
}

$parts = @(
    @{ path = "definition.json";                                                   payload = (To-B64 @{});          payloadType = "InlineBase64" },
    @{ path = "EntityTypes/$entityId/definition.json";                             payload = (To-B64 $entityDef);    payloadType = "InlineBase64" },
    @{ path = "EntityTypes/$entityId/DataBindings/$($nonTsBinding.id).json";       payload = (To-B64 $nonTsBinding); payloadType = "InlineBase64" },
    @{ path = "EntityTypes/$entityId/DataBindings/$($tsBinding.id).json";          payload = (To-B64 $tsBinding);    payloadType = "InlineBase64" }
)

$displayName = "TestOntology_$Variant"
# Delete if exists
try {
    $existing = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies" -Headers $headers).value | Where-Object { $_.displayName -eq $displayName }
    if ($existing) {
        Write-Host "Deleting existing $displayName ($($existing.id))"
        Invoke-RestMethod -Method Delete -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies/$($existing.id)" -Headers $headers | Out-Null
        Start-Sleep -Seconds 15
    }
} catch { Write-Host "List/Delete skipped: $($_.Exception.Message)" }

$payload = @{
    displayName = $displayName
    description = "Variant=$Variant test"
    definition  = @{ parts = $parts }
}
$body = $payload | ConvertTo-Json -Depth 20 -Compress

Write-Host "`n>>> POST ontology (body $($body.Length) bytes)"
try {
    $resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies" -Headers $headers -Body $body
    Write-Host "Status: $($resp.StatusCode)"
    Write-Host "Body: $($resp.Content)"
    if ($resp.Headers.Location) {
        $loc = $resp.Headers.Location
        Write-Host "LRO: $loc"
        for ($i = 0; $i -lt 30; $i++) {
            Start-Sleep -Seconds 5
            $poll = Invoke-RestMethod -Uri $loc -Headers $headers
            $st = $poll.status
            Write-Host "  status=$st"
            if ($st -in @("Succeeded","Failed","Cancelled")) {
                $poll | ConvertTo-Json -Depth 10 | Write-Host
                if ($st -eq "Failed") {
                    try {
                        $res = Invoke-WebRequest -Uri "$loc/result" -Headers $headers -SkipHttpErrorCheck
                        Write-Host "result body: $($res.Content)"
                    } catch { Write-Host "no result body: $($_.Exception.Message)" }
                }
                break
            }
        }
    }
} catch {
    Write-Host "POST ERR: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) { Write-Host "Details: $($_.ErrorDetails.Message)" }
}
