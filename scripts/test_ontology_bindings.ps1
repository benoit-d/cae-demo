param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$EventhouseItemId = "8f255656-5bd5-49dd-a4aa-0a7b7d597ee1",
    [string]$SqlDbItemId = "6c31cad3-74a3-4eae-91f3-e2a4ed845e7e",
    [string]$LakehouseItemId = "d9589856-ea47-4ecd-9057-878ea59da3c0",
    [string]$KqlDbName = "CAEManufacturingKQLDB",
    [ValidateSet("TSOnly","NonTS-SQLDB","NonTS-LH")]
    [string]$Variant = "TSOnly"
)
$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$eh = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/eventhouses/$EventhouseItemId" -Headers $headers
$kustoUri = $eh.properties.queryServiceUri

function To-B64 { param($obj) [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($obj | ConvertTo-Json -Depth 20 -Compress))) }
function Get-Id { ([int64](Get-Random -Minimum 1000000000000 -Maximum 999999999999999)).ToString() }

$eId = Get-Id; $pKey = Get-Id; $pName = Get-Id; $pTs = Get-Id; $pVal = Get-Id
$tsProps = @(
    @{ id = $pTs;  name = "ts_timestamp"; redefines = $null; baseTypeNamespaceType = $null; valueType = "DateTime" }
    @{ id = $pVal; name = "ts_value";     redefines = $null; baseTypeNamespaceType = $null; valueType = "Double" }
)
$entity = @{
    id = $eId; namespace = "usertypes"; baseEntityTypeId = $null; name = "Machine"
    entityIdParts = @($pKey); displayNamePropertyId = $pName
    namespaceType = "Custom"; visibility = "Visible"
    properties = @(
        @{ id = $pKey;  name = "machine_id";   redefines = $null; baseTypeNamespaceType = $null; valueType = "String" }
        @{ id = $pName; name = "machine_name"; redefines = $null; baseTypeNamespaceType = $null; valueType = "String" }
    )
    timeseriesProperties = @($tsProps)
}

$parts = @(
    @{ path = "definition.json"; payload = (To-B64 @{}); payloadType = "InlineBase64" }
    @{ path = "EntityTypes/$eId/definition.json"; payload = (To-B64 $entity); payloadType = "InlineBase64" }
)

switch ($Variant) {
    "TSOnly" {
        $b = @{ id=([guid]::NewGuid().ToString()); dataBindingConfiguration = @{
            dataBindingType = "TimeSeries"; timestampColumnName = "timestamp"
            propertyBindings = @(
                @{ sourceColumnName = "timestamp";  targetPropertyId = $pTs }
                @{ sourceColumnName = "value";      targetPropertyId = $pVal }
                @{ sourceColumnName = "machine_id"; targetPropertyId = $pKey }
            )
            sourceTableProperties = @{
                sourceType = "KustoTable"; workspaceId = $WorkspaceId; itemId = $EventhouseItemId
                clusterUri = $kustoUri; databaseName = $KqlDbName; sourceTableName = "MachineTelemetry"
            }
        } }
        $parts += @{ path = "EntityTypes/$eId/DataBindings/$($b.id).json"; payload = (To-B64 $b); payloadType = "InlineBase64" }
    }
    "NonTS-SQLDB" {
        $b = @{ id=([guid]::NewGuid().ToString()); dataBindingConfiguration = @{
            dataBindingType = "NonTimeSeries"
            propertyBindings = @(
                @{ sourceColumnName = "machine_id";   targetPropertyId = $pKey }
                @{ sourceColumnName = "machine_name"; targetPropertyId = $pName }
            )
            sourceTableProperties = @{
                sourceType = "LakehouseTable"; workspaceId = $WorkspaceId; itemId = $SqlDbItemId
                sourceTableName = "machines"; sourceSchema = "erp"
            }
        } }
        $parts += @{ path = "EntityTypes/$eId/DataBindings/$($b.id).json"; payload = (To-B64 $b); payloadType = "InlineBase64" }
    }
    "NonTS-LH" {
        $b = @{ id=([guid]::NewGuid().ToString()); dataBindingConfiguration = @{
            dataBindingType = "NonTimeSeries"
            propertyBindings = @(
                @{ sourceColumnName = "machine_id";   targetPropertyId = $pKey }
                @{ sourceColumnName = "machine_name"; targetPropertyId = $pName }
            )
            sourceTableProperties = @{
                sourceType = "LakehouseTable"; workspaceId = $WorkspaceId; itemId = $LakehouseItemId
                sourceTableName = "machines"
            }
        } }
        $parts += @{ path = "EntityTypes/$eId/DataBindings/$($b.id).json"; payload = (To-B64 $b); payloadType = "InlineBase64" }
    }
}

$Name = "TestOntology_" + ($Variant -replace "-","_")
$existing = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies" -Headers $headers).value | ? { $_.displayName -eq $Name }
if ($existing) { Invoke-RestMethod -Method Delete -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies/$($existing.id)" -Headers $headers | Out-Null; Start-Sleep 10 }

$body = (@{ displayName = $Name; description = $Variant; definition = @{ parts = $parts } } | ConvertTo-Json -Depth 20 -Compress)
$resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies" -Headers $headers -Body $body -SkipHttpErrorCheck
Write-Host "[$Variant] status=$($resp.StatusCode)"; Write-Host $resp.Content
$opId = $resp.Headers['x-ms-operation-id']
if ($opId) {
    for ($i=0;$i -lt 25;$i++) { Start-Sleep 5; $p = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $headers; Write-Host "  $($p.status)"; if ($p.status -in @('Succeeded','Failed','Cancelled')) { $p | ConvertTo-Json -Depth 10 | Write-Host; break } }
}
