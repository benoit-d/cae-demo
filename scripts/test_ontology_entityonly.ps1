param(
    [string]$WorkspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609",
    [string]$Name = "TestOntology_EntityOnly"
)
$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

function To-B64 { param($obj) [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($obj | ConvertTo-Json -Depth 20 -Compress))) }
function Get-Id { ([int64](Get-Random -Minimum 1000000000000 -Maximum 999999999999999)).ToString() }

$eId = Get-Id; $pKey = Get-Id; $pName = Get-Id
$entity = @{
    id = $eId; namespace = "usertypes"; baseEntityTypeId = $null; name = "Machine"
    entityIdParts = @($pKey); displayNamePropertyId = $pName
    namespaceType = "Custom"; visibility = "Visible"
    properties = @(
        @{ id = $pKey;  name = "machine_id";   redefines = $null; baseTypeNamespaceType = $null; valueType = "String" }
        @{ id = $pName; name = "machine_name"; redefines = $null; baseTypeNamespaceType = $null; valueType = "String" }
    )
    timeseriesProperties = @()
}

$parts = @(
    @{ path = "definition.json"; payload = (To-B64 @{}); payloadType = "InlineBase64" }
    @{ path = "EntityTypes/$eId/definition.json"; payload = (To-B64 $entity); payloadType = "InlineBase64" }
)

# Delete if exists
$existing = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies" -Headers $headers).value | ? { $_.displayName -eq $Name }
if ($existing) { Invoke-RestMethod -Method Delete -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies/$($existing.id)" -Headers $headers | Out-Null; Start-Sleep 10 }

$body = (@{ displayName = $Name; description = "entity only"; definition = @{ parts = $parts } } | ConvertTo-Json -Depth 20 -Compress)
$resp = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/ontologies" -Headers $headers -Body $body -SkipHttpErrorCheck
Write-Host "status=$($resp.StatusCode)"
Write-Host "content=$($resp.Content)"
$opId = $resp.Headers['x-ms-operation-id']
if ($opId) {
    Write-Host "opId=$opId"
    for ($i=0;$i -lt 20;$i++) { Start-Sleep 5; $p = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId" -Headers $headers; Write-Host $p.status; if ($p.status -in @('Succeeded','Failed','Cancelled')) { $p | ConvertTo-Json -Depth 10 | Write-Host; break } }
}
