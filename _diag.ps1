$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token" }
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$nbId = "5ebb2b67-3a70-4a61-b8c9-5d79a0d925af"  # AnomalyDetection (works)
$r = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items/$nbId/getDefinition" -Headers $headers -SkipHttpErrorCheck
$opId = $r.Headers['x-ms-operation-id']
Start-Sleep 6
$def = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/operations/$opId/result" -Headers $headers
$nb = $def.definition.parts | Where-Object { $_.path -like 'notebook-content*' }
$content = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($nb.payload))
Write-Host "=== AnomalyDetection (WORKS) first 15 lines ==="
($content -split "`n")[0..15] -join "`n"
