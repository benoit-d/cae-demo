$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"
# list items to find AnomalyDetection notebook id
$items = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items" -Headers $headers
$ad = $items.value | Where-Object { $_.displayName -eq 'AnomalyDetection' -and $_.type -eq 'Notebook' }
Write-Host "AnomalyDetection id=$($ad.id)"
$nbId = $ad.id
$r = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items/$nbId/jobs/instances?jobType=RunNotebook" -Headers $headers -Body "{}" -SkipHttpErrorCheck
$iid = ($r.Headers['Location'] -split '/')[-1]
Write-Host "iid=$iid"
for ($i=0; $i -lt 20; $i++) {
    Start-Sleep 20
    $st = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items/$nbId/jobs/instances/$iid" -Headers $headers
    Write-Host "[$($i*20)s] $($st.status)"
    if ($st.status -in 'Completed','Failed','Cancelled') { Write-Host "reason=$($st.failureReason.message)"; break }
}
