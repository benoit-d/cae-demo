$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$nbId = "89706039-4b7a-4cf9-8d38-9193a1360dd0"
$r = Invoke-WebRequest -Method Post -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items/$nbId/jobs/instances?jobType=RunNotebook" -Headers $headers -Body "{}" -SkipHttpErrorCheck
$iid = ($r.Headers['Location'] -split '/')[-1]
$start = (Get-Date).ToUniversalTime()
Write-Host "startUtc=$start iid=$iid"
for ($i=0; $i -lt 40; $i++) {
    Start-Sleep 20
    $st = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/items/$nbId/jobs/instances/$iid" -Headers $headers
    $mm = [int]($i*20/60); $ss = ($i*20) % 60
    Write-Host "[t+${mm}m${ss}s] $($st.status)"
    if ($st.status -in 'Completed','Failed','Cancelled') {
        Write-Host "end=$($st.endTimeUtc) reason=$($st.failureReason.message)"
        break
    }
}
