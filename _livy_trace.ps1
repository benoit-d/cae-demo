$token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token" }
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$nbId = "89706039-4b7a-4cf9-8d38-9193a1360dd0"
# get livy session for the latest failure
$mon = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/notebooks/$nbId/livySessions" -Headers $headers
$latest = $mon.value[0]
Write-Host "livyId=$($latest.livyId) state=$($latest.state) reason=$($latest.cancellationReason)"
Write-Host "app=$($latest.sparkApplicationId)"
Write-Host "submitted=$($latest.submittedDateTime) running=$($latest.runningDuration.value)s"
# Try to get statement output
$app = $latest.sparkApplicationId
$attempts = @(
    "v1/workspaces/$ws/sparkmonitor/applications/$app",
    "v1/workspaces/$ws/spark/sessions/$($latest.livyId)",
    "v1/workspaces/$ws/spark/notebookSnapshots/$nbId/$($latest.livyId)",
    "v1/workspaces/$ws/spark/livySessions/$($latest.livyId)?detailed=true"
)
foreach ($p in $attempts) {
    Write-Host "`n--- $p ---"
    try {
        $r = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/$p" -Headers $headers
        ($r | ConvertTo-Json -Depth 6 -Compress).Substring(0, [Math]::Min(500, ($r | ConvertTo-Json -Depth 6 -Compress).Length))
    } catch {
        Write-Host "  $($_.Exception.Message)"
    }
}
