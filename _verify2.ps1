$cluster = "https://trd-d7uc0kt9eex2bc7e1q.z9.kusto.fabric.microsoft.com"
$tenant  = "9234ce32-5dd2-4ca3-be92-120f1f359289"
$token   = az account get-access-token --tenant $tenant --resource $cluster --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
$db = "f0d1437e-f6e4-4c73-8b41-2f71c228d2de"

$mgmt = Invoke-RestMethod -Method Post -Uri "$cluster/v1/rest/mgmt" -Headers $headers -Body (@{db=$db; csl=".show tables"}|ConvertTo-Json -Compress)
Write-Host "=== Tables ==="
$mgmt.Tables[0].Rows | ForEach-Object { $_[0] }

function Q($csl) {
    $body = @{ db=$db; csl=$csl } | ConvertTo-Json -Compress
    try { (Invoke-RestMethod -Method Post -Uri "$cluster/v2/rest/query" -Headers $headers -Body $body) | Where-Object { $_.TableKind -eq 'PrimaryResult' } }
    catch { Write-Host "  err: $($_.Exception.Message)"; $null }
}

Write-Host "`n=== CNC-003 by minute bucket last 20m (Warning+Critical) ==="
$t = Q "MachineTelemetry | where machine_id=='CNC-003' and timestamp > ago(20m) | summarize n=countif(alert_level=='Normal'), w=countif(alert_level=='Warning'), c=countif(alert_level=='Critical') by bin(timestamp,1m) | order by timestamp asc"
if ($t) { "timestamp                Normal Warn Crit"; $t.Rows | ForEach-Object { "{0}  {1,5} {2,4} {3,4}" -f $_[0], $_[1], $_[2], $_[3] } }

Write-Host "`n=== CNC-003 per-sensor max/min last 15m (to see if near Critical) ==="
$t = Q "MachineTelemetry | where machine_id=='CNC-003' and timestamp > ago(15m) | summarize mn=min(value), mx=max(value), warn=countif(alert_level=='Warning'), crit=countif(alert_level=='Critical') by sensor_name, unit | order by sensor_name asc"
if ($t) { "{0,-25} {1,10} {2,10} {3,5} {4,5} {5}" -f 'sensor','min','max','W','C','unit'; $t.Rows | ForEach-Object { "{0,-25} {1,10} {2,10} {3,5} {4,5} {5}" -f $_[0], $_[2], $_[3], $_[4], $_[5], $_[1] } }
