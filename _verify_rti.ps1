$cluster = "https://trd-d7uc0kt9eex2bc7e1q.z9.kusto.fabric.microsoft.com"
$tenant  = "9234ce32-5dd2-4ca3-be92-120f1f359289"
$token   = az account get-access-token --tenant $tenant --resource $cluster --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

# find DB
$mgmt = Invoke-RestMethod -Method Post -Uri "$cluster/v1/rest/mgmt" -Headers $headers -Body (@{csl=".show databases"}|ConvertTo-Json -Compress)
$db = @($mgmt.Tables[0].Rows)[0][0]
Write-Host "Using db: $db"
Write-Host "Using db: $db`n"

function Q($csl) {
    $body = @{ db=$db; csl=$csl } | ConvertTo-Json -Compress
    (Invoke-RestMethod -Method Post -Uri "$cluster/v2/rest/query" -Headers $headers -Body $body) |
        Where-Object { $_.TableKind -eq 'PrimaryResult' }
}

Write-Host "=== CNC-003 last 20 min by alert_level ==="
$t = Q "MachineTelemetry | where machine_id == 'CNC-003' and timestamp > ago(20m) | summarize cnt=count(), firstTs=min(timestamp), lastTs=max(timestamp) by tostring(alert_level) | order by alert_level"
if ($t) { $t.Rows | ForEach-Object { "{0,-10} {1,5}  {2} -> {3}" -f $_[0], $_[1], $_[2], $_[3] } }

Write-Host "`n=== CNC-003 Critical readings (last 20m) ==="
$t = Q "MachineTelemetry | where machine_id == 'CNC-003' and tostring(alert_level) == 'Critical' and timestamp > ago(20m) | project timestamp, sensor_name, value, unit | order by timestamp asc | take 20"
if ($t) { $t.Rows | ForEach-Object { "{0}  {1,-25} {2,8} {3}" -f $_[0], $_[1], $_[2], $_[3] } } else { "(no rows returned)" }

Write-Host "`n=== AnomalyAlerts last 20m ==="
try {
    $t = Q "AnomalyAlerts | where ingestion_time() > ago(20m) | project timestamp, machine_id, severity=tostring(severity), anomaly_type | order by timestamp desc | take 20"
    if ($t) { $t.Rows | ForEach-Object { "{0}  {1,-8} {2,-10} {3}" -f $_[0], $_[1], $_[2], $_[3] } } else { "(no rows)" }
} catch { "(table missing or query error: $($_.Exception.Message))" }

Write-Host "`n=== AnomalyDetection table last 20m ==="
try {
    $t = Q "AnomalyDetection | where ingestion_time() > ago(20m) | project timestamp, machine_id, anomaly_confidence_pct, anomaly_type, severity=tostring(severity) | order by timestamp desc | take 20"
    if ($t) { $t.Rows | ForEach-Object { "{0}  {1,-8} {2,6}%  {3,-15} {4}" -f $_[0], $_[1], $_[2], $_[3], $_[4] } } else { "(no rows)" }
} catch { "(err: $($_.Exception.Message))" }
