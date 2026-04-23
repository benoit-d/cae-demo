$ErrorActionPreference = "Stop"
$ws = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$it = "28e26ad1-2baf-47cb-957b-275f7634cbaf"

Write-Host "Getting access token..."
$t = az account get-access-token --resource "https://api.fabric.microsoft.com" -o tsv --query accessToken
Write-Host "Token obtained. Length: $($t.Length)"

Write-Host "`nCalling getDefinition..."
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$ws/items/$it/getDefinition"
$resp = Invoke-WebRequest -Uri $uri -Method POST -Headers @{"Authorization"="Bearer $t"} -UseBasicParsing

Write-Host "Response status: $($resp.StatusCode)"

if ($resp.StatusCode -eq 202) {
    $location = $resp.Headers["Location"]
    if ($location -is [array]) { $location = $location[0] }
    Write-Host "Async operation - polling: $location"
    
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 2
        $pollResp = Invoke-WebRequest -Uri $location -Method GET -Headers @{"Authorization"="Bearer $t"} -UseBasicParsing
        $data = $pollResp.Content | ConvertFrom-Json
        
        if ($data.status) {
            Write-Host "  Attempt $i - Status: $($data.status)"
            if ($data.status -ne "Running") {
                Write-Host "Operation completed with status: $($data.status)"
                break
            }
        } else {
            Write-Host "`n=== Definition Retrieved ==="
            Write-Host "Parts:"
            foreach ($part in $data.definition.parts) {
                Write-Host "  - $($part.path) (payload: $($part.payload.Length) chars)"
            }
            Write-Host "`nNotebook definition successfully retrieved!"
            break
        }
    }
} elseif ($resp.StatusCode -eq 200) {
    $data = $resp.Content | ConvertFrom-Json
    Write-Host "=== Definition Retrieved ==="
    Write-Host "Parts:"
    foreach ($part in $data.definition.parts) {
        Write-Host "  - $($part.path) (payload: $($part.payload.Length) chars)"
    }
}
