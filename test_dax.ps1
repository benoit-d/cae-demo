$ErrorActionPreference = "Continue"
$env:AZURE_CORE_COLLECT_TELEMETRY = "false"

$pbiToken = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv
$workspaceId = "161c43a4-6a14-4b8f-81eb-070f0981a609"
$itemId = "2e76bd04-3994-485f-b2b6-847fea6aa0aa"
$daxUrl = "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$itemId/executeQueries"
$headers = @{ "Authorization" = "Bearer $pbiToken"; "Content-Type" = "application/json" }

# Full query with measures
$fullQuery = @{
    queries = @(
        @{
            query = "EVALUATE TOPN(5, SELECTCOLUMNS(tasks, `"Task_ID`", tasks[Task_ID], `"CalcStart`", [Calculated Start Date], `"CalcEnd`", [Calculated End Date], `"IsMilestone`", [Is Milestone]), [Task_ID], ASC)"
        }
    )
} | ConvertTo-Json -Depth 5

Write-Host "=== DAX Query with All Three Measures ===" -ForegroundColor Cyan
try {
    $response = Invoke-RestMethod -Uri $daxUrl -Method POST -Headers $headers -Body $fullQuery
    Write-Host "SUCCESS!" -ForegroundColor Green
    $response | ConvertTo-Json -Depth 10
} catch {
    Write-Host "FAILED!" -ForegroundColor Red
    Write-Host "Exception: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) {
        Write-Host "Error Details:"
        Write-Host $_.ErrorDetails.Message
    }
    # Try to get response body
    try {
        $stream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $reader.BaseStream.Position = 0
        $responseBody = $reader.ReadToEnd()
        Write-Host "Response Body:"
        Write-Host $responseBody
    } catch {
        Write-Host "Could not read response body"
    }
}
