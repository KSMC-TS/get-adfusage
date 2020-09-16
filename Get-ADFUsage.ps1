<#
.DESCRIPTION
- This script should pull Azure Data Factory (v2) pipeline executions for the specified date range and return usage.
- Consumption costs are calculated pulling current pricing data from the Azure Pricing API: https://azure.microsoft.com/api/v2/pricing/data-factory/calculator
- Requires the Az module.
- This will take some time to execute. Make sure you are running this on a host with reliable connectivity. Processing 175,000 activity runs can take roughly 2-3 hours.
.PARAMETER subscriptionId
- Specify the subscription ID the data factory is located in.
.PARAMETER resourceGroup
- Specify the resource group name that the data factory is located in.
.PARAMETER factoryName
- Specify the data factory name.
.PARAMETER startDays
- How many days back from script execution would you like to capture usage information.
- Defaults to 7 days and if that works, you don't need to specify this parameter.
.PARAMETER endDays
- Specify number of days back to end filter range.
- Defaults to 0 days to include runs up to script execution.
.PARAMETER exportPath
- Location and filename to drop exported CSV in file system.
.EXAMPLE
.\Get-ADFUsage -subscriptionId "a0a0a0a0-1111-1a23-b456-7890cdef1234" `
-resourceGroup "adf-rg" `
-factoryName "adf-rg-adf1" `
-startDays 5 `
-endDays 1 `
-exportPath "C:\temp\file.csv"
- This example will get usage for the ADF, adf-rg-adf1, from 5 days ago to 1 day ago and save the CSV to C:\temp\file.csv.
.NOTES
    Version:        0.2
    Last updated:   09/16/2020
    Modified by:    Zachary Choate
    URL:            https://github.com/KSMC-TS/get-adfusage
#>

#Requires -Modules Az

[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    [string]
    $subscriptionId,
    [Parameter(Mandatory=$true)]
    [string]
    $resourceGroup,
    [Parameter(Mandatory=$true)]
    [string]
    $factoryName,
    [Parameter(Mandatory=$false)]
    [int]
    $startDays = 7,
    [Parameter(Mandatory=$false)]
    [int]
    $endDays = 0,
    [Parameter(Mandatory=$true)]
    [string]
    $exportPath
)


## Auth function
function Get-AccessToken {

    param(
        [Parameter(Mandatory=$false)]
        [string]
        $tokenExpiration,
        [Parameter(Mandatory=$true)]
        [string]
        $subscriptionId
    )
    $ticks = (Get-Date).AddMinutes(5).ToUniversalTime().Ticks
    If(($tokenExpiration -lt $ticks) -or (-not $tokenExpiration)) {
        Try {
            Get-AzSubscription -SubscriptionId $subscriptionId | Set-AzContext
        } Catch {
            Write-Output "Something went wrong. Make sure you're connected to the Azure AD account using Connect-AzAccount. $($_.Exception.Message)"
            Pause
            Exit
        }
    }
    $context = Get-AzContext
    $cacheItems = $context.TokenCache.ReadItems()
    $cachedContext = ($cacheItems | Where-Object {$_.Resource -eq "https://management.core.windows.net/" -and $_.TenantId -eq $context.Tenant.Id -and $_.ExpiresOn.Ticks -gt (Get-Date).Ticks})[0]
    $accessHeader = @{"Authorization" = "Bearer $($cachedContext.AccessToken)"}
    $tokenExpiration = $cachedContext.ExpiresOn.Ticks
    $tokenObj = [PSCustomObject]@{
        Header = $accessHeader
        Expiration = $tokenExpiration
    }

    return $tokenObj
    
}

# Get the initial access token.
$token = Get-AccessToken -SubscriptionId $subscriptionId

# Set variables for API endpoints
$azureEndpoint = "https://management.azure.com/"
$subscriptions = "/subscriptions/$subscriptionId"
$resourceGroups = "/resourcegroups/$resourceGroup"
$providers = "/providers/Microsoft.DataFactory"
$factories = "/factories/$factoryName"
$apiVersion = "api-version=2018-06-01"

# Get time in ISO 8601 format and create filter for date range
$lastUpdatedAfter = Get-Date (Get-Date).AddDays(-($startDays)) -Format "o"
$lastUpdatedBefore = Get-Date (Get-Date).AddDays(-($endDays)) -Format "o"
$filterJson = [ordered]@{
    lastUpdatedAfter = $lastUpdatedAfter
    lastUpdatedBefore = $lastUpdatedBefore
} | ConvertTo-Json

# Check for valid token
$token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
# Begin discovery of pipeline runs that fall within date range specified. Return to pipeline details to array for further processing.
$pipelineRunObj = @()
$pipelineRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories/queryPipelineRuns?RunStart=`"$runStartAfter`"&$apiVersion" -Headers $token.Header -Method POST -Body $filterJson -ContentType 'application/json'
$pipelineRunObj += $pipelineRuns.Value
# Iterate through pages returned by API - only 100 results returned per request. Do this until there isn't any more results (when the continuationToken isn't passed with the response).
Do {
    $pipelineFilterJsonObj = $filterJson | ConvertFrom-Json 
    $pipelineFilterJsonObj | Add-Member -MemberType NoteProperty -Name continuationToken -Value $pipelineRuns.continuationToken
    $pipelineFilterBody = $pipelineFilterJsonObj | ConvertTo-Json
    $token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
    $pipelineRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories/queryPipelineRuns?RunStart=`"$runStartAfter`"&$apiVersion" -Headers $token.Header -Method POST -Body $pipelineFilterBody -ContentType 'application/json'
    $pipelineRunObj += $pipelineRuns.Value
} while (-not ([string]::IsNullOrEmpty($pipelineRuns.continuationToken)))

$runSummary = @()

$i = 1

Write-Output "$($pipelineRunObj.count) pipelines were found for the date range specified."

# Go through each pipeline and build an object for each pipeline that has activity run details. Return to array of objects.
# Ideally need to get this to setup to run in parallel to dramatically reduce runtime of script.
ForEach($runObj in $pipelineRunObj) {
    # Build the query for the pipeline runId
    $runId = "/pipelineruns/$($runObj.RunId)"
    
    # Provide status update
    Write-Output "$i of $($pipelineRunObj.count)..."
    Write-Output "Working on $($runObj.RunId) - $($runObj.PipelineName)"

    $activityRunObj = @()
    # Validate token is still valid and acquire a new one if near expiration
    $token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
    # Get activity runs that the pipeline executed. This is where the actual usage information is located. 
    $activityRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories$runId/queryactivityruns?$apiVersion" -Headers $token.Header -Method POST -Body $filterJson -ContentType 'application/json'
    $activityRunObj += $activityRuns.Value
    # Same situation as pipeline run details - have to iterate through each return of 100 results.
    Do {
        $activityFilterJsonObj = $filterJson | ConvertFrom-Json
        $activityFilterJsonObj | Add-Member -MemberType NoteProperty -Name continuationToken -Value $activityRuns.continuationToken
        $activityFilterBody = $activityFilterJsonObj | ConvertTo-Json
        $token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
        $activityRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories$runId/queryactivityruns?$apiVersion" -Headers $token.Header -Method POST -Body $activityFilterBody -ContentType 'application/json'
        $activityRunObj += $($activityRuns.Value)
    } while (-not ([string]::IsNullOrEmpty($activityRuns.continuationToken)))
    
    # Provide more status updates.
    Write-Output "$($activityRunObj.Output.Count) activity runs were found. Working on organizing usage details.`n`n"

    # Get usage details from the Billing Reference details
    $meterTypes = $activityRunObj.output.billingReference.billableDuration.meterType | Select-Object -Unique
    $meterTypeObj = @{}
    ForEach($meterType in $meterTypes) {
        $activityTypes = $activityRunObj.output.billingReference.activityType | Select-Object -Unique
        $billableDetails = @{}
        ForEach($activityType in $activityTypes) {
            $data = $activityRunObj.output.billingReference | Where-Object {($_.ActivityType -eq $activityType) -and ($_.billableDuration.MeterType -eq $meterType)}
            $activityTypeObj = @()
            $meterTypes = $data.billableDuration.meterType | Select-Object -Unique
            $activityTypeObj = [PSCustomObject]@{
                ActivityType = $activityType
                MeterType = $data.billableDuration.MeterType | Select-Object -Unique
                Duration = ($data.billableDuration | Measure-Object duration -sum).sum
                Unit = $data.billableDuration.unit | Select-Object -Unique
            }
            $billableDetails.$activityType = $activityTypeObj
        }

        $meterTypeObj.$meterType = $billableDetails

    }

    # Build an object with the pipeline run details including sums of the activity run usages.
    $runDetails = [PSCustomObject]@{
        RunId = $runObj.RunId
        PipelineName = $runObj.PipelineName
        TimeStarted = $runObj.runStart
        TotalActivityRuns = $activityRunObj.Output.Count
        AzureActivityRuns = ($activityRunObj.Output | Where-Object {$_.effectiveIntegrationRuntime -ne "SelfHostedRuntime"}).Count
        AzureIRDataMovement_DIUHour = $meterTypeObj.AzureIR.DataMovement.Duration
        AzureIRPipeline_Hour = $meterTypeObj.AzureIR.PipelineActivity.Duration
        AzureIRExternal_Hour = $meterTypeObj.AzureIR.ExternalActivity.Duration
        SelfHostedActivityRuns = ($activityRunObj.Output | Where-Object {$_.effectiveIntegrationRuntime -eq "SelfHostedRuntime"}).Count
        SelfHostedDataMovement_Hour = $meterTypeObj.SelfHostedIR.DataMovement.Duration
        SelfHostedPipeline_Hour = $meterTypeObj.SelfHostedIR.PipelineActivity.Duration
        SelfHostedExternal_Hour = $meterTypeObj.SelfHostedIR.ExternalActivity.Duration
        ComputeGeneralPurpose_coreHour = $meterTypeObj.GeneralPurpose.executedataflow.Duration
        ComputeComputedOptimized_coreHour = $meterTypeObj.ComputedOptimized.executedataflow.Duration
        ComputeMemoryOptimized_coreHour = $meterTypeObj.MemoryOptimized.executedataflow.Duration
    }

    $runSummary += $runDetails

    $i++

}

## Everything below here could probably be cleaned up a bit.

# Get price list, could probably add something here to evaluate pricing on different subscription types but for now just standard pay-as-you-go pricing.
$priceList = Invoke-RestMethod -uri "https://azure.microsoft.com/api/v2/pricing/data-factory/calculator/?culture=en-us&discount=mosp"
# Get region of ADF, price calculator API slug doesn't match up with management API's naming scheme. Central US is selected with us-central rather than centralus.
$token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
$dataFactoryInfo = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories`?$apiVersion" -Headers $token.Header -Method Get
$dataFactoryLocation = $dataFactoryInfo.location
$region = ($priceList.regions | Where-Object {($_.DisplayName).Replace(" ","") -eq $dataFactoryLocation}).slug

# Build hash table containing pricing info for the specific region and components we need.
$priceHashTable = @{
    AzureActivityRuns = ($priceList.offers.'orchestration-cloud-v2'.prices."$($region)".value)*.001
    AzureIRDataMovement_DIUHour = $priceList.offers.'datamovement-cloud-v2'.prices."$($region)".value
    AzureIRPipeline_Hour = $priceList.offers.'pipelineactivity-cloud-v2'.prices."$($region)".value
    AzureIRExternal_Hour = $priceList.offers.'pipelineactivity-external-cloud-v2'.prices."$($region)".value
    SelfHostedActivityRuns = ($priceList.offers.'orchestration-self-hosted'.prices."$($region)".value)*.001
    SelfHostedDataMovement_Hour = $priceList.offers.'datamovement-self-hosted'.prices."$($region)".value
    SelfHostedPipeline_Hour = $priceList.offers.'pipelineactivity-self-hosted-v2'.prices."$($region)".value
    SelfHostedExternal_Hour = $priceList.offers.'pipelineactivity-external-self-hosted-v2'.prices."$($region)".value
    ComputeGeneralPurpose_coreHour = $priceList.offers.'data-flow-general-purpose-vcore-v2'.prices."$($region)".value
    ComputeComputedOptimized_coreHour = $priceList.offers.'data-flow-compute-optimized-vcore-v2'.prices."$($region)".value
    ComputeMemoryOptimized_coreHour = $priceList.offers.'data-flow-memory-optimized-vcore-v2'.prices."$($region)".value
}

$durations = @(
    'AzureActivityRuns';
    'AzureIRDataMovement_DIUHour';
    'AzureIRPipeline_Hour';
    'AzureIRExternal_Hour';
    'SelfHostedActivityRuns';
    'SelfHostedDataMovement_Hour';
    'SelfHostedPipeline_Hour';
    'SelfHostedExternal_Hour';
    'ComputeGeneralPurpose_coreHour';
    'ComputeComputedOptimized_coreHour';
    'ComputeMemoryOptimized_coreHour'
)
# Build objects for total usage and calculated totals.
$totalDurations = [PSCustomObject]@{
    "Total Usage" = "Total Usage"
}
$totalCosts = [PSCustomObject]@{
    "Calculated Costs" = "Calculated Costs"
    "Total Cost" = 0
}
ForEach($duration in $durations) {
    $total = ($runSummary | Measure-Object $duration -sum).sum
    $totalDurations | Add-Member -MemberType NoteProperty -Name $duration -Value $total
    $totalCost = $total * $priceHashTable.$duration
    $totalCosts."Total Cost" = $totalCosts."Total Cost" + $totalCost
    $totalCosts | Add-Member -MemberType NoteProperty -Name $duration -Value $totalCost
}
# Add those totals to the runSummary array. That makes for an easy export to CSV.
$runSummary += $totalDurations
$runSummary += $totalCosts

$runSummary | Export-Csv -Path $exportPath
