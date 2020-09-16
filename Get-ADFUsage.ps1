<#
.DESCRIPTION
- This script should pull Azure Data Factory (v2) pipeline executions for the specified date range and return usage.
- Consumption costs are calculated pulling current pricing data from the Azure Pricing API: https://azure.microsoft.com/api/v2/pricing/data-factory/calculator
- Requires the Az module.
.PARAMETER subscriptionId
- Specify the subscription ID the data factory is located in.
.PARAMETER resourceGroup
- Specify the resource group name that the data factory is located in.
.PARAMETER factoryName
- Specify the data factory name.
.PARAMETER startDays
- How many days back from script execution would you like to capture usage information.
- Defaults to 7 days and if that works, you don't need to specify this parameter.
- Enhancement needed: add filter for end date
.PARAMETER exportPath
- Location and filename to drop exported CSV in file system.
.EXAMPLE
.\Get-ADFUsage -subscriptionId "a0a0a0a0-1111-1a23-b456-7890cdef1234" `
-resourceGroup "adf-rg" `
-factoryName "adf-rg-adf1" `
-startDays 5 `
-exportPath "C:\temp\file.csv"
.NOTES
    Version:        0.1
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

# Get time in ISO 8601 format
$lastUpdatedAfter = Get-Date (Get-Date).AddDays(-($startDays)) -Format "o"
$lastUpdatedBefore = Get-Date -Format "o"
$filterJson = [ordered]@{
    lastUpdatedAfter = $lastUpdatedAfter
    lastUpdatedBefore = $lastUpdatedBefore
} | ConvertTo-Json
$token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
$pipelineRunObj = @()
$pipelineRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories/queryPipelineRuns?RunStart=`"$runStartAfter`"&$apiVersion" -Headers $token.Header -Method POST -Body $filterJson -ContentType 'application/json'
$pipelineRunObj += $pipelineRuns.Value
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

ForEach($runObj in $pipelineRunObj) {
    $runId = "/pipelineruns/$($runObj.RunId)"
    
    Write-Output "$i of $($pipelineRunObj.count)..."
    Write-Output "Working on $($runObj.RunId) - $($runObj.PipelineName)"

    $activityRunObj = @()
    $token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
    $activityRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories$runId/queryactivityruns?$apiVersion" -Headers $token.Header -Method POST -Body $filterJson -ContentType 'application/json'
    $activityRunObj += $activityRuns.Value
    Do {
        $activityFilterJsonObj = $filterJson | ConvertFrom-Json
        $activityFilterJsonObj | Add-Member -MemberType NoteProperty -Name continuationToken -Value $activityRuns.continuationToken
        $activityFilterBody = $activityFilterJsonObj | ConvertTo-Json
        $token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
        $activityRuns = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories$runId/queryactivityruns?$apiVersion" -Headers $token.Header -Method POST -Body $activityFilterBody -ContentType 'application/json'
        $activityRunObj += $($activityRuns.Value)
    } while (-not ([string]::IsNullOrEmpty($activityRuns.continuationToken)))
    
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

# Get price list
$priceList = Invoke-RestMethod -uri "https://azure.microsoft.com/api/v2/pricing/data-factory/calculator/?culture=en-us&discount=mosp"
# Get region of ADF
$token = Get-AccessToken -tokenExpiration $token.Expiration -SubscriptionId $subscriptionId
$dataFactoryInfo = Invoke-RestMethod -Uri "$azureEndpoint$subscriptions$resourceGroups$providers$factories`?$apiVersion" -Headers $token.Header -Method Get
$dataFactoryLocation = $dataFactoryInfo.location
$region = ($priceList.regions | Where-Object {($_.DisplayName).Replace(" ","") -eq $dataFactoryLocation}).slug

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

$runSummary += $totalDurations
$runSummary += $totalCosts

$runSummary | Export-Csv -Path $exportPath
