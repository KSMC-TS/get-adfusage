# Get-ADFUsage.ps1
## Gather consumption and calculate costs for Azure Data Factory v2


### DESCRIPTION
- This script should pull Azure Data Factory (v2) pipeline executions for the specified date range and return usage.
- Consumption costs are calculated pulling current pricing data from the Azure Pricing API: https://azure.microsoft.com/api/v2/pricing/data-factory/calculator
- Requires the Az module.
- This will take some time to execute. Make sure you are running this on a host with reliable connectivity. Processing 175,000 activity runs can take roughly 2-3 hours.
### PARAMETER subscriptionId
- Specify the subscription ID the data factory is located in.
### PARAMETER resourceGroup
- Specify the resource group name that the data factory is located in.
### PARAMETER factoryName
- Specify the data factory name.
### PARAMETER startDays
- How many days back from script execution would you like to capture usage information.
- Defaults to 7 days and if that works, you don't need to specify this parameter.
### PARAMETER endDays
- Specify number of days back to end filter range.
- Defaults to 0 days to include runs up to script execution.
### PARAMETER exportPath
- Location and filename to drop exported CSV in file system.
### EXAMPLE
```
.\Get-ADFUsage -subscriptionId "a0a0a0a0-1111-1a23-b456-7890cdef1234" `
-resourceGroup "adf-rg" `
-factoryName "adf-rg-adf1" `
-startDays 5 `
-endDays 1 `
-exportPath "C:\temp\file.csv"
```
- This example will get usage for the ADF, adf-rg-adf1, from 5 days ago to 1 day ago and save the CSV to C:\temp\file.csv.
### NOTES
- Version:        0.2
- Last updated:   09/16/2020
- Modified by:    Zachary Choate
- URL:            https://github.com/KSMC-TS/get-adfusage
