$dotnetPath = Get-Command -ErrorAction SilentlyContinue dotnet | Select-Object -ExpandProperty Source

if (!(Test-Path $dotnetPath)) {
	Write-Error "The dotnet command is not installed on this system.  Please install the latest .NET 8 SDK from https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
	Exit
}
$sdk8 = dotnet --list-sdks | where { $_ -like "8.*" }

if ($sdk8.Count -eq 0) {
	Write-Error "The dotnet 8 SDK is not installed on this system.  Please install the latest .NET 8 SDK from https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
	Exit
}

$pansqlPath = Get-Command -ErrorAction SilentlyContinue PanSQL | Select-Object -ExpandProperty Source
