# Places Microsoft's ConPTY (conpty.dll and OpenConsole.exe) in <Destination>\conpty, where the
# rmux daemon beside it loads it in place of the system conhost.
param(
    [Parameter(Mandatory = $true)]
    [string]$Destination,
    [ValidateSet("x64", "arm64")]
    [string]$Arch = "x64"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$version = "1.25.260930003"
$sha256 = "02B07B349AF66D801159BDF9E440D4A1CE78BB951F37FC8609731665AFDAE7EE"
$url = "https://api.nuget.org/v3-flatcontainer/microsoft.windows.console.conpty/$version/microsoft.windows.console.conpty.$version.nupkg"

$work = Join-Path ([IO.Path]::GetTempPath()) "rmux-conpty-$version"
$package = "$work.zip"
Invoke-WebRequest -Uri $url -OutFile $package
$actual = (Get-FileHash -LiteralPath $package -Algorithm SHA256).Hash
if ($actual -ne $sha256) {
    Write-Error "error: ConPTY package hash $actual does not match $sha256"
    exit 1
}
Expand-Archive -LiteralPath $package -DestinationPath $work -Force

$bundle = Join-Path $Destination "conpty"
New-Item -ItemType Directory -Force -Path (Join-Path $bundle $Arch) | Out-Null
Copy-Item -LiteralPath (Join-Path $work "runtimes\win-$Arch\native\conpty.dll") -Destination $bundle
Copy-Item -LiteralPath (Join-Path $work "build\native\runtimes\$Arch\OpenConsole.exe") -Destination (Join-Path $bundle $Arch)
Remove-Item -LiteralPath $package, $work -Recurse -Force
Write-Output "ConPTY $version placed in $bundle"
