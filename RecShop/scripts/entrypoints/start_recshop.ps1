[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$Config,
    [string]$Python = "python",
    [switch]$Render,
    [string]$Out
)
$ErrorActionPreference = "Stop"
$entry = Join-Path $PSScriptRoot "start_recshop.py"
$arguments = @("-B", "-X", "utf8", $entry, "--config", $Config)
if ($Render) { $arguments += "--render" }
if ($Out) { $arguments += @("--out", $Out) }
& $Python @arguments
exit $LASTEXITCODE
