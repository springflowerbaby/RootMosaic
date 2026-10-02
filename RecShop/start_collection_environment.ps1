[CmdletBinding()]
param(
    [switch]$CheckOnly,
    [string]$Python = 'python',
    [Parameter(Mandatory=$true)][string]$Environment,
    [switch]$RegisterReadOnly,
    [string]$EnvFile = '',
    [ValidateRange(10, 900)][int]$TimeoutSeconds = 300
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Python = (Get-Command $Python -ErrorAction Stop).Source
$env:PYTHONDONTWRITEBYTECODE = '1'
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw 'The selected Python interpreter is missing; use -Python to select an installed environment.' }
$launcher = Join-Path $PSScriptRoot 'scripts\environment\start_collection_environment.py'
$arguments = @('-B', '-X', 'utf8', $launcher, '--timeout-seconds', "$TimeoutSeconds", '--environment', $Environment)
if ($RegisterReadOnly) { $arguments += @('--check-only', '--register-readonly') }
if ($CheckOnly -and -not $RegisterReadOnly) { $arguments += '--check-only' }
if ($EnvFile) { $arguments += @('--env-file', $EnvFile) }
& $Python @arguments
exit $LASTEXITCODE
