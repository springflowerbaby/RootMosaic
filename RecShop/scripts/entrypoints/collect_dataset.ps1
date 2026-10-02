[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$Plan,
    [string]$Python = "python",
    [int]$Round,
    [switch]$Status,
    [switch]$Resume,
    [switch]$Execute,
    [string]$DecisionFile
)

$ErrorActionPreference = "Stop"
$Python = (Get-Command $Python -ErrorAction Stop).Source
$env:PYTHONDONTWRITEBYTECODE = "1"
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "../.."))
if (-not [IO.Path]::IsPathRooted($Plan)) { $Plan = Join-Path $repoRoot $Plan }
$Plan = (Resolve-Path -LiteralPath $Plan -ErrorAction Stop).Path
$repo = $repoRoot
$planData = Get-Content -LiteralPath $Plan -Raw -Encoding UTF8 | ConvertFrom-Json
$versions = @($planData.scenarios | ForEach-Object { $_.design_version } | Sort-Object -Unique)
if ($versions.Count -ne 1) { throw "A campaign must contain exactly one design version; run old and incremental campaigns serially." }
$entryModule = switch ($versions[0]) {
    "rq4-composition-proposal-20260916-v1" { "scripts.collection.run_campaign" }
    "rq4-d06-15ms-incremental-20260929-v1" { "scripts.collection.gateway_cpu_network" }
    default { throw "Unsupported campaign design version: $($versions[0])" }
}
$entry = Join-Path $repoRoot ($entryModule.Replace('.', '\') + '.py')
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw "Selected Python is missing: $Python" }
if (-not (Test-Path -LiteralPath $entry -PathType Leaf)) { throw "repeat batch entry is missing: $entry" }

$runnerArgs = @("-B", "-X", "utf8", "-m", $entryModule)
if ($entryModule -eq "scripts.collection.gateway_cpu_network") { $runnerArgs += "batch" }
$runnerArgs += @("--plan", $Plan)
if ($null -ne $PSBoundParameters["Round"]) { $runnerArgs += @("--round", [string]$Round) }
if ($Status) { $runnerArgs += "--status" }
if (-not [string]::IsNullOrWhiteSpace($DecisionFile)) { $runnerArgs += @("--decision-file", $DecisionFile) }
if ($Resume) { $runnerArgs += "--resume" }
if ($Execute) { $runnerArgs += "--execute" }

Push-Location -LiteralPath $repo
try {
    & $Python @runnerArgs
    $resultCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $resultCode
