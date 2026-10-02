<#
Install the unified RecShop Python requirements, then check the three collection host packages.
CheckOnly verifies that host subset only; it does not certify all business dependencies.
Examples:
  .\install_dependencies.ps1
  .\install_dependencies.ps1 -CheckOnly
  .\install_dependencies.ps1 -CheckOnly -PythonOnly

Docker Desktop/Kubernetes, kubectl, Chaos Mesh, the existing deployments,
database and model/data volumes must already be provisioned. This script
reports them separately; it never installs drivers or recreates data.
#>
[CmdletBinding()]
param(
    [string]$Python = "python",
    [switch]$CheckOnly,
    [switch]$PythonOnly,
    [string]$Context = "",
    [string]$Namespace = "",
    [string]$DbEnvFile = ".env.collection"
)

$ErrorActionPreference = "Stop"
$Python = (Get-Command $Python -ErrorAction Stop).Source
$env:PYTHONDONTWRITEBYTECODE = "1"
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    Write-Host "Selected Python is missing. Create a Python 3.10 environment first, or pass -Python explicitly."
    exit 1
}
if (-not $Context -and -not $Namespace) { $PythonOnly = $true }
$requirements = Join-Path $PSScriptRoot "requirements.txt"
$checker = Join-Path $PSScriptRoot "scripts\environment\check_dependencies.py"
if (-not (Test-Path -LiteralPath $requirements -PathType Leaf) -or
    -not (Test-Path -LiteralPath $checker -PathType Leaf)) {
    Write-Host "The unified dependency manifest or collection host checker is missing."
    exit 1
}

Push-Location -LiteralPath $PSScriptRoot
try {
    # Do not install into a different major/minor version by accident.
    & $Python -B -X utf8 -c "import sys; sys.exit(0 if sys.version_info[:2] == (3, 10) else 1)"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "collection's verified host runtime is Python 3.10. No packages were changed."
        exit 1
    }
    if (-not $CheckOnly) {
        Write-Host "Installing the unified RecShop Python requirements into the selected Python..."
        # Pip diagnostics may include private index URLs. Never echo captured output.
        $savedPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        $pipOutput = & $Python -B -X utf8 -m pip install --disable-pip-version-check --no-input --quiet -r $requirements 2>&1
        $pipExit = $LASTEXITCODE
        $ErrorActionPreference = $savedPreference
        $pipOutput = $null
        if ($pipExit -ne 0) {
            Write-Host "Python package installation failed (exit $pipExit). Review your pip/network configuration locally; diagnostic output was withheld to protect credentials."
            exit $pipExit
        }
    }
    $checkArgs = @("-B", "-X", "utf8", $checker, "--requirements", $requirements)
    if ($PythonOnly) {
        $checkArgs += "--python-only"
    }
    else {
        if (-not $Context -or -not $Namespace) {
            Write-Host "Supply both -Context and -Namespace, or use -PythonOnly."
            exit 1
        }
        $checkArgs += @("--context", $Context, "--namespace", $Namespace, "--db-env-file", $DbEnvFile)
    }
    & $Python @checkArgs
    $checkExit = $LASTEXITCODE
    if ($checkExit -eq 2) {
        Write-Host "The three collection host packages passed the subset check; external prerequisites need attention. Run start_collection_environment.cmd after Docker Desktop and the existing collection stack are available."
    }
    exit $checkExit
}
finally {
    Pop-Location
}
