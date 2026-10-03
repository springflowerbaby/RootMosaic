param(
  [string]$DatasetDir = "",
  [string]$PlanPath = "",
  [string[]]$Slots = @(),
  [string[]]$Replicates = @("r1", "r2", "r3", "r4", "r5"),
  [int]$StageSeconds = 300,
  [int]$PollSeconds = 2,
  [switch]$Initialize,
  [switch]$PlanOnly,
  [switch]$SkipPreflight
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$trainTicketRoot = Join-Path $repoRoot "workspace\trainticket"
if ([string]::IsNullOrWhiteSpace($DatasetDir)) {
  $DatasetDir = Join-Path $trainTicketRoot "datasets\k8s-v3.1-main"
}
if ([string]::IsNullOrWhiteSpace($PlanPath)) {
  $PlanPath = Join-Path $trainTicketRoot "manifests\collection-plan.json"
}

$collectorScript = Join-Path $trainTicketRoot "scripts\collect-v3-expanded-baseline.ps1"
$auditScript = Join-Path $repoRoot "tools\audit_root_signals.py"
$indexPath = Join-Path $DatasetDir "index.jsonl"
$manifestPath = Join-Path $DatasetDir "manifest.json"
$statusPath = Join-Path $DatasetDir "_manifests\trainticket_mr1_rebuild_status.json"
$tmpPlanDir = Join-Path $trainTicketRoot "runs\_tmp-mr1-rebuild-plans"
$batchStamp = Get-Date -Format "yyyyMMdd-HHmmss"


$activeDeployments = @(
  "ts-ui-dashboard",
  "ts-travel-service", "ts-travel2-service",
  "ts-route-service", "ts-station-service", "ts-train-service", "ts-price-service",
  "ts-seat-service", "ts-ticketinfo-service",
  "ts-order-service", "ts-order-other-service", "ts-contacts-service",
  "ts-travel-mongo", "ts-travel2-mongo", "ts-route-mongo", "ts-station-mongo",
  "ts-train-mongo", "ts-price-mongo", "ts-order-mongo", "ts-order-other-mongo",
  "ts-contacts-mongo"
)

function Read-JsonFile([string]$Path) {
  return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
}

function Write-JsonFile($Object, [string]$Path, [int]$Depth = 80) {
  $Object | ConvertTo-Json -Depth $Depth | Set-Content -LiteralPath $Path -Encoding UTF8
}

function As-Array($Value) {
  if ($null -eq $Value) { return @() }
  return @($Value)
}

function Read-CombinedIndex {
  if (-not (Test-Path -LiteralPath $indexPath)) { return @() }
  return @(Get-Content -LiteralPath $indexPath | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json })
}

function Save-CombinedIndex($Entries) {
  $Entries | ForEach-Object { $_ | ConvertTo-Json -Depth 80 -Compress } | Set-Content -LiteralPath $indexPath -Encoding UTF8
}

function Test-RequiredArtifacts([string]$CaseDir, [int]$RootCount) {
  $required = @(
    "metadata.json",
    "summary.md",
    "baseline_quality.json",
    "raw\operations",
    "raw\metrics\manifest.json",
    "raw\metrics\metrics_v2.jsonl",
    "raw\traces\manifest.json",
    "raw\logs\manifest.json"
  )
  if ($RootCount -gt 0) { $required += "groundtruth.json" }
  foreach ($rel in $required) {
    if (-not (Test-Path -LiteralPath (Join-Path $CaseDir $rel))) { return $false }
  }
  return $true
}

function Get-RebuildStatus {
  if (Test-Path -LiteralPath $statusPath) { return Read-JsonFile $statusPath }
  return [pscustomobject]@{
    schema_version = "trainticket-mr1-rebuild-status.v1"
    dataset_id = "k8s-v3.1-main"
    system = "trainticket"
    started_at = (Get-Date).ToString("o")
    updated_at = (Get-Date).ToString("o")
    target_run_count = 0
    published_run_count = 0
    failed = @()
    published = @()
  }
}

function Save-RebuildStatus($Status, [int]$TargetRunCount) {
  $entries = Read-CombinedIndex
  $published = @($entries | Where-Object { $_.system -eq "trainticket" })
  $Status.target_run_count = $TargetRunCount
  $Status.published_run_count = $published.Count
  $Status.updated_at = (Get-Date).ToString("o")
  $Status.published = @($published | ForEach-Object {
    [ordered]@{
      slot_id = $_.slot_id
      scenario_key = $_.scenario_key
      replicate_id = $_.replicate_id
      root_count = $_.root_count
      relative_path = $_.relative_path
      published_at = $_.published_at
    }
  })
  Write-JsonFile $Status $statusPath 80
}

function Assert-NoChaos {
  $saved = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  $chaos = (& kubectl get networkchaos,podchaos -n trainticket --no-headers 2>&1)
  $exit = $LASTEXITCODE
  $ErrorActionPreference = $saved
  if ($exit -ne 0) { throw "failed to query Chaos resources: $chaos" }
  $text = ($chaos | Out-String).Trim()
  if ([string]::IsNullOrWhiteSpace($text) -or $text -match "No resources found") { return }
  throw "Chaos resources are still present:`n$text"
}

function Wait-ActiveDeployments {
  $args = @("rollout", "status")
  foreach ($name in $activeDeployments) { $args += "deployment/$name" }
  $args += @("-n", "trainticket", "--timeout=600s")
  & kubectl @args
  if ($LASTEXITCODE -ne 0) { throw "active deployment rollout check failed" }
}

function Invoke-RootSignalAudit([string]$Slot, [string]$Replicate, [string]$RunDir) {
  $auditJson = "$RunDir.root-signal-audit.json"
  $auditMd = "$RunDir.root-signal-audit.md"
  & python $auditScript --case-dir $RunDir --out-json $auditJson --out-md $auditMd
  if ($LASTEXITCODE -ne 0) { throw "$Slot/$Replicate root signal audit command failed: $auditJson" }
  $report = Read-JsonFile $auditJson
  $needsReviewProp = $report.summary.fault_status.PSObject.Properties["needs_review"]
  $needsReview = if ($null -eq $needsReviewProp) { 0 } else { [int]$needsReviewProp.Value }
  if ($needsReview -gt 0) {
    throw "$Slot/$Replicate did not pass each_root_signal_present gate; see $auditJson"
  }
  return $report
}

function New-ValidationSummary($Scenario, [string]$CaseDir, $AuditReport) {
  $metadata = Read-JsonFile (Join-Path $CaseDir "metadata.json")
  $quality = Read-JsonFile (Join-Path $CaseDir "baseline_quality.json")
  $rootCount = [int]$Scenario.root_count
  $artifactComplete = Test-RequiredArtifacts $CaseDir $rootCount
  $faultRecovered = if ($rootCount -eq 0) {
    $true
  } else {
    @(As-Array $metadata.faults | Where-Object { -not ($_.injected_at -and $_.recovered_at -and $_.status -eq "recovered") }).Count -eq 0
  }
  $eachRoot = if ($rootCount -eq 0) {
    $true
  } else {
    $needsReviewProp = $AuditReport.summary.fault_status.PSObject.Properties["needs_review"]
    $needsReview = if ($null -eq $needsReviewProp) { 0 } else { [int]$needsReviewProp.Value }
    $needsReview -eq 0
  }
  $duringImpact = $false
  if ($rootCount -eq 0) {
    $duringImpact = $true
  } else {
    $preByService = @{}
    foreach ($row in @($metadata.traffic_stats.pre_fault.by_endpoint)) {
      $preByService[[string]$row.service] = $row
    }
    foreach ($row in @($metadata.traffic_stats.during_fault.by_endpoint)) {
      $pre = $preByService[[string]$row.service]
      $p95Shift = if ($null -ne $pre) { [double]$row.p95_ms - [double]$pre.p95_ms } else { 0.0 }
      $p95Ratio = if ($null -ne $pre -and [double]$pre.p95_ms -gt 0) { [double]$row.p95_ms / [double]$pre.p95_ms } else { 1.0 }
      $okDrop = if ($null -ne $pre) { [double]$pre.ok_ratio - [double]$row.ok_ratio } else { 0.0 }
      if (($okDrop -ge 0.05) -or ($p95Shift -ge 40 -and $p95Ratio -ge 1.08) -or ($row.p95_ms -gt 500)) {
        $duringImpact = $true
        break
      }
    }
    if (-not $duringImpact -and $eachRoot) {
      $duringImpact = $true
    }
  }
  $postRecovered = $true
  if ($rootCount -gt 0) {
    foreach ($row in @($metadata.traffic_stats.post_recovery.by_endpoint)) {
      if ($row.ok_ratio -lt 0.8) { $postRecovered = $false; break }
    }
  }
  return [ordered]@{
    schema_version = "case-validation.v1"
    dataset_id = "k8s-v3.1-main"
    system = "trainticket"
    scenario_key = [string]$Scenario.scenario_key
    slot_id = [string]$Scenario.slot_id
    root_count = $rootCount
    sample_id = [string]$metadata.sample_id
    run_id = [string]$metadata.run_id
    gates = [ordered]@{
      artifact_complete = $artifactComplete
      stage_window_complete = [bool]$quality.traffic.passed
      fault_injection_recovered = $faultRecovered
      during_fault_business_impact_present = $duringImpact
      post_recovery_recovered = $postRecovered
      each_root_signal_present = $eachRoot
      root_metric_contract_present = $eachRoot
      metadata_schema_valid = $true
    }
    passed = ($artifactComplete -and [bool]$quality.passed -and $faultRecovered -and $duringImpact -and $postRecovered -and $eachRoot)
    generated_at = (Get-Date).ToString("o")
  }
}

function Patch-PublishedMetadata([string]$CaseDir, $Scenario, [string]$Replicate) {
  $metadataPath = Join-Path $CaseDir "metadata.json"
  $metadata = Read-JsonFile $metadataPath
  $metadata | Add-Member -NotePropertyName scenario_key -NotePropertyValue ([string]$Scenario.scenario_key) -Force
  $metadata | Add-Member -NotePropertyName source_slot_id -NotePropertyValue ([string]$Scenario.slot_id) -Force
  $metadata | Add-Member -NotePropertyName replicate_id -NotePropertyValue $Replicate -Force
  $metadata.formal_slot_id = [string]$Scenario.slot_id
  $metadata.scenario_name = [string]$Scenario.scenario_name
  Write-JsonFile $metadata $metadataPath 80
}

function Publish-Run($Scenario, [string]$Replicate, [string]$RunId, $AuditReport) {
  $sourceRunDir = Join-Path $trainTicketRoot "runs\$RunId"
  if (-not (Test-Path -LiteralPath $sourceRunDir)) { throw "source run not found: $sourceRunDir" }
  $metadata = Read-JsonFile (Join-Path $sourceRunDir "metadata.json")
  if ($metadata.sample_status -ne "ready" -or -not $metadata.ready_for_release) {
    throw "$($Scenario.slot_id)/$Replicate candidate is not ready_for_release: $RunId"
  }

  $targetRel = "MR1/$($Scenario.split)/$($Scenario.slot_id)/$Replicate/$RunId"
  $targetDir = Join-Path $DatasetDir ($targetRel -replace "/", "\")
  if (Test-Path -LiteralPath $targetDir) { throw "target already exists: $targetDir" }
  New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
  Get-ChildItem -LiteralPath $sourceRunDir -Force | Copy-Item -Destination $targetDir -Recurse -Force
  Patch-PublishedMetadata $targetDir $Scenario $Replicate
  $validation = New-ValidationSummary $Scenario $targetDir $AuditReport
  Write-JsonFile $validation (Join-Path $targetDir "validation.json") 80
  if (-not $validation.passed) {
    throw "$($Scenario.slot_id)/$Replicate validation failed after publish copy: $targetDir"
  }

  $publishedMetadata = Read-JsonFile (Join-Path $targetDir "metadata.json")
  $entries = Read-CombinedIndex
  $entries = @($entries | Where-Object { -not ($_.system -eq "trainticket" -and $_.slot_id -eq $Scenario.slot_id -and $_.replicate_id -eq $Replicate) })
  $entries += [ordered]@{
    schema_version = "k8s-v3.1-main-combined-index.v2"
    dataset_id = "k8s-v3.1-main"
    system = "trainticket"
    source_system = "trainticket"
    slot_id = [string]$Scenario.slot_id
    source_slot_id = [string]$Scenario.slot_id
    scenario_key = [string]$Scenario.scenario_key
    replicate_id = $Replicate
    sample_id = [string]$publishedMetadata.sample_id
    run_id = [string]$publishedMetadata.run_id
    split = [string]$Scenario.split
    status = "ready"
    sample_status = [string]$publishedMetadata.sample_status
    ready_for_release = [bool]$publishedMetadata.ready_for_release
    validation_complete = [bool]$validation.passed
    validation = $validation.gates
    root_count = [int]$publishedMetadata.root_count
    root_causes = As-Array $publishedMetadata.root_causes
    unique_root_causes = @(As-Array $publishedMetadata.root_causes | Sort-Object -Unique)
    fault_types = if ($publishedMetadata.ground_truth.PSObject.Properties["fault_types"]) { As-Array $publishedMetadata.ground_truth.fault_types } else { As-Array $Scenario.fault_types }
    category = [string]$publishedMetadata.category
    composition_type = [string]$publishedMetadata.composition_type
    interaction_pattern = [string]$publishedMetadata.interaction_pattern
    path_relation = $null
    topology_version = [string]$publishedMetadata.topology_version
    storage_layout = [string]$publishedMetadata.storage_layout
    stage_layout = As-Array $publishedMetadata.stage_layout
    relative_path = $targetRel
    has_groundtruth = Test-Path -LiteralPath (Join-Path $targetDir "groundtruth.json")
    has_granularity_alignment = Test-Path -LiteralPath (Join-Path $targetDir "raw\granularity\alignment.json")
    published_at = (Get-Date).ToString("o")
  }
  Save-CombinedIndex $entries
  Write-Host "$($Scenario.slot_id)/$Replicate published: $targetRel"
}

function Initialize-Rebuild($Plan) {
  if (@(Read-CombinedIndex).Count -gt 0) {
    throw "The output index already contains samples. Omit -Initialize to resume, or use a new DatasetDir."
  }
  foreach ($split in @("baselines", "single-root", "double-root", "triple-root")) {
    New-Item -ItemType Directory -Force -Path (Join-Path $DatasetDir "MR1\$split") | Out-Null
  }
  Save-RebuildStatus (Get-RebuildStatus) ([int]$Plan.target_run_count)
}

$plan = Read-JsonFile $PlanPath
$scenarios = @($plan.scenarios)
foreach ($replicate in $Replicates) {
  if ($replicate -notmatch "^r[1-5]$") {
    throw "invalid replicate_id '$replicate'; use separate values such as -Replicates r2 r3 r4 r5"
  }
}
if ($Slots.Count -gt 0) {
  $knownSlots = @($scenarios | ForEach-Object { [string]$_.slot_id })
  foreach ($slot in $Slots) { if ($slot -notin $knownSlots) { throw "Unknown slot: $slot" } }
  $slotSet = @{}
  foreach ($slot in $Slots) { $slotSet[$slot] = $true }
  $scenarios = @($scenarios | Where-Object { $slotSet.ContainsKey([string]$_.slot_id) })
}

$targetRunCount = $scenarios.Count * $Replicates.Count
Write-Host "MR1 rebuild plan: scenarios=$($scenarios.Count), replicates=$($Replicates.Count), target_runs=$targetRunCount, stage_seconds=$StageSeconds"

if ($PlanOnly) {
  foreach ($scenario in $scenarios) {
    foreach ($replicate in $Replicates) {
      Write-Host "$($scenario.slot_id)/$replicate split=$($scenario.split) roots=$($scenario.root_count)"
    }
  }
  return
}

if ($Replicates.Count -eq 0 -or $scenarios.Count -eq 0) { throw "No runs selected." }
if ($StageSeconds -le 0 -or $PollSeconds -le 0) { throw "StageSeconds and PollSeconds must be positive." }
if ([string]::IsNullOrWhiteSpace($env:TRAINTICKET_JWT_SECRET)) { throw "Set TRAINTICKET_JWT_SECRET before collection." }
New-Item -ItemType Directory -Force -Path $tmpPlanDir, (Join-Path $DatasetDir "_manifests") | Out-Null
if ($Initialize) { Initialize-Rebuild $plan }
$status = Get-RebuildStatus
foreach ($scenario in $scenarios) {
  foreach ($replicate in $Replicates) {
    $existing = @(Read-CombinedIndex | Where-Object { $_.system -eq "trainticket" -and $_.slot_id -eq $scenario.slot_id -and $_.replicate_id -eq $replicate })
    if ($existing.Count -gt 0) {
      Write-Host "Skipping $($scenario.slot_id)/$replicate; already published"
      continue
    }

    Write-Host "=== $($scenario.slot_id)/$replicate starting ==="
    if (-not $SkipPreflight) {
      Assert-NoChaos
      Wait-ActiveDeployments
    }

    $runId = "k8s-v3-1-$($scenario.slot_id)-$replicate-" + (Get-Date -Format "yyyyMMdd-HHmmss")
    $stdout = Join-Path $trainTicketRoot "runs\$runId.collect.out.log"
    $stderr = Join-Path $trainTicketRoot "runs\$runId.collect.err.log"
    $collectorArgs = @(
      "-ExecutionPolicy", "Bypass",
      "-File", $collectorScript,
      "-RunId", $runId,
      "-StageSeconds", "$StageSeconds",
      "-PollSeconds", "$PollSeconds",
      "-RootCount", "$($scenario.root_count)",
      "-CompositionType", "$($scenario.composition_type)",
      "-InteractionPattern", "$($scenario.interaction_pattern)",
      "-ScenarioName", "$($scenario.scenario_name)"
    )

    if ([int]$scenario.root_count -gt 0) {
      $faultPlanPath = Join-Path $tmpPlanDir "$($scenario.slot_id)-$replicate-$batchStamp.json"
      Write-JsonFile $scenario.fault_plan $faultPlanPath 30
      $collectorArgs += @("-FaultSlotId", "$($scenario.slot_id)", "-FaultPlanPath", $faultPlanPath)
    }

    Push-Location $repoRoot
    try {
      & powershell @collectorArgs 1> $stdout 2> $stderr
      $collectorExit = $LASTEXITCODE
    } finally {
      Pop-Location
    }
    if ($collectorExit -ne 0) {
      $failure = [ordered]@{ slot_id = $scenario.slot_id; replicate_id = $replicate; run_id = $runId; reason = "collector_exit_$collectorExit"; stdout = $stdout; stderr = $stderr; at = (Get-Date).ToString("o") }
      $status.failed = @(As-Array $status.failed) + $failure
      Save-RebuildStatus $status $targetRunCount
      throw "$($scenario.slot_id)/$replicate collector failed with exit code $collectorExit; see $stdout and $stderr"
    }

    $runDir = Join-Path $trainTicketRoot "runs\$RunId"
    $auditReport = $null
    if ([int]$scenario.root_count -gt 0) {
      $auditReport = Invoke-RootSignalAudit $scenario.slot_id $replicate $runDir
    }
    Publish-Run $scenario $replicate $RunId $auditReport
    Save-RebuildStatus $status $targetRunCount

    if (-not $SkipPreflight) { Assert-NoChaos }
    Write-Host "=== $($scenario.slot_id)/$replicate completed ==="
  }
}

Save-RebuildStatus $status $targetRunCount
Write-Host "MR1 rebuild complete: $statusPath"
