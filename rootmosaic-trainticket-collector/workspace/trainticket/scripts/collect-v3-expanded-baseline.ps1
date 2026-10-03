param(
  [string]$Namespace = "trainticket",
  [string]$RunId = "",
  [int]$StageSeconds = 300,
  [int]$PollSeconds = 2,
  [string]$ProbePod = "deploy/ts-travel2-service",
  [string]$TopologyPath = "",
  [string]$Prometheus = "http://127.0.0.1:19090",
  [string]$Jaeger = "http://127.0.0.1:16686",
  [string]$FaultSlotId = "",
  [string]$FaultTarget = "",
  [string]$FaultMode = "none",
  [string]$FaultClass = "",
  [string]$FaultType = "",
  [string]$InjectionFault = "",
  [string]$ScenarioName = "",
  [string]$FaultPlanJson = "",
  [string]$FaultPlanPath = "",
  [int]$RootCount = 1,
  [string]$CompositionType = "single",
  [string]$InteractionPattern = "single_root",
  [int]$FaultDurationSeconds = 0,
  [int]$NetworkDelayMs = 700,
  [int]$NetworkJitterMs = 0,
  [int]$PacketLossPercent = 40,
  [int]$MemoryMiB = 160
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($env:TRAINTICKET_JWT_SECRET)) {
  throw "Set TRAINTICKET_JWT_SECRET to the signing key of your benchmark deployment."
}
if ($StageSeconds -le 0 -or $PollSeconds -le 0) { throw "StageSeconds and PollSeconds must be positive." }
if ($RunId -and $RunId -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]*$') { throw "RunId must be a simple directory name." }

$hasFaultPlan = (-not [string]::IsNullOrWhiteSpace($FaultPlanJson)) -or (-not [string]::IsNullOrWhiteSpace($FaultPlanPath))
$isSingleFaultRun = (-not [string]::IsNullOrWhiteSpace($FaultSlotId)) -and $FaultMode -ne "none"
$isFaultRun = $hasFaultPlan -or $isSingleFaultRun
$SampleSlotId = $FaultSlotId
if ([string]::IsNullOrWhiteSpace($RunId)) {
  $prefix = if ($isFaultRun) { "k8s-v3-1-$SampleSlotId" } else { "k8s-v3-1-expanded-three-stage-baseline" }
  $RunId = $prefix + "-" + (Get-Date -Format "yyyyMMdd-HHmmss")
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$root = Join-Path $repoRoot "workspace\trainticket"
if ([string]::IsNullOrWhiteSpace($TopologyPath)) {
  $TopologyPath = Join-Path $root "manifests\k8s-v3.1-contacts-topology.json"
}

$topology = Get-Content -Raw -LiteralPath $TopologyPath | ConvertFrom-Json
$runDir = Join-Path $root "runs\$RunId"
if (Test-Path $runDir) {
  throw "run exists: $runDir"
}

$opsDir = Join-Path $runDir "raw\operations"
$metricsDir = Join-Path $runDir "raw\metrics"
$logsDir = Join-Path $runDir "raw\logs"
$traceDir = Join-Path $runDir "raw\traces"
$scriptsDir = Join-Path $runDir "scripts"
New-Item -ItemType Directory -Force -Path $opsDir, $metricsDir, $logsDir, $traceDir, $scriptsDir | Out-Null
Copy-Item -LiteralPath $PSCommandPath -Destination (Join-Path $scriptsDir "collect-v3-expanded-baseline.ps1")

$components = @($topology.components | ForEach-Object { [string]$_.name })
$traceExpectedServices = @($topology.components | Where-Object { $_.trace_expected } | ForEach-Object { [string]$_.name })
$faultPlan = @()
if ($hasFaultPlan) {
  if ([string]::IsNullOrWhiteSpace($FaultPlanJson) -and -not [string]::IsNullOrWhiteSpace($FaultPlanPath)) {
    $FaultPlanJson = Get-Content -Raw -LiteralPath $FaultPlanPath
  }
  $faultPlan = @(($FaultPlanJson | ConvertFrom-Json))
} elseif ($isSingleFaultRun) {
  $faultPlan = @([pscustomobject]@{
    fault_instance_id = "F1"
    target = $FaultTarget
    mode = $FaultMode
    class = $FaultClass
    type = $FaultType
    injection = $InjectionFault
    role = "primary"
  })
}
$probes = @($topology.workload_probes)
if ($isFaultRun -and @($faultPlan | Where-Object { $_.target -eq "ts-ui-dashboard" }).Count -gt 0) {
  $probes += [pscustomobject]@{
    name = "ui-proxy-travel"
    service = "ts-ui-dashboard"
    method = "GET"
    url = "http://ts-ui-dashboard:8080/api/v1/travelservice/welcome"
    auth = $null
  }
}
$stages = @("pre_fault", "during_fault", "post_recovery")
$metricsPath = Join-Path $metricsDir "metrics_v2.jsonl"
if ($FaultDurationSeconds -le 0) {
  $FaultDurationSeconds = [math]::Max(30, $StageSeconds - 20)
}

function Iso([datetime]$d = (Get-Date)) {
  return $d.ToUniversalTime().ToString("o")
}

function Write-Json($Object, [string]$Path) {
  $Object | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Add-JsonLine($Object, [string]$Path) {
  ($Object | ConvertTo-Json -Depth 40 -Compress) | Add-Content -LiteralPath $Path -Encoding UTF8
}

function ConvertTo-JsonLine($Object) {
  return ($Object | ConvertTo-Json -Depth 40 -Compress)
}

function ConvertTo-Base64Url {
  param([byte[]]$Bytes)
  return [Convert]::ToBase64String($Bytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

function New-TrainTicketJwt {
  param(
    [string]$Subject = "fdse_microservice",
    [string[]]$RoleList = @("ROLE_USER"),
    [string]$UserId = "4d2a46c7-71cb-4cf1-b5bb-b68406d9da6f",
    [int]$LifetimeSeconds = 7200
  )

  $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
  $headerJson = '{"alg":"HS256","typ":"JWT"}'
  $payload = [ordered]@{
    sub = $Subject
    roles = $RoleList
    id = $UserId
    iat = $now
    exp = $now + $LifetimeSeconds
  }
  $header = ConvertTo-Base64Url ([Text.Encoding]::UTF8.GetBytes($headerJson))
  $body = ConvertTo-Base64Url ([Text.Encoding]::UTF8.GetBytes(($payload | ConvertTo-Json -Compress)))
  $unsigned = "$header.$body"
  $hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($env:TRAINTICKET_JWT_SECRET))
  $signature = ConvertTo-Base64Url ($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($unsigned)))
  return "$unsigned.$signature"
}

function Ensure-PortForward([int]$Port, [string]$NamespaceName, [string]$Resource, [string]$Mapping) {
  if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    Add-Content -LiteralPath (Join-Path $opsDir "port-forwards.txt") -Value "reuse port=$Port resource=$Resource mapping=$Mapping"
    return
  }

  $proc = Start-Process -FilePath "kubectl.exe" -ArgumentList @("port-forward", "-n", $NamespaceName, $Resource, $Mapping) -WindowStyle Hidden -PassThru
  Start-Sleep -Seconds 4
  if (-not (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)) {
    throw "port-forward failed: $Resource $Mapping"
  }
  Add-Content -LiteralPath (Join-Path $opsDir "port-forwards.txt") -Value "started port=$Port pid=$($proc.Id) resource=$Resource mapping=$Mapping"
}

function MetricRecord([datetime]$At, [string]$Stage, [string]$Source, [string]$EntityType, [string]$Entity, [string]$Service, [string]$Metric, $Value, [string]$Unit, [string]$MetricType, $Labels = @{}) {
  $containerLabel = if ($Labels.ContainsKey("container")) { $Labels["container"] } else { $Entity }
  return [ordered]@{
    schema_version = "metrics.v2"
    timestamp = Iso $At
    stage = $Stage
    run_id = $RunId
    source = $Source
    entity_type = $EntityType
    entity = $Entity
    service = $Service
    container = $containerLabel
    metric = $Metric
    value = $Value
    unit = $Unit
    metric_type = $MetricType
    labels = $Labels
    quality = "observed"
  }
}

function ServiceFromLabels($Labels) {
  if ($Labels.ContainsKey("deployment") -and $Labels["deployment"]) { return [string]$Labels["deployment"] }
  if ($Labels.ContainsKey("container") -and $Labels["container"] -and $Labels["container"] -ne "POD") { return [string]$Labels["container"] }
  $name = if ($Labels.ContainsKey("pod") -and $Labels["pod"]) { [string]$Labels["pod"] } elseif ($Labels.ContainsKey("instance")) { [string]$Labels["instance"] } else { "" }
  foreach ($component in $components) {
    if ($name -like "$component-*") { return $component }
  }
  return $name
}

function Invoke-Probe([string]$Stage, $Probe, [int]$Iteration, [string]$Token) {
  $started = Get-Date
  $headerArg = ""
  if ($Probe.auth -eq "trainticket_jwt_role_user") {
    $headerArg = "--header='Authorization: Bearer $Token'"
  }
  $cmd = "wget -qSO- $headerArg '$($Probe.url)' 2>&1 | head -100"
  $saved = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  $output = & kubectl exec -n $Namespace $ProbePod -- sh -c $cmd 2>&1
  $exit = $LASTEXITCODE
  $ErrorActionPreference = $saved
  $finished = Get-Date
  $text = ($output -join "`n")
  $status = $null
  if ($text -match "HTTP/1\.1\s+(\d+)") {
    $status = [int]$Matches[1]
  }
  $ok = ($exit -eq 0 -and $status -ge 200 -and $status -lt 300)
  $durationMs = [math]::Round(($finished - $started).TotalMilliseconds, 3)
  $labels = @{
    method = [string]$Probe.method
    endpoint = [string]$Probe.name
    service = [string]$Probe.service
    url = [string]$Probe.url
    status_code = $status
    kubectl_exit = $exit
  }

  Add-JsonLine (MetricRecord $started $Stage "http_probe" "endpoint" $Probe.url $Probe.service "request_duration_ms" $durationMs "milliseconds" "gauge" $labels) $metricsPath
  Add-JsonLine (MetricRecord $started $Stage "http_probe" "endpoint" $Probe.url $Probe.service "request_success" $(if ($ok) { 1 } else { 0 }) "boolean" "gauge" $labels) $metricsPath
  Add-JsonLine (MetricRecord $started $Stage "http_probe" "endpoint" $Probe.url $Probe.service "http_status_code" $(if ($null -eq $status) { 0 } else { $status }) "code" "gauge" $labels) $metricsPath

  return [ordered]@{
    schema_version = "traffic.v3.1"
    timestamp = Iso $started
    stage = $Stage
    run_id = $RunId
    iteration = $Iteration
    endpoint = [string]$Probe.name
    service = [string]$Probe.service
    method = [string]$Probe.method
    url = [string]$Probe.url
    status_code = $status
    ok = $ok
    duration_ms = $durationMs
    kubectl_exit = $exit
    output_head = $text
  }
}

function Capture-TrafficStage([string]$Stage, [int]$Seconds, [string]$Token) {
  $trafficPath = Join-Path $opsDir "traffic_${Stage}.jsonl"
  $start = Get-Date
  $deadline = $start.AddSeconds($Seconds)
  $iteration = 0
  while ((Get-Date) -lt $deadline) {
    $loop = Get-Date
    $iteration++
    foreach ($probe in $probes) {
      Add-JsonLine (Invoke-Probe $Stage $probe $iteration $Token) $trafficPath
    }
    $sleepMs = [int][math]::Max(0, ($PollSeconds - ((Get-Date) - $loop).TotalSeconds) * 1000)
    if ($sleepMs -gt 0) { Start-Sleep -Milliseconds $sleepMs }
  }
  return [ordered]@{
    stage = $Stage
    start = $start
    end = Get-Date
    iterations = $iteration
    traffic_artifact = "raw/operations/traffic_${Stage}.jsonl"
  }
}

function Capture-Logs([string]$Stage, [datetime]$StartAt) {
  $since = Iso $StartAt
  foreach ($component in $components) {
    $path = Join-Path $logsDir "${Stage}__${component}.log"
    $podName = ""
    $saved = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $podName = (& kubectl get pod -n $Namespace -l "app=$component" -o jsonpath="{.items[0].metadata.name}" 2>$null)
    $podExit = $LASTEXITCODE
    $ErrorActionPreference = $saved
    if ($podExit -ne 0 -or [string]::IsNullOrWhiteSpace($podName)) {
      ("{0} collector_log_status stage={1} service={2} message=no_current_pod_found" -f (Iso), $Stage, $component) | Set-Content -LiteralPath $path -Encoding UTF8
      Add-Content -LiteralPath (Join-Path $opsDir "log-warnings.txt") -Value "stage=$Stage component=$component message=no_current_pod_found"
      continue
    }

    $saved = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $logOutput = @(& kubectl logs -n $Namespace $podName.Trim() "--since-time=$since" --timestamps 2>&1)
    $exit = $LASTEXITCODE
    $ErrorActionPreference = $saved
    if ($logOutput.Count -gt 0) {
      $logOutput | Set-Content -LiteralPath $path -Encoding UTF8
    } else {
      ("{0} collector_log_status stage={1} service={2} message=no_application_log_lines_observed" -f (Iso), $Stage, $component) | Set-Content -LiteralPath $path -Encoding UTF8
    }
    if ($exit -ne 0) {
      Add-Content -LiteralPath (Join-Path $opsDir "log-warnings.txt") -Value "stage=$Stage component=$component kubectl_exit=$exit"
    }
  }
}

function PromRange([string]$Query, [datetime]$StartAt, [datetime]$EndAt) {
  $startEpoch = ([DateTimeOffset]$StartAt.ToUniversalTime()).ToUnixTimeSeconds()
  $endEpoch = ([DateTimeOffset]$EndAt.ToUniversalTime()).ToUnixTimeSeconds()
  $encoded = [uri]::EscapeDataString($Query)
  $url = "$Prometheus/api/v1/query_range?query=$encoded&start=$startEpoch&end=$endEpoch&step=${PollSeconds}s"
  return (Invoke-RestMethod $url -TimeoutSec 90).data.result
}

function Export-Prometheus($Windows) {
  $podRe = "(" + (($components | ForEach-Object { [regex]::Escape($_) }) -join "|") + ")-.*"
  $deployRe = "(" + (($components | ForEach-Object { [regex]::Escape($_) }) -join "|") + ")"
  $defs = @(
    @{n="container_cpu_usage_cores"; q="rate(container_cpu_usage_seconds_total{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`",image!=`"`"}[30s])"; u="cores"; t="gauge"},
    @{n="container_cpu_throttled_periods_rate"; q="rate(container_cpu_cfs_throttled_periods_total{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`"}[30s])"; u="periods_per_second"; t="gauge"},
    @{n="container_cpu_throttled_seconds_rate"; q="rate(container_cpu_cfs_throttled_seconds_total{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`"}[30s])"; u="seconds_per_second"; t="gauge"},
    @{n="container_memory_usage_bytes"; q="container_memory_usage_bytes{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`",image!=`"`"}"; u="bytes"; t="gauge"},
    @{n="container_memory_working_set_bytes"; q="container_memory_working_set_bytes{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`",image!=`"`"}"; u="bytes"; t="gauge"},
    @{n="container_memory_rss_bytes"; q="container_memory_rss{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`",image!=`"`"}"; u="bytes"; t="gauge"},
    @{n="container_memory_failcnt"; q="container_memory_failcnt{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`",image!=`"`"}"; u="events"; t="counter"},
    @{n="container_start_time_seconds"; q="container_start_time_seconds{namespace=`"$Namespace`",pod=~`"$podRe`",container!=`"`",image!=`"`"}"; u="epoch_seconds"; t="gauge"},
    @{n="container_network_receive_bytes_rate"; q="rate(container_network_receive_bytes_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="bytes_per_second"; t="gauge"},
    @{n="container_network_transmit_bytes_rate"; q="rate(container_network_transmit_bytes_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="bytes_per_second"; t="gauge"},
    @{n="container_network_receive_packets_rate"; q="rate(container_network_receive_packets_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="packets_per_second"; t="gauge"},
    @{n="container_network_transmit_packets_rate"; q="rate(container_network_transmit_packets_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="packets_per_second"; t="gauge"},
    @{n="container_network_receive_dropped_rate"; q="rate(container_network_receive_packets_dropped_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="packets_per_second"; t="gauge"},
    @{n="container_network_transmit_dropped_rate"; q="rate(container_network_transmit_packets_dropped_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="packets_per_second"; t="gauge"},
    @{n="container_network_receive_errors_rate"; q="rate(container_network_receive_errors_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="errors_per_second"; t="gauge"},
    @{n="container_network_transmit_errors_rate"; q="rate(container_network_transmit_errors_total{namespace=`"$Namespace`",pod=~`"$podRe`"}[30s])"; u="errors_per_second"; t="gauge"},
    @{n="pod_ready"; q="kube_pod_status_ready{namespace=`"$Namespace`",pod=~`"$podRe`",condition=`"true`"}"; u="boolean"; t="state"},
    @{n="container_ready"; q="kube_pod_container_status_ready{namespace=`"$Namespace`",pod=~`"$podRe`"}"; u="boolean"; t="state"},
    @{n="container_running"; q="kube_pod_container_status_running{namespace=`"$Namespace`",pod=~`"$podRe`"}"; u="boolean"; t="state"},
    @{n="container_restart_count"; q="kube_pod_container_status_restarts_total{namespace=`"$Namespace`",pod=~`"$podRe`"}"; u="restarts"; t="counter"},
    @{n="deployment_replicas_ready"; q="kube_deployment_status_replicas_ready{namespace=`"$Namespace`",deployment=~`"$deployRe`"}"; u="replicas"; t="gauge"}
  )

  $allStart = @($Windows.Values | ForEach-Object { $_.start } | Sort-Object | Select-Object -First 1)[0].AddSeconds(-30)
  $allEnd = @($Windows.Values | ForEach-Object { $_.end } | Sort-Object | Select-Object -Last 1)[0].AddSeconds(5)
  $batch = [System.Collections.Generic.List[string]]::new()
  function Flush-PrometheusBatch {
    if ($batch.Count -gt 0) {
      Add-Content -LiteralPath $metricsPath -Encoding UTF8 -Value $batch.ToArray()
      $batch.Clear()
    }
  }
  foreach ($def in $defs) {
    $seriesList = @(PromRange $def.q $allStart $allEnd)
    foreach ($series in $seriesList) {
      foreach ($valuePair in @($series.values)) {
        $at = [DateTimeOffset]::FromUnixTimeSeconds([int64][double]$valuePair[0]).UtcDateTime
        $stage = $null
        foreach ($name in $stages) {
          if ($at -ge $Windows[$name].start.ToUniversalTime() -and $at -le $Windows[$name].end.ToUniversalTime()) {
            $stage = $name
            break
          }
        }
        if (-not $stage) { continue }
        $labels = @{}
        foreach ($prop in $series.metric.PSObject.Properties) {
          if ($prop.Name -ne "__name__") { $labels[$prop.Name] = $prop.Value }
        }
        $entity = if ($labels.ContainsKey("pod") -and $labels["pod"]) { $labels["pod"] } elseif ($labels.ContainsKey("deployment") -and $labels["deployment"]) { $labels["deployment"] } elseif ($labels.ContainsKey("instance")) { $labels["instance"] } else { "" }
        $entityType = if ($labels.ContainsKey("deployment") -and $labels["deployment"]) { "deployment" } elseif ($labels.ContainsKey("container") -and $labels["container"]) { "container" } else { "pod" }
        $batch.Add((ConvertTo-JsonLine (MetricRecord $at $stage "prometheus" $entityType $entity (ServiceFromLabels $labels) $def.n ([double]$valuePair[1]) $def.u $def.t $labels)))
        if ($batch.Count -ge 5000) {
          Flush-PrometheusBatch
        }
      }
    }
  }
  Flush-PrometheusBatch
}

function Capture-Traces([string]$Stage, [datetime]$StartAt, [datetime]$EndAt) {
  $out = Join-Path $traceDir "${Stage}_traces.jsonl"
  $startMicros = [int64](($StartAt.ToUniversalTime() - [datetime]"1970-01-01").TotalMilliseconds * 1000)
  $endMicros = [int64](($EndAt.ToUniversalTime() - [datetime]"1970-01-01").TotalMilliseconds * 1000)
  $count = 0
  foreach ($service in $traceExpectedServices) {
    $url = "$Jaeger/api/traces?service=$service&start=$startMicros&end=$endMicros&limit=1000"
    $raw = Invoke-RestMethod $url -TimeoutSec 60
    foreach ($trace in @($raw.data)) {
      foreach ($span in @($trace.spans)) {
        $process = $trace.processes.($span.processID)
        $micros = [int64]$span.startTime
        $millis = [int64][math]::Floor($micros / 1000)
        $startTime = [DateTimeOffset]::FromUnixTimeMilliseconds($millis).UtcDateTime.AddTicks(($micros % 1000) * 10)
        $endTime = $startTime.AddTicks([int64]$span.duration * 10)
        $refs = @($span.references)
        $parent = @($refs | Where-Object refType -eq "CHILD_OF" | Select-Object -First 1)
        Add-JsonLine ([ordered]@{
          schema_version = "traces.v1"
          timestamp = Iso $startTime
          stage = $Stage
          run_id = $RunId
          trace_id = $span.traceID
          span_id = $span.spanID
          parent_span_id = if ($parent.Count) { $parent[0].spanID } else { $null }
          references = $refs
          service = $process.serviceName
          process_id = $span.processID
          process_tags = @($process.tags)
          operation = $span.operationName
          start_time = Iso $startTime
          end_time = Iso $endTime
          duration_ms = [math]::Round([double]$span.duration / 1000, 3)
          tags = @($span.tags)
          collector_query_service = $service
        }) $out
        $count++
      }
    }
  }
  return $count
}

function Get-DeploymentReadiness {
  $rows = @()
  $deployJson = (& kubectl get deploy -n $Namespace @($components) -o json) | ConvertFrom-Json
  foreach ($item in $deployJson.items) {
    $desired = [int]$item.spec.replicas
    $ready = if ($null -eq $item.status.readyReplicas) { 0 } else { [int]$item.status.readyReplicas }
    $rows += [ordered]@{
      component = $item.metadata.name
      desired = $desired
      ready = $ready
      ok = ($desired -gt 0 -and $ready -eq $desired)
    }
  }
  return $rows
}

function Get-LivePodName([string]$Component) {
  $items = (& kubectl get pod -n $Namespace -l "app=$Component" -o json) | ConvertFrom-Json
  $pod = @($items.items |
    Where-Object { $null -eq $_.metadata.PSObject.Properties["deletionTimestamp"] } |
    Sort-Object { [datetime]$_.metadata.creationTimestamp } -Descending |
    Select-Object -First 1)
  if (-not $pod) { throw "missing live pod for $Component" }
  return [string]$pod[0].metadata.name
}

function Wait-DeploymentReady([string]$Component, [int]$TimeoutSeconds = 420) {
  $saved = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  & kubectl rollout status "deployment/$Component" -n $Namespace "--timeout=${TimeoutSeconds}s" 2>&1 |
    Set-Content -LiteralPath (Join-Path $opsDir "rollout-$Component.log") -Encoding UTF8
  $exit = $LASTEXITCODE
  $ErrorActionPreference = $saved
  return ($exit -eq 0)
}

function New-NetworkChaosYaml([string]$Name, [string]$Component, [int]$LatencyMs, [int]$JitterMs) {
  return @"
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: $Name
  namespace: $Namespace
spec:
  action: delay
  mode: all
  selector:
    namespaces:
      - $Namespace
    labelSelectors:
      app: $Component
  delay:
    latency: "${LatencyMs}ms"
    correlation: "0"
    jitter: "${JitterMs}ms"
  direction: to
"@
}

function New-NetworkLossYaml([string]$Name, [string]$Component, [int]$LossPercent) {
  return @"
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: $Name
  namespace: $Namespace
spec:
  action: loss
  mode: all
  selector:
    namespaces:
      - $Namespace
    labelSelectors:
      app: $Component
  loss:
    loss: "$LossPercent"
    correlation: "0"
  direction: to
"@
}

function New-PodFailureYaml([string]$Name, [string]$Component, [int]$DurationSeconds) {
  return @"
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: $Name
  namespace: $Namespace
spec:
  action: pod-failure
  mode: one
  selector:
    namespaces:
      - $Namespace
    labelSelectors:
      app: $Component
  duration: "${DurationSeconds}s"
"@
}

function New-NginxProxyConfig([int]$TimeoutMs, [bool]$RetryDisabled, [bool]$ForceTimeout = $false) {
  $retry = if ($RetryDisabled) {
@"
      proxy_next_upstream off;
      proxy_next_upstream_tries 1;
"@
  } else {
@"
      proxy_next_upstream error timeout http_502 http_503 http_504;
      proxy_next_upstream_tries 2;
"@
  }
  $upstream = if ($RetryDisabled) {
@"
  upstream travel_backend {
    server 127.0.0.1:9 max_fails=0;
    server ts-travel-service:12346 backup;
  }
"@
  } else { "" }
  $proxyPass = if ($ForceTimeout) {
    "http://10.255.255.1:81/api/v1/travelservice/"
  } elseif ($RetryDisabled) {
    "http://travel_backend/api/v1/travelservice/"
  } else {
    "http://ts-travel-service:12346/api/v1/travelservice/"
  }
  return @"
worker_processes 1;
error_log logs/error.log notice;
events { worker_connections 1024; }
http {
  log_format rca_main '`$time_iso8601 `$remote_addr `$request_method `$uri `$status `$request_time `$upstream_addr `$upstream_status `$upstream_response_time';
  access_log logs/access.log rca_main;
$upstream
  server {
    listen 8080;
    location = /health { return 200 'ok'; }
    location /api/v1/travelservice/ {
      proxy_pass $proxyPass;
      proxy_set_header Host ts-travel-service:12346;
      proxy_connect_timeout ${TimeoutMs}ms;
      proxy_send_timeout ${TimeoutMs}ms;
      proxy_read_timeout ${TimeoutMs}ms;
$retry
    }
    location / { return 404; }
  }
}
"@
}

function Apply-NginxProxyConfig([string]$Tag, [int]$TimeoutMs, [bool]$RetryDisabled, [bool]$ForceTimeout = $false) {
  $pod = Get-LivePodName "ts-ui-dashboard"
  $cfgPath = Join-Path $opsDir "nginx-$Tag.conf"
  [System.IO.File]::WriteAllText($cfgPath, (New-NginxProxyConfig $TimeoutMs $RetryDisabled $ForceTimeout), [System.Text.UTF8Encoding]::new($false))
  $saved = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  $cfgDir = Split-Path -Parent $cfgPath
  $cfgLeaf = Split-Path -Leaf $cfgPath
  Push-Location $cfgDir
  try {
    & kubectl cp ".\$cfgLeaf" "$Namespace/${pod}:/usr/local/openresty/nginx/conf/nginx.conf" 2>&1 |
      Set-Content -LiteralPath (Join-Path $opsDir "nginx-$Tag-cp.log") -Encoding UTF8
    $cpExit = $LASTEXITCODE
  } finally {
    Pop-Location
  }
  & kubectl exec -n $Namespace $pod -- /usr/local/openresty/nginx/sbin/nginx -t 2>&1 |
    Set-Content -LiteralPath (Join-Path $opsDir "nginx-$Tag-test.log") -Encoding UTF8
  $testExit = $LASTEXITCODE
  & kubectl exec -n $Namespace $pod -- /usr/local/openresty/nginx/sbin/nginx -s reload 2>&1 |
    Set-Content -LiteralPath (Join-Path $opsDir "nginx-$Tag-reload.log") -Encoding UTF8
  $reloadExit = $LASTEXITCODE
  $ErrorActionPreference = $saved
  if ($cpExit -ne 0 -or $testExit -ne 0 -or $reloadExit -ne 0) { return 1 }
  return 0
}

function Start-SingleFault {
  if (-not $isFaultRun) { return $null }
  if ([string]::IsNullOrWhiteSpace($FaultTarget)) { throw "FaultTarget is required for fault runs" }

  $faultName = ("v31-" + $FaultSlotId.ToLower() + "-" + ($FaultMode.ToLower() -replace "[^a-z0-9]+", "-")).Trim("-")
  $started = Get-Date
  $record = [ordered]@{
    slot_id = $FaultSlotId
    mode = $FaultMode
    target = $FaultTarget
    started_at = Iso $started
    recovered_at = $null
    inject_exit = 1
    recover_exit = 1
    details = @{}
  }

  switch ($FaultMode) {
    "cpu" {
      $pod = Get-LivePodName $FaultTarget
      $record.details.pod_before = $pod
      $cmd = "nohup sh -c 'while true; do :; done' >/tmp/multirca-cpu-stress.log 2>&1 & echo `$! >/tmp/multirca-cpu-stress.pid"
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl exec -n $Namespace $pod -- sh -c $cmd 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-cpu-start.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
    }
    "memory" {
      $pod = Get-LivePodName $FaultTarget
      $record.details.pod_before = $pod
      $record.details.memory_mib = $MemoryMiB
      $cmd = "nohup perl -e 'my `$a = `"x`" x ($MemoryMiB * 1024 * 1024); sleep 100000' >/tmp/multirca-memory-stress.log 2>&1 & echo `$! >/tmp/multirca-memory-stress.pid"
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl exec -n $Namespace $pod -- sh -c $cmd 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-memory-start.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
    }
    "pause" {
      $pod = Get-LivePodName $FaultTarget
      $record.details.pod_before = $pod
      $record.details.signal = "STOP"
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl exec -n $Namespace $pod -- sh -c "kill -STOP 1" 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-pause-stop.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
    }
    "network_delay" {
      $yamlPath = Join-Path $opsDir "fault-$FaultSlotId-network-delay.yaml"
      New-NetworkChaosYaml $faultName $FaultTarget $NetworkDelayMs $NetworkJitterMs |
        Set-Content -LiteralPath $yamlPath -Encoding UTF8
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl apply -f $yamlPath 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-network-apply.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
      $record.details.chaos_name = $faultName
      $record.details.chaos_kind = "networkchaos"
    }
    "packet_loss" {
      $yamlPath = Join-Path $opsDir "fault-$FaultSlotId-packet-loss.yaml"
      New-NetworkLossYaml $faultName $FaultTarget $PacketLossPercent |
        Set-Content -LiteralPath $yamlPath -Encoding UTF8
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl apply -f $yamlPath 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-loss-apply.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
      $record.details.chaos_name = $faultName
      $record.details.chaos_kind = "networkchaos"
      $record.details.loss_percent = $PacketLossPercent
    }
    "pod_kill" {
      $pod = Get-LivePodName $FaultTarget
      $record.details.pod_before = $pod
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl delete pod $pod -n $Namespace --grace-period=0 --force --wait=false 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-pod-kill.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
    }
    "pod_failure" {
      $yamlPath = Join-Path $opsDir "fault-$FaultSlotId-pod-failure.yaml"
      New-PodFailureYaml $faultName $FaultTarget $FaultDurationSeconds |
        Set-Content -LiteralPath $yamlPath -Encoding UTF8
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl apply -f $yamlPath 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-podchaos-apply.log") -Encoding UTF8
      $record.inject_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
      $record.details.chaos_name = $faultName
      $record.details.chaos_kind = "podchaos"
    }
    "nginx_timeout" {
      $record.inject_exit = Apply-NginxProxyConfig "fault-$FaultSlotId-timeout" 300 $false $true
      $record.details.pod_before = Get-LivePodName "ts-ui-dashboard"
      $record.details.timeout_ms = 300
      $record.details.force_timeout_upstream = "10.255.255.1:81"
      $record.details.retry_disabled = $false
    }
    "nginx_retry_disabled" {
      $record.inject_exit = Apply-NginxProxyConfig "fault-$FaultSlotId-retry-disabled" 2000 $true
      $record.details.pod_before = Get-LivePodName "ts-ui-dashboard"
      $record.details.timeout_ms = 2000
      $record.details.retry_disabled = $true
    }
    default {
      throw "unsupported FaultMode: $FaultMode"
    }
  }

  Write-Json $record (Join-Path $opsDir "fault_injection.json")
  return $record
}

function Stop-SingleFault($FaultRecord) {
  if (-not $isFaultRun -or $null -eq $FaultRecord) { return $FaultRecord }
  $recovered = Get-Date
  switch ($FaultMode) {
    "cpu" {
      $pod = Get-LivePodName $FaultTarget
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl exec -n $Namespace $pod -- sh -c "if [ -f /tmp/multirca-cpu-stress.pid ]; then kill `$(cat /tmp/multirca-cpu-stress.pid) 2>/dev/null || true; rm -f /tmp/multirca-cpu-stress.pid; fi" 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-cpu-stop.log") -Encoding UTF8
      $FaultRecord.recover_exit = 0
      $ErrorActionPreference = $saved
      $FaultRecord.details.pod_after = $pod
    }
    "memory" {
      $pod = Get-LivePodName $FaultTarget
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl exec -n $Namespace $pod -- sh -c "if [ -f /tmp/multirca-memory-stress.pid ]; then kill `$(cat /tmp/multirca-memory-stress.pid) 2>/dev/null || true; rm -f /tmp/multirca-memory-stress.pid; fi" 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-memory-stop.log") -Encoding UTF8
      $FaultRecord.recover_exit = 0
      $ErrorActionPreference = $saved
      $FaultRecord.details.pod_after = $pod
    }
    "pause" {
      $pod = Get-LivePodName $FaultTarget
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl exec -n $Namespace $pod -- sh -c "kill -CONT 1" 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-pause-cont.log") -Encoding UTF8
      $FaultRecord.recover_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
      $FaultRecord.details.pod_after = $pod
    }
    "network_delay" {
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl delete networkchaos $FaultRecord.details.chaos_name -n $Namespace --ignore-not-found=true --wait=true 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-network-delete.log") -Encoding UTF8
      $FaultRecord.recover_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
    }
    "packet_loss" {
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl delete networkchaos $FaultRecord.details.chaos_name -n $Namespace --ignore-not-found=true --wait=true 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-loss-delete.log") -Encoding UTF8
      $FaultRecord.recover_exit = $LASTEXITCODE
      $ErrorActionPreference = $saved
    }
    "pod_kill" {
      $ready = Wait-DeploymentReady $FaultTarget
      $FaultRecord.recover_exit = if ($ready) { 0 } else { 1 }
      $FaultRecord.details.pod_after = Get-LivePodName $FaultTarget
    }
    "pod_failure" {
      $saved = $ErrorActionPreference
      $ErrorActionPreference = "Continue"
      & kubectl delete podchaos $FaultRecord.details.chaos_name -n $Namespace --ignore-not-found=true --wait=true 2>&1 |
        Set-Content -LiteralPath (Join-Path $opsDir "fault-$FaultSlotId-podchaos-delete.log") -Encoding UTF8
      $deleteExit = $LASTEXITCODE
      $ErrorActionPreference = $saved
      $ready = Wait-DeploymentReady $FaultTarget
      $FaultRecord.recover_exit = if ($deleteExit -eq 0 -and $ready) { 0 } else { 1 }
    }
    "nginx_timeout" {
      $FaultRecord.recover_exit = Apply-NginxProxyConfig "recover-$FaultSlotId-baseline" 2000 $false
      $FaultRecord.details.pod_after = Get-LivePodName "ts-ui-dashboard"
    }
    "nginx_retry_disabled" {
      $FaultRecord.recover_exit = Apply-NginxProxyConfig "recover-$FaultSlotId-baseline" 2000 $false
      $FaultRecord.details.pod_after = Get-LivePodName "ts-ui-dashboard"
    }
  }
  $FaultRecord.recovered_at = Iso $recovered
  Write-Json $FaultRecord (Join-Path $opsDir "fault_injection.json")
  return $FaultRecord
}

function Summarize-Stage([string]$Stage) {
  $trafficPath = Join-Path $opsDir "traffic_${Stage}.jsonl"
  $records = @()
  if (Test-Path $trafficPath) {
    $records = @(Get-Content -LiteralPath $trafficPath | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json })
  }
  $byEndpoint = @()
  foreach ($probe in $probes) {
    $items = @($records | Where-Object { $_.endpoint -eq $probe.name })
    $ok = @($items | Where-Object { $_.ok }).Count
    $latencies = @($items | ForEach-Object { [double]$_.duration_ms } | Sort-Object)
    $byEndpoint += [ordered]@{
      endpoint = [string]$probe.name
      service = [string]$probe.service
      total = $items.Count
      ok = $ok
      ok_ratio = if ($items.Count) { [math]::Round($ok / $items.Count, 3) } else { 0 }
      p95_ms = if ($latencies.Count) { $latencies[[math]::Min($latencies.Count - 1, [math]::Floor($latencies.Count * 0.95))] } else { 0 }
      status_codes = @($items | ForEach-Object { $_.status_code } | Sort-Object -Unique)
    }
  }
  return [ordered]@{
    stage = $Stage
    total_records = $records.Count
    by_endpoint = $byEndpoint
    bad_endpoints = @($byEndpoint | Where-Object { $_.ok_ratio -lt 0.95 })
  }
}

Write-Host "RunId=$RunId"
Write-Host "RunDir=$runDir"
Write-Host "Topology=$TopologyPath"
Write-Host "Collecting 3 stages x $StageSeconds seconds, $($components.Count) components, $($probes.Count) probes"
if ($isFaultRun) {
  Write-Host "FaultSlot=$FaultSlotId root_count=$RootCount faults=$($faultPlan.Count)"
}

Ensure-PortForward 19090 "monitoring" "service/rca-prometheus" "19090:9090"
Ensure-PortForward 16686 $Namespace "service/jaeger-query" "16686:16686"

(& kubectl get deploy -n $Namespace @($components) -o wide 2>&1) | Set-Content -LiteralPath (Join-Path $opsDir "deploy_snapshot.txt") -Encoding UTF8
$savedPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
(& kubectl get endpoints -n $Namespace @($components) -o wide 2>&1) | Set-Content -LiteralPath (Join-Path $opsDir "endpoint_snapshot.txt") -Encoding UTF8
$ErrorActionPreference = $savedPreference

$createdAt = Get-Date
$token = New-TrainTicketJwt
$windows = @{}
$traceCounts = @{}
$faultRecord = $null
$faultRecords = @()
$hasUiDashboardFault = ($isFaultRun -and @($faultPlan | Where-Object { $_.target -eq "ts-ui-dashboard" }).Count -gt 0)

function Set-FaultGlobals($Spec) {
  $script:FaultTarget = [string]$Spec.target
  $script:FaultMode = [string]$Spec.mode
  $script:FaultClass = [string]$Spec.class
  $script:FaultType = [string]$Spec.type
  $script:InjectionFault = [string]$Spec.injection
  $instanceId = if ($Spec.PSObject.Properties["fault_instance_id"]) { [string]$Spec.fault_instance_id } else { "F1" }
  $script:FaultSlotId = "$($script:RunId)-$instanceId"
}

foreach ($stage in $stages) {
  if ($stage -eq "pre_fault" -and $hasUiDashboardFault) {
    $baselineExit = Apply-NginxProxyConfig "preflight-$SampleSlotId-baseline" 2000 $false
    if ($baselineExit -ne 0) { throw "failed to apply UI baseline nginx config before $SampleSlotId" }
  }
  Write-Host "Stage $stage started"
  if ($stage -eq "during_fault" -and $isFaultRun) {
    $faultRecords = @()
    foreach ($spec in $faultPlan) {
      Set-FaultGlobals $spec
      $faultRecords += Start-SingleFault
      Start-Sleep -Seconds 2
    }
    Start-Sleep -Seconds 5
  }
  $window = Capture-TrafficStage $stage $StageSeconds $token
  $windows[$stage] = $window
  if ($stage -eq "during_fault" -and $isFaultRun) {
    $stopped = @()
    for ($i = $faultRecords.Count - 1; $i -ge 0; $i--) {
      $rec = $faultRecords[$i]
      $spec = @($faultPlan | Where-Object {
        $instanceId = if ($_.PSObject.Properties["fault_instance_id"]) { [string]$_.fault_instance_id } else { "F1" }
        $rec.slot_id -like "*-$instanceId"
      } | Select-Object -First 1)
      if ($spec.Count -gt 0) { Set-FaultGlobals $spec[0] }
      $stopped += Stop-SingleFault $rec
      Start-Sleep -Seconds 2
    }
    $faultRecords = @($stopped | Sort-Object slot_id)
    $faultRecord = if ($faultRecords.Count -gt 0) { $faultRecords[0] } else { $null }
    Write-Json ([ordered]@{ faults = $faultRecords }) (Join-Path $opsDir "fault_injection.json")
  }
  Capture-Logs $stage $window.start
  $traceCounts[$stage] = Capture-Traces $stage $window.start $window.end
  Write-Host "Stage $stage completed: iterations=$($window.iterations), traces=$($traceCounts[$stage])"
}

Write-Host "Exporting Prometheus infrastructure metrics..."
Export-Prometheus $windows

$deployReadiness = @(Get-DeploymentReadiness)
$trafficSummary = @{}
foreach ($stage in $stages) {
  $trafficSummary[$stage] = Summarize-Stage $stage
}

$metricRecords = @(Get-Content -LiteralPath $metricsPath | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json })
$metricNames = @($metricRecords | ForEach-Object { $_.metric } | Sort-Object -Unique)
$promRecords = @($metricRecords | Where-Object { $_.source -eq "prometheus" })
$promServices = @($promRecords | ForEach-Object { $_.service } | Sort-Object -Unique)

$traceRecords = @(Get-ChildItem -LiteralPath $traceDir -Filter "*_traces.jsonl" | ForEach-Object {
  Get-Content -LiteralPath $_.FullName | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json }
})
$traceServices = @($traceRecords | ForEach-Object { $_.service } | Sort-Object -Unique)
$missingTraceServices = @($traceExpectedServices | Where-Object { $_ -notin $traceServices })
$missingPromServices = @($components | Where-Object { $_ -notin $promServices })
$notReady = @($deployReadiness | Where-Object { -not $_.ok })
$trafficGateStages = if ($isFaultRun) { @("pre_fault", "post_recovery") } else { $stages }
$badTrafficStages = @($trafficGateStages | Where-Object { @($trafficSummary[$_].bad_endpoints).Count -gt 0 })
$logFiles = @(Get-ChildItem -LiteralPath $logsDir -Filter "*.log")
$nonEmptyLogFiles = @($logFiles | Where-Object { $_.Length -gt 0 })
$expectedLogFiles = $components.Count * $stages.Count
$faultPassed = $true
if ($isFaultRun) {
  $faultPassed = ($faultRecords.Count -eq $faultPlan.Count -and @($faultRecords | Where-Object { [int]$_.inject_exit -ne 0 -or [int]$_.recover_exit -ne 0 }).Count -eq 0)
}

$effectiveRootCount = if ($isFaultRun) { [math]::Max($RootCount, $faultPlan.Count) } else { 0 }
$effectiveComposition = if ($isFaultRun) { $CompositionType } else { "no_fault" }
$effectiveInteraction = if ($isFaultRun) { $InteractionPattern } else { "no_injection" }
$effectiveCategory = if ($isFaultRun) {
  if ($faultPlan.Count -gt 1) { "multi_root" } else { $FaultClass }
} else { "baseline" }
$effectiveRootCauses = @()
$effectiveFaults = @()
if ($isFaultRun) {
  foreach ($spec in $faultPlan) {
    $instanceId = if ($spec.PSObject.Properties["fault_instance_id"]) { [string]$spec.fault_instance_id } else { "F1" }
    $rec = @($faultRecords | Where-Object { $_.slot_id -like "*-$instanceId" } | Select-Object -First 1)
    $target = [string]$spec.target
    $targetInstance = $target
    $startedAt = $null
    $recoveredAt = $null
    $status = "failed_or_unverified"
    if ($rec.Count -gt 0) {
      $record = $rec[0]
      $startedAt = $record.started_at
      $recoveredAt = $record.recovered_at
      $status = if ([int]$record.inject_exit -eq 0 -and [int]$record.recover_exit -eq 0) { "recovered" } else { "failed_or_unverified" }
      if ($record.details -and $record.details.Contains("pod_after") -and $record.details["pod_after"]) {
        $targetInstance = $record.details["pod_after"]
      } elseif ($record.details -and $record.details.Contains("pod_before") -and $record.details["pod_before"]) {
        $targetInstance = $record.details["pod_before"]
      }
    }
    $effectiveRootCauses += $target
    $effectiveFaults += [ordered]@{
      fault_instance_id = $instanceId
      fault_class = [string]$spec.class
      fault_type = [string]$spec.type
      injection_fault = [string]$spec.injection
      target_component = $target
      target_container = $targetInstance
      role = if ($spec.PSObject.Properties["role"]) { [string]$spec.role } else { "co-primary" }
      injected_at = $startedAt
      recovered_at = $recoveredAt
      status = $status
    }
  }
}

$effectiveGroundTruth = if ($isFaultRun) {
  $windowsByFault = [ordered]@{}
  foreach ($fault in $effectiveFaults) {
    $windowsByFault[$fault.fault_instance_id] = @{
      start_time = $fault.injected_at
      end_time = $fault.recovered_at
    }
  }
  [ordered]@{
    sample_id = $RunId
    answer_type = if ($effectiveRootCount -gt 1) { "multi_root" } else { "single_root" }
    root_count = $effectiveRootCount
    fault_category = $effectiveCategory
    composition_type = $effectiveComposition
    interaction_pattern = $effectiveInteraction
    root_cause_services = $effectiveRootCauses
    root_cause_instances = @($effectiveFaults | ForEach-Object { $_.target_container })
    fault_types = @($effectiveFaults | ForEach-Object { $_.fault_type })
    injection_faults = @($effectiveFaults | ForEach-Object { $_.injection_fault })
    component_ground_truth = $effectiveFaults
    component_fault_windows = $windowsByFault
  }
} else {
  [ordered]@{ sample_id = $RunId; answer_type = "no_fault"; root_count = 0; root_cause_services = @(); component_ground_truth = @() }
}

$obs = [ordered]@{}
foreach ($stage in $stages) {
  $obs[$stage] = [ordered]@{
    window_start_at = Iso $windows[$stage].start
    window_end_at = Iso $windows[$stage].end
    window_seconds = [math]::Round(($windows[$stage].end - $windows[$stage].start).TotalSeconds, 3)
    poll_interval_seconds = $PollSeconds
    traffic_artifact = $windows[$stage].traffic_artifact
    metrics_manifest = "raw/metrics/manifest.json"
    metrics_filter = @{ stage = $stage }
    traces_manifest = "raw/traces/manifest.json"
    traces_filter = @{ stage = $stage }
    logs_manifest = "raw/logs/manifest.json"
    logs_filter = @{ stage = $stage }
  }
}

$quality = [ordered]@{
  schema_version = "k8s-v3.1-expanded-baseline-quality.v1"
  run_id = $RunId
  topology_version = $topology.topology_version
  topology_manifest = (Resolve-Path $TopologyPath).Path
  active_component_count = $components.Count
  business_probe_count = $probes.Count
  stage_seconds = $StageSeconds
  poll_seconds = $PollSeconds
  generated_at = Iso
  deployment_readiness = [ordered]@{
    total = $deployReadiness.Count
    ready = @($deployReadiness | Where-Object { $_.ok }).Count
    not_ready = $notReady
    passed = ($notReady.Count -eq 0)
  }
  traffic = [ordered]@{
    stages = $trafficSummary
    bad_stages = $badTrafficStages
    passed = ($badTrafficStages.Count -eq 0)
  }
  prometheus = [ordered]@{
    source = "kube-state-metrics + kubelet/cadvisor infrastructure metrics"
    record_count = $promRecords.Count
    metric_names = $metricNames
    observed_services = $promServices
    missing_services = $missingPromServices
    app_metrics_endpoint_status = "not claimed; TrainTicket business /metrics endpoints are not part of this baseline contract"
    passed = ($promRecords.Count -gt 0)
  }
  traces = [ordered]@{
    expected_services = $traceExpectedServices
    observed_services = $traceServices
    missing_services = $missingTraceServices
    per_stage_span_count = $traceCounts
    passed = ($missingTraceServices.Count -eq 0 -and (@($stages | Where-Object { $traceCounts[$_] -le 0 }).Count -eq 0))
  }
  logs = [ordered]@{
    expected_files = $expectedLogFiles
    actual_files = $logFiles.Count
    non_empty_files = $nonEmptyLogFiles.Count
    passed = ($logFiles.Count -eq $expectedLogFiles -and $nonEmptyLogFiles.Count -eq $expectedLogFiles)
  }
}
$quality.fault = [ordered]@{
  enabled = $isFaultRun
  slot_id = $SampleSlotId
  target = $FaultTarget
  mode = $FaultMode
  injection = $faultRecord
  passed = $faultPassed
}
$quality.passed = ($quality.deployment_readiness.passed -and $quality.traffic.passed -and $quality.prometheus.passed -and $quality.traces.passed -and $quality.logs.passed -and $faultPassed)

Write-Json $quality (Join-Path $runDir "baseline_quality.json")

Write-Json ([ordered]@{
  schema_version = "metrics-manifest.v3.1-baseline"
  storage_layout = "single_dir_stage_tagged"
  artifact_root = "raw/metrics"
  files = @(@{ artifact = "raw/metrics/metrics_v2.jsonl"; kind = "unified_timeseries"; stages = $stages })
  stage_windows = $obs
  validation = @{ valid = $quality.prometheus.passed; metric_count = $metricNames.Count; record_count = $metricRecords.Count }
}) (Join-Path $metricsDir "manifest.json")

Write-Json ([ordered]@{
  schema_version = "traces-manifest.v3.1-baseline"
  storage_layout = "single_dir_stage_tagged"
  artifact_root = "raw/traces"
  files = @($stages | ForEach-Object { @{ artifact = "raw/traces/${_}_traces.jsonl"; stage = $_ } })
  collector_query_services = $traceExpectedServices
  observed_span_services = $traceServices
  validation = @{ valid = $quality.traces.passed; missing_services = $missingTraceServices; per_stage_span_count = $traceCounts }
}) (Join-Path $traceDir "manifest.json")

Write-Json ([ordered]@{
  schema_version = "logs-manifest.v3.1-baseline"
  storage_layout = "single_dir_stage_tagged"
  artifact_root = "raw/logs"
  files = @($logFiles | ForEach-Object {
    $parts = $_.BaseName -split "__", 2
    @{ artifact = "raw/logs/$($_.Name)"; stage = $parts[0]; service = $parts[1]; bytes = $_.Length }
  })
  validation = @{ valid = $quality.logs.passed; expected_files = $expectedLogFiles; non_empty_files = $nonEmptyLogFiles.Count }
}) (Join-Path $logsDir "manifest.json")

Write-Json ([ordered]@{
  schema_version = "v1.2"
  metric_schema_version = "metrics.v2"
  storage_layout = "single_dir_stage_tagged"
  stage_layout = $stages
  sample_id = $RunId
  run_id = $RunId
  formal_slot_id = if ($isFaultRun) { $SampleSlotId } else { "k8s-v3.1-expanded-baseline" }
  scenario_name = if ($isFaultRun) { if ([string]::IsNullOrWhiteSpace($ScenarioName)) { "$SampleSlotId $FaultMode on $FaultTarget" } else { $ScenarioName } } else { "Expanded TrainTicket operational subset baseline" }
  system = "trainticket"
  platform = "kubernetes"
  phase = if ($isFaultRun -and $effectiveRootCount -gt 1) { "formal_multi_root_k8s_v3_1" } elseif ($isFaultRun) { "formal_single_root_k8s_v3_1" } else { "formal_expanded_baseline_k8s_v3_1" }
  sample_status = if ($quality.passed) { "ready" } else { "draft" }
  ready_for_release = $quality.passed
  validation_complete = $quality.passed
  root_count = $effectiveRootCount
  category = $effectiveCategory
  composition_type = $effectiveComposition
  interaction_pattern = $effectiveInteraction
  root_causes = $effectiveRootCauses
  faults = $effectiveFaults
  ground_truth = $effectiveGroundTruth
  topology_version = $topology.topology_version
  topology_manifest = "manifests/k8s-v3.1-contacts-topology.json"
  active_components = $components
  workload_probes = @($probes | ForEach-Object { $_.name })
  observation_stages = $obs
  traffic_stats = $trafficSummary
  trace_stats = @{ per_stage_span_count = $traceCounts; expected_services = $traceExpectedServices; observed_span_services = $traceServices }
  validation_results = @(
    @{ id = "expanded_topology_ready"; status = if ($quality.deployment_readiness.passed) { "passed" } else { "failed" } },
    @{ id = "three_stage_endpoint_traffic"; status = if ($quality.traffic.passed) { "passed" } else { "failed" } },
    @{ id = "infrastructure_prometheus_present"; status = if ($quality.prometheus.passed) { "passed" } else { "failed" } },
    @{ id = "trace_services_present"; status = if ($quality.traces.passed) { "passed" } else { "failed" } },
    @{ id = "three_stage_logs_complete"; status = if ($quality.logs.passed) { "passed" } else { "failed" } },
    @{ id = "fault_injection_completed"; status = if ($faultPassed) { "passed" } else { "failed" } }
  )
  artifacts = @{
    metadata = "metadata.json"
    quality = "baseline_quality.json"
    metrics = "raw/metrics"
    traces = "raw/traces"
    logs = "raw/logs"
    operations = "raw/operations"
  }
  created_at = Iso $createdAt
  updated_at = Iso
}) (Join-Path $runDir "metadata.json")

if ($isFaultRun) {
  $metaForGroundTruth = Get-Content -Raw -LiteralPath (Join-Path $runDir "metadata.json") | ConvertFrom-Json
  Write-Json $metaForGroundTruth.ground_truth (Join-Path $runDir "groundtruth.json")
}

@(
  "# $RunId",
  "",
  "- topology: $($topology.topology_version)",
  "- active_components: $($components.Count)",
  "- workload_probes: $($probes.Count)",
  "- stages: pre_fault, during_fault, post_recovery",
  "- stage_seconds: $StageSeconds",
  "- poll_seconds: $PollSeconds",
  "- sample_status: $(if ($quality.passed) { "ready" } else { "draft" })",
  "- ready_for_release: $($quality.passed)",
  "- fault_slot: $(if ($isFaultRun) { $SampleSlotId } else { "none" })",
  "- fault_target: $(if ($isFaultRun) { $FaultTarget } else { "none" })",
  "- fault_mode: $(if ($isFaultRun) { $FaultMode } else { "none" })",
  "- prometheus_scope: kube-state-metrics + kubelet/cadvisor infrastructure metrics",
  "- trace_missing_services: $($missingTraceServices -join ',')",
  "- log_files: $($nonEmptyLogFiles.Count)/$expectedLogFiles non-empty"
) | Set-Content -LiteralPath (Join-Path $runDir "summary.md") -Encoding UTF8

Write-Host "Baseline quality passed=$($quality.passed)"
Write-Host "RUN_ID=$RunId"
Write-Host "RUN_DIR=$runDir"
if (-not $quality.passed) {
  exit 1
}
