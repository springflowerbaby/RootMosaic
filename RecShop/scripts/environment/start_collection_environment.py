"""Start only existing collection runtime components, then check the collection environment.

No collector, fault, rollout, deployment creation, experimental recovery or
database write is performed. Known Docker startup sockets can be backed up once.
This is an installed-environment launcher, not a first-install tool.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEFAULT_ENV = None
from scripts.collection import environment as portable_env
DOCKER_DESKTOP = Path('C:/Program Files/Docker/Docker/Docker Desktop.exe')
APPS = tuple('address admin-audit ai-memory announcement backend cart catalog checkout interaction inventory llm-rerank merchant notification order payment pricing promotion rec-agent review review-query sasrec search shipping shop-web user'.split())
DEPLOYMENTS = set(APPS) | {'catalog-gw', 'kube-state-metrics', 'm1-cri-resource-exporter'}
METRICS_ENDPOINT = 'http://host.docker.internal:14317'
# Existing container identities: (image, compose project, compose service, ports).
CONTAINERS = {
    'recweb2-jaeger': ('jaegertracing/all-in-one:1.76.0', 'ops', 'jaeger', {'16686/tcp': '16686'}),
    'recweb2-loki': ('grafana/loki:3.5.3', 'ops', 'loki', {'3100/tcp': '3100'}),
    'recweb2-otel-collector': ('otel/opentelemetry-collector-contrib:0.128.0', 'ops', 'otel-collector', {'4317/tcp': '4317', '4318/tcp': '4318', '8889/tcp': '8889'}),
    'recshop-m1-otel-metrics': ('otel/opentelemetry-collector-contrib:0.128.0', 'recshop-m1-sampling', 'm1-otel-metrics', {'4317/tcp': '14317', '8889/tcp': '18899'}),
    'recshop-m1-prometheus': ('prom/prometheus:v3.5.0', 'recshop-m1-sampling', 'm1-prometheus', {'9090/tcp': '19090'}),
}
MOUNTS = {
    'recweb2-otel-collector': ('/etc/otel-config.yaml', 'ops/otel-config.yaml'),
    'recweb2-loki': ('/etc/loki/loki-config.yaml', 'ops/loki-config.yaml'),
    'recshop-m1-otel-metrics': ('/etc/m1/otel-metrics-config.yaml', 'ops/metrics/otel-metrics-config.yaml'),
    'recshop-m1-prometheus': ('/etc/prometheus/prometheus.yml', 'ops/metrics/prometheus-container-resources.yml'),
}


class Blocked(RuntimeError):
    """Only fixed public reasons; command errors and environment values stay private."""


def need(condition, reason):
    if not condition:
        raise Blocked(reason)


def hidden_options():
    if os.name != 'nt':
        return {'start_new_session': True}
    startup = subprocess.STARTUPINFO()
    startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup.wShowWindow = subprocess.SW_HIDE
    return {'startupinfo': startup, 'creationflags': subprocess.CREATE_NO_WINDOW}


class Host:
    def __init__(self):
        self.env = dict(os.environ, NO_PROXY='*', no_proxy='*', PYTHONIOENCODING='utf-8')
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.docker = self.executable('docker')
        self.kubectl = self.executable('kubectl')

    @staticmethod
    def executable(name):
        try:
            return portable_env.resolve_cli(name)
        except ValueError:
            raise Blocked(name + '_missing_or_changed:check_runtime_CLI_path') from None

    def run(self, args, timeout=20):
        try:
            reply = subprocess.run(args, cwd=ROOT, env=self.env, text=True, encoding='utf-8',
                                   errors='replace', capture_output=True, timeout=timeout, **hidden_options())
        except (OSError, subprocess.TimeoutExpired):
            raise Blocked('command_unavailable_or_timed_out') from None
        need(reply.returncode == 0, 'command_failed')
        return reply.stdout.strip()

    def json(self, args, timeout=20):
        try:
            return json.loads(self.run(args, timeout))
        except (ValueError, TypeError):
            raise Blocked('invalid_command_json') from None

    def http(self, url, *, as_json=False):
        try:
            with self.opener.open(url, timeout=5) as reply:
                need(reply.status == 200, 'endpoint_not_ready')
                body = reply.read(4_000_001)
            need(len(body) <= 4_000_000, 'endpoint_response_too_large')
            return json.loads(body) if as_json else True
        except Blocked:
            raise
        except Exception:
            raise Blocked('endpoint_not_ready') from None

    def start_hidden(self, args, label):
        logs = ROOT / '.recshop-collection/env-launcher'
        logs.mkdir(parents=True, exist_ok=True)
        log = logs / (label + '-' + str(time.time_ns()) + '.log')
        try:
            with log.open('ab') as stream:
                proc = subprocess.Popen(args, cwd=ROOT, env=self.env, stdin=subprocess.DEVNULL,
                                        stdout=stream, stderr=stream, **hidden_options())
            return proc.pid
        except OSError:
            raise Blocked(label + '_start_failed') from None

    def collector_pids(self):
        # Legacy process names remain only to prevent a second concurrent collector.
        if os.name != 'nt':
            rows = self.run(['ps', '-eo', 'pid=,args=']).splitlines()
            return [int(row.strip().split(None, 1)[0]) for row in rows
                    if re.search(r'(?:collection[./](scenario_runner|run_campaign|gateway_cpu_network|run_scenario)|rq4_collect[./](common_driver|repeat_batch|formal_incremental|driver_entry))', row)]
        script = r"""$p = @(Get-CimInstance Win32_Process | Where-Object {
 $_.Name -match '^(python|pythonw|powershell|pwsh)\.exe$' -and
 $_.CommandLine -match 'collection[./\\](scenario_runner|run_campaign|gateway_cpu_network|run_scenario)|collect_dataset\.ps1|rq4_collect[./\\](common_driver|repeat_batch|formal_incremental)|collect_m1_repeats\.ps1|run_(six_firstpass|firstpass_queue|with_db_env)\.py|chaos_k8s_runner(_m1)?\.py|traditional_v2_lite[./\\](b1_lite|collect_lite)' });
 ConvertTo-Json -Compress -InputObject @($p | ForEach-Object { [int]$_.ProcessId })"""
        # Encoding keeps the process query itself out of its own text matching.
        import base64
        encoded = base64.b64encode(script.encode('utf-16-le')).decode('ascii')
        return self.json(['powershell.exe', '-NoProfile', '-EncodedCommand', encoded])


def wait_for(check, timeout, reason, check_only):
    deadline = time.monotonic() + (0 if check_only else timeout)
    last_reason = None
    while True:
        try:
            value = check()
            if value:
                return value
        except Blocked as exc:
            last_reason = str(exc)
        if time.monotonic() >= deadline:
            raise Blocked(reason + (':' + last_reason if last_reason else ''))
        time.sleep(min(3, max(0, deadline - time.monotonic())))


def container_identity(doc, name):
    image, project, service, ports = CONTAINERS[name]
    labels = doc['Config'].get('Labels') or {}
    need(doc['Name'].lstrip('/') == name and doc['Config']['Image'] == image and
         labels.get('com.docker.compose.project') == project and
         labels.get('com.docker.compose.service') == service, 'container_identity_mismatch:' + name)
    bindings = doc['HostConfig'].get('PortBindings') or {}
    need(all(len(bindings.get(port) or []) == 1 and bindings[port][0]['HostPort'] == host
             for port, host in ports.items()), 'container_port_mismatch:' + name)
    need(portable_env.current()['windows_startup']['container_network'] in doc['NetworkSettings']['Networks'], 'container_network_mismatch:' + name)
    if name in MOUNTS:
        destination, source_suffix = MOUNTS[name]
        rows = [m for m in doc['Mounts'] if m['Destination'] == destination]
        sources = portable_env.startup_bind_sources(portable_env.current())
        expected_source = sources.get(name)
        need(len(rows) == 1 and rows[0]['Type'] == 'bind' and rows[0]['RW'] is False and
             (portable_env.normalize_bind_source(rows[0]['Source']) == expected_source if expected_source is not None
              else rows[0]['Source'].replace('\\', '/').endswith('/' + source_suffix)), 'container_mount_mismatch:' + name)
    if name == 'recshop-m1-prometheus':
        need(any(m['Destination'] == '/prometheus' and m['Type'] == 'volume' and
                 m.get('Name') == portable_env.current()['windows_startup']['prometheus_volume'] for m in doc['Mounts']),
             'm1_tsdb_volume_mismatch')
    return doc['State']['Status']


def start_containers(host, check_only, actions):
    portable_env.startup_bind_sources(portable_env.current())
    try:
        rows = host.json([host.docker, 'inspect', *CONTAINERS])
    except Blocked:
        raise Blocked('required_container_missing:first_install_required') from None
    by = {r['Name'].lstrip('/'): r for r in rows}
    need(set(by) == set(CONTAINERS), 'required_container_missing:first_install_required')
    states = {name: container_identity(by[name], name) for name in CONTAINERS}
    for name, state in states.items():
        if state == 'running':
            continue
        need(state in ('exited', 'created'), 'container_not_safely_stopped:' + name)
        need(not check_only, 'container_stopped:' + name)
        # Start the verified ID, never recreate or replace mounts/volumes.
        host.run([host.docker, 'start', by[name]['Id']], timeout=60)
        fresh = host.json([host.docker, 'inspect', by[name]['Id']])[0]
        need(container_identity(fresh, name) == 'running', 'container_start_failed:' + name)
        actions.append({'action': 'started_existing_container', 'name': name, 'id': by[name]['Id'][:12]})
    return {'required': len(states), 'reused': sum(v == 'running' for v in states.values())}


def check_deployments(deployments, pods):
    by = {d['metadata']['name']: d for d in deployments}
    extra = portable_env.current().get('additional_deployments', [])
    need(type(extra) is list and len(extra) == len(set(extra)) and set(extra) <= {'recshop-mysql'}, 'unsupported_additional_deployment')
    expected = DEPLOYMENTS | set(extra)
    need(set(by) == expected, 'deployment_roster_differs:first_install_or_restore_required')
    for name, d in by.items():
        need(not d['metadata'].get('deletionTimestamp') and d['spec'].get('replicas') == 1,
             'deployment_not_configured_for_one_replica:' + name)
        status = d.get('status') or {}
        need(status.get('observedGeneration', 0) >= d['metadata']['generation'] and
             status.get('readyReplicas') == status.get('updatedReplicas') == status.get('availableReplicas') == 1,
             'deployment_not_ready:' + name)
        if name in APPS:
            containers = d['spec']['template']['spec']['containers']
            need(len(containers) == 1, 'application_container_identity_mismatch:' + name)
            env = {e['name']: e.get('value') for e in containers[0].get('env', [])}
            need(env.get('NACOS_ENABLED') == 'false' and env.get('OTEL_METRIC_EXPORT_INTERVAL') == '2000' and
                 env.get('OTEL_EXPORTER_OTLP_METRICS_ENDPOINT') == METRICS_ENDPOINT,
                 'application_sampling_or_nacos_mismatch:' + name)
    need(len(pods) == len(expected), 'pod_count_differs')
    for pod in pods:
        need(not pod['metadata'].get('deletionTimestamp') and
             any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in pod.get('status', {}).get('conditions', [])),
             'pod_not_ready')
    return {'deployments': len(by), 'ready_pods': len(pods), 'application_metric_interval_ms': 2000}


def check_targets(payload):
    need(payload.get('status') == 'success', 'prometheus_target_read_failed')
    targets = payload.get('data', {}).get('activeTargets', [])
    expected = portable_env.current()['telemetry']['prometheus_targets']
    for job, url in expected.items():
        rows = [r for r in targets if r.get('labels', {}).get('job') == job]
        need(len(rows) == 1 and rows[0].get('scrapeUrl') == url and rows[0].get('health') == 'up' and
             not rows[0].get('lastError') and rows[0].get('scrapeInterval') == '2s', 'prometheus_target_not_ready:' + job)
    return {'up_jobs': list(expected), 'configured_interval_seconds': 2,
            'observed_source_cadence': 'not_certified_by_environment_readiness'}


def check_chaos_controllers(items):
    by = {(r['kind'], r['metadata']['name']): r for r in items}
    for name in ('chaos-controller-manager', 'chaos-dns-server'):
        row = by.get(('Deployment', name), {})
        desired = row.get('spec', {}).get('replicas', 0)
        need(desired > 0 and row.get('status', {}).get('readyReplicas') == desired and
             row.get('status', {}).get('availableReplicas') == desired, 'chaos_controller_not_ready:' + name)
    daemon = by.get(('DaemonSet', 'chaos-daemon'), {}).get('status', {})
    need(daemon.get('desiredNumberScheduled', 0) > 0 and daemon.get('numberReady') ==
         daemon.get('updatedNumberScheduled') == daemon.get('desiredNumberScheduled'), 'chaos_daemon_not_ready')
    return {'controllers': 'ready', 'daemon_ready': daemon['numberReady']}


def read_database(env_file, expected_uuid, expected_checksums, connector=None):
    """Fixed read-only SQL; refuse any target lock before issuing CHECKSUM."""
    from dotenv import dotenv_values
    need(Path(env_file).is_file(), 'database_env_file_missing')
    keys = ('DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME')
    values = dotenv_values(env_file)
    need(all(values.get(k) for k in keys), 'database_environment_incomplete')
    need(values['DB_NAME'] == portable_env.current()['database']['name'], 'database_schema_binding_mismatch')
    need(re.fullmatch('[A-Za-z0-9_]+', values['DB_NAME']) is not None, 'invalid_database_identifier')
    connection = cursor = timer = None
    try:
        if connector is None:
            import mysql.connector
            connector = mysql.connector.connect
        connection = connector(host=values['DB_HOST'], port=int(values['DB_PORT']), user=values['DB_USER'],
                               password=values['DB_PASSWORD'], database=values['DB_NAME'],
                               use_pure=True, connection_timeout=5)
        sock = connection._socket.sock
        sock.settimeout(5)
        def abort():
            try: sock.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            try: sock.close()
            except OSError: pass
        timer = threading.Timer(20, abort); timer.start()
        cursor = connection.cursor()
        def query(sql, params=None):
            cursor.execute(sql, params)
            rows = cursor.fetchmany(257)
            need(len(rows) <= 256, 'database_read_limit_exceeded')
            return rows
        server = query('SELECT @@server_uuid, @@performance_schema')
        need(server == [(expected_uuid, 1)], 'database_identity_mismatch')
        instrument = query("SELECT ENABLED,TIMED FROM performance_schema.setup_instruments WHERE NAME='wait/lock/metadata/sql/mdl'")
        need(instrument == [('YES', 'YES')], 'database_lock_instrumentation_disabled')
        for table in ('metadata_locks', 'data_locks'):
            rows = query("SELECT OBJECT_NAME FROM performance_schema." + table +
                         " WHERE OBJECT_SCHEMA=%s AND OBJECT_NAME IN ('items','inventory')", (values['DB_NAME'],))
            need(not rows, 'database_target_locks_present:checksum_not_run')
        rows = query('CHECKSUM TABLE `' + values['DB_NAME'] + '`.`items`, `' + values['DB_NAME'] + '`.`inventory`')
        checks = {r[0].rsplit('.', 1)[-1]: str(r[1]) for r in rows}
        need(len(rows) == 2 and checks == expected_checksums, 'database_checksum_mismatch')
        return {'server_uuid': expected_uuid, 'metadata_locks': 0, 'data_locks': 0, 'checksums': checks}
    except Blocked:
        raise
    except Exception:
        raise Blocked('database_read_failed:credentials_not_logged') from None
    finally:
        if timer is not None:
            timer.cancel(); timer.join(timeout=1)
        try:
            if cursor is not None: cursor.close()
            if connection is not None: connection.close()
        except Exception:
            raise Blocked('database_connection_close_unconfirmed') from None


def run_environment(host, *, check_only=False, timeout=300, env_file=DEFAULT_ENV, shared=None, actions=None, initial_registration=False):
    if shared is None:
        from scripts.collection import run_campaign
        shared = run_campaign._load_helpers()
    cd = shared.cd
    config = portable_env.apply(cd)
    portable_env.startup_bind_sources(config)
    portable_env.assert_live()
    global METRICS_ENDPOINT
    METRICS_ENDPOINT = config["telemetry"]["metrics_endpoint_for_apps"]
    startup = config.get("windows_startup", {})
    if startup.get('enabled') is True:
        need(type(startup.get('containers')) is dict and bool(startup['containers']), 'explicit_existing_container_identities_required')
        global CONTAINERS
        CONTAINERS = {name: (row['image'], row['project'], row['service'], row['ports']) for name, row in startup['containers'].items()}
    env_file = portable_env.resolve_path(env_file or config["database"]["credential_file"])
    actions = [] if actions is None else actions
    need(not host.collector_pids(), 'active_collector:environment_not_changed')
    try:
        lease = ({"status": "unregistered", "workers": "not_started"} if initial_registration else shared.require_clean_lease())
    except Exception:
        raise Blocked('lease_not_clean_drained:manual_recovery_required') from None
    need(host.run([host.kubectl, 'config', 'current-context']) == cd.CONTEXT, 'wrong_kubernetes_context')
    if not check_only:
        need(os.name == 'nt' and startup.get('enabled') is True, 'automatic_start_disabled:use_check_only_or_configure_windows_startup')
    if startup.get('enabled') is True:
        try:
            host.run([host.docker, 'info', '--format', '{{.ServerVersion}}'])
        except Blocked:
            need(not check_only, 'docker_engine_not_running')
            from scripts.environment.recover_docker import DockerStartup, RecoveryBlocked
            try:
                DockerStartup(host, shared, actions, ROOT, Path(startup["docker_desktop_executable"])).start(timeout)
            except RecoveryBlocked as exc:
                raise Blocked(str(exc)) from None
    prefix = [host.kubectl, '--context', cd.CONTEXT, '--namespace', cd.NAMESPACE]
    def kube(*args): return host.json(prefix + list(args) + ['--request-timeout=10s'])
    wait_for(lambda: kube('get', 'namespace', 'kube-system', '-o', 'json'), timeout, 'kubernetes_api_unavailable', check_only)
    need(kube('get', 'namespace', 'kube-system', '-o', 'json')['metadata']['uid'] == cd.CLUSTER_UID and
         kube('get', 'namespace', cd.NAMESPACE, '-o', 'json')['metadata']['uid'] == cd.NAMESPACE_UID, 'cluster_or_namespace_identity_mismatch')
    need(not kube('get', 'networkchaos,podchaos,stresschaos', '-o', 'json')['items'], 'chaos_residual:manual_recovery_required')
    need(not kube('get', 'deployment', '-l', 'app=stressor', '-o', 'json')['items'], 'stressor_residual:manual_recovery_required')
    chaos = wait_for(lambda: check_chaos_controllers(host.json([host.kubectl, '--context', cd.CONTEXT,
        '--namespace', config['chaos_namespace'], 'get', 'deployments,daemonsets', '-o', 'json', '--request-timeout=10s'])['items']),
        timeout, 'chaos_mesh_not_ready', check_only)
    containers = {"management": "external", "required": 0, "reused": 0}
    if startup.get('enabled') is True:
        containers = start_containers(host, check_only, actions)
        proxy_url = startup['proxy_origin'].rstrip('/') + '/api/v1/namespaces/kube-system'
        try:
            proxy = host.http(proxy_url, as_json=True)
        except Blocked:
            need(not check_only, 'kubernetes_proxy_missing_or_unhealthy')
            with socket.socket() as probe:
                need(probe.connect_ex(('127.0.0.1', startup['proxy_port'])) != 0, 'port_8001_occupied:existing_process_not_stopped')
            pid = host.start_hidden([host.kubectl, '--context', cd.CONTEXT, 'proxy', '--port=' + str(startup['proxy_port']),
                                     '--address=' + startup['proxy_listen_address'], '--accept-hosts=.*'], 'kubectl-proxy')
            actions.append({'action': 'started_existing_proxy_route', 'pid': pid, 'listen': startup['proxy_listen_address'] + ':' + str(startup['proxy_port']),
                            'reason': 'existing Docker telemetry targets use host.docker.internal:8001'})
            proxy = wait_for(lambda: host.http(proxy_url, as_json=True), 30, 'kubernetes_proxy_start_failed', False)
        need(proxy.get('metadata', {}).get('uid') == cd.CLUSTER_UID, 'proxy_cluster_identity_mismatch')
    def ready():
        return check_deployments(kube('get', 'deployments', '-o', 'json')['items'], kube('get', 'pods', '-o', 'json')['items'])
    need(kube('get', 'node', config['node_name'], '-o', 'json')['metadata']['uid'] == config['node_uid'], 'node_identity_mismatch')
    deployment = wait_for(ready, timeout, 'deployments_not_ready:check_missing_images_models_or_database', check_only)
    try:
        route = shared.route_baseline()
    except Exception:
        raise Blocked('pricing_route_not_stable_direct_or_nacos_enabled') from None
    targets = wait_for(lambda: check_targets(host.http(cd.PROM + '/api/v1/targets?state=active', as_json=True)),
                       30, 'm1_prometheus_targets_not_ready', check_only)
    for label, url in [('jaeger', cd.JAEGER + '/api/services'), ('loki', config['telemetry']['loki'].rstrip('/') + '/ready')]:
        wait_for(lambda url=url: host.http(url), 30, label + '_not_ready', check_only)
    database = read_database(env_file, cd.DB_UUID, cd.CHECKSUMS)
    need(not host.collector_pids(), 'collector_started_during_environment_check')
    try:
        if not initial_registration:
            shared.require_clean_lease()
    except Exception:
        raise Blocked('lease_changed_during_environment_check') from None
    return {'status': 'READY', 'mode': 'check_only' if check_only else 'start', 'actions': actions,
            'context': cd.CONTEXT, 'namespace': cd.NAMESPACE, 'lease': {k: lease[k] for k in ('status', 'workers')},
            'containers': containers, 'deployment': deployment, 'pricing_route': route,
            'metric_targets': targets, 'chaos_mesh': chaos, 'database': database, 'collection_started': False,
            'scope': 'Environment readiness only; collection still requires a current prepared plan and explicit execution.'}


def register_environment(host, *, timeout=300, env_file=None):
    """Verify a fresh environment, then create only new local binding/state under its global lock."""
    from scripts.collection import campaign_runtime as shared, runner, journal
    from ops.metrics.maintenance_receipt import verify_maintenance_marker
    cd = shared.cd
    config = portable_env.apply(cd)
    portable_env.assert_live()
    identity = runner.EnvironmentIdentity(cd.CLUSTER_UID, cd.NAMESPACE_UID)
    registry = runner.environment_registry_root()
    registry.mkdir(parents=True, exist_ok=True)
    need(registry.resolve() == registry, 'registry_redirected')
    binding = registry / (identity.key + '.binding.json')
    state = cd.LEASE_ROOT / (identity.key + '.state.json')
    lock_path = registry / (identity.key + '.lock')
    need(lock_path.resolve() == lock_path, 'registry_lock_redirected')
    fd = journal._lock_file(lock_path)
    try:
        need(not binding.exists() and not state.exists(), 'registration_exists:never_reset_existing_environment')
        verify_maintenance_marker(registry, identity.key, evidence_kind='observed')
        result = run_environment(host, check_only=True, timeout=timeout, env_file=env_file,
                                 shared=shared, initial_registration=True)
        cd.LEASE_ROOT.mkdir(parents=True, exist_ok=True)
        need(cd.LEASE_ROOT.resolve() == cd.LEASE_ROOT, 'lease_root_redirected')
        payload = {'schema_version': runner.RUNNER_SCHEMA, 'cluster_uid': cd.CLUSTER_UID,
                   'namespace_uid': cd.NAMESPACE_UID, 'lease_root': str(cd.LEASE_ROOT)}
        with binding.open('x', encoding='utf-8') as stream:
            json.dump(payload, stream, sort_keys=True); stream.flush(); os.fsync(stream.fileno())
        with state.open('x', encoding='utf-8') as stream:
            json.dump({'schema_version': runner.RUNNER_SCHEMA, 'environment_key': identity.key,
                       'status': 'clean', 'workers': 'drained', 'mode': 'controlled_pilot',
                       'attempt': None, 'registration': 'fresh_read_only_environment_check'}, stream, sort_keys=True)
            stream.flush(); os.fsync(stream.fileno())
        return dict(result, status='REGISTERED_READY_SNAPSHOT', registration={'binding': str(binding), 'state': str(state)},
                    collection_started=False)
    finally:
        journal._unlock_file(fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-only', action='store_true')
    parser.add_argument('--environment', type=Path, required=True)
    parser.add_argument('--register-readonly', action='store_true', help="Read-only site verification plus first local lease registration; never overwrites existing state")
    parser.add_argument('--env-file', type=Path, default=DEFAULT_ENV)
    parser.add_argument('--timeout-seconds', type=int, default=300)
    args = parser.parse_args(argv)
    actions = []
    try:
        need(10 <= args.timeout_seconds <= 900, 'timeout_out_of_range')
        portable_env.load(args.environment)
        if args.register_readonly:
            need(args.check_only, 'register_requires_check_only')
            result = register_environment(Host(), timeout=args.timeout_seconds, env_file=args.env_file)
        else:
            result = run_environment(Host(), check_only=args.check_only, timeout=args.timeout_seconds, env_file=args.env_file, actions=actions)
    except Blocked as exc:
        print(json.dumps({'status': 'BLOCKED', 'reason': str(exc), 'actions': actions, 'collection_started': False}, ensure_ascii=False))
        return 2
    except Exception as exc:
        reason = str(exc) if str(exc).startswith('collection environment: telemetry.') else 'unexpected_readiness_error:details_suppressed'
        print(json.dumps({'status': 'BLOCKED', 'reason': reason, 'actions': actions, 'collection_started': False}))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
