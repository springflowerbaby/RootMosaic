"""One bounded recovery for Docker Desktop's known inaccessible runtime sockets.

Only a fresh, exact error from this start or a still-live failed backend authorizes
the same-directory backup. An older backend instance's log never authorizes it.
No volume, WSL distribution, Docker setting, service or experimental lease is changed.
"""
from __future__ import annotations

from datetime import datetime, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
import uuid

LOCAL_PIPE = 'npipe:////./pipe/dockerDesktopLinuxEngine'
RUNTIME_NAMES = {'Docker/run': {'dockerInference', 'dockerEthernetVfkit', 'userAnalyticsOtlpHttp.sock'},
                 'docker-secrets-engine': {'engine.sock'}}
DOCKER_ROOT = Path('C:/Program Files/Docker/Docker')
DESKTOP_PLUGIN = Path('C:/Program Files/Docker/cli-plugins/docker-desktop.exe')
PROCESS_PATHS = {str(DOCKER_ROOT / name).replace('\\', '/').lower() for name in
                 ('Docker Desktop.exe', 'frontend/Docker Desktop.exe', 'resources/com.docker.backend.exe', 'resources/com.docker.build.exe')}
SERVICE_PATH = str(DOCKER_ROOT / 'com.docker.service').replace('\\', '/').lower() + '.exe'
REPARSE = 0x400


class RecoveryBlocked(RuntimeError):
    pass


def require(ok, reason):
    if not ok:
        raise RecoveryBlocked(reason)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value):
    # Docker uses nanoseconds and CIM uses seven digits; Python 3.10 accepts six.
    normalized = re.sub(r'(\.\d{6})\d+(?=Z|[+-]\d\d:\d\d$)', r'\1', value)
    parsed = datetime.fromisoformat(normalized.replace('Z', '+00:00'))
    require(parsed.tzinfo is not None, 'docker_timestamp_timezone_missing')
    return parsed.astimezone(timezone.utc)


def powershell_json(host, script):
    encoded = base64.b64encode(script.encode('utf-16-le')).decode('ascii')
    return host.json(['powershell.exe', '-NoProfile', '-EncodedCommand', encoded], timeout=20)


def local_appdata():
    require(os.name == 'nt', 'docker_socket_recovery_windows_only')
    value = Path(os.environ['LOCALAPPDATA'])
    expected = Path(os.environ['USERPROFILE']) / 'AppData/Local'
    require(os.path.normcase(os.path.abspath(value)) == os.path.normcase(os.path.abspath(expected)),
            'docker_runtime_localappdata_redirected')
    return value


class FreshBackendLog:
    def __init__(self, appdata, since=None):
        self.path = appdata / 'Docker/log/host/com.docker.backend.exe.log'
        self.started = since or datetime.now(timezone.utc)
        self.offset = 0
        self.identity = None
        self.tail = b''
        self._record_end()
        if since is not None:
            self.offset = max(0, self.offset - 524288)

    def _record_end(self):
        try:
            info = self.path.stat()
            self.identity = (info.st_dev, info.st_ino)
            self.offset = info.st_size
        except FileNotFoundError:
            pass

    def known_error(self, appdata):
        try:
            info = self.path.stat()
            if (info.st_dev, info.st_ino) != self.identity or info.st_size < self.offset:
                self.offset = max(0, info.st_size - 524288)
                self.identity = (info.st_dev, info.st_ino)
                self.tail = b''
            with self.path.open('rb') as source:
                source.seek(max(self.offset, info.st_size - 524288))
                data = self.tail + source.read(524288)
                self.offset = source.tell()
            lines = data.split(b'\n')
            self.tail = lines.pop()[-8192:]
        except (FileNotFoundError, PermissionError):
            return None
        latest = None
        for raw in lines:
            line = raw.decode('utf-8', errors='replace')
            match = re.match(r'^\[([^]]+)\]', line)
            if not match:
                continue
            try:
                stamp = parse_utc(match[1])
                if stamp < self.started:
                    continue
            except (ValueError, RecoveryBlocked):
                continue
            normalized = line.replace('\\\\', '\\').replace('\\', '/').lower()
            if 'starting services:' not in normalized:
                continue
            latest = None  # A newer unknown startup error supersedes a known one.
            if not ('the file cannot be accessed by the system' in normalized or 'error_cant_access_file' in normalized):
                continue
            for relative, family in [('Docker/run/dockerInference', 'initializing Inference manager'),
                                     ('docker-secrets-engine/engine.sock', 'initializing Secrets Engine')]:
                endpoint = str(appdata / relative).replace('\\', '/').lower()
                if family.lower() in normalized and 'remove ' + endpoint + ':' in normalized:
                    latest = {'kind': 'known_inaccessible_runtime_socket', 'family': family,
                            'endpoint': str(appdata / relative), 'log_path': str(self.path),
                            'log_timestamp': stamp.isoformat(), 'line_sha256': hashlib.sha256(raw).hexdigest()}
        return latest


def inspect_runtime_directories(appdata):
    """Inspect without following socket reparses; reject redirecting ancestors."""
    results = []
    for relative, allowed in RUNTIME_NAMES.items():
        path = appdata / relative
        for parent in reversed((path, *path.parents)):
            info = parent.lstat()
            require(stat.S_ISDIR(info.st_mode) and not (getattr(info, 'st_file_attributes', 0) & REPARSE),
                    'docker_runtime_parent_redirected_or_not_directory')
        info = path.lstat()
        children = []
        with os.scandir(path) as listing:
            for item in listing:
                child = item.stat(follow_symlinks=False)
                attrs = getattr(child, 'st_file_attributes', 0)
                require(item.name in allowed and child.st_size == 0 and attrs & REPARSE and not attrs & 0x10,
                        'docker_runtime_unexpected_entry')
                children.append({'name': item.name, 'size': child.st_size, 'attributes': attrs,
                                 'mtime_ns': child.st_mtime_ns, 'ctime_ns': child.st_ctime_ns})
        results.append({'path': str(path), 'device': info.st_dev, 'inode': info.st_ino,
                        'children': sorted(children, key=lambda row: row['name'])})
    return results


def runtime_processes(host):
    rows = powershell_json(host, r"""$items = @(Get-CimInstance Win32_Process | Where-Object {
 $_.Name -like 'com.docker.*' -or $_.Name -eq 'Docker Desktop.exe' -or $_.Name -eq 'dockerd.exe'
} | ForEach-Object { @{pid=[int]$_.ProcessId; name=$_.Name; exe=$_.ExecutablePath;
 created=$_.CreationDate.ToUniversalTime().ToString('o')} });
ConvertTo-Json -Compress -InputObject $items""")
    result = []
    for row in rows:
        path = (row.get('exe') or '').replace('\\', '/').lower()
        if path == SERVICE_PATH and row['name'].lower() == 'com.docker.service.exe':
            continue  # Shared privileged service is neither stopped nor reconfigured.
        require(path in PROCESS_PATHS and row.get('created') and int(row['pid']) > 0,
                'docker_runtime_unknown_process')
        result.append(row)
    return result


def stop_exact_processes(host, identities):
    encoded = base64.b64encode(json.dumps(identities).encode('utf-8')).decode('ascii')
    script = r"""$ErrorActionPreference='Stop'
$rows = ConvertFrom-Json -InputObject ([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('REPLACE')))
$stopped = @()
foreach ($row in $rows) {
 $current = Get-CimInstance Win32_Process -Filter ("ProcessId="+[int]$row.pid)
 if ($null -eq $current) { continue }
 if ($current.ExecutablePath -ine $row.exe -or $current.CreationDate.ToUniversalTime().ToString('o') -cne $row.created) {
   throw 'Docker process identity changed'
 }
 try {
  Stop-Process -Id ([int]$row.pid) -Force -ErrorAction Stop
  $stopped += [int]$row.pid
 } catch {
  $after = Get-CimInstance Win32_Process -Filter ("ProcessId="+[int]$row.pid)
  if ($null -ne $after) { throw 'Docker process stop not confirmed or identity changed' }
 }
}
ConvertTo-Json -Compress -InputObject $stopped""".replace('REPLACE', encoded)
    return powershell_json(host, script)


def process_created_utc(proc):
    """Read creation from the owned Windows process handle, without PID lookup."""
    if os.name != 'nt':
        return utc_now()
    import ctypes
    from ctypes import wintypes
    created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
    ok = ctypes.windll.kernel32.GetProcessTimes(wintypes.HANDLE(int(proc._handle)),
        ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user))
    require(bool(ok), 'docker_stop_cli_creation_unconfirmed')
    ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
    return datetime.fromtimestamp(ticks / 10000000 - 11644473600, timezone.utc).isoformat()


def bounded_stop_process(args, *, cwd, environment, log, record, timeout=35):
    """Direct official plugin, file output, owned-handle kill: never communicate().

    docker.exe can launch a plugin that inherits stdout. A captured PIPE can then
    keep subprocess.run's timeout cleanup blocked after docker.exe is killed.
    This path launches the plugin itself and never waits for output-pipe EOF.
    """
    from scripts.environment.start_collection_environment import hidden_options
    started = time.monotonic()
    with log.open('xb') as output:
        proc = subprocess.Popen(args, cwd=cwd, env=environment, stdin=subprocess.DEVNULL,
            stdout=output, stderr=output, **hidden_options())
        timed_out = False
        try:
            identity = {'pid': proc.pid, 'exe': str(args[0]), 'created': process_created_utc(proc),
                        'stdout_stderr': str(log)}
            record('docker_stop_cli_started', **identity)
            try:
                exit_code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                proc.kill()  # Windows TerminateProcess uses this Popen's owned handle.
                exit_code = proc.wait(timeout=5)
        except Exception:
            if proc.poll() is None:
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    raise RecoveryBlocked('docker_stop_cli_exit_unconfirmed') from None
            raise
    return {**identity, 'exit_code': exit_code, 'timed_out': timed_out,
            'elapsed_seconds': round(time.monotonic() - started, 3), 'exit_confirmed': True}


class DockerStartup:
    def __init__(self, host, shared, actions, root, desktop, appdata=None):
        self.host, self.shared, self.actions = host, shared, actions
        self.root, self.desktop = root, desktop
        self.appdata = local_appdata() if appdata is None else appdata
        self.journal = root / '.recshop-collection/env-launcher' / ('docker-startup-' + uuid.uuid4().hex + '.json')
        self.events = []
        self.repaired = False
        self.error_backend_owners = []

    def record(self, action, **values):
        row = {'action': action, 'observed_at': utc_now(), **values}
        self.events.append(row)
        self.actions.append(row)
        self.journal.parent.mkdir(parents=True, exist_ok=True)
        self.journal.write_text(json.dumps({'schema': 'm1-known-docker-startup-recovery-v1',
                                          'events': self.events}, ensure_ascii=False, indent=2), encoding='utf-8')

    def local_engine(self):
        try:
            return bool(self.host.run([self.host.docker, '--host', LOCAL_PIPE, 'info', '--format', '{{.ServerVersion}}'], timeout=10))
        except Exception:
            return False

    def validate_endpoint(self):
        configured = getattr(self.host, 'env', {}).get('DOCKER_HOST')
        require(not configured or configured.lower() == LOCAL_PIPE.lower(), 'docker_recovery_nonlocal_endpoint')
        context = self.host.run([self.host.docker, 'context', 'show'])
        rows = self.host.json([self.host.docker, 'context', 'inspect', context])
        require(len(rows) == 1 and rows[0].get('Endpoints', {}).get('docker', {}).get('Host', '').lower() == LOCAL_PIPE.lower(),
                'docker_recovery_nonlocal_endpoint')

    def safety(self):
        self.validate_endpoint()
        require(not self.local_engine(), 'docker_engine_became_available:recovery_not_performed')
        require(not self.host.collector_pids(), 'active_collector:docker_recovery_not_performed')
        try:
            lease = self.shared.require_clean_lease()
            require(lease.get('status') == 'clean' and lease.get('workers') == 'drained', 'lease_not_clean_drained')
        except Exception:
            raise RecoveryBlocked('lease_not_clean_drained:docker_recovery_not_performed') from None

    def request_official_stop(self):
        require(DESKTOP_PLUGIN.is_file(), 'docker_desktop_stop_plugin_missing')
        log = self.journal.with_suffix('.stop-cli.log')
        return bounded_stop_process([str(DESKTOP_PLUGIN), 'desktop', 'stop', '--timeout', '30'],
            cwd=self.root, environment=self.host.env, log=log, record=self.record)

    def repair_once(self, error):
        require(not self.repaired, 'docker_socket_failure_repeated:automatic_recovery_exhausted')
        self.repaired = True
        self.safety()
        before = inspect_runtime_directories(self.appdata)  # Both directories, before any stop/move.
        self.record('known_docker_socket_failure', error=error, journal=str(self.journal), runtime_before=before)
        identities = runtime_processes(self.host)
        if self.error_backend_owners:
            identify = lambda rows: {(r['pid'], r['exe'].lower(), r['created']) for r in rows}
            backends = [row for row in identities if row['name'].lower() == 'com.docker.backend.exe']
            require(identify(backends) == identify(self.error_backend_owners),
                    'docker_backend_identity_changed:startup_not_repaired')
        self.record('docker_processes_identified', processes=identities)
        if identities:
            try:
                outcome = self.request_official_stop()
                self.record('docker_desktop_stop_requested', outcome='returned' if outcome['exit_code'] == 0 else 'not_confirmed', cli=outcome)
            except RecoveryBlocked:
                raise
            except Exception:
                self.record('docker_desktop_stop_requested', outcome='operation_failed')
                raise RecoveryBlocked('docker_desktop_stop_operation_failed:automatic_recovery_stopped') from None
            remaining = runtime_processes(self.host)
            expected = {(r['pid'], r['exe'].lower(), r['created']) for r in identities}
            require(all((r['pid'], r['exe'].lower(), r['created']) in expected for r in remaining),
                    'docker_process_set_changed:recovery_stopped')
            if remaining:
                self.record('identified_docker_stop_requested', processes=remaining)
                try:
                    stopped = stop_exact_processes(self.host, remaining)
                except Exception:
                    raise RecoveryBlocked('identified_docker_stop_failed_or_exit_unconfirmed') from None
                self.record('stopped_identified_docker_processes', processes=remaining, stopped_pids=stopped)
        deadline = time.monotonic() + 10
        while runtime_processes(self.host):
            require(time.monotonic() < deadline, 'docker_process_exit_unconfirmed')
            time.sleep(0.25)
        self.safety()
        fresh = inspect_runtime_directories(self.appdata)
        require(before == fresh, 'docker_runtime_changed_before_backup')
        suffix = '.m1-socket-backup-' + datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:12]
        for row in fresh:
            self.safety()
            require(not runtime_processes(self.host), 'docker_process_reappeared_before_backup')
            path = Path(row['path'])
            # Recheck this source and every ancestor directly before the same-parent rename.
            current = inspect_runtime_directories(self.appdata)
            actual = next(r for r in current if r['path'] == str(path))
            require(actual == row, 'docker_runtime_changed_before_backup')
            backup = path.with_name(path.name + suffix)
            require(not os.path.lexists(backup), 'docker_runtime_backup_collision')
            self.record('docker_runtime_backup_intent', source=str(path), destination=str(backup))
            try:
                path.rename(backup)  # Never removes sockets or traverses their reparse targets.
                self.record('docker_runtime_directory_backed_up', source=str(path), destination=str(backup), original=row)
                path.mkdir()
                self.record('docker_runtime_directory_recreated', path=str(path))
            except OSError as exc:
                self.record('docker_runtime_backup_or_recreation_failed', source=str(path), destination=str(backup),
                            error_type=type(exc).__name__, windows_error=getattr(exc, 'winerror', None))
                raise RecoveryBlocked('docker_runtime_backup_incomplete:see_preserved_journal') from None
        self.record('docker_socket_recovery_complete', backup_suffix=suffix)

    def start(self, timeout):
        self.validate_endpoint()
        require(self.desktop.is_file(), 'docker_desktop_not_installed')
        existing = runtime_processes(self.host)
        owners = [row for row in existing if row['name'].lower() == 'com.docker.backend.exe']
        since = max(parse_utc(row['created']) for row in owners) if owners else None
        watcher = FreshBackendLog(self.appdata, since=since)
        if existing:
            self.record('reused_existing_docker_startup', processes=existing,
                        error_log_since=since.isoformat() if since else 'new_lines_only', journal=str(self.journal))
        else:
            pid = self.host.start_hidden([str(self.desktop)], 'docker-desktop')
            self.record('started_docker_desktop', pid=pid, journal=str(self.journal))
        deadline = time.monotonic() + timeout
        try:
            while True:
                if self.local_engine():
                    return
                error = watcher.known_error(self.appdata)
                if error:
                    if owners:
                        current = [row for row in runtime_processes(self.host) if row['name'].lower() == 'com.docker.backend.exe']
                        identify = lambda rows: {(r['pid'], r['exe'].lower(), r['created']) for r in rows}
                        require(identify(current) == identify(owners), 'docker_backend_identity_changed:startup_not_repaired')
                        self.record('known_error_bound_to_live_backend', backend_processes=current, error=error)
                    self.error_backend_owners = owners
                    self.repair_once(error)
                    watcher = FreshBackendLog(self.appdata)
                    owners = []
                    pid = self.host.start_hidden([str(self.desktop)], 'docker-desktop-retry')
                    self.record('restarted_docker_desktop_after_socket_backup', pid=pid)
                    deadline = time.monotonic() + timeout
                require(time.monotonic() < deadline, 'docker_engine_start_timeout:no_matching_fresh_socket_error')
                time.sleep(min(3, max(0, deadline - time.monotonic())))
        except Exception as exc:
            self.record('docker_startup_blocked', automatic_recovery_attempted=self.repaired, journal=str(self.journal),
                        reason=str(exc) if isinstance(exc, RecoveryBlocked) else 'startup_operation_failed')
            raise
