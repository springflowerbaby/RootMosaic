"""Bind only consumed source artifacts and process one verified byte snapshot.

The manifest records both presence and absence. Missing observations are not
promoted to present values, and this module does not judge scientific quality.
"""
from __future__ import annotations

from contextvars import ContextVar
import hashlib
import json
from pathlib import Path

SCHEMA = 'm1-consumed-inputs-v1'
PHASES = ('pre_fault', 'during_fault', 'post_recovery')
_ACTIVE = ContextVar('migration_input_snapshot', default=None)


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def consumed_paths(summary_path, native_root):
    run = Path(summary_path).parent
    native = Path(native_root)
    rows = [('run', 'SUMMARY.json', run / 'SUMMARY.json'),
            ('run', 'recovery-input.json', run / 'recovery-input.json')]
    names = ['contract.json', 'artifacts/operations.json',
             'artifacts/annotation-draft.json', 'artifacts/quality/result.json']
    for phase in PHASES:
        names.extend(f'artifacts/{phase}/{modality}/{filename}'
                     for modality in ('metrics', 'logs', 'traces')
                     for filename in ('bundle.json', 'projection.json'))
        names.extend((f'artifacts/{phase}/workload.json',
                      f'artifacts/{phase}/phase-observations.json'))
    rows.extend(('native', name, native / name) for name in names)
    return [{'basis': basis, 'relative_path': name, 'path': str(path.resolve())}
            for basis, name, path in rows]


def freeze_consumed_inputs(summary_path, native_root):
    files = []
    for row in consumed_paths(summary_path, native_root):
        path = Path(row['path'])
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            files.append(dict(row, exists=False, sha256=None, bytes=None))
        else:
            files.append(dict(row, exists=True, sha256=_digest(raw), bytes=len(raw)))
    return {'schema': SCHEMA, 'files': files}


def require_consumed_manifest(case):
    manifest = case.get('consumed_inputs')
    if not isinstance(manifest, dict) or manifest.get('schema') != SCHEMA:
        raise ValueError('plan lacks consumed-input binding; prepare a new plan')
    expected = consumed_paths(case['summary_path'], case['native_root'])
    actual = manifest.get('files', [])
    if len(actual) != len(expected) or any(
            {k: row.get(k) for k in ('basis', 'relative_path', 'path')} != want
            for row, want in zip(actual, expected)):
        raise ValueError('consumed-input manifest is incomplete or has foreign paths; prepare a new plan')
    for row in actual:
        if type(row.get('exists')) is not bool:
            raise ValueError('invalid consumed-input presence record')
        if row['exists']:
            if not isinstance(row.get('sha256'), str) or len(row['sha256']) != 64 or type(row.get('bytes')) is not int:
                raise ValueError('invalid consumed-input hash/size')
        elif row.get('sha256') is not None or row.get('bytes') is not None:
            raise ValueError('absent source cannot claim a hash or size')
    for basis, name, key in [('run', 'SUMMARY.json', 'summary_sha256'),
                              ('run', 'recovery-input.json', 'recovery_input_sha256'),
                              ('native', 'contract.json', 'native_contract_sha256')]:
        row = next(r for r in actual if r['basis'] == basis and r['relative_path'] == name)
        if not row['exists'] or row['sha256'] != case[key]:
            raise ValueError('consumed-input identity binding is inconsistent')
    return actual


def verify_consumed_inputs(case):
    require_consumed_manifest(case)
    if freeze_consumed_inputs(case['summary_path'], case['native_root']) != case['consumed_inputs']:
        raise ValueError('consumed input changed after preparation; prepare a new plan')


def _local(root, relative):
    value = Path(relative)
    path = (root / value).resolve()
    if value.is_absolute() or '..' in value.parts or not path.is_relative_to(root.resolve()):
        raise ValueError('foreign cached-history path')
    return path


class InputSnapshot:
    """One case's verified bytes; source changes after capture cannot be consumed."""
    def __init__(self, case, inputs, history_dir, expected_queries):
        self.data = {}
        for row in require_consumed_manifest(case):
            self._capture(row['path'], row['sha256'], row['exists'], row['bytes'])
        for ref in inputs:
            self._capture(ref['path'], ref['sha256'])
        history_dir = Path(history_dir)
        hm_path = history_dir / 'manifest.json'
        self._capture(hm_path)
        hm = json.loads(self.data[str(hm_path.resolve())])
        if hm.get('case') != case:
            raise ValueError('history belongs to another attempt')
        rows = hm.get('raw', [])
        if len(rows) != 2 or {r.get('path') for r in rows} != {'resources.json', 'http.json'}:
            raise ValueError('history must bind exactly resources and http matrices')
        for row in rows:
            group = row['path'].removesuffix('.json')
            if row.get('receipt') != group + '.receipt.json' or not row.get('receipt_sha256'):
                raise ValueError('history lacks receipt binding; create a new verified export')
            raw_path = _local(history_dir, row['path'])
            receipt_path = _local(history_dir, row['receipt'])
            self._capture(raw_path, row['sha256'])
            self._capture(receipt_path, row['receipt_sha256'])
            receipt = json.loads(self.data[str(receipt_path)])
            if (receipt.get('case') != case or receipt.get('response_sha256') != row['sha256']
                    or receipt.get('query') != expected_queries[group]):
                raise ValueError('cached-history receipt identity/query differs')

    def _capture(self, path, expected=None, exists=True, size=None):
        path = Path(path).resolve()
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            raw = None
        if (raw is not None) != exists:
            raise ValueError('consumed input presence changed: ' + str(path))
        if raw is not None and ((expected is not None and _digest(raw) != expected)
                                or (size is not None and len(raw) != size)):
            raise ValueError('consumed input changed after preparation: ' + str(path))
        key = str(path)
        if key in self.data and self.data[key] != raw:
            raise ValueError('source changed during snapshot: ' + key)
        self.data[key] = raw

    def __enter__(self):
        self.token = _ACTIVE.set(self.data)
        return self

    def __exit__(self, *exc):
        _ACTIVE.reset(self.token)


def input_bytes(path):
    snapshot = _ACTIVE.get()
    if snapshot is None:
        return Path(path).read_bytes()
    key = str(Path(path).resolve())
    if key not in snapshot:
        raise ValueError('source was not frozen for this conversion: ' + key)
    if snapshot[key] is None:
        raise FileNotFoundError('source was absent at preparation: ' + key)
    return snapshot[key]


def input_json(path):
    return json.loads(input_bytes(path))


def input_sha(path):
    return _digest(input_bytes(path))
