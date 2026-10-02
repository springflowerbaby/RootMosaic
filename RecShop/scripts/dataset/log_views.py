"""Resolve log producers from capture identity, never from fault association."""
import collections
import json
import re
from pathlib import Path

from metric_views import canonical_service

RULE = 'log-producer-from-capture-identity-v1'


def group_logs(records, bundle):
    sources = {}
    for query in bundle['queries']:
        source = query.get('source', {})
        identity = query.get('backend_provenance', {}).get('identity', {})
        key = (source.get('source_id'), query.get('raw_ref'))
        service = identity.get('deployment_name') or identity.get('app')
        if not all(key) or not service:
            continue
        service = canonical_service(service)
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', service):
            raise ValueError('unsafe captured log producer')
        if key in sources and sources[key] != service:
            raise ValueError('ambiguous captured log producer')
        sources[key] = service
    groups = collections.defaultdict(list)
    for row in records:
        key = (row['source_id'], row['raw_ref'])
        if key not in sources:
            raise ValueError('log producer has no capture identity; do not guess from entity')
        groups[sources[key]].append(row)
    return groups


def log_bytes(rows):
    return ''.join(r['timestamp_original'] + ' ' + r['message'] + '\n' for r in rows).encode('utf-8')


def validate_log_view(case_dir):
    """Check actual producer, verbatim messages and absence of retired aliases."""
    root = Path(case_dir)
    expected, counts = [], {}
    for phase in ('pre_fault', 'during_fault', 'post_recovery'):
        native = root / 'raw/logs/native'
        rows = json.loads((native / f'{phase}.json').read_text(encoding='utf-8'))
        bundle = json.loads((native / f'{phase}.bundle.json').read_text(encoding='utf-8'))
        groups = group_logs(rows, bundle)
        for service, records in sorted(groups.items()):
            relative = f'raw/logs/{phase}__{service}.log'
            if (root / relative).read_bytes() != log_bytes(records):
                raise ValueError('log text or producer differs from capture: ' + relative)
            expected.append({'artifact': relative, 'stage': phase, 'service': service, 'records': len(records)})
        counts[phase] = len(rows)
    manifest = json.loads((root / 'raw/logs/manifest.json').read_text(encoding='utf-8'))
    if manifest['files'] != expected:
        raise ValueError('log manifest does not describe actual captured producers')
    paths = {r['artifact'] for r in expected}
    actual = {p.relative_to(root).as_posix() for p in (root / 'raw/logs').glob('*.log')}
    if actual != paths:
        raise ValueError('unexpected or retired log files remain in the dataset view')
    return counts
