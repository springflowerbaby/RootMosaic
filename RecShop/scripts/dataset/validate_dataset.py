"""Portable format reader/validator; does not run downstream RCA algorithms."""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from log_views import validate_log_view

METRIC_KEYS=set('schema_version timestamp stage run_id source entity_type entity service metric value unit metric_type labels fault_window_membership container quality'.split())
TRACE_KEYS=set('schema_version timestamp stage run_id fault_window_membership trace_id span_id parent_span_id service operation start_time end_time duration_ms tags process_id process_tags references collector_query_service'.split())
PHASES=('pre_fault','during_fault','post_recovery')


def read(path): return json.loads(path.read_text(encoding='utf-8-sig'))


def local(root, value):
    p=(root/value).resolve()
    if not p.is_relative_to(root.resolve()) or not p.is_file(): raise ValueError('invalid package reference: '+value)
    return p


RELEASE_ID = 'RecShop-M1-v1.0'
DELIVERY_CHECKS = {
    'accepted_slot_and_attempt_binding', 'groundtruth_mapping',
    'data_format_readability', 'file_integrity', 'declared_limitations_preserved',
}
AUDIT_FILES = (
    'audit/collection-acceptance-original.json', 'audit/collector-quality-original.json',
    'audit/collector-summary-original.json', 'audit/annotation-draft-original.json',
)


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _accepted_row(document, entry):
    """Read a selected record or an original root decision without rejudging QC."""
    _require(document.get('round') == entry['round'], 'acceptance round mismatch')
    if 'row' in document:
        _require(document.get('accepted') is True, 'original acceptance is not accepted')
        rows = [document['row']]
    else:
        # Original root decisions use accepted as a count, not a boolean.
        _require(document.get('accepted', 0) > 0, 'original acceptance is not accepted')
        rows = document.get('rows', [])
    rows = [r for r in rows if r.get('scenario_id') == entry['scenario_id']
            and r.get('attempt_id') == entry['attempt_id']]
    _require(len(rows) == 1, 'accepted decision does not bind exactly one attempt')
    row = rows[0]
    accepted = (row.get('decision') in ('ACCEPTED_MINIMUM_COLLECTION', 'ACCEPT_MINIMUM_COLLECTION')
                if 'decision' in row else row.get('state') in
                ('P0_COUNTED', 'P0_COUNTED_REVIEWED', 'P0_COUNTED_LEGACY'))
    _require(accepted and row.get('accepted', True) is True, 'selected decision is not accepted')
    for source in (document, row):
        for key in ('scenario_id', 'attempt_id', 'run_id', 'design_version', 'condition_id'):
            if key in source:
                _require(source[key] == entry[key], 'acceptance identity mismatch: ' + key)
    for key in ('round', 'repeat_number'):
        if key in row:
            _require(int(row[key]) == entry['round'], 'acceptance row round mismatch')
    return row


def validate_delivery_fields(case_dir, *, root_entry=None, expected_counts=None, require_final=True):
    """Check small delivery/audit artifacts; never scan telemetry.

    expected_counts must come from an independently verified observation input for
    metadata-only replay. The full reader supplies its freshly counted records.
    With require_final=False, a consistent unpromoted candidate may be checked.
    This result certifies field consistency, not telemetry readability or full QC.
    """
    try:
        return _validate_delivery_fields(Path(case_dir), root_entry, expected_counts, require_final)
    except (KeyError, TypeError, IndexError, AttributeError) as exc:
        raise ValueError('missing or malformed delivery field: ' + str(exc)) from exc


def _validate_delivery_fields(d, root_entry, expected_counts, require_final):
    m = read(d/'metadata.json'); gt = read(d/'groundtruth.json')
    migration = read(d/'migration.json'); e = migration['entry']
    a = read(d/'raw/operations/acceptance.json'); delivery = m['delivery']
    if root_entry is not None:
        _require(e == root_entry, 'root MANIFEST/migration.entry mismatch')
    _require(m['migration']['version'] == migration['version'], 'migration version mismatch')
    identity = ('sample_id', 'run_id', 'attempt_id', 'design_version', 'scenario_id', 'round', 'condition_id')
    _require(all(e.get(k) not in (None, '') for k in identity), 'missing entry identity')
    _require(m['sample_id'] == e['sample_id'] == d.name, 'sample identity mismatch')
    _require(m['run_id'] == e['run_id'], 'run identity mismatch')
    _require(m['source_case_id'] == e['attempt_id'] == gt['source_case_id'], 'source attempt mismatch')
    for key in ('attempt_id', 'design_version', 'scenario_id', 'round', 'condition_id'):
        _require(m['migration'][key] == e[key], 'metadata migration identity mismatch: ' + key)
    for key in ('sample_id', 'attempt_id', 'scenario_id', 'round'):
        _require(a[key] == e[key], 'acceptance view identity mismatch: ' + key)
    for key in ('run_id', 'design_version', 'condition_id'):
        if key in a:
            _require(a[key] == e[key], 'acceptance view identity mismatch: ' + key)
    _require(gt == m['ground_truth'] and gt['sample_id'] == e['sample_id'], 'GT/sample mismatch')
    roots = sorted(set(gt['root_cause_services']))
    _require(roots == sorted(m['root_causes']) and len(roots) == e['root_count'] ==
             m['root_count'] == gt['root_count'] == gt['n_distinct_root_services'], 'GT/root count mismatch')
    _require(bool(roots), 'empty GT')
    _require(m['migration']['accepted'] is True and e['accepted'] is True and a['accepted'] is True,
             'delivery accepted flag conflict')
    ready = m['ready_for_release']
    _require(type(ready) is bool and (not require_final or ready), 'delivery is not ready')
    status = 'accepted' if ready else 'accepted_minimum_collection'
    for doc in (m, e, a):
        _require(doc['ready_for_release'] is ready and doc['validation_complete'] is ready,
                 'delivery ready/validation conflict')
        _require(doc['validation_scope'] == 'delivery_v1', 'delivery validation scope mismatch')
    _require(m['sample_status'] == e['sample_status'] == status, 'delivery sample status conflict')
    _require(a['status'] == delivery['status'] == status, 'acceptance delivery status conflict')
    if 'sample_status' in a:
        _require(a['sample_status'] == status, 'acceptance sample status conflict')
    _require(m['release_version'] == '1.0' and m['release_status'] ==
             ('final' if ready else 'pending_validation'), 'delivery release state conflict')
    for doc in (e, a, delivery):
        _require(doc['release_id'] == RELEASE_ID, 'delivery release id mismatch')
    for doc in (e, a):
        for key in ('release_version', 'release_status'):
            if key in doc or migration['version'] != 'm1-strict255-adapter-v3':
                _require(doc[key] == m[key], 'delivery release state conflict: ' + key)
    if 'release_id' in m:
        _require(m['release_id'] == RELEASE_ID, 'metadata release id mismatch')
    _require(delivery['validation_scope'] == 'delivery_v1', 'delivery scope mismatch')
    results = m['validation_results']
    if ready:
        _require(len(results) == len(DELIVERY_CHECKS) and
                 {r['check'] for r in results} == DELIVERY_CHECKS and
                 all(r['status'] == 'PASS' and r['scope'] == 'delivery_v1' for r in results),
                 'delivery validation results incomplete')
    else:
        _require(not results, 'unvalidated candidate claims validation PASS')
    _require(delivery['acceptance_evidence'] == 'raw/operations/acceptance.json', 'acceptance reference mismatch')
    _require(delivery['original_qc']['path'] == m['migration']['raw_qc'] == AUDIT_FILES[1],
             'original QC reference mismatch')
    _require(delivery['audit_directory'] == 'audit/', 'audit directory mismatch')
    _require(delivery['release_record'] == '../../../RELEASE.json' and
             delivery['status_guide'] == '../../../STATUS-GUIDE.md', 'package status reference mismatch')

    # Hash the small identity/audit artifacts even without a full telemetry scan.
    hashes = migration['files_sha256']
    small_files = ['metadata.json', 'groundtruth.json', 'raw/operations/acceptance.json',
                   'raw/metrics/quality.json', *AUDIT_FILES,
                   *[f'raw/{mod}/manifest.json' for mod in ('metrics', 'logs', 'traces', 'traces_calltree')]]
    for name in small_files:
        _require(bool(re.fullmatch('[0-9a-f]{64}', str(hashes.get(name, '')))), 'missing artifact hash: ' + name)
        _require(file_sha(local(d, name)) == hashes[name], 'artifact hash mismatch: ' + name)
    source = a['source_acceptance']
    _require(source['path'] == AUDIT_FILES[0] and source['path_basis'] == 'sample_directory',
             'original acceptance reference mismatch')
    _require(source['sha256'] == hashes[AUDIT_FILES[0]], 'original acceptance hash mismatch')
    if 'sha256' in delivery['original_qc']:
        _require(delivery['original_qc']['sha256'] == hashes[AUDIT_FILES[1]], 'original QC hash mismatch')
    _require(m['migration']['source_summary_sha256'] == hashes[AUDIT_FILES[2]], 'source summary hash mismatch')
    _require(sorted(set(read(d/AUDIT_FILES[2])['truth_entities'])) == roots, 'original summary GT mismatch')
    original = read(d/source['path']); row = _accepted_row(original, e)
    if 'summary_sha256' in original:
        _require(original['summary_sha256'] == hashes[AUDIT_FILES[2]], 'accepted summary hash mismatch')
    if 'ledger_artifact' in original:
        ledger = local(d, original['ledger_artifact'])
        _require(hashes.get(original['ledger_artifact']) == original['ledger_sha256'] == file_sha(ledger),
                 'original ledger hash mismatch')
        if ledger.suffix == '.csv':
            with ledger.open(encoding='utf-8-sig', newline='') as f:
                rows = list(csv.DictReader(f))
        else:
            ledger_doc = read(ledger)
            rows = ledger_doc['rows']
            rounds = {int(v) for v in (ledger_doc.get('round'), ledger_doc.get('logical_round'),
                                      row.get('round'), row.get('repeat_number')) if v is not None}
            _require(rounds == {e['round']}, 'original ledger round mismatch')
        matched = [r for r in rows if r.get('scenario_id') == e['scenario_id'] and
                   r.get('attempt_id') == e['attempt_id']]
        _require(matched == [row], 'accepted row differs from original ledger')
    elif migration.get('version') != 'm1-strict255-adapter-v3':
        raise ValueError('generated delivery requires original ledger artifact')
    elif 'row' in original:
        _require(bool(re.fullmatch('[0-9a-f]{64}', str(original.get('ledger_sha256', '')))),
                 'missing acceptance ledger hash')
    for ref in row.get('evidence_refs', []):
        basename = str(ref.get('path', '')).replace('\\', '/').rsplit('/', 1)[-1]
        if basename == 'SUMMARY.json':
            _require(ref['sha256'] == hashes[AUDIT_FILES[2]], 'decision summary evidence mismatch')
        elif basename == 'result.json':
            _require(ref['sha256'] == hashes[AUDIT_FILES[1]], 'decision QC evidence mismatch')

    counts = e['counts']
    _require(set(counts) == set(PHASES), 'count phases mismatch')
    for phase in PHASES:
        _require(set(counts[phase]) == {'metrics', 'logs', 'traces', 'flat_traces'} and
                 all(type(v) is int and v >= 0 for v in counts[phase].values()), 'invalid observation count')
    if expected_counts is not None:
        _require(counts == expected_counts, 'verified observation count mismatch')
    expected_availability = {mod: {p: {'status': 'available' if counts[p][mod] else 'missing',
                                      'record_count': counts[p][mod]} for p in PHASES}
                             for mod in ('metrics', 'logs', 'traces')}
    _require(delivery['modality_availability'] == a['modality_availability'] == expected_availability,
             'delivery modality count/availability mismatch')
    manifests = {mod: read(d/f'raw/{mod}/manifest.json')
                 for mod in ('metrics', 'logs', 'traces', 'traces_calltree')}
    stages = m['observation_stages']
    _require(set(stages) == set(PHASES) and manifests['metrics']['stage_windows'] == stages,
             'repeated observation stage mismatch')
    for phase in PHASES:
        for mod in ('metrics', 'logs', 'traces'):
            coverage = 'partial' if counts[phase][mod] else 'missing'
            _require(stages[phase][mod+'_validation_status'] == coverage, 'stage coverage mismatch: ' + mod)
        allowed = {'missing'} if not counts[phase]['logs'] else {'partial'}
        if migration.get('version') == 'm1-strict255-adapter-v3' and counts[phase]['logs']:
            allowed.add('available')
        _require(manifests['logs']['phase_availability'][phase] in allowed, 'log phase availability mismatch')
    for mod, count_key in (('logs', 'logs'), ('traces', 'flat_traces'), ('traces_calltree', 'traces')):
        totals = dict.fromkeys(PHASES, 0); artifacts = set()
        for record in manifests[mod]['files']:
            _require(record['stage'] in PHASES and type(record['records']) is int and record['records'] >= 0,
                     'invalid modality manifest count')
            _require(record['artifact'] not in artifacts, 'duplicate modality artifact')
            artifacts.add(record['artifact']); totals[record['stage']] += record['records']
        _require(totals == {p: counts[p][count_key] for p in PHASES}, 'modality manifest count mismatch: ' + mod)
    quality = read(d/'raw/metrics/quality.json')['stages']
    _require(set(quality) == set(PHASES), 'metric quality phases mismatch')
    for phase in PHASES:
        _require(quality[phase]['record_count'] == counts[phase]['metrics'], 'metric quality record count mismatch')
        _require(quality[phase]['availability'] == ('partial' if counts[phase]['metrics'] else 'missing'),
                 'metric quality availability mismatch')
    for key in ('annotation_scope', 'isolation_check', 'isolation_degraded'):
        _require(key not in m, 'obsolete public field: ' + key)
    for key in ('RQ4_pairing', 'label_status'):
        _require(key not in m['migration'] and key not in e, 'obsolete migration field: ' + key)
    _require('excluded_from_v1_validation' not in delivery and
             all('role_status' not in f for f in m['faults']), 'obsolete annotation state field')
    return {'state': 'PASS_DELIVERY_FIELDS', 'sample_id': e['sample_id'], 'counts': counts}


def iter_cases(root):
    root=Path(root)
    for e in read(root/'MANIFEST.json')['cases']:
        d=(root/e['path']).resolve()
        if not d.is_relative_to(root.resolve()): raise ValueError('case escapes package')
        meta=read(d/'metadata.json');gt=read(d/'groundtruth.json')
        if meta['sample_id']!=d.name or gt!=meta['ground_truth'] or gt['sample_id']!=d.name: raise ValueError('GT/sample mismatch')
        if sorted(set(gt['root_cause_services']))!=sorted(meta['root_causes']): raise ValueError('root mismatch')
        if len(set(gt['root_cause_services']))!=e['root_count'] or e['root_count']!=gt['n_distinct_root_services']: raise ValueError('G mismatch')
        yield d,meta,gt,e


def validate_package_fields(root, *, require_final=True):
    """Check final package records without rescanning telemetry.

    During construction, RELEASE/DELIVERY-VALIDATION may not yet exist (or may
    describe the preceding incremental package). Per-case candidate checks still
    run in validate; the package gate applies only to the final read.
    """
    root = Path(root)
    manifest = read(local(root, 'MANIFEST.json'))
    if not require_final:
        return {'state': 'CANDIDATE_PACKAGE', 'samples': len(manifest['cases'])}
    try:
        cases = manifest['cases']; n = len(cases)
        _require(n > 0, 'empty final package')
        legacy = manifest['version'] == 'm1-strict255-adapter-v3'
        states = {'release_id': RELEASE_ID, 'release_version': '1.0', 'release_status': 'final',
                  'ready_for_release': True, 'validation_complete': True, 'validation_scope': 'delivery_v1'}
        for key, value in states.items():
            _require(manifest[key] == value and type(manifest[key]) is type(value),
                     'package MANIFEST state mismatch: ' + key)
        _require(manifest.get('release_record', 'RELEASE.json') == 'RELEASE.json',
                 'package release reference mismatch')
        release_path = local(root, 'RELEASE.json'); release = read(release_path)
        report_path = local(root, 'DELIVERY-VALIDATION.json'); report = read(report_path)
        guide_path = local(root, 'STATUS-GUIDE.md')
        for key, value in states.items():
            if legacy and key == 'release_status' and key not in release:
                continue
            _require(release[key] == value and type(release[key]) is type(value),
                     'package RELEASE state mismatch: ' + key)
        _require(release['status'] == 'final', 'package RELEASE status mismatch')
        _require(type(release['accepted_samples']) is int and release['accepted_samples'] == n,
                 'package accepted sample count mismatch')
        _require(release['scenario_definitions'] == len({(e['design_version'], e['scenario_id']) for e in cases})
                 and release['rounds'] == sorted({e['round'] for e in cases}), 'package scenario/round count mismatch')
        _require(release['status_guide'] == 'STATUS-GUIDE.md', 'package status guide reference mismatch')
        basis = release['validation_basis']
        _require(len(basis['checks']) == len(DELIVERY_CHECKS) and set(basis['checks']) == DELIVERY_CHECKS,
                 'package release checks mismatch')
        _require(basis.get('report', 'DELIVERY-VALIDATION.json' if legacy else None) == 'DELIVERY-VALIDATION.json',
                 'package validation report reference mismatch')
        _require(report['state'] == 'PASS' and report['release_id'] == RELEASE_ID and
                 report.get('validation_scope', report.get('scope') if legacy else None) == 'delivery_v1',
                 'package validation report state mismatch')
        missing = [{'sample_id': e['sample_id'], 'modality': mod, 'phase': p} for e in cases
                   for mod in ('metrics', 'logs', 'traces') for p in PHASES if e['counts'][p][mod] == 0]
        _require(sorted(release['known_missing_views'], key=lambda r: (r['sample_id'], r['modality'], r['phase'])) ==
                 sorted(missing, key=lambda r: (r['sample_id'], r['modality'], r['phase'])), 'package missing views mismatch')
        _require(len({e['sample_id'] for e in cases}) == n and len({e['path'] for e in cases}) == n,
                 'duplicate package sample/path')
        for entry in cases:
            for key, value in states.items():
                if legacy and key in ('release_version', 'release_status') and key not in entry:
                    continue
                _require(entry[key] == value and type(entry[key]) is type(value),
                         'package entry state mismatch: ' + key)
            _require(entry['accepted'] is True and entry['sample_status'] == 'accepted', 'package entry not accepted')
            metadata_path = local(root, entry['path'] + '/metadata.json')
            delivery = read(metadata_path)['delivery']
            for key, target in (('release_record', release_path), ('status_guide', guide_path)):
                reference = delivery[key]
                _require(isinstance(reference, str) and not Path(reference).is_absolute() and
                         (metadata_path.parent/reference).resolve() == target,
                         'case package reference mismatch: ' + key)
        if legacy:
            for key in ('samples_checked', 'manifest_accepted_count', 'metadata_ready_count', 'metadata_validation_complete_count'):
                _require(type(report[key]) is int and report[key] == n, 'package validation count mismatch: ' + key)
            _require(report['all_consuming_status_fields_consistent'] is True, 'package legacy status report conflict')
        else:
            _require(len(report['checks']) == len(DELIVERY_CHECKS) and set(report['checks']) == DELIVERY_CHECKS,
                     'package validation checks mismatch')
            readback = report['readback']
            _require(readback['state'] == 'PASS_FORMAT_READ' and readback['actual_loaded'] == n and
                     readback['phases_loaded'] == n * len(PHASES) and len(readback['rows']) == n,
                     'package readback count/state mismatch')
            for entry, row in zip(cases, readback['rows']):
                _require((row['scenario_id'], row['round'], row['G'], row['counts']) ==
                         (entry['scenario_id'], entry['round'], entry['root_count'], entry['counts']),
                         'package readback case/count mismatch')
            _require(report['original_collector_qc_modified'] is False and report['scientific_annotations_inferred'] is False,
                     'package validation scientific scope conflict')
        return {'state': 'PASS_PACKAGE_FIELDS', 'samples': n}
    except (KeyError, TypeError, IndexError, AttributeError) as exc:
        raise ValueError('missing or malformed package field: ' + str(exc)) from exc


def validate(root, expected=None, check_hashes=False, require_final=True):
    root=Path(root);slots=set();summary=[]
    validate_package_fields(root, require_final=require_final)
    for d,m,gt,e in iter_cases(root):
        key=(e['design_version'],e['scenario_id'],e['round'])
        if key in slots: raise ValueError('duplicate slot')
        slots.add(key);counts={p:{'metrics':0,'logs':0,'traces':0,'flat_traces':0} for p in PHASES}
        # All raw response matrices and source evidence are parsed as JSON.
        for p in d.rglob('*.json'): read(p)
        for modality in ('metrics','logs','traces','traces_calltree'):
            man=read(d/f'raw/{modality}/manifest.json')
            for r in man['files']: local(d,r['artifact'])
        with (d/'raw/metrics/metrics_v2.jsonl').open(encoding='utf-8') as f:
            for line in f:
                r=json.loads(line)
                if set(r)!=METRIC_KEYS or r['run_id']!=e['run_id'] or not isinstance(r['labels'],dict): raise ValueError('metric schema/identity')
                counts[r['stage']]['metrics']+=1
        for ph in PHASES:
            for folder,key2 in [('traces','flat_traces'),('traces_calltree','traces')]:
                seen=set()
                with (d/f'raw/{folder}/{ph}_traces.jsonl').open(encoding='utf-8') as f:
                    for line in f:
                        r=json.loads(line)
                        if set(r)!=TRACE_KEYS or r['run_id']!=e['run_id'] or r['stage']!=ph: raise ValueError('trace schema/identity')
                        k=(r['trace_id'],r['span_id'])
                        if k in seen: raise ValueError('duplicate span')
                        seen.add(k);counts[ph][key2]+=1
                        if folder=='traces' and (r['span_id']!=r['trace_id'] or r['references'] or r['parent_span_id'] is not None): raise ValueError('flat trace compatibility')
            counts[ph]['logs']=len(read(d/f'raw/logs/native/{ph}.json'))
        if counts!=e['counts']: raise ValueError('record count mismatch')
        validate_delivery_fields(d, root_entry=e, expected_counts=counts, require_final=require_final)
        if validate_log_view(d)!={p:counts[p]['logs'] for p in PHASES}:
            raise ValueError('log capture/view count mismatch')
        info=read(d/'eval/build_info.json')
        with (d/'eval/data.csv').open(encoding='utf-8',newline='') as f:
            reader=csv.reader(f);header=next(reader);nrows=0
            if header[0]!='time' or len(header)-1!=info['n_cols']: raise ValueError('eval header')
            if any('__' not in h or h.split('__')[0] not in info['candidate_services'] for h in header[1:]): raise ValueError('non-observation feature')
            for row in reader:
                if len(row)!=len(header): raise ValueError('ragged CSV')
                [float(x) for x in row];nrows+=1
            if nrows!=info['n_rows'] or nrows==0: raise ValueError('empty/mismatched eval')
        if check_hashes:
            for p,h in read(d/'migration.json')['files_sha256'].items():
                hh=hashlib.sha256()
                with local(d,p).open('rb') as f:
                    for block in iter(lambda:f.read(1024*1024),b''):hh.update(block)
                if hh.hexdigest()!=h: raise ValueError('file changed: '+p)
        summary.append({'scenario_id':e['scenario_id'],'round':e['round'],'G':e['root_count'],'counts':counts,'eval_columns':info['n_cols'],'eval_services':len(info['services'])})
        print(json.dumps({'validated':e['scenario_id'],'round':e['round']}),flush=True)
    if not summary or expected is not None and len(summary)!=expected: raise ValueError('unexpected actual loaded count')
    return {'state':'PASS_FORMAT_READ','actual_loaded':len(summary),'phases_loaded':len(summary)*3,
            'algorithm_evaluation':'NOT_AVAILABLE_NOT_RUN','rows':summary}


if __name__=='__main__':
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('root');ap.add_argument('--expected',type=int);ap.add_argument('--hashes',action='store_true');ap.add_argument('--report')
    a=ap.parse_args()
    if a.report:
        report_path=Path(a.report).resolve(); package_root=Path(a.root).resolve()
        if report_path.exists() or report_path.is_relative_to(package_root) or package_root.is_relative_to(report_path):
            ap.error('reader report must be a new file outside the package')
    r=validate(a.root,a.expected,a.hashes)
    if a.report:Path(a.report).write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in r.items() if k!='rows'}))
