"""Make clearly synthetic format fixtures; never calls an API or runs a collector.

The formal-shaped records exercise the migration format, not sample validity.
All values, identities and decisions are invented test data, not RecShop results.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / 'scripts/dataset'))
from check_metrics_retention import PHASES, METRICS, digest, read_json, write_json
from export_metrics import VERSION, CONTEXT_SECONDS, case_key, coverage, queries, verify_plan
from metric_views import iso
from source_paths import guard_output, protected_sources

DESIGN = 'synthetic-format-example-v1'
ATTEMPT = 'synthetic-not-collected-demo-r1'


def save(path, doc):
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, doc)
    return path


def create_inputs(destination):
    root = guard_output(destination)
    root.mkdir(parents=True)
    note = {'synthetic_example': True, 'scientific_sample': False,
            'meaning': 'Invented format fixture. No collection, injection, model or API call occurred.'}
    save(root / 'SYNTHETIC-NOT-COLLECTED.json', note)
    native = root / 'native/demo'
    contract = {'purpose': 'formal', 'synthetic_example': True,
        'scenario': {'scenario_id': 'DEMO01', 'design_version': DESIGN},
        'context': {'attempt_id': ATTEMPT, 'run_id': 'synthetic-run',
                    'namespace': 'synthetic-namespace', 'replicate': 1,
                    'evidence_root': 'native/demo', 'kube_context': 'synthetic-no-cluster',
                    'fingerprints': {'synthetic_example': True}},
        'metric_interval_s': 2, 'request_profile': {'streams': []},
        'faults': [{'fault_instance_id': 'F1', 'normalized_root_entity': 'catalog',
                    'fault_class': 'synthetic', 'fault_type': 'synthetic-example',
                    'mechanism': 'synthetic-no-injection',
                    'raw_target': {'kind': 'Deployment', 'name': 'catalog'},
                    'parameters': {}, 'planned_window': {}}]}
    save(native / 'contract.json', contract)
    summary = save(root / 'runs/demo/SUMMARY.json', {'synthetic_example': True,
        'condition_id': 'synthetic-condition', 'attempt_id': ATTEMPT, 'run_id': 'synthetic-run',
        'truth_entities': ['catalog'], 'runtime': {'pins': {}}, 'observation_queries': []})
    recovery = save(root / 'runs/demo/recovery-input.json', {'contract': contract})
    save(native / 'artifacts/operations.json', {'synthetic_example': True,
        'attempt_id': ATTEMPT, 'run_id': 'synthetic-run', 'operations': []})
    save(native / 'artifacts/annotation-draft.json', {'synthetic_example': True,
        'attempt_id': ATTEMPT, 'run_id': 'synthetic-run',
        'fault_instances': [{'fault_instance_id': 'F1', 'role': {'label': None}}]})
    save(native / 'artifacts/quality/result.json', {'synthetic_example': True,
        'original_quality': 'FAIL', 'reason': 'Deliberately retained test marker; no scientific assessment.'})
    for i, phase in enumerate(PHASES):
        start = 1000 + i * 301
        scope = {'attempt_id': ATTEMPT, 'run_id': 'synthetic-run', 'phase': phase,
                 'actual_window': {'time_basis': 'unix_epoch', 'start': start, 'end': start + 300}}
        base = native / 'artifacts' / phase
        save(base / 'metrics/bundle.json', {'scope': scope, 'queries': []})
        save(base / 'metrics/projection.json', [])
        save(base / 'workload.json', {'clock': {'phase_epoch_s': start}, 'requests': []})
        save(base / 'phase-observations.json', {'synthetic_example': True, 'scope': scope})
        save(base / 'traces/bundle.json', {'scope': scope, 'queries': []})
        save(base / 'traces/projection.json', [{'trace_id': 'synthetic-trace-' + str(i),
            'span_id': 'synthetic-span-' + str(i), 'start_time_us': (start + 1) * 1_000_000,
            'end_time_us': (start + 1) * 1_000_000 + 1000, 'duration_us': 1000,
            'service': 'catalog', 'operation': 'synthetic-example', 'tags': {},
            'process_id': 'synthetic-process', 'process_tags': {}, 'references': [],
            'observed_in': [{'queried_service': 'catalog'}]}])
        save(base / 'logs/bundle.json', {'scope': scope, 'queries': [{
            'source': {'source_id': 'synthetic-log'}, 'raw_ref': 'synthetic.txt',
            'backend_provenance': {'identity': {'deployment_name': 'catalog'}}}]})
        save(base / 'logs/projection.json', [{'source_id': 'synthetic-log', 'raw_ref': 'synthetic.txt',
            'timestamp_original': iso(start + 1), 'message': 'SYNTHETIC FORMAT EXAMPLE; NOT COLLECTED'}])
    row = {'scenario_id': 'DEMO01', 'design_version': DESIGN, 'round': 1,
           'repeat_number': 1, 'attempt_id': ATTEMPT, 'decision': 'ACCEPTED_MINIMUM_COLLECTION',
           'synthetic_example': True, 'evidence_refs': [
               {'path': p.relative_to(root).as_posix(), 'sha256': digest(p.read_bytes())}
               for p in (summary, recovery, native / 'contract.json')]}
    save(root / 'accepted-ledger.json', {'round': 1, 'synthetic_example': True, 'rows': [row]})
    return root


def create_cache(plan_path, destination):
    """Create offline cache ONLY for the bundled invented DEMO01 identity."""
    plan_path = Path(plan_path).resolve()
    plan = read_json(plan_path)
    verify_plan(plan)
    if len(plan['cases']) != 1:
        raise ValueError('synthetic cache supports one bundled demonstration only')
    c = plan['cases'][0]
    if (c['design_version'], c['scenario_id'], c['attempt_id']) != (DESIGN, 'DEMO01', ATTEMPT):
        raise ValueError('refuse synthetic values for a non-demonstration attempt')
    if read_json(c['summary_path']).get('synthetic_example') is not True:
        raise ValueError('synthetic source marker required')
    out = guard_output(destination, [plan_path, *protected_sources(plan)])
    out.mkdir(parents=True)
    save(out / 'SYNTHETIC-NOT-COLLECTED.json', {'synthetic_example': True, 'network_requests': 0})
    save(out / 'PLAN.json', {'version': VERSION, 'input_plan_sha256': digest(plan_path.read_bytes()),
        'inputs': plan['inputs'], 'cases': plan['cases'], 'metrics': METRICS,
        'source_resolution': plan['source_resolution'], 'context_seconds': CONTEXT_SECONDS,
        'origin': 'synthetic:no-network', 'synthetic_example': True,
        'interval_semantics': 'phase [start,end); context excluded from phase observations'})
    folder = out / case_key(c)
    refs = []
    for group, names, query, start, end in queries(c):
        values = [[t, str(100 + i)] for i, phase in enumerate(PHASES)
                  for t in range(int(c['windows'][phase]['start']), int(c['windows'][phase]['end']), 2)]
        result = [] if group == 'http' else [{'metric': {
            '__name__': 'container_memory_usage_bytes', 'namespace': c['namespace'],
            'deployment': 'catalog', 'pod': 'catalog-synthetic', 'container': 'catalog'}, 'values': values}]
        raw_path = save(folder / (group + '.json'), {'status': 'success', 'data': {'resultType': 'matrix', 'result': result}})
        receipt = {'synthetic_example': True, 'queried_at': None, 'elapsed_s': 0,
            'response_bytes': raw_path.stat().st_size, 'response_sha256': digest(raw_path.read_bytes()),
            'slow': False, 'group': group, 'query': query, 'query_start': start, 'query_end': end,
            'context_seconds': CONTEXT_SECONDS, 'case': c, 'endpoint': '/api/v1/query',
            'coverage': coverage(read_json(raw_path), c, names), 'source': 'synthetic-no-request'}
        rec_path = save(folder / (group + '.receipt.json'), receipt)
        refs.append({'path': raw_path.name, 'receipt': rec_path.name,
                     'sha256': receipt['response_sha256'], 'receipt_sha256': digest(rec_path.read_bytes())})
    save(folder / 'manifest.json', {'version': VERSION, 'case': c, 'raw': refs, 'synthetic_example': True})
    return out


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True, help='new independent fixture or cache directory')
    parser.add_argument('--cache-from-plan', type=Path, help='make only the bundled synthetic cached matrices')
    args = parser.parse_args()
    output = create_cache(args.cache_from_plan, args.out) if args.cache_from_plan else create_inputs(args.out)
    print(json.dumps({'state': 'SYNTHETIC_FORMAT_ONLY', 'network_requests': 0, 'output': str(output)}))
