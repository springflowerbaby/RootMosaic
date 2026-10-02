"""Serial, resumable export of original historical samples; never runs collectors."""
from __future__ import annotations

import argparse
import json
import math
import time
import urllib.parse
from pathlib import Path

from check_metrics_retention import (ROOT, PHASES, METRICS, Reader, digest,
                                    load_cases, now, read_json, write_json)
from source_paths import guard_output, protected_sources
from source_integrity import require_consumed_manifest, verify_consumed_inputs

VERSION = "m1-history-export-v2-bound-inputs"
CONTEXT_SECONDS = 60


def case_key(c):
    return f"r{c['round']:02d}/{c['scenario_id']}"


def queries(c):
    # A range-vector instant query returns original scrape timestamps/values.
    # It does NOT evaluate/resample a metric every two seconds.
    start = min(w['start'] for w in c['windows'].values()) - CONTEXT_SECONDS
    end = max(w['end'] for w in c['windows'].values())
    duration = math.ceil((end - start) * 1000)
    for group in ('resources', 'http'):
        names = [m for m, family in METRICS.items() if (family == 'http') == (group == 'http')]
        nskey = 'k8s_namespace_name' if group == 'http' else 'namespace'
        selector = '{__name__=~' + json.dumps('|'.join(names)) + ',' + nskey + '=' + json.dumps(c['namespace']) + '}'
        yield group, names, f'{selector}[{duration}ms] @ {end:.9f}', start, end


def coverage(doc, c, names):
    if doc.get('status') != 'success' or doc.get('warnings') or doc.get('infos'):
        raise ValueError('query failed or returned annotations')
    if doc['data']['resultType'] != 'matrix':
        raise ValueError('original sample matrix required')
    cells = {(p, m): {'points': 0, 'finite_points': 0, 'series': 0} for p in PHASES for m in names}
    for series in doc['data']['result']:
        m = series['metric']['__name__']
        if m not in names:
            raise ValueError('foreign metric')
        ns = series['metric'].get('k8s_namespace_name' if METRICS[m] == 'http' else 'namespace')
        if ns != c['namespace']:
            raise ValueError('foreign namespace')
        values = series.get('values', [])
        if any(not math.isfinite(float(t)) for t, _ in values):
            raise ValueError('invalid timestamp')
        if any(values[i][0] >= values[i+1][0] for i in range(len(values)-1)):
            raise ValueError('unordered/duplicate raw timestamps')
        for phase, w in c['windows'].items():
            points = [(t, v) for t, v in values if w['start'] <= t < w['end']]
            cell = cells[(phase, m)]
            cell['points'] += len(points)
            cell['finite_points'] += sum(math.isfinite(float(v)) for _, v in points)
            cell['series'] += bool(points)
    return [dict(phase=p, metric=m, **counts, status='PRESENT' if counts['points'] else 'NO_SAMPLES')
            for (p, m), counts in cells.items()]


class ExportReader(Reader):
    def get_raw(self, query):
        if self.last_end is not None:
            time.sleep(max(0, self.gap - (time.monotonic() - self.last_end)))
        if self.stop_path and self.stop_path.exists():
            raise RuntimeError('STOP file requested migration pause')
        url = self.origin + '/api/v1/query?' + urllib.parse.urlencode({'query': query, 'timeout': '2s'})
        started, at = time.monotonic(), now()
        with self.opener.open(url, timeout=5) as response:
            raw = response.read(64_000_001)
        self.last_end = time.monotonic()
        elapsed = self.last_end - started
        if len(raw) > 64_000_000:
            raise RuntimeError('export response requires smaller chunks; pause, not a dataset failure')
        return raw, {'queried_at': at, 'elapsed_s': elapsed, 'response_bytes': len(raw),
                     'response_sha256': digest(raw), 'slow': elapsed > self.slow}


def verify_plan(plan):
    for case in plan['cases']:
        require_consumed_manifest(case)
        verify_consumed_inputs(case)
    for ref in plan['inputs']:
        if digest(Path(ref['path']).read_bytes()) != ref['sha256']:
            raise ValueError('accepted ledger changed; build a new input plan')
    first = next((Path(r['path']) for r in plan['inputs'] if r['path'].endswith('.csv')), None)
    ledgers = [Path(r['path']) for r in plan['inputs'] if r['path'].endswith('.json')]
    policy = plan.get('source_resolution') or (plan['cases'][0].get('source_resolution') if plan['cases'] else None)
    if policy is None:
        raise ValueError('plan has no explicit source resolution; prepare a new plan')
    fresh, _ = load_cases(Path(policy['source_root']), first, ledgers, policy.get('mappings', []))
    if fresh != plan['cases']:
        raise ValueError('attempts or original windows changed')


def run(args):
    plan = read_json(args.plan)
    verify_plan(plan)
    out = guard_output(args.out, [*protected_sources(plan), Path(args.plan)], resume_marker='PLAN.json')
    out.mkdir(parents=True, exist_ok=True)
    frozen = {'version': VERSION, 'input_plan_sha256': digest(Path(args.plan).read_bytes()),
              'inputs': plan['inputs'], 'cases': plan['cases'], 'metrics': METRICS,
              'source_resolution': plan['source_resolution'],
              'context_seconds': CONTEXT_SECONDS, 'origin': args.origin,
              'interval_semantics': 'phase [start,end); context excluded from phase observations'}
    if (out / 'PLAN.json').exists():
        if read_json(out / 'PLAN.json') != frozen:
            raise ValueError('existing export uses another input/protocol')
    else:
        write_json(out / 'PLAN.json', frozen)
    client = ExportReader(args.origin, max(5, args.min_gap_s), args.slow_query_s)
    client.stop_path = out / 'STOP'
    cases = [c for c in plan['cases'] if not args.case or f"{c['scenario_id']}:r{c['round']}" in args.case]
    if args.limit:
        cases = cases[:args.limit]
    if not args.execute_readonly:
        print(json.dumps({'state': 'PLANNED', 'cases': len(cases), 'requests_per_case': 2})); return
    state, reason = 'COMPLETE', None
    try:
        for c in cases:
            folder = out / case_key(c)
            folder.mkdir(parents=True, exist_ok=True)
            refs = []
            for group, names, query, start, end in queries(c):
                raw_path, rec_path = folder / f'{group}.json', folder / f'{group}.receipt.json'
                if rec_path.exists():
                    rec = read_json(rec_path)
                    if rec['query'] != query or digest(raw_path.read_bytes()) != rec['response_sha256']:
                        raise ValueError('export resume hash/query mismatch')
                else:
                    if raw_path.exists():
                        raise ValueError('unreceipted raw output retained; use a new output directory')
                    raw, rec = client.get_raw(query)
                    raw_path.write_bytes(raw)
                    cells = coverage(json.loads(raw), c, names)
                    rec.update(group=group, query=query, query_start=start, query_end=end,
                               context_seconds=CONTEXT_SECONDS, case=c, coverage=cells,
                               endpoint='/api/v1/query', exporter_sha256=digest(Path(__file__).read_bytes()))
                    write_json(rec_path, rec)
                    if rec['slow']:
                        raise RuntimeError(f'slow query ({rec["elapsed_s"]:.3f}s); saved raw, pause migration')
                refs.append({'path': raw_path.name, 'receipt': rec_path.name,
                             'sha256': rec['response_sha256'], 'receipt_sha256': digest(rec_path.read_bytes())})
            manifest = {'version': VERSION, 'case': c, 'raw': refs}
            if (folder / 'manifest.json').exists() and read_json(folder / 'manifest.json') != manifest:
                raise ValueError('existing case manifest differs')
            write_json(folder / 'manifest.json', manifest)
            print(json.dumps({'exported': case_key(c), 'attempt_id': c['attempt_id']}, ensure_ascii=False), flush=True)
    except Exception as exc:
        state, reason = 'PAUSED', str(exc)
    completed = sum((out / case_key(c) / 'manifest.json').exists() for c in plan['cases'])
    write_json(out / 'STATUS.json', {'state': state, 'reason': reason, 'completed': completed,
                                   'planned': len(plan['cases']), 'updated_at': now()})
    print(json.dumps({'state': state, 'completed': completed, 'reason': reason}), flush=True)
    return 0 if state == 'COMPLETE' else 2


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', required=True); p.add_argument('--out', required=True)
    p.add_argument('--origin', default='http://127.0.0.1:19090')
    p.add_argument('--min-gap-s', type=float, default=5); p.add_argument('--slow-query-s', type=float, default=.75)
    p.add_argument('--limit', type=int); p.add_argument('--case', action='append')
    p.add_argument('--execute-readonly', action='store_true')
    raise SystemExit(run(p.parse_args()))
