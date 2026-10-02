"""GT-blind metric mapping and explicit, stage-local evaluation view.

The long-record schema/source alias and service__metric columns follow the
compatible metric-table interfaces. Historical raw matrices stay
available separately: counters and histogram buckets are not discarded there.
"""
from __future__ import annotations

import bisect
import collections
import csv
import json
import math
from datetime import datetime, timezone

# Deployment/service inventory, not the root labels of any experiment.
SERVICES = tuple(sorted(('address', 'admin-audit', 'ai-memory', 'announcement',
    'backend', 'cart', 'catalog', 'catalog-gw', 'checkout', 'interaction', 'inventory',
    'llm-rerank', 'merchant', 'notification', 'order', 'payment', 'pricing', 'promotion',
    'rec-agent', 'review', 'review-query', 'sasrec', 'search', 'shipping', 'shop-web', 'user')))
PHASES = ('pre_fault', 'during_fault', 'post_recovery')
RATE_MAP = {
    'container_cpu_usage_seconds_total': ('container_cpu_usage_cores', 'cores'),
    'container_cpu_cfs_throttled_periods_total': ('container_cpu_throttled_periods_rate', 'periods_per_second'),
    'container_cpu_cfs_throttled_seconds_total': ('container_cpu_throttled_seconds_rate', 'seconds_per_second'),
    'container_network_receive_bytes_total': ('container_network_receive_bytes_rate', 'bytes_per_second'),
    'container_network_transmit_bytes_total': ('container_network_transmit_bytes_rate', 'bytes_per_second'),
    'container_network_receive_packets_total': ('container_network_receive_packets_rate', 'packets_per_second'),
    'container_network_transmit_packets_total': ('container_network_transmit_packets_rate', 'packets_per_second'),
    'container_network_receive_packets_dropped_total': ('container_network_receive_dropped_rate', 'packets_per_second'),
    'container_network_transmit_packets_dropped_total': ('container_network_transmit_dropped_rate', 'packets_per_second'),
    'container_network_receive_errors_total': ('container_network_receive_errors_rate', 'events_per_second'),
    'container_network_transmit_errors_total': ('container_network_transmit_errors_rate', 'events_per_second'),
}
GAUGE_MAP = {
    'container_memory_usage_bytes': ('container_memory_usage_bytes', 'bytes'),
    'container_memory_working_set_bytes': ('container_memory_working_set_bytes', 'bytes'),
    'container_memory_rss': ('container_memory_rss_bytes', 'bytes'),
    'container_memory_failcnt': ('container_memory_failcnt', 'events'),
    'container_start_time_seconds': ('container_start_time_seconds', 'epoch_seconds'),
    'kube_pod_status_ready': ('pod_ready', 'boolean'),
    'kube_pod_container_status_ready': ('container_ready', 'boolean'),
    'kube_pod_container_status_running': ('container_running', 'boolean'),
    'kube_pod_container_status_restarts_total': ('container_restart_count', 'restarts'),
    'kube_deployment_status_replicas_ready': ('deployment_replicas_ready', 'replicas'),
}


def iso(t):
    return datetime.fromtimestamp(float(t), timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def canonical_service(s):
    if s and s.endswith('_service'):
        s = s[:-8].replace('_', '-')
    return s


def canonical_http_service(s):
    # OTel HTTP names differ from Deployment names for these four services.
    # Keep this mapping local to historical HTTP; native/log views stay stable.
    aliases = {'backend_api': 'backend', 'recommendation_agent': 'rec-agent',
               'sasrec_api': 'sasrec', 'shop_web': 'shop-web'}
    return aliases.get(s, canonical_service(s))


def phase_at(t, windows):
    return next((p for p in PHASES if windows[p]['start'] <= t < windows[p]['end']), None)


def membership(t, phase, windows):
    if phase == 'pre_fault': return 'baseline'
    if phase == 'post_recovery': return 'recovery'
    active = sorted(k for k, w in windows.items() if w and w[0] <= t < w[1])
    return ('_and_'.join(active) + ('_only' if len(active) == 1 else '')) if active else 'outside_confirmed_operations'


def record(c, t, phase, service, metric, value, unit, labels, member,
           source='prometheus_history', entity=None, entity_type='service', derived=False):
    # Metric-specific units, unlike the old generic seconds -> epoch conversion.
    finite = value is not None and math.isfinite(float(value))
    return {'schema_version': 'metrics.v2', 'timestamp': iso(t), 'stage': phase,
            'run_id': c['run_id'], 'source': 'prometheus', 'entity_type': entity_type,
            'entity': entity or service, 'service': service, 'metric': metric,
            'value': float(value) if finite else None, 'unit': unit,
            'metric_type': 'gauge', 'labels': dict(labels, source_raw=source),
            'fault_window_membership': member, 'container': labels.get('container') or service,
            'quality': ('derived' if derived else 'observed') if finite else 'missing'}


def owners(series):
    rs, pod, info = {}, {}, {}
    for row in series:
        l = row['metric']; m = l['__name__']
        if m == 'kube_replicaset_owner' and l.get('owner_kind') == 'Deployment':
            rs.setdefault(l['replicaset'], set()).add(l['owner_name'])
        if m == 'kube_pod_owner' and l.get('owner_kind') == 'ReplicaSet':
            pod.setdefault(l['pod'], set()).add(l['owner_name'])
        if m == 'kube_pod_info':
            info.setdefault(l['pod'], set()).add(l.get('uid'))
    result = {}
    for name, parents in pod.items():
        deployments = set().union(*(rs.get(r, set()) for r in parents))
        if len(deployments) == 1:
            result[name] = next(iter(deployments))
    return result, {p: next(iter(v)) for p, v in info.items() if len(v) == 1}


def intervals(values):
    prev = None
    for t, raw in values:
        v = float(raw)
        # No rate through a missing point or a counter reset. Raw survives.
        if prev is not None:
            pt, pv = prev; dt = t - pt
            yield t, (v - pv) / dt if 0 < dt <= 60 and math.isfinite(v) and math.isfinite(pv) and v >= pv else None, dt
        prev = (t, v)


def history_records(c, resources, http, fault_windows):
    pod_to_service, pod_uid = owners(resources)
    for idx, s in enumerate(resources):
        l = s['metric']; name = l['__name__']
        if name not in RATE_MAP and name not in GAUGE_MAP: continue
        service = l.get('deployment') or pod_to_service.get(l.get('pod'))
        if service not in SERVICES: continue
        if name.startswith('container_') and not name.startswith('container_network_') and l.get('container') in (None, '', 'POD'): continue
        if name == 'kube_pod_status_ready' and l.get('condition') != 'true': continue
        labels = {k: l[k] for k in ('pod', 'container', 'interface', 'cpu', 'job', 'uid') if k in l}
        if l.get('pod') in pod_uid: labels['historical_pod_uid'] = pod_uid[l['pod']]
        labels.update(raw_metric=name, series_index=idx, raw_artifact='raw/metrics/history/resources.json',
                      feature_scope='fixed_candidate', timestamp_basis='original_scrape')
        derived = name in RATE_MAP
        metric, unit = (RATE_MAP if derived else GAUGE_MAP)[name]
        points = intervals(s['values']) if derived else ((t, float(v), None) for t, v in s['values'])
        for t, v, dt in points:
            ph = phase_at(t, c['windows'])
            if not ph: continue
            lab = dict(labels)
            if derived: lab.update(transform='adjacent_counter_delta_per_second_reset_is_missing', delta_seconds=dt)
            yield record(c, t, ph, service, metric, v, unit, lab, membership(t, ph, fault_windows),
                         entity=l.get('pod') or service,
                         entity_type='deployment' if 'deployment' in l else ('container' if 'container' in l else 'pod'), derived=derived)
    # Sum/count siblings have exactly the same original label set. Derive a
    # service mean using sums of request increments, never mean-of-means.
    # Default features exclude /health for every service; raw includes it.
    sums = {}
    for i, s in enumerate(http):
        if s['metric']['__name__'].endswith('_sum'):
            key = tuple(sorted((k, v) for k, v in s['metric'].items() if k != '__name__'))
            sums[key] = (i, dict(s['values']))
    groups = collections.defaultdict(lambda: [0., 0., 0., set(), [], False])
    for idx, s in enumerate(http):
        l = s['metric']
        if not l['__name__'].endswith('_count'): continue
        svc = canonical_http_service(l.get('service_name'))
        if svc not in SERVICES: continue
        if l.get('http_target', '').split('?')[0].rstrip('/') in ('/health', '/metrics', '/ready', '/live'): continue
        key = tuple(sorted((k, v) for k, v in l.items() if k != '__name__'))
        if key not in sums: continue
        si, sv = sums[key]; prev = None
        for t, raw in s['values']:
            n, total = float(raw), float(sv.get(t, 'nan'))
            if prev is not None:
                pt, pn, ps = prev; dt = t - pt; dn, ds = n-pn, total-ps
                if 0 < dt <= 60 and all(math.isfinite(v) for v in (dn, ds)) and dn >= 0 and ds >= 0:
                    ph = phase_at(t, c['windows'])
                    if ph:
                        g = groups[(svc, t, ph)]; g[0] += dn/dt; g[1] += ds/dt
                        if str(l.get('http_status_code', '')).startswith(('4', '5')): g[2] += dn/dt
                        g[3].add(idx); g[3].add(si); g[4].append(dt)
                        if not l.get('http_status_code'): g[5] = True
            prev = (t, n, total)
    for (svc, t, ph), (rate, durations, errors, indices, dts, unknown_status) in sorted(groups.items()):
        labels = {'raw_artifact': 'raw/metrics/history/http.json', 'series_indices': sorted(indices),
                  'feature_scope': 'fixed_candidate', 'timestamp_basis': 'original_scrape',
                  'transform': 'sum_of_adjacent_deltas; health_routes_excluded',
                  'delta_seconds_min': min(dts), 'delta_seconds_max': max(dts)}
        member = membership(t, ph, fault_windows)
        yield record(c,t,ph,svc,'http_server_request_rate',rate,'requests_per_second',labels,member,derived=True)
        yield record(c,t,ph,svc,'http_server_mean_latency_ms',durations/rate if rate>0 else None,'milliseconds',labels,member,derived=True)
        yield record(c,t,ph,svc,'http_server_error_ratio',errors/rate if rate>0 and not unknown_status else None,'ratio',labels,member,derived=True)


class WideView:
    """No GT access. Same old time/service__metric interface; no zero fill.

    Stage-local bounded carry, then backfill the leading edge only, is marked
    imputation. No recovery/context rows. Sparse grid is also shipped.
    """
    def __init__(self, windows, step=2):
        self.windows, self.step = windows, step
        self.points = collections.defaultdict(list)

    def add(self, r):
        if r['stage'] not in ('pre_fault','during_fault') or r['labels'].get('feature_scope') != 'fixed_candidate': return
        if r['value'] is None or r['service'] not in SERVICES: return
        t = datetime.fromisoformat(r['timestamp'].replace('Z','+00:00')).timestamp()
        ph = r['stage']; idx = int((t-self.windows[ph]['start'])//self.step)
        self.points[(ph,idx,r['service']+'__'+r['metric'])].append((r['entity'],r['labels'].get('container'),r['value']))

    def write(self, path, write_json):
        columns = sorted({k[2] for k in self.points})
        rows, stages = [], []
        for ph in ('pre_fault','during_fault'):
            w=self.windows[ph]
            for i in range(math.ceil((w['end']-w['start'])/self.step)):
                row=[w['start']+i*self.step]
                for col in columns:
                    vals=self.points.get((ph,i,col),[])
                    # Per-entity temporal mean; sum additive resource channels
                    # across pods/containers. Readiness/start times use min.
                    by=collections.defaultdict(list)
                    for ent,cont,v in vals: by[(ent,cont)].append(v)
                    means=[sum(a)/len(a) for a in by.values()]
                    metric=col.split('__',1)[1]
                    if not means: value=None
                    elif metric in ('pod_ready','container_ready','container_running','container_start_time_seconds'): value=min(means)
                    elif metric.startswith('http_'): value=sum(means)/len(means)
                    else: value=sum(means)
                    row.append(value)
                rows.append(row);stages.append(ph)
        path.mkdir(parents=True,exist_ok=True)
        with (path/'data_missing.csv').open('w',newline='',encoding='utf-8') as f:
            wr=csv.writer(f);wr.writerow(['time']+columns);wr.writerows(rows)
        filled=[list(r) for r in rows]; imputed=collections.Counter(); dropped={}
        for j,col in enumerate(columns,1):
            for ph in ('pre_fault','during_fault'):
                indices=[i for i,p in enumerate(stages) if p==ph]
                observed=[i for i in indices if rows[i][j] is not None]
                if not observed: continue
                for i in indices:
                    if rows[i][j] is not None: continue
                    pos=bisect.bisect_right(observed,i)-1
                    donor=observed[pos] if pos>=0 else observed[0]
                    if abs(rows[i][0]-rows[donor][0])<=60:
                        filled[i][j]=rows[donor][j];imputed[col]+=1
            if any(r[j] is None for r in filled): dropped[col]='missing_after_stage_local_60s_fill'
        keep=[j for j,c in enumerate(columns,1) if c not in dropped]
        with (path/'data.csv').open('w',newline='',encoding='utf-8') as f:
            wr=csv.writer(f);wr.writerow(['time']+[columns[j-1] for j in keep]);wr.writerows([[r[0]]+[r[j] for j in keep] for r in filled])
        (path/'inject_time.txt').write_text(str(self.windows['during_fault']['start'])+'\n',encoding='utf-8')
        info={'schema_version':'eval-view.v1','built_by':'scripts/dataset/metric_views.py','source':'raw/metrics/metrics_v2.jsonl',
              'bucket_seconds':self.step,'inject_time':self.windows['during_fault']['start'],
              'inject_time_definition':'during_fault observation boundary, not actual injection',
              'n_rows':len(rows),'n_cols':len(keep),'pre_points':stages.count('pre_fault'),
              'during_points':stages.count('during_fault'),'services':sorted({columns[j-1].split('__')[0] for j in keep}),
              'candidate_services':list(SERVICES),'full_observation_columns':len(columns),'dropped_columns':dropped,
              'imputed_cells':dict(imputed),'imputation':'within each phase: forward fill <=60s; leading-only backward fill <=60s; never zero fill',
              'sampling_note':'2s is an alignment grid, not independent 2s resource measurements',
              'applicable':bool(keep),'no_gt_features':True,'scenario_selected_observations_excluded':True}
        write_json(path/'build_info.json',info)
        return info
