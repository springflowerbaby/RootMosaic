"""Build a dataset package from accepted native experiment runs."""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import io
import json
import math
import re
import shutil
import time
from pathlib import Path

from check_metrics_retention import ROOT, PHASES, digest, read_json, write_json
from export_metrics import case_key, verify_plan, queries
from source_paths import SourcePaths, guard_output, protected_sources
from source_integrity import InputSnapshot, input_bytes, input_json, input_sha
from trace_formats import _flatten_traces_jsonl
from log_views import RULE as LOG_SOURCE_RULE, group_logs, log_bytes
from metric_views import (SERVICES, WideView, canonical_service, history_records,
                          iso, membership, record)

VERSION = 'm1-strict255-adapter-v4-portable2'
RELEASE_ID = 'RecShop-M1-v1.0'
DELIVERY_CHECKS = ('accepted_slot_and_attempt_binding','groundtruth_mapping',
                   'data_format_readability','file_integrity','declared_limitations_preserved')
AUDIT_PATHS = {'quality':'audit/collector-quality-original.json',
               'summary':'audit/collector-summary-original.json',
               'annotation':'audit/annotation-draft-original.json',
               'acceptance':'audit/collection-acceptance-original.json'}


def jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='\n') as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(',', ':'), allow_nan=False)+'\n')


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()


def code_sha():
    files=sorted(Path(__file__).parent.glob('*.py'))
    return digest(''.join(p.name+sha(p) for p in files if not p.name.startswith('test')).encode())


def copy_source(src, dest, refs, base):
    src, dest=Path(src),Path(dest)
    dest.parent.mkdir(parents=True,exist_ok=True)
    raw=input_bytes(src);before=digest(raw);dest.write_bytes(raw)
    if sha(dest)!=before: raise ValueError('copy hash mismatch')
    refs.append({'artifact':dest.relative_to(base).as_posix(),'sha256':before,'source_path':str(src)})


def native_for(c):
    s=Path(c['summary_path'])
    if input_sha(s)!=c['summary_sha256']: raise ValueError('SUMMARY changed')
    recovery=Path(c.get('recovery_input_path',s.parent/'recovery-input.json'))
    if c.get('recovery_input_sha256') and input_sha(recovery)!=c['recovery_input_sha256']:
        raise ValueError('recovery input changed')
    inp=input_json(recovery);contract=inp['contract'];ctx=contract['context']
    if ctx['attempt_id']!=c['attempt_id'] or ctx['run_id']!=c['run_id'] or contract['purpose']!='formal':
        raise ValueError('attempt/purpose mismatch')
    for r in c['phase_sources']:
        if input_sha(r['path'])!=r['sha256']: raise ValueError('phase bundle changed')
    root=SourcePaths.from_case(c).resolve(ctx['evidence_root'])
    if c.get('native_root') and root != Path(c['native_root']).resolve():
        raise ValueError('native evidence path changed')
    if c.get('native_contract_sha256') and input_sha(root/'contract.json')!=c['native_contract_sha256']:
        raise ValueError('original contract changed')
    return root,contract,input_json(s)


def assert_identity(doc,c):
    for key in ('attempt_id','run_id'):
        if key in doc and doc[key]!=c[key]: raise ValueError('foreign '+key)


def fault_metadata(c,contract,summary,ops,annotation):
    assert_identity(ops,c);assert_identity(annotation,c)
    roots=sorted({f['normalized_root_entity'] for f in contract['faults']})
    if sorted(summary['truth_entities'])!=roots: raise ValueError('SUMMARY/contract GT mismatch')
    faults,windows=[],{}
    annotations={f['fault_instance_id']:f for f in annotation['fault_instances']}
    for f in contract['faults']:
        fid=f['fault_instance_id']; relevant=[r for r in ops['operations'] if r['instance_id']==fid]
        for r in relevant: assert_identity(r,c)
        starts=[r['timestamp_epoch_s'] for r in relevant if r['action']=='inject' and r['return_code']==0]
        ends=[r['timestamp_epoch_s'] for r in relevant if r['action']=='recover' and r['return_code']==0]
        start=min(starts) if starts else None;end=max(ends) if ends else None
        if start is not None and end is not None and end<=start: raise ValueError('inverted operation window')
        windows[fid]=(start,end) if start is not None and end is not None else None
        # A Deployment is not a container/Pod. Retain exact pinned Pod if known.
        parent_id=(summary.get('local_fault_to_parent') or {}).get(fid,fid)
        pin=summary.get('runtime',{}).get('pins',{}).get(parent_id,{})
        if f['raw_target']['kind']!='Deployment':
            pin={}  # A MySQL lock is not an unrelated observation gateway Pod.
        elif pin and not pin.get('pod_name','').startswith(f['raw_target']['name']+'-'):
            raise ValueError('pinned Pod does not belong to the injected Deployment')
        role=annotations.get(fid,{}).get('role') or {}
        role_label=role.get('label') if isinstance(role,dict) else role
        # Do not promote draft labels to reviewed interaction facts.
        cg={'fault_instance_id':fid,'fault_class':f['fault_class'],'fault_type':f['fault_type'],
            'injection_fault':f['fault_type'],'target_component':f['normalized_root_entity'],
            'target_container':pin.get('pod_name'), 'role':role_label,
            'injected_at':iso(start) if start else None,'recovered_at':iso(end) if end else None,
            'status':'recovered' if end else 'not_assessed'}
        faults.append(dict(cg,component_ground_truth=cg,mechanism=f['mechanism'],
                           raw_target=f['raw_target'],parameters=f['parameters'],
                           planned_window=f['planned_window']))
    return roots,faults,windows


def native_metrics(c,base,summary,fault_windows):
    queries={q['query_id']:q for q in summary.get('observation_queries',[])}
    seen=set()
    for ph in PHASES:
        path=base/'artifacts'/ph/'metrics/projection.json'
        for r in input_json(path):
            q=queries.get(r['query_id'],{})
            if not q.get('expression'): raise ValueError('missing original query mapping')
            svc=canonical_service(r.get('labels',{}).get('service_name'))
            if not svc: raise ValueError('do not substitute root entity for missing observed service')
            key=(ph,q['expression'],json.dumps(r['labels'],sort_keys=True),r['timestamp_epoch_s'])
            if key in seen: continue
            seen.add(key)
            t=r['timestamp_epoch_s']
            lab=dict(r['labels'],query_expression=q['expression'],raw_value=r.get('raw_value'),
                     feature_scope='scenario_selected_not_default',raw_artifact=f'raw/metrics/native/{ph}.json',
                     timestamp_basis='prometheus_evaluation_time')
            yield record(c,t,ph,svc,'query_mean_latency_ms',r['value'],'milliseconds',lab,
                         membership(t,ph,fault_windows),source=r['source_id'],derived=True)


def workload_records(c,contract,phase,doc,fault_windows):
    streams={s['stream_id']:s for s in contract['request_profile']['streams']}
    # The phase epoch comes from the same-attempt actual clock, not wall-clock now.
    offset=c['windows'][phase]['start']-doc['clock']['phase_epoch_s']
    for r in doc['requests']:
        assert_identity(r,c)
        stream=streams[r['stream_id']];endpoint=stream['endpoint']
        match=re.search(r'/services/([^:/]+)',endpoint)
        svc=match.group(1) if match else 'unknown_endpoint'
        send=r.get('http_send_started_at_s');terminal=r.get('terminal_at_s')
        if send is None or terminal is None: continue  # drops stay in original requests
        if terminal<send: raise ValueError('negative request duration')
        t=send+offset
        lab={'feature_scope':'scenario_selected_not_default','stream_id':r['stream_id'],
             'endpoint':endpoint,'request_id':r['request_id'],'status':r['status'],
             'raw_artifact':f'raw/operations/workload-{phase}.json'}
        member=membership(t,phase,fault_windows)
        yield record(c,t,phase,svc,'request_duration_ms',(terminal-send)*1000,'milliseconds',lab,member,source='m1_http_workload',entity=endpoint,entity_type='endpoint',derived=True)
        if r.get('http_status') is not None:
            yield record(c,t,phase,svc,'http_status_code',r['http_status'],'code',lab,member,source='m1_http_workload',entity=endpoint,entity_type='endpoint')
            yield record(c,t,phase,svc,'request_success',int(200<=r['http_status']<400),'boolean',lab,member,source='m1_http_workload',entity=endpoint,entity_type='endpoint',derived=True)


def traces_and_logs(c,base,out,windows,refs):
    t_files=[];l_files=[];counts={};services=set();cross=False
    for ph in PHASES:
        tracepath=base/'artifacts'/ph/'traces/projection.json'
        logpath=base/'artifacts'/ph/'logs/projection.json'
        traces=input_json(tracepath);logs=input_json(logpath)
        copy_source(tracepath,out/f'raw/traces_calltree/native/{ph}.json',refs,out)
        copy_source(logpath,out/f'raw/logs/native/{ph}.json',refs,out)
        for modality in ('logs','traces'):
            # Preserve per-query scope and original provenance (including log
            # deployment identity, which must not be relabeled as a Pod UID).
            copy_source(base/'artifacts'/ph/modality/'bundle.json',out/f'raw/{"traces_calltree" if modality=="traces" else "logs"}/native/{ph}.bundle.json',refs,out)
        rows=[];seen={}
        for r in traces:
            k=(r['trace_id'],r['span_id'])
            if k in seen:
                if seen[k]!=r: raise ValueError('conflicting duplicate span')
                continue
            seen[k]=r;t=r['start_time_us']/1e6
            parents=[x['spanID'] for x in r['references'] if x.get('refType')=='CHILD_OF']
            services.add(r['service'])
            rows.append({'schema_version':'traces.v1','timestamp':iso(t),'stage':ph,'run_id':c['run_id'],
                         'fault_window_membership':membership(t,ph,windows),'trace_id':r['trace_id'],
                         'span_id':r['span_id'],'parent_span_id':parents[0] if parents else None,
                         'service':r['service'],'operation':r['operation'],'start_time':iso(t),
                         'end_time':iso(r['end_time_us']/1e6),'duration_ms':r['duration_us']/1000,
                         'tags':r['tags'],'process_id':r['process_id'],'process_tags':r['process_tags'],
                         'references':r['references'],'collector_query_service':','.join(sorted({x['queried_service'] for x in r['observed_in']}))})
        per_trace=collections.defaultdict(set)
        for r in rows: per_trace[r['trace_id']].add(r['service'])
        cross|=any(len(v)>1 for v in per_trace.values())
        ct=out/f'raw/traces_calltree/{ph}_traces.jsonl';flat=out/f'raw/traces/{ph}_traces.jsonl'
        jsonl(ct,rows);jsonl(flat,rows)
        nflat,_=_flatten_traces_jsonl(flat)
        t_files.append({'stage':ph,'artifact':f'raw/traces/{ph}_traces.jsonl','records':nflat})
        # The native entity may be an associated root (e.g. a MySQL lock),
        # while the actual producer is catalog. Keep the native rows untouched.
        loggroups=group_logs(logs,input_json(base/'artifacts'/ph/'logs/bundle.json'))
        for svc,rs in sorted(loggroups.items()):
            file=f'raw/logs/{ph}__{svc}.log'
            (out/file).parent.mkdir(parents=True,exist_ok=True)
            # Docker capture timestamp prefix + verbatim application message.
            (out/file).write_bytes(log_bytes(rs))
            l_files.append({'artifact':file,'stage':ph,'service':svc,'records':len(rs)})
        counts[ph]={'traces':len(rows),'flat_traces':nflat,'logs':len(logs)}
    tm={'schema_version':'traces-manifest.v2.1','storage_layout':'single_dir_stage_tagged','artifact_root':'raw/traces',
        'files':t_files,'validation':{'valid':all(v['traces']>0 for v in counts.values()),'flat_projection':True,
                                    'cross_service_span_graph_available':cross,'cross_service_span_graph_artifact_root':'raw/traces_calltree'}}
    write_json(out/'raw/traces/manifest.json',tm)
    write_json(out/'raw/traces/trace_profile.json',{'schema_version':'trace-profile.v2.1','counts':counts,'services':sorted(services),
                'flat_projection':True,'full_parent_relations':'raw/traces_calltree','complete_graph_not_claimed':True})
    write_json(out/'raw/traces_calltree/manifest.json',dict(tm,artifact_root='raw/traces_calltree',files=[dict(x,artifact=x['artifact'].replace('raw/traces/','raw/traces_calltree/'),records=counts[x['stage']]['traces']) for x in t_files],validation={'partial_capture':True,'original_relations_preserved':True}))
    write_json(out/'raw/traces_calltree/calltree_stats.json',{'schema_version':'calltree-stats.v1','phase_counts':counts,'observed_services':sorted(services)})
    (out/'raw/logs').mkdir(parents=True,exist_ok=True)
    write_json(out/'raw/logs/manifest.json',{'schema_version':'logs-manifest.v2.1','storage_layout':'single_dir_stage_tagged',
               'artifact_root':'raw/logs','files':l_files,'validation':{'valid':all(v['logs']>0 for v in counts.values()),'meaning':'nonempty, not exhaustive'},
               'phase_availability':{p:'partial' if v['logs'] else 'missing' for p,v in counts.items()}})
    return counts,sorted(services)


def acceptance(c,inputs):
    matches=[]
    for ref in inputs:
        path=Path(ref['path'])
        if input_sha(path)!=ref['sha256']: raise ValueError('ledger changed')
        if path.suffix=='.csv' and c['round']==1:
            with io.StringIO(input_bytes(path).decode('utf-8-sig'),newline='') as f:
                rows=[r for r in csv.DictReader(f) if r['scenario_id']==c['scenario_id'] and r['attempt_id']==c['attempt_id']]
            for row in rows:
                if row['state'] not in ('P0_COUNTED','P0_COUNTED_REVIEWED','P0_COUNTED_LEGACY'): raise ValueError('not accepted')
                summary=input_json(c['summary_path'])
                if row.get('condition_id')!=summary.get('condition_id'): raise ValueError('acceptance condition mismatch')
        elif path.suffix=='.json':
            d=input_json(path)
            rows=[]
            for row in d.get('rows',[]):
                if row.get('scenario_id')!=c['scenario_id'] or row.get('attempt_id')!=c['attempt_id']: continue
                rounds={int(v) for v in (d.get('round'),d.get('logical_round'),row.get('round'),row.get('repeat_number')) if v is not None}
                if len(rounds)!=1: raise ValueError('ambiguous acceptance round')
                if rounds!={c['round']}: continue
                if row.get('decision') not in ('ACCEPTED_MINIMUM_COLLECTION','ACCEPT_MINIMUM_COLLECTION'): raise ValueError('not accepted')
                if row.get('accepted',True) is not True: raise ValueError('not accepted')
                if row.get('design_version',c['design_version'])!=c['design_version']: raise ValueError('acceptance design mismatch')
                if row.get('run_id',c['run_id'])!=c['run_id']: raise ValueError('acceptance run mismatch')
                summary_refs=[r for r in row.get('evidence_refs',[]) if Path(r['path']).name=='SUMMARY.json']
                if len(summary_refs)!=1: raise ValueError('expected one accepted SUMMARY')
                source=SourcePaths.from_case(c).resolve(summary_refs[0]['path'])
                if source.resolve()!=Path(c['summary_path']).resolve() or summary_refs[0]['sha256']!=c['summary_sha256'] or input_sha(source)!=c['summary_sha256']:
                    raise ValueError('acceptance SUMMARY mismatch')
                rows.append(row)
        else: continue
        for row in rows:
            matches.append({'accepted':True,**{k:c[k] for k in ('round','scenario_id','design_version','attempt_id','run_id')},
                'summary_sha256':c['summary_sha256'],'ledger_sha256':ref['sha256'],'ledger_path':str(path.resolve()),
                'ledger_artifact':'audit/acceptance-ledger-original'+path.suffix,'row':row})
    if len(matches)!=1: raise ValueError('ambiguous acceptance' if matches else 'no acceptance')
    if input_sha(c['summary_path'])!=c['summary_sha256']: raise ValueError('SUMMARY changed')
    return matches[0]


def sample_location(c, root_count):
    category={1:'single',2:'dual',3:'triple'}[root_count]
    name=f'mr{root_count}_m1-{c["scenario_id"].lower()}-k8s-v2-formal-r{c["round"]}-'+iso(c['windows']['pre_fault']['start'])[:19].replace('-','').replace(':','').replace('T','')
    return name, f'traditional/{category}/{name}'


def annotation_label(annotation, key, pair_key):
    def label(value):
        value=value.get('label') if isinstance(value,dict) else value
        return None if value in (None,'','not_assessed','not_provided') else value
    direct=label(annotation.get(key))
    if direct is not None: return direct
    labels={label(p.get(pair_key)) for p in annotation.get('pair_relations',[])}-{None}
    if len(labels)>1: raise ValueError('conflicting scientific annotations: '+key)
    return next(iter(labels),'not_provided')


def generate_case_fields(c, contract, summary, ops, annotation, counts, trace_services,
                         acceptance_record, *, audit_refs, eval_info, delivery_checked=False):
    """Generate only fields from bound source documents and verified observations.

    delivery_checked is supplied only after independent read/count/hash checks.
    This pure entry is shared with offline replay; it never reads old metadata.
    """
    acc=acceptance_record
    for k in ('attempt_id','run_id','scenario_id','design_version','round'):
        if acc.get(k)!=c[k]: raise ValueError('acceptance identity mismatch: '+k)
    if acc.get('accepted') is not True or acc.get('summary_sha256')!=c['summary_sha256']:
        raise ValueError('unbound acceptance')
    if contract.get('purpose')!='formal': raise ValueError('formal source required')
    assert_identity(contract['context'],c)
    assert_identity(summary,c)
    if any(contract['scenario'].get(k)!=c[k] for k in ('scenario_id','design_version')):
        raise ValueError('contract scenario mismatch')
    if set(counts)!=set(PHASES): raise ValueError('incomplete observation stages')
    for values in counts.values():
        if set(values)!=set(('metrics','traces','logs','flat_traces')) or any(type(v) is not int or v<0 for v in values.values()):
            raise ValueError('invalid observation counts')
    for key in ('quality','acceptance','summary','annotation'):
        ref=audit_refs[key]
        if not ref['path'].startswith('audit/') or not re.fullmatch('[0-9a-f]{64}',ref['sha256']):
            raise ValueError('invalid audit reference')
    roots,faults,fw=fault_metadata(c,contract,summary,ops,annotation)
    n=len(roots);category={1:'single',2:'dual',3:'triple'}[n]
    name,relative_path=sample_location(c,n)
    stage_windows={}
    for ph,w in c['windows'].items():
        stage_windows[ph]={'window_start_at':iso(w['start']),'window_end_at':iso(w['end']),
            'window_seconds':w['end']-w['start'],'poll_interval_seconds':contract['metric_interval_s'],
            **{m+'_manifest':f'raw/{m}/manifest.json' for m in ('metrics','traces','logs')},
            **{m+'_filter':{'stage':ph} for m in ('metrics','traces','logs')},
            **{m+'_validation_status':'partial' if counts[ph][m] else 'missing' for m in ('metrics','traces','logs')},
            'gate_passed':None,'status':'captured_partial','start':iso(w['start']),'end':iso(w['end'])}
    cfw={fid:{'start_time':iso(w[0]) if w else None,'end_time':iso(w[1]) if w else None} for fid,w in fw.items()}
    overlap=None
    if n>1 and all(fw.values()):
        a=max(w[0] for w in fw.values());b=min(w[1] for w in fw.values())
        if a<b: overlap={'start_time':iso(a),'end_time':iso(b)}
    classes={f['fault_class'] for f in faults};composition='none' if n==1 else ('intra_class' if len(classes)==1 else 'cross_class')
    gt={'sample_id':name,'answer_type':'single_root' if n==1 else 'multi_root','root_count':n,'fault_category':category,
        'composition_type':composition,'interaction_pattern':'single_root' if n==1 else annotation_label(annotation,'interaction_pattern','interaction'),
        'root_cause_services':roots,'n_distinct_root_services':n,'root_cause_instances':[f['target_container'] for f in faults if f['target_container']],
        'fault_types':[f['fault_type'] for f in faults],'injection_faults':[f['injection_fault'] for f in faults],
        'component_ground_truth':[f['component_ground_truth'] for f in faults],'component_fault_windows':cfw,
        'overlap_window':overlap,'source_case_id':c['attempt_id']}
    meta={'schema_version':'v1.2','stage_layout':list(PHASES),'sample_id':name,'run_id':c['run_id'],'system':'recweb2',
        'platform':'kubernetes','kubernetes_context':contract['context']['kube_context'],
        'chaos_engine':','.join(sorted({f['mechanism'] for f in faults})),'root_count':n,'category':category,
        'composition_type':composition,'interaction_pattern':gt['interaction_pattern'],'path_relation':annotation_label(annotation,'path_relation','request_path'),
        'faults':faults,'root_causes':roots,'component_fault_windows':cfw,'overlap_window':overlap,
        'observation_stages':stage_windows,'traffic_error_stats':{},'validation_results':[],
        'root_metric_contract':None,'ground_truth':gt,
        'artifacts':dict({m:'raw/'+m for m in ('metrics','traces','logs','operations')},summary='summary.md',ground_truth='groundtruth.json',metadata='metadata.json'),
        'checksum_guard':None,'config':{'poll':contract['metric_interval_s'],'stage_seconds':300,'request_profile':contract['request_profile']},
        'sample_status':'accepted_minimum_collection','ready_for_release':False,'validation_complete':False,
        'trace_stats':{'observed_span_services':trace_services,**{p:{'total':counts[p]['flat_traces']} for p in PHASES}},
        'metric_schema_version':'metrics.v2','storage_layout':'single_dir_stage_tagged','formal_slot_id':f'{c["scenario_id"]}_r{c["round"]:02d}',
        'scenario_name':c['scenario_id'],'phase':'formal','updated_at':iso(c['windows']['post_recovery']['end']),
        'created_at':iso(c['windows']['pre_fault']['start']),'source_case_id':c['attempt_id'],
        'migration':{'version':VERSION,'design_version':c['design_version'],'scenario_id':c['scenario_id'],'round':c['round'],
            'condition_id':summary['condition_id'],'attempt_id':c['attempt_id'],'source_summary_sha256':c['summary_sha256'],
            'accepted':True,
            'timing_semantics':'injected_at/recovered_at are successful operation-return bounds, not exact physical effect boundaries',
            'candidate_services':list(SERVICES),'source_fingerprints':contract['context']['fingerprints'],
            'local_fault_to_parent':summary.get('local_fault_to_parent'), 'raw_qc':'audit/collector-quality-original.json'}}
    state={'sample_status':'accepted' if delivery_checked else 'accepted_minimum_collection',
           'ready_for_release':bool(delivery_checked),'validation_complete':bool(delivery_checked),
           'validation_scope':'delivery_v1','release_version':'1.0','release_id':RELEASE_ID,
           'release_status':'final' if delivery_checked else 'pending_validation'}
    availability={m:{p:{'status':'available' if counts[p][m] else 'missing',
                           'record_count':counts[p][m]} for p in PHASES} for m in ('metrics','logs','traces')}
    meta.update(state)
    meta['validation_results']=[{'check':check,'status':'PASS','scope':'delivery_v1'} for check in DELIVERY_CHECKS] if delivery_checked else []
    meta['delivery']={'schema_version':'recshop-delivery-acceptance-v1','release_id':RELEASE_ID,
        'status':state['sample_status'],'validation_scope':'delivery_v1','release_record':'../../../RELEASE.json',
        'acceptance_evidence':'raw/operations/acceptance.json','modality_availability':availability,
        'coverage_note':'Available means recorded observations exist, not exhaustive coverage.',
        'original_qc':dict(audit_refs['quality'],role='historical_collector_diagnostics_not_v1_delivery_gate'),
        'audit_directory':'audit/','status_guide':'../../../STATUS-GUIDE.md'}
    accepted={'schema_version':'recshop-v1-sample-acceptance','sample_id':name,
        **{k:c[k] for k in ('scenario_id','design_version','round','attempt_id','run_id')},
        'accepted':True,'status':state['sample_status'],**state,'release_id':RELEASE_ID,
        'source_acceptance':dict(audit_refs['acceptance'],path_basis='sample_directory'),
        'modality_availability':availability,
        'measurement_notes':['raw/metrics/sampling.json','raw/metrics/quality.json','raw/logs/manifest.json']}
    entry={k:c[k] for k in ('design_version','scenario_id','round','attempt_id','run_id')}
    entry.update(condition_id=summary['condition_id'],sample_id=name,root_count=n,path=relative_path,
                 accepted=True,counts=counts,eval_columns=eval_info['n_cols'],eval_services=eval_info['services'],**state)
    injection={'faults':faults,'component_fault_windows':cfw,'actual_operations':'raw/operations/operations_log.json',
               'timing_semantics':meta['migration']['timing_semantics']}
    return {'metadata':meta,'groundtruth':gt,'injection':injection,'stage_windows':stage_windows,
            'acceptance':accepted,'entry':entry}


def write_case_fields(out, fields):
    for key,relative in [('metadata','metadata.json'),('groundtruth','groundtruth.json'),
                         ('injection','raw/operations/injection.json'),('acceptance','raw/operations/acceptance.json')]:
        write_json(out/relative,fields[key])
    metric_manifest=out/'raw/metrics/manifest.json'
    if metric_manifest.exists():
        doc=read_json(metric_manifest);doc['stage_windows']=fields['stage_windows'];write_json(metric_manifest,doc)
    log_manifest=out/'raw/logs/manifest.json'
    if log_manifest.exists():
        doc=read_json(log_manifest)
        doc['phase_availability']={p:'partial' if fields['entry']['counts'][p]['logs'] else 'missing' for p in PHASES}
        write_json(log_manifest,doc)


def build_case(c,history,delivery,inputs,adapter_hash):
    # Snapshot only this case's actual input set. All later source reads/copies
    # use these verified bytes, so replacement between verification and use
    # cannot silently change converted values or copied audit evidence.
    expected_queries={group: query for group,_,query,_,_ in queries(c)}
    with InputSnapshot(c,inputs,Path(history)/case_key(c),expected_queries):
        return _build_case(c,history,delivery,inputs,adapter_hash)


def _build_case(c,history,delivery,inputs,adapter_hash):
    base,contract,summary=native_for(c)
    acc=acceptance(c,inputs)
    ops=input_json(base/'artifacts/operations.json');annotation=input_json(base/'artifacts/annotation-draft.json')
    roots,faults,fw=fault_metadata(c,contract,summary,ops,annotation)
    n=len(roots);category={1:'single',2:'dual',3:'triple'}[n]
    name,relative_path=sample_location(c,n)
    out=delivery/relative_path
    hd=history/case_key(c);hm=input_json(hd/'manifest.json')
    if hm['case']!=c: raise ValueError('history belongs to another attempt')
    content_identity={'adapter_sha256':adapter_hash,'case':c,'history_sha256':input_sha(hd/'manifest.json'),'acceptance':acc}
    identity=digest(json.dumps(content_identity,sort_keys=True).encode())
    if out.exists():
        completed=out/'migration.json'
        if not completed.exists(): raise ValueError('existing output is partial: '+str(out))
        doc=read_json(completed)
        correction=doc.get('log_source_fix',{})
        corrected_identity=(correction.get('rule')==LOG_SOURCE_RULE and
                            correction.get('adapter_sha256')==adapter_hash and
                            correction.get('input_identity')==identity)
        if doc['identity']!=identity and not corrected_identity:
            raise ValueError('existing output differs: '+str(out))
        for f,h in doc['files_sha256'].items():
            if sha(out/f)!=h: raise ValueError('existing migrated file changed: '+f)
        return doc['entry'],True
    out.mkdir(parents=True)
    refs=[]
    for r in hm['raw']:
        if input_sha(hd/r['path'])!=r['sha256']: raise ValueError('historical raw hash mismatch')
        copy_source(hd/r['path'],out/'raw/metrics/history'/r['path'],refs,out)
        copy_source(hd/r['receipt'],out/'raw/metrics/history'/r['receipt'],refs,out)
    copy_source(hd/'manifest.json',out/'raw/metrics/history/manifest.json',refs,out)
    # Original evidence stays unmodified. These are independent delivery copies.
    for f,dst in [('artifacts/operations.json','raw/operations/operations_log.json'),
                  ('artifacts/annotation-draft.json',AUDIT_PATHS['annotation']),
                  ('artifacts/quality/result.json',AUDIT_PATHS['quality'])]:
        copy_source(base/f,out/dst,refs,out)
    copy_source(c['summary_path'],out/AUDIT_PATHS['summary'],refs,out)
    copy_source(base/'contract.json',out/'scripts/contract-original.json',refs,out)
    # Executed contract sometimes differs from pre-execution contract.json.
    write_json(out/'scripts/contract-executed.json',contract)
    write_json(out/AUDIT_PATHS['acceptance'],acc)
    copy_source(acc['ledger_path'],out/acc['ledger_artifact'],refs,out)
    counts,trace_services=traces_and_logs(c,base,out,fw,refs)
    for ph in PHASES:
        copy_source(base/'artifacts'/ph/'metrics/projection.json',out/f'raw/metrics/native/{ph}.json',refs,out)
        copy_source(base/'artifacts'/ph/'workload.json',out/f'raw/operations/workload-{ph}.json',refs,out)
        copy_source(base/'artifacts'/ph/'phase-observations.json',out/f'raw/operations/phase-observations-{ph}.json',refs,out)
    res=input_json(hd/'resources.json')['data']['result'];http=input_json(hd/'http.json')['data']['result']
    wide=WideView(c['windows']);metric_counts=collections.Counter();finite_counts=collections.Counter();metric_names=set();observed_services=set();cadences=collections.defaultdict(list)
    for s in res+http:
        vals=s['values'];name0=s['metric']['__name__']
        cadences[name0].extend(vals[i+1][0]-vals[i][0] for i in range(len(vals)-1))
    def emit():
        streams=[history_records(c,res,http,fw),native_metrics(c,base,summary,fw)]
        streams.extend(workload_records(c,contract,ph,input_json(base/'artifacts'/ph/'workload.json'),fw) for ph in PHASES)
        for rows in streams:
            for r in rows:
                metric_counts[r['stage']]+=1;finite_counts[r['stage']]+=r['value'] is not None
                metric_names.add(r['metric']);observed_services.add(r['service']);wide.add(r);yield r
    jsonl(out/'raw/metrics/metrics_v2.jsonl',emit())
    info=wide.write(out/'eval',write_json)
    for ph in PHASES: counts[ph]['metrics']=metric_counts[ph]
    audit_refs={key:{'path':relative,'sha256':sha(out/relative)} for key,relative in AUDIT_PATHS.items()}
    fields=generate_case_fields(c,contract,summary,ops,annotation,counts,trace_services,acc,
                               audit_refs=audit_refs,eval_info=info)
    meta=fields['metadata'];gt=fields['groundtruth'];stage_windows=fields['stage_windows']
    write_case_fields(out,fields)
    write_json(out/'raw/metrics/manifest.json',{'schema_version':'metrics-manifest.v2.1','storage_layout':'single_dir_stage_tagged',
        'artifact_root':'raw/metrics','files':[{'stages':list(PHASES),'artifact':'raw/metrics/metrics_v2.jsonl','kind':'unified_timeseries'}],
        'stage_windows':stage_windows,'validation':{'valid':all(finite_counts[p]>0 for p in PHASES),'meaning':'readable/nonempty, not full QC'},
        'history':'raw/metrics/history/manifest.json','candidate_services':list(SERVICES),'observed_services':sorted(observed_services)})
    write_json(out/'raw/metrics/quality.json',{'schema_version':'metrics-quality.v2.1','stages':{p:{'schema_version':'metrics-quality.v2.1',
        'stage':p,'expected_snapshots':None,'observed_snapshots':None,'coverage_ratio':None,'max_gap_seconds':None,
        'required_metrics':[],'missing_required_metrics':[],'null_required_value_records':metric_counts[p]-finite_counts[p],
        'metric_count':len(metric_names),'record_count':metric_counts[p],'prom_query_fail_count':None,'valid':finite_counts[p]>0,
        'availability':'partial' if metric_counts[p] else 'missing','definition':'readable observations; original QC separately preserved'} for p in PHASES}})
    cadence={m:{'intervals':len(v),'min_s':min(v),'median_s':sorted(v)[len(v)//2],'max_s':max(v)} for m,v in cadences.items() if v}
    write_json(out/'raw/metrics/sampling.json',{'timestamp_basis':'original scrape in history; native query evaluation separately identified',
               'query_context_seconds':60,'context_in_default_features':False,'by_metric':cadence})
    write_json(out/'scripts/request-profile.json',contract['request_profile'])
    (out/'scripts/README.md').write_text('Executed contract and request profile are collection records, not a portable cluster-launch script. See the shared adapter/ for offline conversion. No historical collection command is invented.\n',encoding='utf-8')
    (out/'eval/README.md').write_text('data.csv: time/service__metric observation interface, pre_fault + during_fault only. data_missing.csv retains sparse aligned missing cells. build_info.json documents bounded phase-local filling, omitted columns and grid semantics. inject_time.txt is the observation phase boundary; actual fault operation times are in metadata.json. No GT, fault IDs, configuration values or scenario-selected probes are default features.\n',encoding='utf-8')
    (out/'summary.md').write_text(f'# {c["scenario_id"]} · repeat {c["round"]}\n\nRoot entities: {", ".join(roots)}.\n\nThree observation phases, 300 seconds each. See metadata.json for the actual windows and raw/*/manifest.json for modality availability. Historical metric timestamps retain their original cadence.\n',encoding='utf-8')
    write_json(out/'raw/operations/provenance.json',{'version':VERSION,'sources':refs,'source_context_optional':c,'adapter_sha256':adapter_hash})
    entry=fields['entry']
    files={p.relative_to(out).as_posix():sha(p) for p in sorted(out.rglob('*')) if p.is_file()}
    write_json(out/'migration.json',{'version':VERSION,'identity':identity,'entry':entry,'files_sha256':files})
    return entry,False


def finalize_delivery(out):
    """Finalize the existing migration only after a real offline read/hash check."""
    from validate_dataset import validate, validate_delivery_fields
    out=Path(out)
    checked=validate(out,check_hashes=True,require_final=False)
    manifest=read_json(out/'MANIFEST.json')
    for entry,observed in zip(manifest['cases'],checked['rows']):
        if (entry['scenario_id'],entry['round'])!=(observed['scenario_id'],observed['round']):
            raise ValueError('readback order changed')
        d=out/entry['path'];migration=read_json(d/'migration.json')
        c=read_json(d/'raw/operations/provenance.json')['source_context_optional']
        audit_refs={key:{'path':relative,'sha256':sha(d/relative)} for key,relative in AUDIT_PATHS.items()}
        fields=generate_case_fields(c,read_json(d/'scripts/contract-executed.json'),
            read_json(d/AUDIT_PATHS['summary']),read_json(d/'raw/operations/operations_log.json'),
            read_json(d/AUDIT_PATHS['annotation']),observed['counts'],
            read_json(d/'raw/traces/trace_profile.json')['services'],read_json(d/AUDIT_PATHS['acceptance']),
            audit_refs=audit_refs,eval_info=read_json(d/'eval/build_info.json'),delivery_checked=True)
        write_case_fields(d,fields)
        migration['entry']=fields['entry']
        migration['files_sha256']={p.relative_to(d).as_posix():sha(p) for p in sorted(d.rglob('*'))
                                   if p.is_file() and p!=d/'migration.json'}
        write_json(d/'migration.json',migration)
        entry.clear();entry.update(fields['entry'])
        validate_delivery_fields(d,root_entry=entry,expected_counts=observed['counts'])
    states={'release_id':RELEASE_ID,'release_version':'1.0','release_status':'final',
            'ready_for_release':True,'validation_complete':True,'validation_scope':'delivery_v1'}
    manifest.update(states);write_json(out/'MANIFEST.json',manifest)
    missing=[{'sample_id':e['sample_id'],'modality':mod,'phase':ph}
             for e in manifest['cases'] for mod in ('metrics','logs','traces') for ph in PHASES
             if e['counts'][ph][mod]==0]
    write_json(out/'RELEASE.json',{'schema_version':'recshop-dataset-release-v1',**states,'status':'final',
        'accepted_samples':len(manifest['cases']),
        'scenario_definitions':len({(e['design_version'],e['scenario_id']) for e in manifest['cases']}),
        'rounds':sorted({e['round'] for e in manifest['cases']}),'known_missing_views':missing,
        'validation_basis':{'checks':list(DELIVERY_CHECKS),'report':'DELIVERY-VALIDATION.json'},
        'historical_audit':'Collection diagnostics remain byte-preserved in each audit directory; delivery_v1 is not full scientific QC.',
        'default_eval_candidate_scope':list(SERVICES),'status_guide':'STATUS-GUIDE.md'})
    write_json(out/'DELIVERY-VALIDATION.json',{'schema_version':'recshop-delivery-validation-v1',
        'state':'PASS','validation_scope':'delivery_v1','release_id':RELEASE_ID,
        'checks':list(DELIVERY_CHECKS),'readback':checked,
        'original_collector_qc_modified':False,'scientific_annotations_inferred':False})
    (out/'STATUS-GUIDE.md').write_text(
        '# RecShop dataset delivery status\n\n'
        'accepted, ready_for_release and validation_complete describe delivery_v1 acceptance, source identity, GT mapping, readable files and integrity. '
        'They do not promote original scientific QC or certify matched experimental comparisons.\n\n'
        'available means a nonzero observed count; partial describes incomplete temporal coverage. missing means zero observed records. '
        'not_provided and null mean no annotation was supplied. Existing scientific labels are retained.\n\n'
        'Original collector quality, summary, annotation and bound acceptance evidence remain in each audit directory. '
        'Default eval features use application/gateway services; host/DB roots remain in GT and require method support.\n',encoding='utf-8')
    (out/'README.md').write_text(
        '# RecShop dataset v1.0\n\n'
        f'Final delivery with {len(manifest["cases"])} accepted samples. See RELEASE.json, DELIVERY-VALIDATION.json and STATUS-GUIDE.md. '
        'Original collection diagnostics remain in audit/. The adapter contains the offline migration tools; it is not a cluster deployment recipe.\n',encoding='utf-8')
    sums=[f'{sha(p)}  {p.relative_to(out).as_posix()}' for p in sorted(out.rglob('*'))
          if p.is_file() and p!=out/'SHA256SUMS']
    (out/'SHA256SUMS').write_text('\n'.join(sums)+'\n',encoding='utf-8',newline='\n')
    return checked


def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--history',required=True);ap.add_argument('--out',required=True)
    ap.add_argument('--case',action='append',help='e.g. S01:r1; default all accepted input cases')
    ap.add_argument('--gap-s',type=float,default=2);args=ap.parse_args()
    history=Path(args.history).resolve()
    plan=read_json(history/'PLAN.json');verify_plan(plan)
    out=guard_output(args.out,[history,*protected_sources(plan)],resume_marker='MANIFEST.json')
    hash0=code_sha();out.mkdir(parents=True,exist_ok=True)
    cases=[c for c in plan['cases'] if not args.case or f'{c["scenario_id"]}:r{c["round"]}' in args.case]
    prior_manifest=read_json(out/'MANIFEST.json') if (out/'MANIFEST.json').exists() else {}
    prior=prior_manifest.get('cases',[])
    ledgers={r['path']:r for r in prior_manifest.get('input_ledgers',[])}
    for r in plan['inputs']:
        if r['path'] in ledgers and ledgers[r['path']]!=r:raise ValueError('prior input ledger hash changed')
        ledgers[r['path']]=r
    by={(c['design_version'],c['scenario_id'],c['round']):c for c in prior}
    if len(by)!=len(prior): raise ValueError('existing package contains duplicate slots')
    # Reject a competing selected attempt before creating any new case directory.
    for c in cases:
        key=(c['design_version'],c['scenario_id'],c['round'])
        if key in by and by[key]['attempt_id']!=c['attempt_id']:
            raise ValueError('existing slot selects another attempt; use a new output')
    for c in cases:
        if (out/'STOP').exists(): raise SystemExit('migration paused by STOP file')
        if code_sha()!=hash0: raise ValueError('adapter changed during conversion; pause')
        e,skipped=build_case(c,history,out,plan['inputs'],hash0);key=(c['design_version'],c['scenario_id'],c['round'])
        if key in by and by[key]!=e: raise ValueError('existing slot changed')
        by[key]=e
        write_json(out/'MANIFEST.json',{'schema_version':'m1-delivery-v1','version':VERSION,'adapter_sha256':hash0,
             'input_ledgers':list(ledgers.values()),'candidate_services':list(SERVICES),'cases':sorted(by.values(),key=lambda e:(e['round'],e['scenario_id']))})
        print(json.dumps({'converted':case_key(c),'skipped_identical':skipped,'count':len(by)}),flush=True)
        time.sleep(max(0,args.gap_s))
    ad=out/'adapter';ad.mkdir(exist_ok=True)
    for p in Path(__file__).parent.glob('*.py'):
        if not p.name.startswith('test'): shutil.copyfile(p,ad/p.name)
    finalize_delivery(out)
    print(json.dumps({'state':'COMPLETE','count':len(by)}),flush=True)


if __name__=='__main__': main()
