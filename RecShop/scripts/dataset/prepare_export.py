"""Freeze accepted input ledgers for a later incremental export; local reads only."""
import argparse
from pathlib import Path
from check_metrics_retention import ROOT, METRICS, load_cases, now, write_json
from source_paths import guard_output, load_mappings, protected_sources

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--firstpass-ledger',type=Path)
    p.add_argument('--accepted-ledger',type=Path,action='append',default=[])
    p.add_argument('--source-root',type=Path,required=True,help='base for relative evidence paths in ledgers and contracts')
    p.add_argument('--path-map',type=Path,help='explicit absolute-prefix relocation map JSON')
    p.add_argument('--out',type=Path,required=True)
    a=p.parse_args()
    if not a.firstpass_ledger and not a.accepted_ledger: p.error('accepted input ledger required')
    cases,inputs=load_cases(a.source_root.resolve(),a.firstpass_ledger.resolve() if a.firstpass_ledger else None,
                           [x.resolve() for x in a.accepted_ledger],load_mappings(a.path_map))
    if not cases:p.error('no accepted completed samples')
    out=guard_output(a.out,protected_sources({'inputs':inputs,'cases':cases}),file_output=True)
    out.parent.mkdir(parents=True,exist_ok=True)
    write_json(out,{'created_at':now(),'inputs':inputs,'cases':cases,'metrics':METRICS,
                    'source_resolution':cases[0]['source_resolution']})
    print(f'Frozen {len(cases)} accepted samples; no network request')
