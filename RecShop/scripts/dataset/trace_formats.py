# Vendored flat-trace compatibility function; original source hash retained.
# source SHA256: 1fb3c0d44757582c6e67745e27fe82240aece5ce9d3ec0185a39ba338f717848
import json

def _flatten_traces_jsonl(path):
    """Flatten one *_traces.jsonl IN-PLACE to the reference flat shape: exactly 1
    row per trace_id (parent_span_id forced null - a single-span trace has no
    parent). Per trace_id keep the root span (parent_span_id null); if there is
    no single root, keep the earliest-start span; child spans are dropped.
    Idempotent: an already-flat file (1 row/trace) is unchanged. Returns
    (rows_kept, sorted_services). Native source (full call-tree) is NOT touched -
    only this delivery copy is rewritten."""
    rows_by_trace = {}
    order = []
    noid = 0
    with open(path, encoding="utf-8-sig") as f:  # utf-8-sig: tolerate/strip BOM on read
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if not isinstance(r, dict):
                continue  # valid non-object JSON (str/num/bool/list/null) - not a trace record
            tid = r.get("trace_id")
            if tid is None:
                tid = "__noid_%d__" % noid
                noid += 1
            if tid not in rows_by_trace:
                rows_by_trace[tid] = []
                order.append(tid)
            rows_by_trace[tid].append(r)

    def _key(r):
        return str(r.get("start_time") or r.get("timestamp") or "")

    kept = []
    for tid in order:
        grp = rows_by_trace[tid]
        if len(grp) == 1:
            chosen = grp[0]
        else:
            roots = [r for r in grp if r.get("parent_span_id") in (None, "")]
            pool = roots if roots else grp
            chosen = sorted(pool, key=_key)[0]
        chosen = dict(chosen)
        # flat shape (matches reference sample): a 1-span trace has no parent, and
        # span_id == trace_id (the reference collector emits span_id==trace_id for single-
        # span traces). Native span_id stays in the repo's native source.
        chosen["parent_span_id"] = None
        if chosen.get("trace_id") is not None:
            chosen["span_id"] = chosen["trace_id"]
        # Flat 18-field compatibility: the delivery view drops the call-tree refs
        # (parallel to parent_span_id=None above) -- the ONLY non-additive change.
        # process_id / process_tags / collector_query_service pass through unchanged.
        chosen["references"] = []
        kept.append(chosen)

    # Emit UTF-8 without a BOM for ordinary line-wise JSON consumers.
    # Read above uses utf-8-sig (tolerates BOM if re-flattening idempotently).
    with open(path, "w", encoding="utf-8") as f:
        for r in kept:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    services = sorted({str(r.get("service")) for r in kept if r.get("service")})
    return len(kept), services
