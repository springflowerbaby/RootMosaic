"""Scientific condition identity. Historical one-off approval crosswalks are not portable."""
from pathlib import Path
import hashlib
import json
from . import contract as c

def semantic_signature(data: dict) -> dict:
    return {
        "scenario": data["scenario"],
        "faults": [{**{key: fault.get(key) for key in (
            "fault_instance_id", "fault_type", "mechanism", "mechanism_version",
            "normalization_version", "normalized_root_entity", "raw_target", "parameters")},
            "planned_window": {key: fault["planned_window"].get(key)
                               for key in ("start", "end", "time_basis")}}
            for fault in data["faults"]],
        "request_profile": data["request_profile"],
        "phases": {name: {key: phase.get(key) for key in ("start", "end", "time_basis")}
                   for name, phase in data["phases"].items()},
        "metric_interval_s": data["metric_interval_s"],
    }

def load_review_ref(ref):
    raise ValueError("Historical one-off representative reviews are not supported by this release; prepare a new source-bound campaign")

def verify(*args, **kwargs):
    raise ValueError("Historical representative approval cannot qualify this source version")
