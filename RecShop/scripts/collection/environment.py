"""Explicit machine binding for collection; loading this module never contacts a service."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import shutil
import sys
import uuid
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
ENV_KEY = "RECSHOP_COLLECTION_ENV"
_active = None
_path = None
STARTUP_BIND_CONTAINERS = frozenset({"recweb2-otel-collector", "recweb2-loki",
                                   "recshop-m1-otel-metrics", "recshop-m1-prometheus"})


def resolve_cli(name):
    """Resolve at execution time; children must reuse the same discovered path."""
    require(name in {"docker", "kubectl"}, "unsupported runtime CLI")
    found = shutil.which(name)
    if not found and os.name == "nt":
        bundled = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Docker/Docker/resources/bin" / (name + ".exe")
        found = str(bundled) if bundled.is_file() else None
    require(found is not None, name + " CLI missing: install it or expose the existing binary on PATH")
    selected = Path(found).resolve()
    require(selected.is_absolute() and selected.is_file(), name + " CLI is not an existing absolute file")
    key = "RECSHOP_RESOLVED_" + name.upper()
    inherited = os.environ.get(key)
    require(inherited is None or inherited == str(selected), name + " CLI changed since parent resolution")
    os.environ[key] = str(selected)
    return str(selected)


def require_windows_execution():
    require(os.name == "nt", "live collection/recovery requires a Windows control host; offline tools remain available")


def validate_monitoring(doc):
    from . import workload
    telemetry = doc.get("telemetry", {})
    for field in ("prometheus", "jaeger", "metrics_exporter"):
        try:
            workload._origin(telemetry.get(field))
        except (ValueError, TypeError):
            raise ValueError("collection environment: telemetry." + field + " requires an HTTP literal-IP origin without credentials, path, query or fragment") from None
    require(telemetry.get("metrics_collector_config_path") == "/etc/m1/otel-metrics-config.yaml",
            "telemetry.metrics_collector_config_path must be /etc/m1/otel-metrics-config.yaml (container path)")


def normalize_bind_source(value):
    """Compare explicit Docker sources without resolving or reading host files."""
    require(type(value) is str and bool(value) and value == value.strip()
            and not any(ord(char) < 32 for char in value), "invalid startup bind source")
    windows = bool(re.match(r"^[A-Za-z]:[\\/]", value)) or value.startswith(("\\\\", "//"))
    if windows:
        require(PureWindowsPath(value).is_absolute() and not value.replace("\\", "/").startswith(("//?", "//.")),
                "startup bind source must be absolute")
        normalized = value.replace("\\", "/")
    else:
        require(value.startswith("/") and not value.startswith("//"), "startup bind source must be absolute")
        normalized = value
    require(not any(part in {".", ".."} for part in normalized.split("/")), "startup bind source traversal forbidden")
    return normalized


def startup_bind_sources(doc):
    startup = doc.get("windows_startup", {})
    require(type(startup) is dict, "windows_startup must be an object")
    sources = startup.get("bind_sources", {})
    require(type(sources) is dict and set(sources) <= STARTUP_BIND_CONTAINERS,
            "unknown startup bind source container")
    return {name: normalize_bind_source(value) for name, value in sources.items()}


def resolve_path(value):
    p = Path(value).expanduser()
    return (p if p.is_absolute() else ROOT / p).resolve()


def require(ok, message):
    if not ok:
        raise ValueError("collection environment: " + message)


def load(path=None):
    """Read a non-secret environment file. No defaults can identify a live environment."""
    global _active, _path
    value = path or os.environ.get(ENV_KEY)
    require(bool(value), "pass --environment or set RECSHOP_COLLECTION_ENV")
    p = resolve_path(value)
    doc = json.loads(p.read_text(encoding="utf-8-sig"))
    validate_monitoring(doc)
    startup_bind_sources(doc)  # Reject invalid mappings before any runtime adapter.
    require(doc.get("schema_version") == "recshop-collection-environment-v1", "unsupported schema")
    for field in ("kube_context", "namespace", "node_name", "chaos_namespace"):
        require(isinstance(doc.get(field), str) and re.fullmatch(r"[A-Za-z0-9_.:-]+", doc[field]), field + " required")
    for field in ("cluster_uid", "namespace_uid", "node_uid"):
        require(str(uuid.UUID(doc[field])) == doc[field] and uuid.UUID(doc[field]).int != 0, field + " must be a real bound UUID")
    require(doc.get("binding_mode") in {"offline_fixture", "observed"}, "binding_mode required")
    db = doc.get("database", {})
    require(isinstance(db.get("name"), str) and re.fullmatch(r"[A-Za-z0-9_]+", db["name"]), "database schema name required")
    require(str(uuid.UUID(db["server_uuid"])) == db["server_uuid"] and uuid.UUID(db["server_uuid"]).int != 0, "database UUID required")
    require(set(db.get("checksums", {})) == {"items", "inventory"} and
            all(isinstance(v, str) and re.fullmatch(r"\d+", v) for v in db["checksums"].values()), "two explicit checksum baselines required")
    require(isinstance(db.get("credential_file"), str) and db["credential_file"], "credential_file required (contents never included in fingerprints)")
    for key in ("prometheus", "jaeger", "loki", "metrics_exporter", "metrics_endpoint_for_apps", "metrics_scrape_url"):
        url = urlsplit(doc.get("telemetry", {}).get(key, ""))
        require(url.scheme in {"http", "https"} and url.hostname and not url.username and not url.password,
                "plain configured endpoint required: " + key)
    targets = doc["telemetry"].get("prometheus_targets", {})
    require(set(targets) == {"otel-collector", "cadvisor", "kube-state-metrics", "cri-resource"}, "four Prometheus target URLs required")
    for key in ("output_root", "batch_root", "artifact_root", "lease_root"):
        require(isinstance(doc.get("paths", {}).get(key), str) and doc["paths"][key], "path required: " + key)
    paths = {k: resolve_path(v) for k, v in doc["paths"].items()}
    runtime_names = ("output_root", "batch_root", "artifact_root", "lease_root")
    for i, left in enumerate(runtime_names):
        for right in runtime_names[i + 1:]:
            require(not paths[left].is_relative_to(paths[right]) and not paths[right].is_relative_to(paths[left]), "runtime roots must not nest")
    require(len({str(paths[k]) for k in ("output_root", "batch_root", "artifact_root", "lease_root")}) == 4,
            "output, batch, artifact and lease roots must differ")
    require(type(doc.get("request_proxy_port")) is int and 1024 <= doc["request_proxy_port"] <= 65535, "request proxy port required")
    require(type(doc.get("protected_roots")) is list and bool(doc["protected_roots"]), "explicit protected roots required")
    protected = [resolve_path(x) for x in doc["protected_roots"]] + [ROOT / n for n in ("services", "scripts", "configs", "ops", "k8s", "datasets")]
    for key in ("output_root", "batch_root", "artifact_root", "lease_root"):
        candidate = paths[key]
        require(candidate != ROOT and not ROOT.is_relative_to(candidate), "runtime path contains project")
        require(not any(candidate.is_relative_to(x) or x.is_relative_to(candidate) for x in protected), "runtime path overlaps protected assets")
    _path, _active = p, copy.deepcopy(doc)
    os.environ[ENV_KEY] = str(p)
    return copy.deepcopy(doc)


def current():
    return copy.deepcopy(_active) if _active is not None else load()


def binding_ref():
    current()
    return {"path": str(_path), "sha256": hashlib.sha256(_path.read_bytes()).hexdigest()}


def verify_ref(ref):
    require(type(ref) is dict and set(ref) == {"path", "sha256"}, "environment reference missing")
    p = resolve_path(ref["path"])
    require(hashlib.sha256(p.read_bytes()).hexdigest() == ref["sha256"], "environment file changed; prepare a new campaign")
    return load(p)


def protected_roots():
    return tuple(resolve_path(x) for x in current()["protected_roots"]) + tuple(ROOT / n for n in ("services", "scripts", "configs", "ops", "k8s", "datasets"))


def assert_live():
    require(current()["binding_mode"] == "observed", "offline fixture cannot execute or query a live environment")


def credentials():
    assert_live()
    from dotenv import dotenv_values
    path = resolve_path(current()["database"]["credential_file"])
    require(path.is_file(), "database credential file missing")
    values = dotenv_values(path)
    keys = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
    require(all(values.get(key) for key in keys), "database credential fields incomplete")
    require(values["DB_NAME"] == current()["database"]["name"], "credential database differs from bound schema")
    return {key: str(values[key]) for key in keys}


def apply(driver, path=None):
    doc = load(path) if path is not None else current()
    driver.ENVIRONMENT_CONFIG = copy.deepcopy(doc)
    driver.OLD_ROOT = ROOT
    driver.ARTIFACTS = resolve_path(doc["paths"]["artifact_root"])
    driver.OUTPUT = resolve_path(doc["paths"]["output_root"])
    driver.LEASE_ROOT = resolve_path(doc["paths"]["lease_root"])
    driver.CONTEXT, driver.NAMESPACE = doc["kube_context"], doc["namespace"]
    driver.CLUSTER_UID, driver.NAMESPACE_UID = doc["cluster_uid"], doc["namespace_uid"]
    driver.DB_UUID, driver.CHECKSUMS = doc["database"]["server_uuid"], dict(doc["database"]["checksums"])
    driver.PORT = doc["request_proxy_port"]
    telemetry = doc["telemetry"]
    driver.PROM = driver.SAMPLING_PROM_ORIGIN = telemetry["prometheus"].rstrip("/")
    driver.JAEGER = telemetry["jaeger"].rstrip("/")
    driver.SAMPLING_EXPORTER_ORIGIN = telemetry["metrics_exporter"].rstrip("/")
    driver.SAMPLING_PROM_SCRAPE_URL = telemetry["metrics_scrape_url"]
    driver.SAMPLING_COLLECTOR = telemetry["metrics_collector_name"]
    driver.SAMPLING_COLLECTOR_CONFIG = telemetry["metrics_collector_config_path"]
    return doc


def bind_contract(data):
    """Rebind deployment locations only; fault targets, dose, timing and load stay unchanged."""
    from . import contract as c
    doc = current()
    old_namespace = data["context"]["namespace"]
    data["context"].update(kube_context=doc["kube_context"], namespace=doc["namespace"])
    for fault in data["faults"]:
        if fault["raw_target"]["kind"] == "Deployment":
            fault["raw_target"]["scope"] = doc["namespace"]
        elif fault["raw_target"]["kind"] == "MySqlTableLock":
            fault["raw_target"]["scope"] = doc["database"]["name"]
    for stream in data["request_profile"]["streams"]:
        stream["endpoint"] = stream["endpoint"].replace("/namespaces/" + old_namespace + "/", "/namespaces/" + doc["namespace"] + "/")
        stream["entrypoint"] = "http://127.0.0.1:" + str(doc["request_proxy_port"])
    data["context"]["fingerprints"]["environment"] = c.canonical_sha256({"cluster_uid": doc["cluster_uid"], "namespace_uid": doc["namespace_uid"]})
    return data


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        load(args.environment)
        print(json.dumps({"status": "CONFIG_VALIDATED_NOT_LIVE", "environment": binding_ref(),
                          "credentials_read": False, "collection_ready": False}))
        return 0
    except (ValueError, KeyError, OSError, TypeError) as exc:
        reason = str(exc) if str(exc).startswith("collection environment: telemetry.") else "incomplete_or_invalid_environment_config"
        print(json.dumps({"status": "BLOCKED", "reason": reason, "collection_ready": False}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
