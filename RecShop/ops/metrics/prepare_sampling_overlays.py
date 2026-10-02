"""Prepare reviewed source-only contexts and a NOT_BUILT deployment plan.

This module never calls Docker/kubectl, pulls images or changes running state.
Root must freshly validate the input identities, pin each local base by ID,
build/load unique tags, and apply UID/RV guarded Deployment changes separately.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def prepare(repo_root: Path, image_snapshot: dict, source_snapshot: dict, audit: dict,
            destination: Path) -> dict:
    repo_root, destination = repo_root.resolve(), destination.resolve()
    if destination.exists():
        raise ValueError("build plan destination exists; use a new version")
    sources = source_snapshot["services"]
    if len(sources) != 25 or len({s["deployment"] for s in sources}) != 25 or audit["service_count"] != 25:
        raise ValueError("expected exact reviewed 25-service scope")
    images = {d["deployment"]: d for d in image_snapshot["deployments"]}
    audited = {row["relative_source"]: row for row in audit["files"]}
    asset_dir = repo_root / "ops/metrics"
    helper = (repo_root / "shared/otel_metric_config.py").read_bytes()
    dockerfile = (asset_dir / "Dockerfile.metric-overlay").read_bytes()
    guard = (asset_dir / "verify_base_source.py").read_bytes()
    ignore = b"*\n!Dockerfile\n!service.py\n!otel_metric_config.py\n!verify_base_source.py\n!.dockerignore\n"
    pending, services = [], []
    for source in sources:
        deployment = source["deployment"]
        if re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", deployment) is None:
            raise ValueError("unsafe deployment name")
        row = images[deployment]
        local = row["local_image_identity"]
        base_id = local.get("image_id")
        if local.get("exit_code") != 0 or not isinstance(base_id, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", base_id) is None:
            raise ValueError("local base image identity unavailable")
        if row["image"] != source["image"] or re.fullmatch(r"recweb-[a-z0-9-]+:[A-Za-z0-9_.-]+", row["image"]) is None:
            raise ValueError("source/image snapshot mismatch or unexpected repository")
        relative = source["relative_source"]
        path = (repo_root / relative).resolve()
        if not path.is_relative_to(repo_root / "services"):
            raise ValueError("source path escaped service tree")
        modified = path.read_bytes()
        metadata = audited[relative]
        baseline = source["mother_source_sha256"]
        if (source.get("runtime_matches_mother_source") is not True or source["runtime"]["sha256"] != baseline
                or metadata["before_sha256"] != baseline or digest(modified) != metadata["after_sha256"]):
            raise ValueError("reviewed source fingerprints changed")
        container_path = source["container_source"]
        if container_path != "/app/" + relative:
            raise ValueError("container source path is not the audited repo layout")
        context_files = {"Dockerfile": dockerfile, "service.py": modified, "otel_metric_config.py": helper,
                         "verify_base_source.py": guard, ".dockerignore": ignore}
        context_hashes = {name: digest(body) for name, body in context_files.items()}
        identity = digest(json.dumps({"base_image_id": base_id, "files": context_hashes, "destination": container_path},
                                     sort_keys=True, separators=(",", ":")).encode())
        target_tag = row["image"].rsplit(":", 1)[0] + ":m1-metrics2s-" + identity[:16]
        base_alias = "recshop-m1-base:sha256-" + base_id.split(":")[1][:20]
        context = destination / deployment
        args = {"BASE_IMAGE": base_alias, "SERVICE_DESTINATION": container_path,
                "EXPECTED_BASE_SOURCE_SHA256": baseline, "BASE_IMAGE_ID": base_id, "SOURCE_AFTER_SHA256": digest(modified)}
        command = ["docker", "build", "--pull=false", "--network=none"]
        for key, value in args.items():
            command += ["--build-arg", key + "=" + value]
        command += ["--tag", target_tag, str(context)]
        services.append({"deployment": deployment, "deployment_uid_at_snapshot": row["deployment_uid"],
                         "resource_version_at_snapshot": row["resourceVersion"], "container": row["container"],
                         "original_image": row["image"], "base_image_id": base_id,
                         "base_source_sha256": baseline, "modified_source_sha256": digest(modified),
                         "helper_sha256": digest(helper), "context_files": context_hashes,
                         "build_context": str(context), "new_image_tag": target_tag, "built_image_id": None,
                         "pin_base_command": ["docker", "tag", base_id, base_alias], "build_command": command,
                         "execution_status": "NOT_BUILT", "runtime_load": "ROOT_MUST_VERIFY_NODE_IMAGE_STORE_AND_IMPORT",
                         "desired_env": {"OTEL_METRIC_EXPORT_INTERVAL": "2000", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": None},
                         "metrics_endpoint_requirement": "explicit http://host.docker.internal:<verified metrics OTLP gRPC port>; not yet selected",
                         "deployment_apply": "ROOT_FRESH_UID_RV_CAS_REQUIRED_NOT_EXECUTED",
                         "rollback": {"original_image": row["image"],
                                      "capture_before_apply": ["UID", "resourceVersion", "imagePullPolicy", "OTEL_METRIC_EXPORT_INTERVAL exact env presence/value", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT exact env presence/value"],
                                      "restore_only_owned_fields": True, "delete_old_images_or_data": False}})
        pending.append((context, context_files))
    destination.mkdir(parents=True, exist_ok=False)
    for context, files in pending:
        context.mkdir()
        for filename, body in files.items():
            with (context / filename).open("xb") as handle:
                handle.write(body)
    config_names = ("otel-metrics-config.yaml", "prometheus.yml", "docker-compose.metrics.yml")
    result = {"schema_version": "recshop-m1-sampling-plan-v1", "status": "PREPARED_NOT_BUILT_NOT_DEPLOYED",
              "services": services, "service_count": len(services),
              "configuration_sha256": {name: digest((asset_dir / name).read_bytes()) for name in config_names},
              "default_export_interval_ms": 15000, "m1_explicit_export_interval_ms": 2000,
              "base_tag_policy": "create alias only if absent or already the same image ID; never retag foreign content",
              "target_tag_policy": "new sampling-specific tags; refuse an existing tag with a different input identity",
              "source_snapshots_are_historical": True, "fresh_preflight_required": True,
              "legacy_observability_or_tsdb_modified": False, "trace_log_endpoints_modified": False,
              "builds_executed": 0, "deployment_patches_executed": 0}
    with (destination / "PLAN.json").open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return result
