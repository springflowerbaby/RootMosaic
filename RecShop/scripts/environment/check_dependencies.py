"""Read-only host dependency checks. No collection, installation or secret output."""
from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.collection import environment as collection_environment
MODULES = {
    "python-dotenv": "dotenv",
    "mysql-connector-python": "mysql.connector",
    "pyyaml": "yaml",
}


def read_requirements(path: Path) -> dict[str, str]:
    """Read the unified simple manifest and return only the three host pins.

    The project list consists of exact pins plus platform-selected bare torch.
    Pip directives, URLs, extras, markers and duplicate normalized names are
    refused rather than interpreted by this read-only prerequisite checker.
    """
    packages = {}
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        match = re.fullmatch(r"([A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?)(?:==([0-9][A-Za-z0-9_.+-]*))?", line)
        if not match:
            raise ValueError("invalid_project_requirement")
        name, version = match.groups()
        name = re.sub(r"[-_.]+", "-", name).lower()
        if name in packages:
            raise ValueError("duplicate_project_requirement")
        if version is None and name != "torch":
            raise ValueError("exact_pin_required")
        packages[name] = version
    if not set(MODULES).issubset(packages):
        raise ValueError("incomplete_m1_host_requirements")
    return {name: packages[name] for name in MODULES}


def inspect_python(requirements_path: Path | None = None) -> dict:
    pins = read_requirements(requirements_path or ROOT / "requirements.txt")
    packages = []
    for name, expected in pins.items():
        try:
            actual = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            actual = None
        import_ok = False
        if actual is not None:
            try:
                importlib.import_module(MODULES[name])
                import_ok = True
            except Exception:
                # Exception text can include local connection/configuration details.
                pass
        packages.append({"distribution": name, "required": expected, "installed": actual,
                         "import_ok": import_ok, "ok": actual == expected and import_ok})
    version_ok = sys.version_info[:2] == (3, 10)
    return {"ready": version_ok and all(row["ok"] for row in packages),
            "python": ".".join(map(str, sys.version_info[:3])),
            "python_3_10": version_ok, "packages": packages,
            "scope": "m1_host_subset_only", "checked_package_count": len(packages),
            "business_readiness_checked": False}


def command(args: list[str], *, timeout: int = 12) -> tuple[bool, str]:
    """Only callers' fixed read-only commands; stdout remains internal until parsed."""
    try:
        result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8",
                                errors="replace", timeout=timeout, check=False)
        return result.returncode == 0, result.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return False, ""


def find_tool(name: str) -> str | None:
    try:
        return collection_environment.resolve_cli(name)
    except ValueError:
        return None


def inspect_database_config(path: Path) -> dict:
    keys = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
    if not path.is_file():
        return {"configured": False, "tcp_reachable": False, "reason": "credential_file_missing"}
    try:
        from dotenv import dotenv_values
        values = dotenv_values(path)
        missing = [key for key in keys if not values.get(key)]
        if missing:
            return {"configured": False, "tcp_reachable": False, "missing_keys": missing}
        port = int(values["DB_PORT"])
        if not 1 <= port <= 65535:
            raise ValueError("invalid_port")
    except (ImportError, ValueError, OSError):
        return {"configured": False, "tcp_reachable": False,
                "reason": "credential_reader_or_configuration_unavailable"}
    try:
        with socket.create_connection((str(values["DB_HOST"]), port), timeout=3):
            pass
        reachable = True
    except OSError:
        reachable = False
    return {"configured": True, "tcp_reachable": reachable,
            "authenticated_or_checksum_verified": False}


def inspect_external(context: str, namespace: str, db_env_file: Path) -> dict:
    docker, kubectl = find_tool("docker"), find_tool("kubectl")
    engine = command([docker, "version", "--format", "{{.Server.Version}}"])[0] if docker else False
    compose = command([docker, "compose", "version", "--short"])[0] if docker else False
    current_ok, current = command([kubectl, "config", "current-context"]) if kubectl else (False, "")
    matched = current_ok and current == context
    ns_exists = False
    chaos = False
    volumes = {"sasrec-data": False, "recagent-data": False}
    if matched:
        prefix = [kubectl, "--context", context, "--request-timeout=8s"]
        ns_exists = command(prefix + ["get", "namespace", namespace, "-o", "name"])[0]
        if ns_exists:
            chaos = command(prefix + ["get", "crd", "networkchaos.chaos-mesh.org",
                                      "podchaos.chaos-mesh.org", "stresschaos.chaos-mesh.org", "-o", "name"])[0]
            for name in volumes:
                ok, phase = command(prefix + ["-n", namespace, "get", "pvc", name,
                                             "-o", "jsonpath={.status.phase}"])
                volumes[name] = ok and phase == "Bound"
    db = inspect_database_config(db_env_file)
    desktop_path = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Docker/Docker/Docker Desktop.exe"
    ready = all([bool(docker), bool(kubectl), engine, compose, matched, ns_exists,
                 chaos, all(volumes.values()), db["tcp_reachable"]])
    return {
        "prerequisites_observed": ready,
        "docker_cli": bool(docker), "docker_desktop_installation_found": desktop_path.is_file(),
        "docker_engine_reachable": engine, "docker_compose": compose,
        "kubectl": bool(kubectl), "expected_kubernetes_context_active": matched,
        "collection_namespace_exists": ns_exists, "chaos_mesh_crds_present": chaos,
        "model_data_pvc_bound": volumes, "database_configuration": db,
        "collection_ready": False,
        "scope": "Prerequisites only: PVC Bound does not verify model contents. start_collection_environment checks current services; model loading and collection-specific safety remain subject to service readiness and each attempt's preflight.",
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requirements", type=Path, default=ROOT / "requirements.txt")
    parser.add_argument("--python-only", action="store_true")
    parser.add_argument("--context", default=None)
    parser.add_argument("--namespace", default=None)
    parser.add_argument("--db-env-file", type=Path, default=ROOT / ".env.collection")
    args = parser.parse_args(argv)
    try:
        python = inspect_python(args.requirements)
        if not args.python_only and (not args.context or not args.namespace):
            raise ValueError("explicit context and namespace required")
        external = None if args.python_only else inspect_external(args.context, args.namespace, args.db_env_file)
        result = {"schema": "recshop-m1-dependencies-v1", "read_only": True,
                  "python_dependencies": python, "external_prerequisites": external}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if not python["ready"]:
            return 1
        return 2 if external and not external["prerequisites_observed"] else 0
    except Exception:
        print(json.dumps({"schema": "recshop-m1-dependencies-v1", "read_only": True,
                          "error": "dependency_check_failed_no_secrets_emitted"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
