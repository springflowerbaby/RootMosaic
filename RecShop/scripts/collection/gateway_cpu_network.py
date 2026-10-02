"""Prepare and collect the gateway-timeout/shared-node-CPU comparison scenarios.

This entry registers the additional versioned scenarios in its own process.
The common collection engine retains ownership, workload, timing and recovery checks.
"""
from __future__ import annotations

from dataclasses import replace
from copy import deepcopy
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
HERE = ROOT / "configs/collection"
sys.path.insert(0, str(ROOT))
from scripts.collection import scenario_definitions as asm
from scripts.collection import scenario_runner as cd
from scripts.collection import contract as c
from scripts.collection import runner as runner

VERSION = "rq4-d06-15ms-incremental-20260929-v1"
CATALOG = HERE / 'gateway_cpu_network_scenarios.json'
ARM_IDS = {"A": "S29", "B": "S30", "AB": "D34"}
CONDITION_IDS = {arm: "m1-d06-15ms-formal-v1-" + arm.lower() for arm in ARM_IDS}
_installed = False


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def install():
    """Register new definitions and narrowly scoped adapters in this process only."""
    global _installed
    if _installed:
        return
    registry = c.DesignRegistry.from_json(CATALOG.read_bytes())
    cd.require(registry.design_version == VERSION, "incremental catalog version differs")
    cd.require({s.key.scenario_id for s in registry.scenarios} == set(ARM_IDS.values()),
               "incremental catalog must contain exactly A/B/AB")
    old_parent = asm.COMBO_SPECS["D06"]
    # New registrations, never replacements of S05/S24/D06.
    for arm, old_sid, index in (("A", "S05", 0), ("B", "S24", 1)):
        old_atom = asm.ATOM_SPECS[old_sid]
        leg = old_parent.legs[index]
        if arm == "B":
            leg = replace(leg, parameters={**leg.parameters, "read_timeout_ms": 15})
        asm.ATOM_SPECS[ARM_IDS[arm]] = replace(old_atom, scenario_id=ARM_IDS[arm],
            phases=old_parent.phases, leg=leg,
            rule_version_base="d06-15ms-incremental-v1-" + arm.lower())
    for fid in ("F1", "F2"):
        asm.COMBO_LEG_STREAM_BINDINGS[("D34", fid)] = asm.COMBO_LEG_STREAM_BINDINGS[("D06", fid)]
    asm.COMBO_SPECS["D34"] = replace(old_parent, scenario_id="D34",
        rule_version_base="d06-15ms-incremental-v1",
        legs=(asm.ATOM_SPECS["S29"].leg, asm.ATOM_SPECS["S30"].leg),
        reference_dependencies={"F1": ("S29",), "F2": ("S30",)},
        timing=replace(old_parent.timing, decision="Additive 15 ms formal; preserve D06 phases, A then A+B, "
            "host dose, B timing and all settle/recovery budgets. This is a planned comparison, not measured amplification."),
        provenance={**old_parent.provenance, "catalog_scenario": VERSION + ":D34",
            "parameter_status": "NEW 15ms timeout only; host 32 workers/100 percent/275s unchanged",
            "request_profile_status": "Frozen from D06 complete main+bypass workload; common gateway route in all arms",
            "execution_status": "OFFLINE_PREPARED_NOT_EXECUTED"})

    original_registry = asm.load_registry
    def incremental_registry(catalog_json=None, *, repo_root=None):
        if catalog_json is not None:
            return original_registry(catalog_json, repo_root=repo_root)
        return c.DesignRegistry.from_json(CATALOG.read_bytes())
    asm.load_registry = incremental_registry

    original_sources = cd.source_hashes
    def sources():
        return {**original_sources(), 'gateway_cpu_network.entry': digest(__file__),
                'gateway_cpu_network.catalog': digest(CATALOG)}
    cd.source_hashes = sources
    original_prepared_sources = runner._prepared_sources
    def prepared_sources():
        return {**original_prepared_sources(), 'gateway_cpu_network.entry': digest(__file__),
                'gateway_cpu_network.catalog': digest(CATALOG)}
    runner._prepared_sources = prepared_sources

    original_scope = cd.pricing_aux_scope
    def common_route_scope(spec):
        result = original_scope(spec)
        if spec.scenario_id != "D34":
            return result
        cd.require(result is not None, "D34 lost gateway route declaration")
        return {**result, "incremental_design_version": VERSION,
            "consuming_legs": [{"fault_instance_id": "F1", "fault": "host_cpu_saturation@host",
                "q1_face": "carrier_p95_ratio", "statistical_source": "d06-main-pricing-via-gw@pricing:5014",
                "binding_reason": "common_workload_route_precondition_not_an_extra_fault"}, *result["consuming_legs"]]}
    cd.pricing_aux_scope = common_route_scope

    def route_consumers(contract):
        data = contract.to_dict()
        c.RunContract.from_json(contract.to_json(), incremental_registry())
        cd.require(data["scenario"]["design_version"] == VERSION and data["purpose"] == "formal" and data["protocol_status"] == "formal_frozen",
                   "formal incremental adapter requires this version's formal frozen contract")
        sid = data["scenario"]["scenario_id"]
        cd.require(sid in ARM_IDS.values(), "unknown incremental arm")
        expected = {"S29": {"host_cpu_saturation@host"},
                    "S30": {"timeout_misconfiguration@catalog-gw"},
                    "D34": {"host_cpu_saturation@host", "timeout_misconfiguration@catalog-gw"}}[sid]
        cd.require({f["atom_id"] for f in data["faults"]} == expected, "incremental arm truth mismatch")
        gateway = tuple(f["fault_instance_id"] for f in data["faults"] if f["normalized_root_entity"] == "catalog-gw")
        # A alone consumes the common request-route premise via its actual host leg.
        # B/AB retain the existing exact gateway-consumer binding.
        return gateway or tuple(f["fault_instance_id"] for f in data["faults"])

    original_registered_scope = runner._registered_pricing_scope
    def registered_scope(contract, plans):
        if contract.to_dict()["scenario"]["design_version"] != VERSION:
            return original_registered_scope(contract, plans)
        consumers = route_consumers(contract)
        runner._require(len(plans) == 1 and set(plans[0].fault_instance_ids) == set(consumers),
                        "incremental route must bind exactly actual consuming faults")
        namespace = contract.context.to_dict()["namespace"]
        prefix = "/api/v1/namespaces/" + namespace + "/services/pricing:5014/proxy/api/pricing/"
        runner._require(any(s["method"] == "GET" and s["endpoint"].startswith(prefix)
                            for s in contract.to_dict()["request_profile"]["streams"]),
                        "incremental route requires an explicit pricing request carrier")
    runner._registered_pricing_scope = registered_scope

    original_mint = cd.mint_pricing_aux
    def mint(contract, destination, attempt):
        if contract.to_dict()["scenario"]["design_version"] != VERSION:
            return original_mint(contract, destination, attempt)
        consumers = route_consumers(contract)
        if contract.to_dict()["scenario"]["scenario_id"] != "S29":
            return original_mint(contract, destination, attempt)
        coexisting, conflicts = cd.coexisting_restricted_faults(contract)
        cd.require(not conflicts and not coexisting, "incremental A route resource conflict")
        adapter = cd.pr.PricingRouteAdapter(cd.CONTEXT, cd.NAMESPACE, cd.CLUSTER_UID, cd.NAMESPACE_UID,
            process=cd.live.BoundedProcess(execute=True), evidence_kind="observed", kubectl=cd.portable_env.resolve_cli("kubectl"))
        original = adapter.read_baseline()
        desired = json.loads(json.dumps(original["fields"]))
        desired["route_env"] = cd.pr.model._route(cd.pr.model.GATEWAY_URL)
        plan = cd.aux.AuxiliaryPlan(cd.PRICING_AUX_ID, cd.pr.ADAPTER_KIND, consumers,
            {"kind": "Deployment", "name": "pricing", "scope": cd.NAMESPACE}, original, desired)
        receipt = cd.write_new(destination / ("AUX-PLAN-" + attempt + ".json"),
            {"schema_version": cd.PRICING_AUX_PLAN_SCHEMA, "contract_sha256": contract.sha256,
             "plan": plan.to_dict(), "baseline_readback": adapter.last_readback, "coexisting_legs": [],
             "incremental_route_premise": "common_workload_route_not_fault", "design_version": VERSION})
        return (plan,), {cd.PRICING_AUX_ID: adapter}, receipt
    cd.mint_pricing_aux = mint
    original_live_snapshot = cd.live_runtime_snapshot

    def live_snapshot(spec, scenario, attempt, destination, *, profile, action_budget, lateness_budget):
        if scenario != "D34" or spec.scenario_id != "D34":
            return original_live_snapshot(spec, scenario, attempt, destination, profile=profile,
                action_budget=action_budget, lateness_budget=lateness_budget)
        # Provision once. Waiting may only repeat read-only identity snapshots;
        # no build, route setup, worker, sampling, or fault runs before return.
        carriers = cd._stressor_provision_declaration(scenario, attempt, profile=profile,
            action_budget=action_budget, lateness_budget=lateness_budget)
        face = cd.provision_stressor_carriers(carriers, destination, attempt=attempt)
        return wait_for_quiescent_snapshot(spec, face, destination, attempt), face

    cd.live_runtime_snapshot = live_snapshot
    _install_repeat_binding()
    _installed = True


def wait_for_quiescent_snapshot(spec, face, destination, attempt, *, timeout_s=60., poll_s=1.,
                               clock=time.monotonic, sleep=time.sleep):
    """Wait before measurement; preserve every original snapshot predicate."""
    cd.require(timeout_s > 0 and poll_s > 0, "positive startup wait bounds required")
    started = clock()
    deadline = started + timeout_s
    observations = []
    original_reader = cd.reader

    def remaining():
        left = deadline - clock()
        cd.require(left > 0, "formal incremental startup quiescence deadline exhausted")
        return left

    class DeadlineReader:
        def __init__(self, inner):
            self.inner = inner
            self.reads = inner.reads

        def get(self, *args, **kwargs):
            # Each existing bounded GET is also capped by the remaining total
            # startup budget; process termination/drain keeps its own bounds.
            self.inner.timeout_s = min(self.inner.timeout_s, remaining())
            return self.inner.get(*args, **kwargs)

    def reader(enabled):
        inner = original_reader(enabled)
        return DeadlineReader(inner) if enabled else inner

    def verify_owned():
        kube = reader(True)
        for carrier in face["carriers"]:
            meta = kube.get("deployment", carrier["deployment"])["metadata"]
            cd.require(meta.get("uid") == carrier["uid"] and not meta.get("deletionTimestamp")
                and meta.get("annotations", {}).get(cd.p.OWNER_ANNOTATION) == carrier["owner_annotation"],
                "formal incremental startup carrier UID/owner changed")
        return kube.reads

    receipt = {"schema_version": "m1-formal-startup-quiescence-v1", "scenario_id": spec.scenario_id,
               "attempt_id": attempt, "timeout_s": timeout_s, "poll_s": poll_s,
               "carriers": face["carriers"], "observations": observations,
               "scope": "read_only_pre_measurement_snapshot_wait_not_fault_or_sampling"}
    # This process has not started its outer workers. Restore the shared
    # reader even on failure; the original snapshot implementation is intact.
    cd.reader = reader
    try:
        while True:
            remaining()
            owned_reads = verify_owned()
            try:
                runtime = cd.snapshot(spec, enabled=True)
            except RuntimeError as exc:
                if str(exc) != "runtime Pod set not quiescent":
                    raise
                observations.append({"elapsed_s": round(clock() - started, 6),
                    "status": "NOT_QUIESCENT", "reason": str(exc), "owned_reads": owned_reads})
                sleep(min(poll_s, remaining()))
                continue
            remaining()
            verified_reads = verify_owned()
            for index, leg in enumerate(spec.legs, 1):
                if leg.entity == "host":
                    carrier = next(row for row in face["carriers"] if row["deployment"] == leg.deployment)
                    cd.require(runtime["pins"]["F" + str(index)]["deployment_uid"] == carrier["uid"],
                               "formal incremental startup carrier snapshot UID changed")
            remaining()
            observations.append({"elapsed_s": round(clock() - started, 6), "status": "QUIESCENT",
                                 "owned_reads": owned_reads, "verified_reads": verified_reads})
            receipt.update(status="QUIESCENT", elapsed_s=round(clock() - started, 6))
            cd.write_new(destination / ("HOST-QUIESCENCE-" + attempt + ".json"), receipt)
            return runtime
    except Exception as exc:
        receipt.update(status="BLOCKED", elapsed_s=round(clock() - started, 6),
                       error=type(exc).__name__ + ": " + str(exc)[:300],
                       cleanup="carrier retained; root must prove clean before another attempt")
        cd.write_new(destination / ("HOST-QUIESCENCE-" + attempt + ".json"), receipt)
        raise
    finally:
        cd.reader = original_reader


def condition_rows():
    """Derive fresh formal seeds from D06 long300 geometry, not smoke outputs."""
    from scripts.collection import review_samples as dp

    spec = cd.entry_view(cd.entry_spec("D34"))
    runtime = cd.snapshot(spec, enabled=False)
    parent, _, _ = cd.build("D34", "formal-extension-seed", runtime,
        enabled=False, profile="long300", action_budget=17., lateness_budget=2.)
    parent_data = parent.contract.to_dict()
    rows = []
    for sequence, (arm, sid) in enumerate(ARM_IDS.items(), 1):
        data = deepcopy(parent_data)
        keep = {"A": {"F1"}, "B": {"F2"}, "AB": {"F1", "F2"}}[arm]
        kept = [fault for fault in data["faults"] if fault["fault_instance_id"] in keep]
        remap = {fault["fault_instance_id"]: "F" + str(index + 1) for index, fault in enumerate(kept)}
        for fault in kept:
            fault["fault_instance_id"] = remap[fault["fault_instance_id"]]
        data.update(faults=kept, purpose="formal", protocol_status="formal_frozen", qualification="not_assessed")
        # Placeholder is a real source reference, replaced with the sealed
        # campaign admission marker before any preview or execution.
        data["release_gate_ref"] = {"uri": str(CATALOG), "sha256": digest(CATALOG)}
        data["scenario"]["scenario_id"] = sid
        rules_data = deepcopy(parent.rules.to_dict())
        rules_data["injection"] = [{**rule, "instance_id": remap[rule["instance_id"]]}
            for rule in rules_data["injection"] if rule["instance_id"] in remap]
        rules = cd.q.QualityRules.from_dict(rules_data)
        context = data["context"]
        context["fingerprints"]["quality_rules"] = rules.sha256
        context["evidence_root"] = str(Path(context["output_root"]) / "formal" / context["run_id"] / context["attempt_id"])
        contract = c.RunContract.from_dict(data, asm.load_registry())
        science = dp.semantic_signature(contract.to_dict())
        template = {"contract_rules": data["rules"], "context_fingerprints": {
            key: context["fingerprints"][key] for key in ("quality_rules", "generator", "annotation_rules")},
            "repo_root": context["repo_root"], "output_root": context["output_root"],
            "quality_rules_sha256": rules.sha256}
        seed = {"condition_id": CONDITION_IDS[arm], "scenario_id": sid, "design_version": VERSION,
            "contrast_family": "D34-15ms-common-route-v1", "source_combo": "D34",
            "target_atoms": [fault["atom_id"] for fault in kept], "contract": contract.to_dict(),
            "quality_rules_payload": rules.to_dict(), "status": "FORMAL_TEMPLATE_NOT_EXECUTED",
            "profile": "long300", "arm": arm, "phase_lengths_s": [300, 300, 300],
            "route_premise": "pricing via catalog-gw throughout pre/during/post in every arm; catalog direct bypass retained"}
        rows.append({"sequence": sequence, "design_version": VERSION, "scenario_id": sid,
            "condition_id": CONDITION_IDS[arm], "source_combo": "D34", "seed": seed,
            "science": science, "condition_signature_sha256": c.canonical_sha256(science),
            "template": template, "runtime": {"action_budget": 17.0, "lateness_budget": 2.0,
                "first_injection_lateness_budget": None, "budget_mode": "default",
                "execution_family": "d06-15ms-common-route"}})
    return rows


def _install_repeat_binding():
    """Bind the existing repeat pipeline to this process's exact additive entry."""
    from scripts.collection import validate_campaign as admission
    from scripts.collection import run_campaign as batch

    admission._CATALOG_PATH = CATALOG
    extra_sources = {'gateway_cpu_network.entry': Path(__file__).resolve().relative_to(ROOT).as_posix(),
                     'gateway_cpu_network.catalog': CATALOG.relative_to(ROOT).as_posix()}
    admission._COLLECTOR_PATHS.update(extra_sources)
    admission._REQUIRED_SOURCE_PATHS.update(extra_sources.values())
    # The live child must install this same registry and source binding.
    batch.DB_ENV_WRAPPER = Path(__file__).resolve()
    expected_science = {row["scenario_id"]: row["science"] for row in condition_rows()}
    original_content = admission._campaign_content_errors

    def content_errors(campaign):
        errors = original_content(campaign)
        for row in campaign.get("scenarios", []):
            if type(row) is not dict:
                continue
            sid = row.get("scenario_id")
            if (row.get("design_version") != VERSION or sid not in expected_science
                    or row.get("condition") != expected_science.get(sid)
                    or row.get("source_combo") != "D34"):
                errors.append("formal_incremental_science_or_route_mismatch:" + str(sid))
        return errors

    admission._campaign_content_errors = content_errors
    original_load = batch.load_campaign

    def load_campaign(path, *, verify_sources=True):
        campaign, fingerprint, resolved = original_load(path, verify_sources=verify_sources)
        cd.require(all(row["design_version"] == VERSION and row["scenario_id"] in ARM_IDS.values()
                       for row in campaign["scenarios"]), "formal incremental entry rejects mixed or other design versions")
        return campaign, fingerprint, resolved

    batch.load_campaign = load_campaign


def prepare_main(argv):
    from scripts.collection import prepare_campaign as prep

    parser = argparse.ArgumentParser(description="Prepare isolated formal S29/S30/D34 campaigns without live calls")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--rounds", nargs="+", type=int, required=True)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--batch-root", type=Path, default=None)
    parser.add_argument("--environment", type=Path, required=True)
    args = parser.parse_args(argv)
    cd.portable_env.apply(cd, args.environment)
    target = args.output_dir.resolve()
    cd.require(target.is_relative_to(ROOT) and not target.exists(), "formal preparation output must be new and inside workspace")
    # The generated config lives next to, not inside, the prepare-owned output.
    conditions_path = target.with_name(target.name + "-conditions.json")
    prep.write_new(conditions_path, {"schema_version": "m1-repeat-conditions-v1", "conditions": condition_rows()})
    plan = prep.prepare(target, args.campaign_id, conditions_path, args.rounds,
        output_root=args.output_root, batch_root=args.batch_root)
    print(json.dumps({"status": "OFFLINE_VALIDATED_NOT_EXECUTED", "campaign": str(plan),
                      "rounds": args.rounds, "new_slots": 3 * len(args.rounds)}, indent=2))
    return 0


def driver_main(argv):
    """Use the established protected DB environment only in a live child."""
    from scripts.collection import review_samples as dp

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("step", choices=("run", "recover", "preview"))
    parser.add_argument("--condition-file", type=Path, required=True)
    parser.add_argument("--condition-id", required=True)
    parser.add_argument("--live", action="store_true")
    args, _ = parser.parse_known_args(argv)
    rows = json.loads(args.condition_file.read_bytes())
    matching = [row for row in rows if row.get("condition_id") == args.condition_id]
    cd.require(len(matching) == 1, "formal incremental child requires one exact condition")
    data = matching[0]["contract"]
    sid = data["scenario"]["scenario_id"]
    expected = {row["scenario_id"]: row["science"] for row in condition_rows()}
    cd.require(data.get("purpose") == "formal" and data.get("protocol_status") == "formal_frozen"
               and sid in expected and dp.semantic_signature(data) == expected[sid],
               "formal incremental child refuses nonformal, changed science, or route")
    if args.step != "preview":
        cd.portable_env.require_windows_execution()
        cd.portable_env.resolve_cli("kubectl")
        if args.step == "run":
            cd.portable_env.resolve_cli("docker")
        cd.require(args.live, "live child requires explicit --live")
        from dotenv import dotenv_values
        values = cd.portable_env.credentials()
        keys = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
        cd.require(all(values.get(key) for key in keys), "database environment missing for live child")
        os.environ.update({key: str(values[key]) for key in keys})
    sys.argv = [str(Path(__file__).resolve()), *argv]
    cd.main()
    return 0


def _print_command_help(argv):
    """Describe commands without loading an environment, a registry or credentials."""
    command = argv[0] if argv else None
    if command == "prepare":
        return prepare_main(argv[1:])
    if command == "batch":
        from . import run_campaign
        return run_campaign.main(argv[1:])
    if command in ("run", "recover", "preview"):
        parser = argparse.ArgumentParser(
            prog="python -m scripts.collection.gateway_cpu_network " + command,
            description="Use one exact prepared gateway/CPU condition. Help performs no environment or credential reads.")
        parser.add_argument("--condition-file", type=Path, required=True,
                            help="absolute path to the prepared condition manifest")
        parser.add_argument("--condition-id", required=True, help="exact condition ID in that manifest")
        parser.add_argument("--attempt", default="preview-1")
        parser.add_argument("--profile", choices=("short", "long300"), default="short",
                            help="driver option; a supplied formal condition defines its own observation windows")
        parser.add_argument("--action-budget", type=float, default=17)
        parser.add_argument("--lateness-budget", type=float, default=2)
        parser.add_argument("--first-injection-lateness-budget", type=float, default=None)
        parser.add_argument("--environment", type=Path,
                            help="explicit environment configuration, or RECSHOP_COLLECTION_ENV")
        parser.add_argument("--live", action="store_true", help="required for run/recover; preview remains offline")
    else:
        parser = argparse.ArgumentParser(
            prog="python -m scripts.collection.gateway_cpu_network", description=__doc__,
            epilog="Use COMMAND --help for its options. Help does not load an environment or start collection.")
        commands = parser.add_subparsers(dest="command", title="commands")
        for name, description in (
            ("prepare", "build source-bound campaign plans and offline previews"),
            ("batch", "inspect, review or explicitly execute a prepared campaign"),
            ("run", "explicitly execute one prepared condition"),
            ("recover", "explicitly recover the exact recorded attempt"),
            ("preview", "validate one prepared condition without live execution"),
        ):
            commands.add_parser(name, add_help=False, help=description)
    parser.print_help()
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or any(arg in ("--help", "-h") for arg in argv):
        return _print_command_help(argv)
    if "--environment" in argv:
        cd.portable_env.apply(cd, argv[argv.index("--environment") + 1])
    elif os.environ.get(cd.portable_env.ENV_KEY):
        cd.portable_env.apply(cd)
    if argv[0] == "batch":
        plan_index = argv.index("--plan") + 1
        campaign_doc = json.loads(Path(argv[plan_index]).read_bytes())
        cd.portable_env.verify_ref(campaign_doc["runtime"]["environment"])
        cd.portable_env.apply(cd)
    install()
    if argv[0] == "prepare":
        return prepare_main(argv[1:])
    if argv[0] == "batch":
        from scripts.collection import run_campaign as batch
        return batch.main(argv[1:])
    return driver_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
