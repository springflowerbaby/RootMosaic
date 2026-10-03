# Script Guide

This directory contains active launchers, collection modules, dataset tools, and optional database preparation utilities. Run the commands documented here from the repository root unless a linked guide says otherwise. The three primary PowerShell entry points remain at the root; their supporting code and alternative launchers live here.

## Current Entry Points

| Task | Entry point | Behavior and prerequisites |
|---|---|---|
| Install or check dependencies | [install_dependencies.ps1](../install_dependencies.ps1) → [environment/check_dependencies.py](environment/check_dependencies.py) | Installs the root requirements or performs the requested checks. Python-only checks do not certify the complete business or collection environment. See [Environment](../ENVIRONMENT.md). |
| Deploy an isolated business instance | [deploy_recshop.ps1](../deploy_recshop.ps1) → [entrypoints/deploy_recshop.py](entrypoints/deploy_recshop.py) → [deployment/recshop.py](deployment/recshop.py) | Uses an explicit deployment configuration. `-Render` produces an offline plan; actual deployment can create resources and initialize a new dedicated database. See [Deployment](../docs/DEPLOYMENT.md). |
| Check or start an existing business instance | [entrypoints/check_recshop.py](entrypoints/check_recshop.py), [entrypoints/start_recshop.py](entrypoints/start_recshop.py) | Select the `check` or `start` action in the same deployment module. Matching `.ps1` and `.cmd` files are active convenience wrappers, not separate deployment implementations. |
| Start the local development stack | [entrypoints/start_local_services.py](entrypoints/start_local_services.py) | Local Python services and optional Docker observability services; separate from Kubernetes deployment and fault collection. Requires the database, models, and configuration. `--check-only` checks configured endpoints without starting services. |
| Check or restore a prepared collection environment | [start_collection_environment.ps1](../start_collection_environment.ps1) → [environment/start_collection_environment.py](environment/start_collection_environment.py) | `-CheckOnly` performs readiness checks; normal mode can restore supported existing components. This is not first-time deployment, database initialization, or automatic collection. Follow [Collection Environment Preparation](../docs/COLLECTION-ENVIRONMENT.md). |
| Validate a local collection configuration | [collection/environment.py](collection/environment.py) | `python -B -m scripts.collection.environment --environment <config>` validates the explicit binding without certifying live readiness. Credentials are stored separately. |
| Prepare the base campaign | [collection/prepare_campaign.py](collection/prepare_campaign.py) | Builds fresh source-bound contracts, admission artifacts, and offline previews for the base definitions. Writes a new preparation directory; does not inject faults or read database credentials. |
| Prepare the additional scenario family | [collection/gateway_cpu_network.py](collection/gateway_cpu_network.py), `prepare` subcommand | Prepares the separately versioned S29/S30/D34 family. It is an active extension, not a replacement for same-number historical scenarios or a generic network launcher. |
| Inspect or execute a prepared campaign | [entrypoints/collect_dataset.ps1](entrypoints/collect_dataset.ps1) | Requires `-Plan`; dispatches by design version to [collection/run_campaign.py](collection/run_campaign.py) or the incremental module's `batch` action. `-Status` reads status; `-Execute` explicitly starts collection. Decision-file and resume handling retain the existing safety checks. |
| Validate a prepared campaign | [collection/validate_campaign.py](collection/validate_campaign.py) | Checks campaign/admission bindings. Validation is not deployment, collection, or empirical certification of a sample. |
| Run or recover an individual attempt | [collection/run_scenario.py](collection/run_scenario.py) → [collection/scenario_runner.py](collection/scenario_runner.py); incremental `run`/`recover` via [gateway_cpu_network.py](collection/gateway_cpu_network.py) | Low-level execution entries used by campaign orchestration. Recovery uses the same attempt and retained evidence; it is not a clean-state reset. Use the appropriate design version, condition, environment, and documented execution flags. |

The `.cmd` wrappers for dependency installation and collection-environment startup forward to the corresponding root PowerShell scripts. The other launchers in `entrypoints/` remain usable after their move from the root.

Actual fault collection and recovery require a Windows control host. Help, offline preparation, status reading, and dataset conversion do not imply this same runtime requirement. Business readiness does not establish collection readiness. Complete the environment checks and prepare a fresh plan before executing; do not edit old fingerprints or remove lease files to bypass a failure. The full command sequence and versioned scenario contract are in [Collection](../docs/COLLECTION.md).

## Dataset Preparation, Export, Conversion, and Validation

These tools consume accepted completed attempts; they do not create new fault runs or choose which experiments to accept.

| Tool | Role |
|---|---|
| [dataset/prepare_export.py](dataset/prepare_export.py) | Freeze accepted ledgers, explicit source locations, original hashes, and observation windows into a new local plan. |
| [dataset/check_metrics_retention.py](dataset/check_metrics_retention.py) | Optional retention inspection and shared input/query helpers. Monitoring reads require `--execute-readonly`; its CLI is not required for every conversion, but its imported functions are required. |
| [dataset/export_metrics.py](dataset/export_metrics.py) | Export historical Prometheus observations for frozen windows. Without `--execute-readonly`, writes only a local export plan. It does not create port forwarding. |
| [dataset/build_dataset.py](dataset/build_dataset.py) | Convert native evidence and cached observations into an independent package, preserving source/QC information and applying the supported versioned design labels. |
| [dataset/validate_dataset.py](dataset/validate_dataset.py) | Validate/read a package; `--hashes` checks recorded file hashes. It does not rerun experiments or validate RCA effectiveness. |

See [dataset/README.md](dataset/README.md), [Migration](../docs/DATA-MIGRATION.md), [Data Format](../docs/DATA-FORMAT.md), and [FIELD-MAPPING.md](dataset/FIELD-MAPPING.md) for inputs, commands, and output semantics. Dataset CLI file arguments resolve from the current working directory; original evidence paths use the explicit source-root/mapping contract.

Keep [source_paths.py](dataset/source_paths.py), [source_integrity.py](dataset/source_integrity.py), [metric_views.py](dataset/metric_views.py), [log_views.py](dataset/log_views.py), [trace_formats.py](dataset/trace_formats.py), and [design_labels.py](dataset/design_labels.py) with the entry scripts. They are imported runtime dependencies. Design labels use [configs/dataset/design_labels.v1.json](../configs/dataset/design_labels.v1.json); they describe fixed-design intent, not observed interaction certification. Existing deliveries are not rewritten merely by updating these modules.

**Current example limitation:** [examples/migration/synthetic_demo.py](../examples/migration/synthetic_demo.py) creates the invented `DEMO01` scenario. That identity is not in the current 69-scenario design-label mapping, so the example cannot currently complete conversion through the updated builder unchanged. It is not a verified end-to-end example for this mapping. Do not relabel an unrelated example as a supported scientific scenario just to bypass the check.

## Collection Runtime Dependencies

These modules remain part of the active collector even when they are not intended as user-facing commands. Keep them together and preserve their source bindings.

| Module group | Responsibility |
|---|---|
| [scenario_definitions.py](collection/scenario_definitions.py), [contract.py](collection/contract.py), [review_samples.py](collection/review_samples.py) | Versioned scenario definitions, typed contracts, and shared semantic/review helpers. `review_samples.py` is a library used by preparation and execution, not an automatic acceptance command. |
| [environment.py](collection/environment.py), [campaign_runtime.py](collection/campaign_runtime.py) | Explicit environment binding, executable discovery, shared preflight/completion checks, and database/lease evidence checks. |
| [runner.py](collection/runner.py), [journal.py](collection/journal.py), [workload.py](collection/workload.py) | Attempt orchestration, durable operation/recovery records, and controlled request workloads. |
| [primitives.py](collection/primitives.py), [primitives_db.py](collection/primitives_db.py), [gateway.py](collection/gateway.py) | Kubernetes, database-lock, and gateway-configuration fault mechanisms and their guarded recovery paths. |
| [auxiliary.py](collection/auxiliary.py), [pricing_route.py](collection/pricing_route.py), [pricing_route_model.py](collection/pricing_route_model.py) | Owned auxiliary preparation and pricing-route state, identity, and restoration checks. |
| [live_runtime.py](collection/live_runtime.py), [telemetry.py](collection/telemetry.py), [sampling.py](collection/sampling.py), [observability.py](collection/observability.py) | Bounded runtime reads, telemetry collection, sampling evidence, and phase observations. |
| [log_archive.py](collection/log_archive.py), [log_transport.py](collection/log_transport.py), [log_driver_kubectl.py](collection/log_driver_kubectl.py) | Log archives, transport/session ownership, and Kubernetes log/watch drivers. |
| [quality.py](collection/quality.py), [annotations.py](collection/annotations.py), [release_gate.py](collection/release_gate.py), [validate_campaign.py](collection/validate_campaign.py) | Quality results, annotation structures, and release/admission checks. These checks serve different scopes; a low-level release-gate CLI is not a substitute for the current campaign entry. |

[environment/recover_docker.py](environment/recover_docker.py) is another active internal dependency: the environment launcher imports its Docker startup recovery helper when needed. It is not an independent collection controller.

## Optional Database Preparation and Import

These are explicitly invoked, state-changing utilities for a new dedicated business database. They are not readiness checks, dataset conversion tools, or part of normal fault-collection startup.

| File | Intended use |
|---|---|
| [database_schema.sql](database_schema.sql) | Database DDL, also consumed by the isolated deployment renderer. Its `model_metrics` seed values are illustrative, not measured research results. |
| [build_database.sql](build_database.sql) | Thin MySQL `SOURCE scripts/database_schema.sql` wrapper, intended to be invoked from the repository root. It is not a competing or deprecated schema. |
| [import_data.py](import_data.py) | Import explicitly supplied item/interaction files into a selected database; optional metadata and limits are CLI inputs. Reads the password from the named environment variable. `--sample-only` creates synthetic demo users; this is a write operation. |
| [seed_demo_data.sql](seed_demo_data.sql) | Optional synthetic inventory/promotion rows after schema preparation and the deployment demo item. Not research data; do not run it during collection. |

Consult [Environment](../ENVIRONMENT.md) and [Deployment](../docs/DEPLOYMENT.md) before preparing a database. Existing research databases must not be reinitialized or repopulated as a side effect of startup.

## Historical Names and Deprecation

No current script in this directory is designated deprecated solely because it lacks a direct CLI invocation. The entry-point moves are layout changes, not retirement of their functions. Optional utilities and internal libraries have different usage patterns from primary launchers.

Historical schema/design identifiers, source provenance, and legacy process names used by concurrent-collector detection remain for compatibility. Older admission layers in `release_gate.py` likewise do not make the module unused: current orchestration still imports it. For supported execution, follow the root entry points and current collection/dataset guides rather than treating a historical comment or lower-level CLI as a separate workflow.
