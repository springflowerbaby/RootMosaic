<h1 align="center">RecShop</h1>

<p align="center">RecShop is an e-commerce recommendation platform for research on microservice fault diagnosis.</p>

<p align="center">
  <a href="LICENSE.md"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-E11D48.svg?style=for-the-badge&amp;labelColor=334155"></a>
  <a href="ENVIRONMENT.md"><img alt="Python 3.10" src="https://img.shields.io/badge/Python-3.10-B45309.svg?style=for-the-badge&amp;labelColor=334155"></a>
  <a href="docs/DEPLOYMENT.md"><img alt="Deployment: Kubernetes" src="https://img.shields.io/badge/Deployment-Kubernetes-047857.svg?style=for-the-badge&amp;labelColor=334155"></a>
  <a href="docs/COLLECTION-ENVIRONMENT.md"><img alt="Collection host: Windows" src="https://img.shields.io/badge/Collection%20host-Windows-7C3AED.svg?style=for-the-badge&amp;labelColor=334155"></a>
</p>

<p align="center"><strong>English</strong> · <a href="README_CN.md">简体中文</a></p>

<p align="center"><img src="assets/readme/rainbow-divider.svg" width="1000" height="6" alt=""></p>

The platform contains 25 application services: 24 Flask services and 1 FastAPI SASRec inference service. Most business services share MySQL. Recommendation workflows, model serving, and LLM reranking are business features, not fault-collection controllers. Internal module, image, and service identifiers retain compatibility names.

## Architecture

### System Overview

<img src="assets/figures/recshop-system-overview.png" alt="RecShop system overview" width="1000">

[View the full-resolution system overview (3000 × 1639)](assets/figures/recshop-system-overview.png)

### Service Dependencies

<img src="assets/figures/recshop-service-dependencies.png" alt="RecShop service dependency graph" width="1000">

[View the full-resolution dependency diagram (4200 × 1456)](assets/figures/recshop-service-dependencies.png)

### Service Groups

The 25 application services are grouped below. See the [full service guide](services/README.md) for ports and responsibilities.

| Group | Count | Service directories |
|---|---:|---|
| Web | 1 | [shop_web](services/shop_web/) |
| Core commerce | 5 | [checkout_service](services/checkout_service/), [cart_service](services/cart_service/), [pricing_service](services/pricing_service/), [inventory_service](services/inventory_service/), [catalog_service](services/catalog_service/) |
| Product and reviews | 3 | [search_service](services/search_service/), [review_service](services/review_service/), [review_query_service](services/review_query_service/) |
| Orders and fulfillment | 4 | [order_service](services/order_service/), [payment_service](services/payment_service/), [promotion_service](services/promotion_service/), [shipping_service](services/shipping_service/) |
| Users and operations | 8 | [user_service](services/user_service/), [address_service](services/address_service/), [ai_memory_service](services/ai_memory_service/), [announcement_service](services/announcement_service/), [merchant_service](services/merchant_service/), [interaction_service](services/interaction_service/), [notification_service](services/notification_service/), [admin_audit_service](services/admin_audit_service/) |
| Recommendation and AI | 4 | [backend_api](services/backend_api/), [sasrec_api](services/sasrec_api/), [llm_rerank_service](services/llm_rerank_service/), [recommendation_agent](services/recommendation_agent/) |

## Screenshots

| Home page | Product details |
|---|---|
| [![Home page](assets/screenshots/recshop-home.jpg)](assets/screenshots/recshop-home.jpg) | [![Product details](assets/screenshots/recshop-product.jpg)](assets/screenshots/recshop-product.jpg) |

## Quickstart

The three main root entry points cover distinct tasks. Other startup, checking, and collection entry points are in `scripts/entrypoints/`.

| Task | Entry point | Purpose |
|---|---|---|
| Install dependencies | [install_dependencies.ps1](install_dependencies.ps1) | Install the unified Python dependency set. |
| Deploy a business instance | [deploy_recshop.ps1](deploy_recshop.ps1) | Deploy an isolated RecShop business instance. |
| Check or restore the collection environment | [start_collection_environment.ps1](start_collection_environment.ps1) | Check or restore an already prepared collection environment; this is neither first-time deployment nor automatic collection. |

### Install Dependencies and Prepare Assets

Use Python 3.10. The single root `requirements.txt` covers Python dependencies for business services, deployment, collection, and migration. Docker, Kubernetes, application images, databases, and models require separate preparation.

```powershell
.\install_dependencies.ps1 -Python python
```

This dependency set has been installed in a fresh Python 3.10 Conda environment on Windows and checked for dependency consistency, key module imports, and offline tools. Actual model loading and business-service startup still require separate validation. See [Environment](ENVIRONMENT.md) for versions.

You can also install the same complete dependency set with `python -m pip install -r requirements.txt`. The installer's `-CheckOnly -PythonOnly` checks only the three required collection-host packages and Python 3.10; it does not establish that all business dependencies or the business environment are ready. Select the local PyTorch installation source for CPU or CUDA as appropriate. Both the standard service base image and the SASRec image install CPU version 2.5.1 before processing the same requirements file, so the unified list increases the size of standard service images.

Copy `.env.example` to a local `.env` and supply your database settings, session key, and optional LLM configuration. Model weights, the RecBole cache, and item files are not included with the code. See [Assets and Environment](ENVIRONMENT.md) for sources, compatible file sets, and known checksums. Business APIs assume a trusted internal network; do not expose administrative and data endpoints publicly without isolation.

### Deploy the Business System

With Kubernetes, images, and model assets available, follow the [isolated deployment instructions](docs/DEPLOYMENT.md) to create a new instance:

```powershell
Copy-Item deployment.example.json deployment.local.json
# First configure the isolated context, namespace, images, and assets.
.\deploy_recshop.ps1 -Python python -Config deployment.local.json -Render -Out deployment-plan.json
# Supply secret environment variables as documented, then explicitly deploy and check.
.\deploy_recshop.ps1 -Python python -Config deployment.local.json
python scripts/entrypoints/check_recshop.py --config deployment.local.json
```

Use `python scripts/entrypoints/start_recshop.py --config deployment.local.json` to restore the same registered instance later. The machine-readable status `BUSINESS_READY` means that business checks passed; it does not mean that the fault-collection environment is ready. First-time deployment creates isolated MySQL/PVC resources and minimal synthetic item data. It does not take over an existing research database, download models, or call a paid LLM.

For local development, prepare MySQL, models, and configuration, then run `python scripts/entrypoints/start_local_services.py`. This is a separate path from the Kubernetes entry points. See [services/README.md](services/README.md) for services, ports, and business-function boundaries.

## Data Collection

Actual collection and fault recovery require a Windows control host; business containers and Kubernetes worker nodes run Linux. First follow [Collection Environment Preparation](docs/COLLECTION-ENVIRONMENT.md) to configure the gateway, Chaos Mesh, metrics/tracing/logging infrastructure, CRI resource exporter, database, and environment identities, then prepare a new plan. The environment restoration script neither replaces first-time deployment nor starts collection automatically.

For first use, create a private `*.local.json` from the environment example and fill in actual identities for your environment; store secrets separately. Existing environments retain their registered lease and worker records. After moving the code directory, you can explicitly set `windows_startup.bind_sources` for existing Docker containers without remounting old data. Use `start_collection_environment.ps1 -Environment <private-config>` for restoration and read-only checks. The instructions above distinguish first-time preparation from operation of an existing environment.

```powershell
.\start_collection_environment.ps1 -Environment configs/collection/environment.local.json -CheckOnly
# After first-time preparation, when configuration permits restoration:
.\start_collection_environment.ps1 -Environment configs/collection/environment.local.json
```

Formal collection is explicitly started through `scripts/entrypoints/collect_dataset.ps1`. See [Collection](docs/COLLECTION.md) for the workflow, versioned configurations for 69 scenarios, serial execution, and end-of-batch decisions. Plans must bind the actual source code and scenario conditions in the new checkout to the new environment. Do not reuse another machine's UIDs, database checksums, or another environment's readiness records. Data records distinguish individual runs from selected slots; retries do not automatically increase the repetition count.

## Dataset Workflow

For accepted formal samples, follow the [migration tool instructions](scripts/dataset/README.md) to freeze inputs, export existing historical observations, convert them, and read the results back. Use separate `runs/` and `outputs/` directories without overwriting raw data or existing deliveries.

V1 data packages contain sample identities, GT mappings, observation files, and provenance records. See [Data Format](docs/DATA-FORMAT.md) for field definitions, modality availability, and the scope of supported analyses. Experimental data packages are separate inputs and are not bundled with the source code.

## Documentation

| Topic | Guide |
|---|---|
| Dependencies, model assets, and checksums | [Environment](ENVIRONMENT.md) |
| Isolated business deployment and validation | [Deployment](docs/DEPLOYMENT.md) |
| Service ports and business-function boundaries | [Service guide](services/README.md) |
| Collection infrastructure and environment binding | [Collection environment](docs/COLLECTION-ENVIRONMENT.md) |
| Versioned scenarios, collection, and batch decisions | [Collection workflow](docs/COLLECTION.md) |
| Dataset conversion and reading | [Migration tools](scripts/dataset/README.md) |
| Dataset fields and analysis scope | [Data format](docs/DATA-FORMAT.md) |

## Scope and Attribution

- Checkout is a read-only serial preview; payment is a simulated record workflow, not a production-grade distributed transaction system.
- Pricing connects directly to catalog by default. Experiments that use the gateway route through catalog-gw before measurement, keep that route throughout the three observation phases, and restore it afterward.
- Only the corresponding business features call external LLMs; ordinary business operations and SASRec inference do not require external models.
- See [Deployment](docs/DEPLOYMENT.md#验证范围) and [Collection Environment Preparation](docs/COLLECTION-ENVIRONMENT.md) for prerequisites, validation scope, and target-environment checks.
- See [LICENSE.md](LICENSE.md) for the code license. Check redistribution terms separately for models, data, and code.
