# RecShop Application Services

This directory contains 25 application services: 24 Flask services and 1 FastAPI SASRec inference service. Default local ports are defined in the [launcher](../scripts/entrypoints/start_local_services.py); environment variables or deployment configuration can override listening addresses and downstream endpoints.

| Directory | Local port | Responsibility |
|---|---:|---|
| [shop_web](shop_web/README.md) | 3000 | Buyer, merchant, and administrator pages; user authentication |
| [backend_api](backend_api/README.md) | 5000 | User/item/interaction APIs, SASRec candidate requests, and recommendation records |
| [recommendation_agent](recommendation_agent/README.md) | 5001 | Independent multi-agent recommendation API and demo page |
| [llm_rerank_service](llm_rerank_service/README.md) | 5002 | LLM-based item selection from candidates and user history |
| [review_service](review_service/) | 5003 | Review submission, moderation, replies, and other writes |
| [user_service](user_service/) | 5004 | User and account profiles |
| [catalog_service](catalog_service/) | 5005 | Item queries |
| [cart_service](cart_service/) | 5006 | Shopping carts |
| [address_service](address_service/) | 5007 | Shipping addresses |
| [ai_memory_service](ai_memory_service/) | 5008 | Memory storage for assistant conversations |
| [announcement_service](announcement_service/) | 5009 | Announcements |
| [order_service](order_service/) | 5010 | Order list and detail queries |
| [checkout_service](checkout_service/) | 5011 | Read-only checkout previews |
| [payment_service](payment_service/) | 5012 | Simulated payment records |
| [inventory_service](inventory_service/) | 5013 | Inventory queries, reservations, and releases |
| [pricing_service](pricing_service/) | 5014 | Pricing and quotations |
| [promotion_service](promotion_service/) | 5015 | Discounts and promotions |
| [shipping_service](shipping_service/) | 5016 | Shipping records |
| [search_service](search_service/) | 5017 | Item search |
| [review_query_service](review_query_service/) | 5018 | Review reads |
| [merchant_service](merchant_service/) | 5019 | Merchant operations |
| [interaction_service](interaction_service/) | 5020 | User interaction records |
| [notification_service](notification_service/) | 5021 | Notification records |
| [admin_audit_service](admin_audit_service/) | 5022 | Administrative audit records |
| [sasrec_api](sasrec_api/README.md) | 8200 | FastAPI + PyTorch sequential recommendation inference |

## Calls and Shared Dependencies

- Most business services connect to shared MySQL. `shop_web` uses SQLAlchemy ORM; most domain services use MySQL connection pools. `sasrec_api`, `llm_rerank_service`, and `recommendation_agent` do not connect directly to MySQL.
- `shop_web` makes HTTP calls and reads/writes the database directly; it is not merely a thin proxy without business state.
- AI Picks first follows `shop_web → backend_api → sasrec_api`, after which `shop_web` calls `llm_rerank_service`. The independent recommendation-agent workflow is separate from this default call chain.
- By default, pricing accesses catalog directly. Experiments route this service path through `catalog-gw` only when required. Gateway configuration is in the [Kubernetes directory](../k8s/README.md); the gateway is not a 26th application service in this directory.
- Nacos is optional. Its default local behavior when unconfigured differs from the fixed routing used in collection experiments. See the [root README](../README.md) for configuration.

## Running the Services

Start the complete local stack from the repository root with `python scripts/entrypoints/start_local_services.py`. Individual service commands are listed in their READMEs; these commands do not prepare databases, model files, or external LLM configuration.

Each service defines its own `/health` liveness/dependency checks; their coverage is not uniform. Domain APIs assume a trusted internal network. Public deployment requires separate access-control and network-isolation measures. See the [deployment instructions](../docs/DEPLOYMENT.md).
