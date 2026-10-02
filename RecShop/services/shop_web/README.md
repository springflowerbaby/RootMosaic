# ShopWeb

ShopWeb is RecShop's Flask web entry point, running on port 3000 by default. Three Blueprints serve buyers, merchants, and administrators using Jinja2 templates, Bootstrap, and JavaScript. No separate frontend build step is required.

## Pages and Responsibilities

| Entry | Responsibility |
|---|---|
| `/` | Buyer item browsing, carts, addresses, orders, reviews, recommendations, and assistant |
| `/merchant` | Merchant items, orders, reviews, and business statistics |
| `/admin` | User/merchant/item/order management, announcements, and auditing |

Account login, role restrictions, and resource-ownership checks remain in the web layer. Domain service APIs assume a trusted internal network; website authentication does not make those APIs suitable for direct public exposure.

## Current Business Structure

ShopWeb accesses shared MySQL through both HTTP domain services and SQLAlchemy ORM. It is not a pure BFF that delegates all writes to domain services: paths such as order creation and payment-status updates still commit ORM transactions directly.

- Item, user, cart, address, review, inventory, and pricing features integrate with corresponding domain services.
- The checkout service provides read-only previews. Consult the [buyer routes](app/buyer/routes.py) for order creation and payment handling; the preview endpoint is not complete transaction orchestration.
- Inventory reservation/release and simulated payment records are supporting steps in synchronous HTTP calls. They do not establish a complete Saga or a real payment system.
- Buyer, merchant, and administrator assistants can call external LLMs directly. These calls do not all pass through the recommendation agent.

Key source files:

| File/directory | Contents |
|---|---|
| [run.py](run.py) | Startup entry, port, and debug switch |
| [config.py](config.py) | Database and Flask configuration |
| [app/__init__.py](app/__init__.py) | Application factory and Blueprint registration |
| [app/models.py](app/models.py) | ORM models |
| [app/buyer/routes.py](app/buyer/routes.py) | Buyer operations, recommendations, and assistant |
| [app/merchant/](app/merchant/) | Merchant interface |
| [app/admin/](app/admin/) | Administrator interface |
| [app/templates/](app/templates/) | Page templates |
| [app/static/](app/static/) | Static assets |

## Recommendation Path

Standard model recommendations obtain candidates through `shop_web → backend_api → sasrec_api`.

The `/ai-picks` page first requests a candidate list from Backend API (currently `top_k=50`), reads user history and constructs candidates in the web layer, then calls `llm_rerank_service /rerank` to display the selected item and explanation. If reranking fails or the selected item cannot be matched, the page falls back to the first item in the candidate list.

The independent `recommendation_agent` service provides a separate multi-agent recommendation API; it is not a default intermediate node for AI Picks. The availability of these features does not mean that all research collection runs exercise these endpoints.

## Configuration

Configuration is read from the project-root `.env` file or environment variables; see [.env.example](../../.env.example).

| Setting | Description |
|---|---|
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | Shared MySQL; default database `shopify2` |
| `DATABASE_URL` | Optional complete SQLAlchemy connection string; takes precedence over individual DB settings |
| `SECRET_KEY` | Flask session key; set an independent value |
| `SHOPWEB_HOST`, `SHOPWEB_PORT` | Default `0.0.0.0:3000` |
| `SHOPWEB_DEBUG` | Defaults to `false` in the startup entry; keep disabled in externally accessible environments |
| `BACKEND_API_URL`, `RERANK_SERVICE_URL` | Downstream service endpoints |
| `DEEPSEEK_API_KEY`, `DEEPSEEK_API_BASE`, `DEEPSEEK_MODEL` | Configure when using LLM features |
| `NACOS_ENABLED` | Optional discovery mechanism; set to `false` for fixed-address operation |

Do not put real keys in `config_ai.py` or a README. The [database DDL](../../scripts/database_schema.sql) defines business data structures; ORM models provide application access. Historical migration notes do not replace preparation of a new database.

## Startup

See the [root README](../../README.md) for database, item-data, and downstream-service preparation. For the complete local stack, run `python scripts/entrypoints/start_local_services.py` from the repository root.

To start only the web process, activate the environment at the repository root and run:

```powershell
cd services/shop_web
python run.py
```

Open `http://127.0.0.1:3000`. Starting the web process alone does not prepare the database or start downstream services. The example environment does not include publicly shareable accounts, session keys, or external API credentials.
