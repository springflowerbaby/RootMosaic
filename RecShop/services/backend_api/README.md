# Backend API

Backend API is RecShop's Flask JSON service, running on port 5000 by default. It accesses the shared business database through a MySQL connection pool and calls the SASRec API for sequential recommendation candidates. It does not call LLM reranking; `shop_web` initiates the subsequent reranking step for AI Picks.

## Main Endpoints

See [app.py](app.py) for the implementation.

| Method and path | Purpose |
|---|---|
| `GET /health` | Service health check |
| `GET /api/users/<user_token>` | Retrieve a user |
| `POST /api/users` | Create a user |
| `GET /api/users/<user_token>/history` | Retrieve recent interactions |
| `GET /api/items` | List items |
| `GET /api/items/<item_id>` | Retrieve item details |
| `GET /api/items/search` | Search items |
| `POST /api/interactions` | Write an interaction record |
| `POST /api/recommend` | Call SASRec using user interactions, save recommendation records, and attach item information |
| `POST /api/recommend/feedback` | Recommendation feedback |
| `GET /api/stats/model` | Model-related statistics |
| `GET /api/stats/recommendations` | Recommendation-record statistics |
| `GET /api/stats/popular-items` | Popular-item statistics |

`POST /api/recommend` accepts `user_token` and optional `top_k` (default 10). It retrieves at most 50 historical interactions, sends `item_sequence`, `top_k`, and `exclude_history=true` to SASRec, writes the results to `recommendations`, and adds display fields from `items`. It returns an error when no interaction history is available. This endpoint is therefore not a read-only probe.

Example request structure; replace the user identifier with a user who has interactions in the actual database:

```json
{
  "user_token": "example_user",
  "top_k": 10
}
```

A successful response includes `recommendations`, `inference_time`, and `input_sequence_length`. Item fields depend on current database records and the [response construction](app.py). The example user does not imply that demo data is included in the repository.

## Configuration and Startup

See the [project instructions](../../README.md) for dependencies, database schema, and model preparation. Configuration is read from the root `.env` file or the process environment:

| Setting | Purpose |
|---|---|
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | MySQL connection; the default database is `shopify2`, and a password is required |
| `BACKEND_HOST`, `BACKEND_PORT` | Listening address and port; default `0.0.0.0:5000` |
| `SASREC_API_URL` | Downstream SASRec endpoint; local default `http://127.0.0.1:8200` |
| `NACOS_ENABLED` | Service discovery switch; set to `false` for fixed-address operation |

After activating an environment with the required dependencies, run from the repository root:

```powershell
cd services/backend_api
python app.py
```

The health endpoint is `http://127.0.0.1:5000/health`. Starting this service alone does not start MySQL or SASRec, or import business data.

## Dependencies and Scope

- Uses shared tables including `users`, `items`, `interactions`, and `recommendations`.
- Downstream service resolution is implemented in [service_discovery.py](service_discovery.py); container deployments should use the configured Service DNS.
- This API does not replace website user authentication and should run as a controlled internal service.
- OpenTelemetry integration is included; determine actual telemetry coverage from collected results.
