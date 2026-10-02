# Multi-Agent Recommendation Service

This independent Flask recommendation API in RecShop runs on port 5001 by default and includes a demo page. It uses LangGraph to execute recommendation-analysis nodes sequentially, combining SASRec candidates, item titles, and external LLM output to return a recommendation and explanation. It does not connect directly to MySQL.

## Current Workflow

[workflow.py](workflow.py) compiles two graphs with fixed execution orders:

```text
POST /recommend
  Sequence_Recommender
  → User_Behavior_Analyzer
  → Product_Analyzer
  → Recommendation_Synthesizer

POST /recommend/from_candidates
  User_Behavior_Analyzer
  → Product_Analyzer
  → Recommendation_Synthesizer
```

The second endpoint uses candidates supplied in the request and skips SASRec candidate generation. The current graphs have neither dynamic Supervisor scheduling nor parallel analysis branches. Historical role names in prompts do not describe the current execution structure.

This service is separate from the LLM reranking service used by the website's AI Picks; see [service relationships](../README.md).

## API

| Method and path | Purpose |
|---|---|
| `POST /recommend` | Accept a nonempty `item_sequence`, generate candidates, and complete recommendation; `top_k` defaults to 5 |
| `POST /recommend/from_candidates` | Accept `item_sequence` and existing `candidates`, then select an item from them |
| `GET /recommend/health` | Service and SASRec health checks |
| `GET /recommend/chat-messages` | Static messages for the demo page, not actual conversation history for arbitrary requests |

Recommendation request structure:

```json
{
  "item_sequence": ["EXAMPLE_ASIN_1", "EXAMPLE_ASIN_2"],
  "top_k": 5
}
```

Replace item IDs with actual values from the current model vocabulary. Successful responses include `success`, `recommendation`, `conversation`, and `trace_id` when available. `recommendation` contains item ID, title, explanation, and confidence fields. Confidence is model output, not a calibrated probability of correctness.

Each `from_candidates` entry requires a nonempty string `item_id`, an integer `rank`, and a numeric `score`; `title` is optional. The implementation passes the first 20 entries to the workflow, so callers should order candidates beforehand.

## Dependencies and Configuration

- SASRec inference service: local default `http://127.0.0.1:8200`.
- External OpenAI-compatible LLM: configure `DEEPSEEK_API_KEY`, `DEEPSEEK_API_BASE`, and `DEEPSEEK_MODEL`.
- Item-title file: defaults to `electronics.item` under the repository's `shared/data/` directory; override with `ITEM_FILE_PATH`. Recommendation tools filter out candidates without valid titles. A missing file can leave too few candidates or none.
- See the [project instructions](../../README.md) for Python dependencies and model access. Configuration is read from the root `.env` file or the environment.

To obtain candidates with valid titles, `get_sequence_recommendations` requests at most `min(top_k × 10, 100)` SASRec results, then filters and truncates them in their original order. This is not an additional training procedure. See the [agents directory](agents/README.md) for tools and prompts.

## Startup

First start the model service using the [SASRec instructions](../sasrec_api/README.md). After activating the environment at the repository root:

```powershell
cd services/recommendation_agent
python app.py
```

`RECOMMENDATION_HOST` and `RECOMMENDATION_PORT` override the listening address; the default is `0.0.0.0:5001`. Recommendation requests call an external model. Use your configured service and check call costs when testing; a health check does not constitute a completed recommendation validation.

## Files

| File | Contents |
|---|---|
| [app.py](app.py) | Flask entry, demo page, and observability integration |
| [workflow.py](workflow.py) | API, sequential graphs, structured output, and conversation results |
| [agents/tools.py](agents/tools.py) | SASRec and local item-information tools |
| [agents/prompts.py](agents/prompts.py) | Recommendation-analysis prompts |
| [static/](static/) | Independent demo page |
