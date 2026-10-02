# LLM Reranking Service

This Flask API in RecShop runs on port 5002 by default. It accepts previously generated item candidates and user history, uses an external LLM to select one item, and returns an explanation. It neither accesses MySQL directly nor calls SASRec directly.

## Role in the Business Flow

`shop_web /ai-picks` first requests `backend_api /api/recommend` to obtain SASRec candidates, then calls this service's `POST /rerank`. ShopWeb's default port is 3000; the page implementation is in the [buyer routes](../shop_web/app/buyer/routes.py).

Callers should order candidates according to a predefined rule, such as model scores. If the model call or output validation fails, the service makes at most two attempts before returning `candidates[0]` with `source` set to `fallback`. HTTP success does not necessarily mean that an LLM result was used.

## API

### POST /rerank

```json
{
  "user_history": [
    {"item_id": "EXAMPLE_HISTORY", "title": "Example history item"}
  ],
  "candidates": [
    {"item_id": "EXAMPLE_CANDIDATE", "title": "Example candidate item", "score": 6.12}
  ]
}
```

`candidates` must be nonempty, and each entry should contain `item_id` and `title`. These example values illustrate the format only; actual requests should use real candidates from the same recommendation task.

Successful response structure:

```json
{
  "success": true,
  "result": {
    "selected_item_id": "EXAMPLE_CANDIDATE",
    "selected_title": "Example candidate item",
    "reason": "Recommendation reason",
    "source": "llm"
  }
}
```

`source` distinguishes `llm` from `fallback`. The selection must belong to the input candidate set; the implementation validates model output.

### GET /health

Returns the service status and name without making an external LLM request. It cannot establish that credentials, the model, or quota are available.

## Configuration and Startup

Configuration is read from the root `.env` file or the environment:

| Setting | Default/purpose |
|---|---|
| `DEEPSEEK_API_KEY` | Required when using the external LLM |
| `DEEPSEEK_API_BASE` | Default `https://api.deepseek.com/v1` |
| `DEEPSEEK_MODEL` | Default `deepseek-chat` |
| `RERANK_HOST`, `RERANK_PORT` | Default `0.0.0.0:5002` |
| `RERANK_DEBUG` | Default `false` |

After activating an environment with the required dependencies, run from the repository root:

```powershell
cd services/llm_rerank_service
python app.py
```

Users supply their own model-access configuration. Actual reranking and related evaluation make external model calls.

## Files and Attribution

| File | Contents |
|---|---|
| [app.py](app.py) | HTTP API |
| [reranker.py](reranker.py) | Prompts, external calls, validation, and fallback |

The original implementation notes cite LLM4Rerank as an inspiration. The bibliographic record is retained below; this simplified service is not a complete reproduction of the paper:

```bibtex
@inproceedings{gao2025llm4rerank,
  title={LLM4Rerank: LLM-based Auto-Reranking Framework for Recommendations},
  author={Gao, Jingtong and Chen, Bo and Zhao, Xiangyu and Liu, Weiwen and Li, Xiangyang and Wang, Yichao and Wang, Wanyu and Guo, Huifeng and Tang, Ruiming},
  booktitle={Proceedings of the ACM on Web Conference 2025},
  pages={228--239},
  year={2025}
}
```
