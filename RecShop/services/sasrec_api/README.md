# SASRec Inference Service

This directory provides RecShop's FastAPI + PyTorch sequential recommendation service, running on port **8200** by default. It loads trained SASRec weights and a RecBole dataset cache to perform inference on item-ID sequences. It does not connect directly to MySQL or call external LLMs.

## Required Files

| File | Purpose |
|---|---|
| [api_server.py](api_server.py) | FastAPI entry, model loading, inference, and endpoints |
| `SASRec-Feb-24-2026_17-54-22.pth` | Trained model weights; located in this directory by default |
| `standard_cache.pkl` | Model configuration, dataset, and token/ID mappings; located in this directory by default |
| `shared/data/electronics.item` | Item titles; uses the repository's shared file first, then tries the same filename in this directory if it is missing |
| [vendor/recbole/](vendor/recbole/) | RecBole source retained in the repository |

See the [root README](../../README.md) for historical model/data distribution paths and SHA256 checksums. Load weights and pickle caches only from trusted sources. Use and redistribution of third-party source, data, and models are subject to their respective licenses.

Python dependencies are listed in [requirements.txt](../../requirements.txt). The service selects CUDA when available and otherwise uses CPU; CPU operation does not require source changes. Actual availability also depends on the installed PyTorch build and model files.

## Startup and Configuration

After activating the environment at the repository root:

```powershell
cd services/sasrec_api
python api_server.py
```

You can also specify the port explicitly:

```powershell
python api_server.py --host 127.0.0.1 --port 8200
```

| Setting | Default/purpose |
|---|---|
| `SASREC_HOST`, `SASREC_PORT` | Default `0.0.0.0:8200` |
| `SASREC_MODEL_PATH` | Defaults to the model filename in this directory |
| `SASREC_CACHE_PATH` | Default `standard_cache.pkl` |
| `SASREC_ITEM_FILE` | Optional path to the item-title file |

API documentation is at `http://127.0.0.1:8200/docs`, and the health endpoint is at `http://127.0.0.1:8200/health`. Use the API documentation interface or an HTTP client explicitly targeting port 8200.

## Endpoints

| Method and path | Purpose |
|---|---|
| `GET /health` | Model loading status and dataset information |
| `POST /recommend` | Sequential recommendation over the full candidate space |
| `POST /recommend/batch` | Batch recommendation |
| `POST /score/sampled` | Score a candidate set containing a specified positive item and sampled negative items |
| `GET /dataset/test_sequences` | Provide test sequences from the loaded dataset |

### POST /recommend

```json
{
  "item_sequence": ["EXAMPLE_ASIN_1", "EXAMPLE_ASIN_2"],
  "top_k": 10,
  "exclude_history": true
}
```

The input is a list of Amazon ASIN-format item identifiers, with length 1–200. The current model uses the most recent 50 items for inference. Out-of-vocabulary items are ignored, and at least one valid item ID is required. `top_k` defaults to 10 and ranges from 1–100; `exclude_history` defaults to `true`.

Successful responses include `success`, `recommendations`, `inference_time`, and `message`. Each recommendation contains `item_id`, `score`, `title`, and `rank`. Title availability depends on the item file. Scores are model outputs and cannot be interpreted directly as click probabilities.

### Evaluation Scope of Sampled Scoring

`/score/sampled` compares rankings among a specified positive item and sampled negatives. Its evaluation denominator differs from full-catalog recommendation, so the metrics should not be mixed directly. When constructing leave-one-out samples, remove the target item from the history. Keeping it in the history while enabling history exclusion may compromise evaluation validity.

LLM reranking and multi-agent evaluation also require the same candidate set for each method and a separate record of candidate-truncation rules. A returned ranking or successful response does not establish a completed, reproducible recommendation-quality experiment.
