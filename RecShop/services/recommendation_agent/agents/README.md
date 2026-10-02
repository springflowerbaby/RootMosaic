# Recommendation Tools and Prompts

This directory contains the business tools and prompts used by RecShop's multi-agent recommendation feature.

| File | Contents |
|---|---|
| [prompts.py](prompts.py) | Prompts for sequential recommendation, user behavior, product analysis, and synthesis; retained historical prompts do not imply that the current graph uses those nodes |
| [tools.py](tools.py) | SASRec HTTP calls, local item-title lookup, and analysis tools |

The current execution graph is defined in [../workflow.py](../workflow.py): analysis nodes run sequentially, and `from_candidates` skips sequential candidate generation. See the [service README](../README.md) for endpoints and configuration.

## Tools

| Function | Purpose |
|---|---|
| `get_sequence_recommendations` | Call SASRec, filter candidates without valid titles, and take the first K in their original order |
| `analyze_user_history` | Organize item information for the input sequence for subsequent analysis |
| `get_product_details` | Provide locally available item information |
| `check_recommendation_service` | Request the SASRec health endpoint |
| `get_item_title` | Look up a title in the local cache |

`SASREC_API_URL` defaults locally to `http://127.0.0.1:8200`; `ITEM_FILE_PATH` defaults to the repository's `shared/data/electronics.item`. This module does not automatically fill missing titles from MySQL.
