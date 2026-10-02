"""
商品推荐多Agent系统 - Tools定义
"""

import os
import logging
import requests
from pathlib import Path
from langchain_core.tools import tool
from typing import Annotated, List

logger = logging.getLogger(__name__)

SASREC_API_URL = os.environ.get('SASREC_API_URL', 'http://127.0.0.1:8200')

# electronics.item 路径：优先从环境变量读取，默认回退到项目结构下的位置
_DEFAULT_ITEM_FILE = str(Path(__file__).resolve().parent.parent.parent.parent / 'shared' / 'data' / 'electronics.item')
ITEM_FILE_PATH = Path(os.environ.get('ITEM_FILE_PATH', _DEFAULT_ITEM_FILE))

# 商品标题缓存（轻量级，只在首次调用时加载）
_title_cache = None

def _load_title_cache():
    """轻量级加载商品标题，不使用pandas"""
    global _title_cache
    if _title_cache is not None:
        return _title_cache
    
    _title_cache = {}
    item_file = ITEM_FILE_PATH
    
    if item_file.exists():
        try:
            with open(str(item_file), 'r', encoding='utf-8') as f:
                header = f.readline().strip().split('\t')
                id_idx = header.index('item_id') if 'item_id' in header else 0
                title_idx = header.index('title') if 'title' in header else 1
                
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) > max(id_idx, title_idx):
                        item_id = parts[id_idx]
                        title = parts[title_idx][:80] if len(parts[title_idx]) > 80 else parts[title_idx]
                        _title_cache[item_id] = title
        except Exception as e:
            logger.warning("加载商品标题失败: %s", e)
    
    return _title_cache

def get_item_title(item_id: str) -> str:
    """获取单个商品标题"""
    cache = _load_title_cache()
    return cache.get(item_id, "未知商品")


def _filter_real_title(items: List[dict]) -> List[dict]:
    """纯过滤函数：剔除无真实元数据的候选商品。

    剔除条件（满足任一即剔除）：
      (a) item_id 不在 title cache 中；
      (b) cache 中的 title 是占位符 f"Product_{item_id}"；
      (c) cache 中的 title 为空串。

    :param items: 候选商品 dict 列表（每项至少含 'item_id'）
    :return: 仅保留有真实标题的候选，顺序不变
    """
    cache = _load_title_cache()
    filtered = []
    for it in items:
        item_id = str(it.get("item_id", ""))
        title = cache.get(item_id)
        if title is None:
            continue
        if title == "" or title == f"Product_{item_id}":
            continue
        filtered.append(it)
    return filtered

@tool
def get_sequence_recommendations(
    item_sequence: Annotated[List[str], "User's historical interaction item IDs (Amazon ASIN format)"],
    top_k: Annotated[int, "Number of recommendations to return"] = 10,
    exclude_history: Annotated[bool, "Whether to exclude items already in history"] = True
) -> str:
    """
    Call the SASRec sequence recommendation model to get product recommendations
    based on user's historical interaction sequence.

    v2 起（设计稿 §(C)，2026-07-19 获批）：向 SASRec 过采（★ 2026-07-27 ×3 → ×10，上限 100，见下方★），
    再滤除无元数据商品（不在 title cache / 占位符 Product_<id> / 空标题），
    截断回原始 top_k。这是合理产品行为，均匀作用于所有请求；
    过滤后不足 top_k 时有多少返回多少并记 warning（可观测退化，非错误）。

    :param item_sequence: List of item IDs the user has interacted with
    :param top_k: Number of top recommendations to return
    :param exclude_history: Whether to exclude already interacted items
    :return: Recommendation results as string
    """
    try:
        # ★ 2026-07-27 过采倍数 ×3 → ×10（上限 100 = SASRec API 的 top_k le=100）。
        #   理由是实测不是猜：三批数据里都有约 7% 的调用只拿到 4 个候选
        #   （v2 7.0% / B档首轮 6.8% / B档二轮 5.4%），server log 给出直接证据：
        #       候选过滤后不足 top_k: top_k=5, fetch_k=15, 过滤前=15, 过滤后=4
        #   15 个过采里只有 4 个活下来 ⇒ SASRec top-K 里占位符约占一半，
        #   远高于权威表全表的 26.05%（合理：占位符 = 元数据缺失的商品，
        #   而这类恰好在交互日志里高频，于是被序列模型排到前面）。
        #   按 p≈0.5 反推：fetch_k=15 时 P(存活≤4)≈0.06（与实测 7% 吻合），
        #   fetch_k=50 时 ≈2e-8。
        #   ★严格增量改动：filtered[:top_k] 按 SASRec 原序取前 top_k，
        #     原本就有 ≥top_k 个存活的调用输出【逐字不变】，只把那 7% 从 4 补到 5。
        fetch_k = min(top_k * 10, 100)
        response = requests.post(
            f"{SASREC_API_URL}/recommend",
            json={
                "item_sequence": item_sequence,
                "top_k": fetch_k,
                "exclude_history": exclude_history
            },
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                raw_recommendations = data.get("recommendations", [])
                filtered = _filter_real_title(raw_recommendations)
                recommendations = filtered[:top_k]
                if len(recommendations) < top_k:
                    logger.warning(
                        "候选过滤后不足 top_k: seq_len=%d, top_k=%d, fetch_k=%d, "
                        "过滤前=%d, 过滤后=%d",
                        len(item_sequence), top_k, fetch_k,
                        len(raw_recommendations), len(filtered)
                    )
                result_lines = ["推荐结果:"]
                for i, rec in enumerate(recommendations, start=1):
                    title = rec.get("title") or "未知商品"
                    result_lines.append(
                        f"  排名{i}: {rec['item_id']} (得分: {rec['score']:.4f}) - {title}"
                    )
                result_lines.append(f"推理耗时: {data.get('inference_time', 0):.3f}秒")
                return "\n".join(result_lines)
            else:
                return f"推荐失败: {data.get('message', '未知错误')}"
        else:
            return f"API请求失败，状态码: {response.status_code}, 详情: {response.text}"
            
    except requests.exceptions.ConnectionError:
        return "错误: 无法连接到推荐服务，请确保SASRec API服务已启动 (端口8200)"
    except requests.exceptions.Timeout:
        return "错误: 推荐服务响应超时"
    except Exception as e:
        return f"错误: {str(e)}"


@tool
def analyze_user_history(
    item_sequence: Annotated[List[str], "User's historical interaction item IDs"]
) -> str:
    """
    Analyze user's historical interaction sequence to extract behavior patterns.
    
    :param item_sequence: List of item IDs the user has interacted with
    :return: Analysis of user behavior patterns
    """
    if not item_sequence:
        return "用户历史记录为空，无法进行分析"
    
    analysis = []
    analysis.append("用户历史行为分析:")
    analysis.append(f"  - 历史交互商品数量: {len(item_sequence)}")
    
    # 显示历史商品（含标题）
    analysis.append("  - 历史交互商品列表:")
    for i, item_id in enumerate(item_sequence):
        title = get_item_title(item_id)
        label = "(最早)" if i == 0 else ("(最近)" if i == len(item_sequence) - 1 else "")
        analysis.append(f"      {i+1}. {item_id}: {title} {label}")
    
    unique_items = set(item_sequence)
    analysis.append(f"  - 去重后商品数量: {len(unique_items)}")
    
    if len(item_sequence) > len(unique_items):
        repeat_rate = (len(item_sequence) - len(unique_items)) / len(item_sequence) * 100
        analysis.append(f"  - 重复购买率: {repeat_rate:.1f}%")
    
    return "\n".join(analysis)


@tool
def get_product_details(
    item_ids: Annotated[List[str], "List of product IDs to get details for"]
) -> str:
    """
    Get detailed information about products.
    
    :param item_ids: List of product IDs
    :return: Product details as string
    """
    details = ["商品详情:"]
    for i, item_id in enumerate(item_ids[:10]):
        title = get_item_title(item_id)
        details.append(f"  {i+1}. {item_id}: {title}")
    
    if len(item_ids) > 10:
        details.append(f"  ... 还有 {len(item_ids) - 10} 个商品")
    
    return "\n".join(details)


@tool
def check_recommendation_service() -> str:
    """
    Check if the recommendation service is running and healthy.
    
    :return: Service status information
    """
    try:
        response = requests.get(f"{SASREC_API_URL}/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            status = data.get("status", "unknown")
            model_loaded = data.get("model_loaded", False)
            dataset_info = data.get("dataset_info", {})
            
            result = [
                f"推荐服务状态: {status}",
                f"模型已加载: {'是' if model_loaded else '否'}"
            ]
            if dataset_info:
                result.append(f"数据集信息:")
                result.append(f"  - 用户数: {dataset_info.get('user_num', 'N/A')}")
                result.append(f"  - 商品数: {dataset_info.get('item_num', 'N/A')}")
                result.append(f"  - 交互数: {dataset_info.get('interaction_num', 'N/A')}")
            
            return "\n".join(result)
        else:
            return f"服务状态异常，状态码: {response.status_code}"
    except requests.exceptions.ConnectionError:
        return "推荐服务未运行，请启动 api_server.py"
    except Exception as e:
        return f"检查服务状态失败: {str(e)}"
