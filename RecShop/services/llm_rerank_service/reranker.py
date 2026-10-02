"""
LLM Reranker：单节点最小可行 rerank 模块。

职责：
1. 组装 prompt（用户历史 + 候选商品）
2. 调用 LLM
3. 解析并校验输出
4. 失败时重试一次，仍失败则 fallback 到 SASRec top-1
"""

import os
import logging
from pathlib import Path

import time

from opentelemetry import trace as _otel_trace_api
from opentelemetry import metrics as _otel_metrics_api

from utils.validator import validate_rerank_response

logger = logging.getLogger(__name__)

# ==================== 业务 metric instrument ====================
# 懒加载: reranker 在 app.py 设 MeterProvider 之前就被 import, 故首次用时再取 meter
# (此时全局 MeterProvider 已就绪), 用模块级缓存避免重复创建。
_METRICS = {}


def _get_metrics():
    """懒初始化并缓存 rerank/LLM metric instrument。失败返回空 dict(record 处判空)。"""
    if _METRICS.get("_inited"):
        return _METRICS
    _METRICS["_inited"] = True
    try:
        _meter = _otel_metrics_api.get_meter(__name__)
        # Counter: rerank 请求数 / fallback 数(低基数 label source=llm|fallback)
        _METRICS["rerank_request"] = _meter.create_counter(
            name="recweb_rerank_request_total",
            unit="1",
            description="rerank 请求总数(按最终 source 统计)",
        )
        _METRICS["rerank_fallback"] = _meter.create_counter(
            name="recweb_rerank_fallback_total",
            unit="1",
            description="rerank 降级到 SASRec top-1 的次数",
        )
        # Counter: LLM token 累计(低基数 label type=input|output)
        _METRICS["llm_tokens"] = _meter.create_counter(
            name="recweb_llm_tokens_total",
            unit="1",
            description="LLM token 累计消耗",
        )
        # Histogram: 单次 LLM 调用耗时(低基数 label status=ok|error)
        _METRICS["llm_duration"] = _meter.create_histogram(
            name="recweb_llm_request_duration_seconds",
            unit="s",
            description="单次 LLM(DeepSeek)调用耗时(秒)",
        )
    except Exception as _m_e:
        logger.warning(f"[otel] rerank metric init failed (ignored): {_m_e}")
    return _METRICS
# ============================================================

# prompt 模板路径
_PROMPT_DIR = Path(__file__).parent / "prompts"
_RERANK_PROMPT_PATH = _PROMPT_DIR / "rerank_prompt.txt"

# 读取 prompt 模板（启动时一次性加载）
with open(_RERANK_PROMPT_PATH, "r", encoding="utf-8") as f:
    RERANK_PROMPT_TEMPLATE = f.read()


# ============================================================
# LLM 调用接口（预留，后续替换为实际模型调用）
# ============================================================

def request_llm(prompt: str) -> str:
    """
    调用 LLM 获取回复。

    【预留接口】当前使用 DeepSeek API 作为默认实现。
    你可以替换为任意 OpenAI 兼容 API、本地模型等。

    Args:
        prompt: 完整的 prompt 字符串

    Returns:
        LLM 返回的原始文本
    """
    from openai import OpenAI

    _tracer = _otel_trace_api.get_tracer(__name__)
    _model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
    _m = _get_metrics()

    # OTel: 新建 child span 承载 LLM 调用语义(GenAI semantic conventions)
    # span 名按约定 "chat {model}"; httpx auto span(POST api.deepseek.com)会挂在其下
    with _tracer.start_as_current_span(f"chat {_model}") as _span:
        _span.set_attribute("gen_ai.operation.name", "chat")
        _span.set_attribute("gen_ai.system", "deepseek")
        _span.set_attribute("gen_ai.request.model", _model)
        _span.set_attribute("gen_ai.request.temperature", 0.3)
        _span.set_attribute("gen_ai.request.max_tokens", 300)

        client = OpenAI(
            api_key=os.getenv("DEEPSEEK_API_KEY", ""),
            base_url=os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1"),
        )

        # OTel metric: 包住调用计时, status 区分 ok/error(低基数 label)
        _t0 = time.time()
        _status = "ok"
        try:
            response = client.chat.completions.create(
                model=_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=300,
            )
        except Exception:
            _status = "error"
            if _m.get("llm_duration") is not None:
                _m["llm_duration"].record(float(time.time() - _t0), {"system": "deepseek", "status": _status})
            raise
        if _m.get("llm_duration") is not None:
            _m["llm_duration"].record(float(time.time() - _t0), {"system": "deepseek", "status": _status})

        # GenAI conventions 标准 key; usage 可能为 None(异常路径)，取值前判空
        _span.set_attribute("gen_ai.response.model", response.model)
        _span.set_attribute("gen_ai.response.id", response.id)
        if response.usage:
            _span.set_attribute("gen_ai.usage.input_tokens", response.usage.prompt_tokens)
            _span.set_attribute("gen_ai.usage.output_tokens", response.usage.completion_tokens)
            # OTel metric: token 累计(label type=input|output, model 低基数)
            if _m.get("llm_tokens") is not None:
                _m["llm_tokens"].add(int(response.usage.prompt_tokens), {"type": "input", "model": _model})
                _m["llm_tokens"].add(int(response.usage.completion_tokens), {"type": "output", "model": _model})

        return response.choices[0].message.content.strip()


# ============================================================
# Prompt 组装
# ============================================================

def build_prompt(user_history: list, candidates: list) -> str:
    """
    将用户历史和候选商品填入 prompt 模板。

    Args:
        user_history: [{"item_id": "...", "title": "..."}, ...]
        candidates:   [{"item_id": "...", "title": "...", "score": 5.93}, ...]

    Returns:
        完整 prompt 字符串
    """
    # 格式化用户历史
    if not user_history:
        history_str = "(No browsing history available)"
    else:
        lines = []
        for i, item in enumerate(user_history, 1):
            lines.append(f"{i}. {item.get('title', 'Unknown')}")
        history_str = "\n".join(lines)

    # 格式化候选商品（带排名序号，强化顺序感）
    cand_lines = []
    for rank, item in enumerate(candidates, 1):
        cand_lines.append(
            f"#{rank}  item_id: {item['item_id']}  |  "
            f"title: {item['title']}  |  "
            f"score: {item.get('score', 'N/A')}"
        )
    candidates_str = "\n".join(cand_lines)

    return RERANK_PROMPT_TEMPLATE.format(
        user_history=history_str,
        candidates=candidates_str,
    )


# ============================================================
# 核心 rerank 函数
# ============================================================

def rerank(user_history: list, candidates: list) -> dict:
    """
    对 SASRec 的候选结果进行 LLM 重排序，选出 1 个最终商品。

    流程：
    1. 组装 prompt → 调用 LLM → 校验输出
    2. 校验失败 → 重试一次
    3. 仍失败 → fallback 到 SASRec top-1

    Args:
        user_history: 用户历史商品列表
        candidates:   SASRec 返回的候选商品列表（已按 score 降序）

    Returns:
        {
            "selected_item_id": "...",
            "selected_title": "...",
            "reason": "...",
            "source": "llm" | "fallback"
        }
    """
    # OTel: 给当前 Flask server span(POST /rerank)补业务字段(不新建 span)
    _span = _otel_trace_api.get_current_span()
    _span.set_attribute("recweb.rerank.candidates_count", len(candidates))
    _m = _get_metrics()

    if not candidates:
        return {
            "selected_item_id": None,
            "selected_title": None,
            "reason": "候选列表为空",
            "source": "error",
        }

    prompt = build_prompt(user_history, candidates)
    max_attempts = 2  # 首次 + 重试一次
    _last_error = None  # OTel: 保留最后一次失败原因(校验失败 error / 异常 str)，供 fallback event 使用

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"LLM rerank 第 {attempt} 次调用...")
            raw_response = request_llm(prompt)
            logger.info(f"LLM 原始返回: {raw_response[:200]}")

            result, error = validate_rerank_response(raw_response, candidates)

            if result is not None:
                result["source"] = "llm"
                logger.info(f"LLM rerank 成功: {result['selected_item_id']}")
                _span.set_attribute("recweb.rerank.source", "llm")
                _span.set_attribute("recweb.rerank.selected_item_id", str(result["selected_item_id"]))
                _span.set_attribute("recweb.rerank.attempt", attempt)
                # OTel metric: 统一计数点 — source=llm 时仅 request_total++
                if _m.get("rerank_request") is not None:
                    _m["rerank_request"].add(1, {"source": "llm"})
                return result
            else:
                _last_error = error  # 校验失败原因
                logger.warning(f"第 {attempt} 次校验失败: {error}")

        except Exception as e:
            _last_error = str(e)  # LLM 调用异常原因
            logger.warning(f"第 {attempt} 次 LLM 调用异常: {e}")

    # --- Fallback: 返回 SASRec top-1 ---
    logger.warning("LLM rerank 失败，fallback 到 SASRec top-1")
    top1 = candidates[0]
    # OTel: fallback 是成功降级(非 span 失败)，用 attribute 标状态 + event 记触发时刻/原因
    _span.set_attribute("recweb.rerank.source", "fallback")
    _span.set_attribute("recweb.rerank.selected_item_id", str(top1["item_id"]))
    _span.add_event("rerank_fallback", {"recweb.rerank.fallback_reason": str(_last_error or "unknown")[:200]})
    # OTel metric: 统一计数点 — source=fallback 时 request_total++ 且 fallback_total++
    # (与 source=llm 同一处口径, 保证 fallback/total 比值不失真)
    if _m.get("rerank_request") is not None:
        _m["rerank_request"].add(1, {"source": "fallback"})
    if _m.get("rerank_fallback") is not None:
        _m["rerank_fallback"].add(1)
    return {
        "selected_item_id": str(top1["item_id"]),
        "selected_title": str(top1["title"]),
        "reason": "LLM rerank 失败，返回模型推荐的第一名",
        "source": "fallback",
    }
