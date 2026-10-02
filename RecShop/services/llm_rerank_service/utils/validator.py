"""
输出校验模块：验证 LLM 返回的 rerank 结果是否合法。
"""

import json


def validate_rerank_response(raw_text, candidates):
    """
    校验 LLM 输出是否为合法的 rerank 结果。

    Args:
        raw_text: LLM 返回的原始字符串
        candidates: 候选商品列表，每项至少包含 item_id 和 title

    Returns:
        (parsed_dict, error_msg)
        - 合法时: ({"selected_item_id": ..., "selected_title": ..., "reason": ...}, None)
        - 非法时: (None, "错误描述")
    """
    # --- 1. 尝试解析 JSON ---
    parsed = _try_parse_json(raw_text)
    if parsed is None:
        return None, "JSON 解析失败"

    # --- 2. 必需字段检查 ---
    required_keys = ["selected_item_id", "selected_title", "reason"]
    for key in required_keys:
        if key not in parsed:
            return None, f"缺少必需字段: {key}"

    selected_id = str(parsed["selected_item_id"]).strip()
    selected_title = str(parsed["selected_title"]).strip()

    # --- 3. item_id 必须在候选列表中 ---
    candidate_map = {str(c["item_id"]).strip(): str(c["title"]).strip() for c in candidates}

    if selected_id not in candidate_map:
        return None, f"selected_item_id '{selected_id}' 不在候选列表中"

    # --- 4. title 必须和 item_id 对应 ---
    expected_title = candidate_map[selected_id]
    if selected_title != expected_title:
        return None, (
            f"selected_title 与 item_id 不匹配: "
            f"期望 '{expected_title}', 实际 '{selected_title}'"
        )

    # --- 校验通过 ---
    return {
        "selected_item_id": selected_id,
        "selected_title": selected_title,
        "reason": str(parsed.get("reason", "")).strip(),
    }, None


def _try_parse_json(text):
    """
    尝试从 LLM 输出中提取并解析 JSON。
    支持裸 JSON 或被 ```json ... ``` 包裹的格式。
    """
    text = text.strip()

    # 尝试直接解析
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 尝试提取 ```json ... ``` 块
    if "```" in text:
        try:
            start = text.index("```") + 3
            # 跳过可能的 "json" 标签
            if text[start:start + 4].lower() == "json":
                start += 4
            end = text.index("```", start)
            return json.loads(text[start:end].strip())
        except (ValueError, json.JSONDecodeError):
            pass

    # 尝试提取第一个 { ... } 块
    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start != -1 and brace_end > brace_start:
        try:
            return json.loads(text[brace_start:brace_end + 1])
        except json.JSONDecodeError:
            pass

    return None
