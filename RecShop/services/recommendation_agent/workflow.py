"""
商品推荐多Agent系统 - 主工作流

确定性顺序协作链(接口与行为见本目录 README.md):
  Sequence_Recommender → User_Behavior_Analyzer → Product_Analyzer → Recommendation_Synthesizer

工程化(TASK-W):
- 每个 agent 节点包一层带名 OTel span(span 名 "agent.<Name>" + 属性 recweb.agent.name),
  使 trace 里能按 agent 定位故障(Task2 per-agent 故障注入 + RCA 可定位的硬前提)。
- 预留 per-agent 故障注入开关(env 驱动, 默认全关, 不改变正常路径行为):
  AGENT_FAULT_<NAME>=delay|error|garbage, AGENT_FAULT_DELAY_MS=<毫秒>。
- 两个 endpoint 共用建图工厂 build_recommendation_graph(include_sequence_recommender),
  编译后的 graph 按是否含 Sequence_Recommender 缓存(模块级单例, dev server 单线程, 低风险)。
- 已删原 supervisor 动态路由死代码(supervisor_chain/route_tool/parse_supervisor_output/
  AgentState.next 等), 它们曾因 member↔supervisor 递归死循环撞 recursion_limit 而被弃用,
  且从未 add_node 接入图。
"""

from flask import Blueprint, request, jsonify
import os
import time
import logging
import functools
import operator
from typing import Sequence, TypedDict, Annotated, List
from pathlib import Path
from dotenv import load_dotenv

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

from opentelemetry import trace as _otel_trace_api
from opentelemetry.trace import Status, StatusCode

from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.graph import END, StateGraph, START

from agents.prompts import (
    Sequence_Recommender_system_prompt,
    User_Behavior_Analyzer_system_prompt,
    Product_Analyzer_system_prompt,
    Recommendation_Synthesizer_system_prompt
)
from agents.tools import (
    get_sequence_recommendations,
    analyze_user_history,
    get_product_details,
    check_recommendation_service,
    get_item_title
)

recommendation_bp = Blueprint('recommendation', __name__)
logger = logging.getLogger(__name__)
_tracer = _otel_trace_api.get_tracer(__name__)

# 从环境变量读取配置
if "OPENAI_API_KEY" not in os.environ:
    os.environ["OPENAI_API_KEY"] = os.environ.get('DEEPSEEK_API_KEY', 'your_api_key_here')
if "OPENAI_API_BASE" not in os.environ:
    os.environ["OPENAI_API_BASE"] = os.environ.get('DEEPSEEK_API_BASE', 'https://api.deepseek.com/v1')

MODEL_NAME = os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat')


# ==================== AgentState（模块级单一定义） ====================
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]
    item_sequence: List[str]
    top_k: int
    recommended_product: str
    product_title: str
    recommendation_reason: str
    confidence: float


# ==================== per-agent 故障注入（Task2 注入点，默认全关） ====================
# 通过环境变量对指定 agent 注入故障，正常路径(env 未设)零开销、行为不变。
#   AGENT_FAULT_<NAME>  = delay | error | garbage   （NAME 为 agent 节点名）
#   AGENT_FAULT_DELAY_MS= 延迟毫秒（delay 模式用，默认 5000）
# 由 span/节点统一应用，便于在 trace 上按 recweb.agent.name + recweb.agent.fault 给数据集打标。
_GARBAGE_MESSAGE = "（系统提示：本环节分析结果不可用，请基于其他专家的分析继续。）"


def _agent_fault_kind(name: str) -> str:
    """读取该 agent 的故障注入类型；未配置返回空串（正常路径）。"""
    return os.environ.get(f"AGENT_FAULT_{name}", "").strip().lower()


def _apply_agent_fault(name: str, span) -> str:
    """在 agent 执行前应用注入故障；返回实际生效的故障类型（空串=无）。

    - delay  : sleep 后正常执行（注入延迟，模拟某 agent 变慢）
    - error  : 抛异常（由节点的降级逻辑接住或冒泡）
    - garbage : 不在此处生效，由节点改写输出实现（见 _agent_node）
    其它/空 : 无故障。
    """
    kind = _agent_fault_kind(name)
    if not kind:
        return ""
    span.set_attribute("recweb.agent.fault", kind)
    if kind == "delay":
        delay_ms = int(os.environ.get("AGENT_FAULT_DELAY_MS", "5000"))
        span.set_attribute("recweb.agent.fault.delay_ms", delay_ms)
        time.sleep(delay_ms / 1000.0)
        return "delay"
    if kind == "error":
        raise RuntimeError(f"[fault-injection] agent '{name}' forced error")
    if kind == "garbage":
        return "garbage"
    return ""


# ==================== agent 构造 ====================
def create_agent(llm: ChatOpenAI, tools: list, system_prompt: str):
    """创建Agent"""
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="messages"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ]
    )
    agent = create_openai_tools_agent(llm, tools, prompt)
    executor = AgentExecutor(agent=agent, tools=tools)
    return executor


def _agent_node(state, agent, name):
    """Agent 节点执行：包一层带名 span（可定位故障）+ per-agent 故障注入 + 降级容错。

    行为不变（正常路径）：与旧 agent_node 一致，把 agent 输出包成
    HumanMessage(name=name) 累加进 messages。
    增强：
      - 每个 agent = 一条 "agent.<Name>" span，属性 recweb.agent.name，
        其下挂 httpx(DeepSeek)/requests(SASRec) 子 span，可按 agent 归因。
      - 故障注入(env 驱动，默认关)：delay/error/garbage。
      - 降级：analyzer 类 agent 失败不炸整链，set_status(ERROR)+record_exception，
        回写一条"不可用"message 让链继续（Synthesizer 失败仍冒泡，由 endpoint 转 500）。
    """
    with _tracer.start_as_current_span(f"agent.{name}") as span:
        span.set_attribute("recweb.agent.name", name)
        try:
            # 故障注入在 try 内：fault=error 抛出后与"真实 agent 异常"走同一降级路径
            # （analyzer 失败不炸整链，模拟"某 agent 内部故障 → 链降级继续"的故障模式）。
            fault = _apply_agent_fault(name, span)
            if fault == "garbage":
                return {"messages": [HumanMessage(content=_GARBAGE_MESSAGE, name=name)]}
            result = agent.invoke(state)
            return {"messages": [HumanMessage(content=result["output"], name=name)]}
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.warning("agent '%s' failed, degrading and continuing: %s", name, e)
            return {"messages": [HumanMessage(
                content=f"（{name} 暂时不可用，已跳过该环节的分析。）", name=name)]}


def _make_synthesizer_node(synthesizer_chain, name="Recommendation_Synthesizer"):
    """把 Synthesizer 裸链包成节点函数：同样起带名 span（可定位），应用故障注入。

    Synthesizer 是终点、产最终结果；失败按现状冒泡（由 endpoint 顶层 except 转 500），
    不在此处降级（避免静默给出空推荐掩盖故障）。
    """
    def synthesizer_node(state):
        with _tracer.start_as_current_span(f"agent.{name}") as span:
            span.set_attribute("recweb.agent.name", name)
            _apply_agent_fault(name, span)  # delay 生效；error 直接抛、冒泡；garbage 不适用于结构化输出
            try:
                return synthesizer_chain.invoke(state)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
    return synthesizer_node


# 两个 endpoint 各自的 Synthesizer 工具 schema（描述措辞保持与改造前一字不差，
# 避免影响 LLM 输出语义；二者结构相同、仅 description 文案不同）。
_SYNTH_TOOL_RECOMMEND = {
    "type": "function",
    "function": {
        "name": "Synthesize_Recommendation",
        "description": "Synthesize the final recommendation with explanation.",
        "parameters": {
            "type": "object",
            "properties": {
                "recommended_product": {
                    "type": "string",
                    "description": "The recommended product ID (Amazon ASIN)",
                },
                "product_title": {
                    "type": "string",
                    "description": "The recommended product title",
                },
                "recommendation_reason": {
                    "type": "string",
                    "description": "Detailed explanation of why this product is recommended",
                },
                "confidence": {
                    "type": "number",
                    "description": "Confidence score between 0 and 1",
                },
            },
            "required": ["recommended_product", "product_title", "recommendation_reason", "confidence"],
        },
    }
}

_SYNTH_TOOL_FROM_CANDIDATES = {
    "type": "function",
    "function": {
        "name": "Synthesize_Recommendation",
        "description": "Synthesize the final recommendation from the candidate list.",
        "parameters": {
            "type": "object",
            "properties": {
                "recommended_product": {"type": "string", "description": "The recommended product ID (Amazon ASIN) - MUST be from the candidate list"},
                "product_title": {"type": "string", "description": "The recommended product title"},
                "recommendation_reason": {"type": "string", "description": "Detailed explanation"},
                "confidence": {"type": "number", "description": "Confidence score 0-1"},
            },
            "required": ["recommended_product", "product_title", "recommendation_reason", "confidence"],
        },
    }
}


def _build_synthesizer_chain(synthesizer_tool, final_user_messages):
    """构建 Synthesizer 链(prompt | LLM.bind_tools(强制 tool_choice) | parse)。

    :param synthesizer_tool: 强制调用的工具 schema（两 endpoint 各自的措辞）。
    :param final_user_messages: 接在 messages 占位之后的 (role, text) 列表，
        两个 endpoint 的差异在此（/recommend 两条 vs from_candidates 一条）。
    """
    synthesizer_model = ChatOpenAI(model=MODEL_NAME, temperature=0)

    def parse_synthesizer_output(response):
        """解析综合分析输出"""
        if hasattr(response, 'tool_calls') and response.tool_calls:
            return response.tool_calls[0]['args']
        return {
            "recommended_product": "unknown",
            "product_title": "未知商品",
            "recommendation_reason": "无法生成推荐原因",
            "confidence": 0.5
        }

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", Recommendation_Synthesizer_system_prompt),
            MessagesPlaceholder(variable_name="messages"),
            *final_user_messages,
        ]
    )
    return prompt | synthesizer_model.bind_tools(
        tools=[synthesizer_tool],
        tool_choice={"type": "function", "function": {"name": "Synthesize_Recommendation"}}
    ) | parse_synthesizer_output


# ==================== 建图工厂（两 endpoint 共用，编译结果缓存） ====================
@functools.lru_cache(maxsize=2)
def build_recommendation_graph(include_sequence_recommender: bool):
    """构建并编译推荐工作流图，按 include_sequence_recommender 缓存两个模块级单例。

    确定性顺序链：
      include_sequence_recommender=True (/recommend):
        Sequence_Recommender → User_Behavior_Analyzer → Product_Analyzer → Recommendation_Synthesizer
      include_sequence_recommender=False (/recommend/from_candidates，候选已给定，跳过 Sequence_Recommender):
        User_Behavior_Analyzer → Product_Analyzer → Recommendation_Synthesizer

    注：dev server 默认单线程，编译图复用安全；输入经 initial_state 传入，
    每请求只 stream 不重编译。
    """
    llm = ChatOpenAI(model=MODEL_NAME, temperature=0.7)

    User_Behavior_Analyzer = create_agent(
        llm, [analyze_user_history], User_Behavior_Analyzer_system_prompt)
    Product_Analyzer = create_agent(
        llm, [get_product_details], Product_Analyzer_system_prompt)

    if include_sequence_recommender:
        # /recommend 的 Synthesizer 收尾两条 user 指令（保持原文案不变）
        synth_chain = _build_synthesizer_chain(_SYNTH_TOOL_RECOMMEND, [
            ('user', "请综合以上所有分析结果，选出最适合用户的商品，并详细解释推荐原因。"),
            ('user', "置信度应该在0到1之间，表示你对这个推荐的确信程度。"),
        ])
    else:
        # from_candidates 的 Synthesizer 收尾一条 user 指令（保持原文案不变）
        synth_chain = _build_synthesizer_chain(_SYNTH_TOOL_FROM_CANDIDATES, [
            ('user', "请从候选商品列表中选出最适合用户的商品。你必须选择候选列表中的一个商品ID。"),
        ])

    workflow = StateGraph(AgentState)

    if include_sequence_recommender:
        Sequence_Recommender = create_agent(
            llm, [get_sequence_recommendations, check_recommendation_service],
            Sequence_Recommender_system_prompt)
        workflow.add_node("Sequence_Recommender", functools.partial(
            _agent_node, agent=Sequence_Recommender, name="Sequence_Recommender"))

    workflow.add_node("User_Behavior_Analyzer", functools.partial(
        _agent_node, agent=User_Behavior_Analyzer, name="User_Behavior_Analyzer"))
    workflow.add_node("Product_Analyzer", functools.partial(
        _agent_node, agent=Product_Analyzer, name="Product_Analyzer"))
    workflow.add_node("Recommendation_Synthesizer", _make_synthesizer_node(synth_chain))

    if include_sequence_recommender:
        workflow.add_edge(START, "Sequence_Recommender")
        workflow.add_edge("Sequence_Recommender", "User_Behavior_Analyzer")
    else:
        workflow.add_edge(START, "User_Behavior_Analyzer")
    workflow.add_edge("User_Behavior_Analyzer", "Product_Analyzer")
    workflow.add_edge("Product_Analyzer", "Recommendation_Synthesizer")
    workflow.add_edge("Recommendation_Synthesizer", END)

    logger.debug("编译推荐工作流 (include_sequence_recommender=%s)", include_sequence_recommender)
    return workflow.compile()


def _run_graph(graph, initial_state):
    """流式执行图，返回 (最终 Synthesizer 结果 dict, 各节点输出的 chat_log)。"""
    work_res = {}
    chat_log = []
    for s in graph.stream(initial_state):
        if "__end__" not in s:
            work_res = s
            chat_log.append(s)
    final_result = work_res.get('Recommendation_Synthesizer', {})
    return final_result, chat_log


def create_recommendation_workflow(item_sequence: List[str], top_k: int = 5):
    """
    创建并运行推荐工作流（4-agent 顺序链）。

    :param item_sequence: 用户历史交互商品ID列表
    :param top_k: 返回Top-K推荐
    :return: (推荐结果 dict, 对话日志 chat_log)
    """
    user_prompt = f"""
    请根据用户的历史交互序列进行商品推荐分析：
    - 用户历史交互商品: {item_sequence}
    - 需要推荐数量: {top_k}

    请各位专家分析并给出最终推荐结果和推荐原因。
    """

    initial_state = {
        "messages": [HumanMessage(content=user_prompt, name="user")],
        "item_sequence": item_sequence,
        "top_k": top_k,
    }

    graph = build_recommendation_graph(include_sequence_recommender=True)
    final_result, chat_log = _run_graph(graph, initial_state)

    # 获取商品ID和标题
    product_id = final_result.get("recommended_product", "unknown")
    product_title = final_result.get("product_title", "")

    # 如果LLM没返回标题，从本地文件查询
    if not product_title or product_title == "未知商品":
        product_title = get_item_title(product_id)

    # BUG-4: LLM 可能把 confidence 返成字符串，强转防下游格式化/消费 ValueError
    try:
        confidence = float(final_result.get("confidence", 0.5) or 0)
    except (TypeError, ValueError):
        confidence = 0.5

    result_dic = {
        "recommended_product": product_id,
        "product_title": product_title,
        "recommendation_reason": final_result.get("recommendation_reason", ""),
        "confidence": confidence,
    }
    return result_dic, chat_log


def extract_recommendation_conversation(history):
    """提取对话历史"""
    key_mapping = {
        'Sequence_Recommender': 'SequenceRecommender',
        'User_Behavior_Analyzer': 'UserBehaviorAnalyzer',
        'Product_Analyzer': 'ProductAnalyzer',
        'Recommendation_Synthesizer': 'RecommendationSynthesizer'
    }
    result = {}
    for item in history:
        for key, value in item.items():
            if key not in ['supervisor', 'Synthesize']:
                new_key = key_mapping.get(key, key)

                # 处理有messages字段的agent输出
                if isinstance(value, dict) and 'messages' in value:
                    messages = value.get('messages', [])
                    for message in messages:
                        if new_key not in result:
                            result[new_key] = ""
                        result[new_key] += message.content

                # 处理Recommendation_Synthesizer的直接字典输出
                elif isinstance(value, dict) and 'recommendation_reason' in value:
                    reason = value.get('recommendation_reason', '')
                    product_id = value.get('recommended_product', '')
                    # BUG-4: LLM 可能把 confidence 返成字符串，*100/.1f 前强转防 ValueError 击穿
                    try:
                        confidence = float(value.get('confidence', 0) or 0)
                    except (TypeError, ValueError):
                        confidence = 0.0

                    # 获取商品名称
                    product_title = value.get('product_title', '')
                    if not product_title or product_title == "未知商品":
                        product_title = get_item_title(product_id)

                    result[new_key] = f"**推荐商品**: {product_id} - {product_title}\n\n**推荐原因**: {reason}\n\n**置信度**: {confidence*100:.1f}%"

    return result


@recommendation_bp.route('/recommend', methods=['POST'])
def recommend():
    """推荐接口"""
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({'message': 'Request body must be a JSON object'}), 400

    if 'item_sequence' not in data:
        return jsonify({'message': 'Missing item_sequence in request body'}), 400

    item_sequence = data['item_sequence']
    top_k = data.get('top_k', 5)

    if not isinstance(item_sequence, list) or len(item_sequence) == 0:
        return jsonify({'message': 'item_sequence must be a non-empty list'}), 400

    # [TASK-X #1] trace_id 精确归窗：在 try 块内取当前 span 的 trace_id 回写进响应 JSON
    # （+ traceparent 响应头双保险），runner 按该 trace_id 从本地 JSONL 精确捞该窗全部
    # span（不靠 wall-clock 时窗，规避 SimpleSpanProcessor span-END export 的串窗隐患）。
    # 正常路径若无 OTel（trace_id=0）则字段为空串，行为不变。
    try:
        _ctx = _otel_trace_api.get_current_span().get_span_context()
        _trace_id_hex = format(_ctx.trace_id, "032x") if _ctx and _ctx.trace_id else ""
    except Exception:
        _trace_id_hex = ""

    try:
        result, chat_log = create_recommendation_workflow(item_sequence, top_k)
        conversation_dict = extract_recommendation_conversation(chat_log)

        resp = jsonify({
            'success': True,
            'recommendation': result,
            'conversation': conversation_dict,
            'trace_id': _trace_id_hex,
        })
        if _trace_id_hex:
            resp.headers['traceparent'] = f"00-{_trace_id_hex}-{format(_ctx.span_id, '016x')}-01"
        return resp, 200

    except Exception as e:
        logger.exception("Recommendation failed")
        return jsonify({
            'success': False,
            'message': f'Recommendation failed: {str(e)}',
            'trace_id': _trace_id_hex,
        }), 500


@recommendation_bp.route('/recommend/from_candidates', methods=['POST'])
def recommend_from_candidates():
    """从预计算的候选列表中选择最佳推荐（用于公平评估）

    接收 SASRec /score/sampled 返回的 100 个候选商品（含得分和标题），
    使用多Agent工作流从中选出最佳推荐。这样 Agent 和 baseline 在同一候选集上评估。
    """
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({'message': 'Request body must be a JSON object'}), 400

    if 'candidates' not in data or 'item_sequence' not in data:
        return jsonify({'message': 'Missing candidates or item_sequence'}), 400

    candidates = data['candidates']  # list of {item_id, score, title, rank}
    item_sequence = data['item_sequence']

    if not candidates or not item_sequence:
        return jsonify({'message': 'candidates and item_sequence must be non-empty'}), 400

    # BUG-2: 候选必须是 list，且每项含数值 rank/score（下方 :.4f 格式化前校验，
    # 否则畸形候选 KeyError/ValueError/TypeError 被顶层 except 兜成 500）。
    if not isinstance(candidates, list):
        return jsonify({'message': 'candidates must be a list'}), 400
    for c in candidates:
        if not isinstance(c, dict):
            return jsonify({'message': 'each candidate must be an object'}), 400
        rank = c.get('rank')
        score = c.get('score')
        # bool 是 int 子类，rank/score 不接受布尔
        if not isinstance(rank, int) or isinstance(rank, bool):
            return jsonify({'message': 'each candidate must have an integer rank'}), 400
        if not isinstance(score, (int, float)) or isinstance(score, bool):
            return jsonify({'message': 'each candidate must have a numeric score'}), 400
        # item_id 在下方 candidate_text 直接索引 c['item_id']，缺失会 KeyError→500，同 BUG-2 类一并校验
        item_id = c.get('item_id')
        if not isinstance(item_id, str) or not item_id:
            return jsonify({'message': 'each candidate must have a string item_id'}), 400

    try:
        # 构造候选商品描述，传给 multi-agent 工作流
        top_candidates = candidates[:20]  # 给 agent 看 top-20（从100个中）

        candidate_text = "SASRec模型推荐结果（按得分降序）:\n"
        for c in top_candidates:
            title = c.get('title') or '未知商品'
            candidate_text += f"  排名{c['rank']}: {c['item_id']} (得分: {c['score']:.4f}) - {title}\n"

        user_prompt = f"""
请根据用户的历史交互序列和模型推荐结果进行商品推荐分析：
- 用户历史交互商品: {item_sequence}
- 需要从以下候选商品中选出最佳推荐

{candidate_text}

请各位专家分析并从上述候选商品中选出最终推荐结果和推荐原因。
注意：你必须从上述候选商品列表中选择一个商品ID作为推荐结果。
"""

        initial_state = {
            "messages": [HumanMessage(content=user_prompt, name="user")],
            "item_sequence": item_sequence,
            "top_k": len(top_candidates),
        }

        # 简化工作流：跳过 Sequence_Recommender（候选已给定）
        graph = build_recommendation_graph(include_sequence_recommender=False)
        final_result, _ = _run_graph(graph, initial_state)

        product_id = final_result.get("recommended_product", "unknown")
        product_title = final_result.get("product_title", "")

        # 与 /recommend 路径一致：LLM 未返回标题时从本地文件兜底
        if not product_title or product_title == "未知商品":
            product_title = get_item_title(product_id)

        # BUG-4: LLM 可能把 confidence 返成字符串，强转防消费 ValueError
        try:
            confidence = float(final_result.get('confidence', 0.5) or 0)
        except (TypeError, ValueError):
            confidence = 0.5

        return jsonify({
            'success': True,
            'recommendation': {
                'recommended_product': product_id,
                'product_title': product_title,
                'recommendation_reason': final_result.get('recommendation_reason', ''),
                'confidence': confidence,
            }
        }), 200

    except Exception as e:
        logger.exception("Recommendation from candidates failed")
        return jsonify({
            'success': False,
            'message': f'Recommendation from candidates failed: {str(e)}'
        }), 500


@recommendation_bp.route('/recommend/chat-messages', methods=['GET'])
def get_recommendation_messages():
    """获取推荐对话消息（用于前端展示）"""
    return jsonify({
        'leftChats': [
            '序列推荐分析...',
            '用户行为分析...',
        ],
        'rightChats': [
            '商品特征分析...',
            '综合推荐结果...',
        ]
    })


@recommendation_bp.route('/recommend/health', methods=['GET'])
def health_check():
    """健康检查"""
    import requests
    try:
        # ★2026-07-27 修:原来硬编码 "http://127.0.0.1:8200/health",不读 env。
        #   在 K8S 里 sasrec 是独立 pod(SASREC_API_URL=http://sasrec:8200),本 pod 内没有 8200 端口
        #   → 每次 readiness(10s)/liveness(20s)探针打本端点都触发一次必然失败的 loopback 请求
        #   → OTel requests instrumentation 自动标 ERROR span(实测 1012 条/108 case),
        #   且本端点报的 sasrec 状态恒为 unavailable(假的)。
        _sasrec_base = os.environ.get("SASREC_API_URL", "http://127.0.0.1:8200")
        response = requests.get(f"{_sasrec_base}/health", timeout=5)
        sasrec_status = response.json() if response.status_code == 200 else {"status": "error"}
    except Exception:
        sasrec_status = {"status": "unavailable"}

    return jsonify({
        'recommendation_system': 'healthy',
        'sasrec_service': sasrec_status
    })
