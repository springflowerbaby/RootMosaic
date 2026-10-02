"""
LLM Rerank Service — 最小 Flask API。

启动方式：
    cd services/llm_rerank_service
    python app.py

测试方式：
    curl -X POST http://127.0.0.1:5002/rerank \
      -H "Content-Type: application/json" \
      -d '{
        "user_history": [
          {"item_id": "B001", "title": "Sony WH-1000XM5 Headphones"},
          {"item_id": "B002", "title": "Bose QuietComfort 45"}
        ],
        "candidates": [
          {"item_id": "B003", "title": "Apple AirPods Pro", "score": 6.12},
          {"item_id": "B004", "title": "Samsung Galaxy Buds2", "score": 5.87},
          {"item_id": "B005", "title": "JBL Charge 5 Speaker", "score": 5.41}
        ]
      }'
"""

import os
import sys
import logging
from pathlib import Path

from flask import Flask, request, jsonify

# 将项目根目录加入 sys.path，以便加载 .env
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT_ROOT))

# 加载 .env
from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")

from reranker import rerank

# ============================================================
# Flask App
# ============================================================

app = Flask(__name__)

# 选项 A: 把 logging.basicConfig 前移到 OTel init 之前,
# 使 OTel init 块内的 logger.info("[otel] ...") 在 werkzeug reloader
# 子进程下也能稳定输出(werkzeug 会吞 print 但不吞 logger handler)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ==================== OpenTelemetry init ====================
# RERANK_DEBUG: 服务专属 debug 变量,见 app.py 末尾 debug 局部变量来源
# (`debug = os.getenv("RERANK_DEBUG", "false").lower() == "true"`)。
# 守卫与下方 Nacos 守卫同思路,避免父进程残留 BatchSpanProcessor 被 reloader 替换。
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or os.getenv("RERANK_DEBUG", "false").lower() != "true"
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "llm_rerank_service")
    # M1 metric interval: validate before the optional OTel init handler.
    import sys as _otel_config_sys
    from pathlib import Path as _OtelConfigPath
    _otel_config_root = str(_OtelConfigPath(__file__).resolve().parents[2])
    if _otel_config_root not in _otel_config_sys.path:
        _otel_config_sys.path.insert(0, _otel_config_root)
    from shared.otel_metric_config import metric_export_interval_millis as _otel_metric_interval
    _otel_metric_export_interval_ms = _otel_metric_interval()
    try:
        import atexit
        from opentelemetry import trace as _otel_trace
        from opentelemetry.sdk.resources import Resource as _OtelResource
        from opentelemetry.sdk.trace import TracerProvider as _OtelTracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor as _OtelBSP
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as _OTLPSpanExporter
        from opentelemetry.instrumentation.flask import FlaskInstrumentor as _FlaskInstr
        from opentelemetry.instrumentation.requests import RequestsInstrumentor as _RequestsInstr
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor as _HttpxInstr
        from opentelemetry.instrumentation.logging import LoggingInstrumentor as _LoggingInstr
        # Resource 提取成局部变量, TracerProvider / MeterProvider 共用(同 service.name)
        _resource = _OtelResource.create({"service.name": os.environ["OTEL_SERVICE_NAME"]})
        _otel_provider = _OtelTracerProvider(resource=_resource)
        _otel_provider.add_span_processor(_OtelBSP(_OTLPSpanExporter()))
        _otel_trace.set_tracer_provider(_otel_provider)
        # 进程退出时 flush BSP 队列,确保紧急退出(sys.exit/SIGTERM)未发送的 span 不丢
        atexit.register(_otel_provider.shutdown)

        # --- MeterProvider(与 TracerProvider 共存, 共用同一 Resource/OTLP endpoint) ---
        from opentelemetry import metrics as _otel_metrics
        from opentelemetry.sdk.metrics import MeterProvider as _OtelMeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader as _OtelPEMR
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as _OTLPMetricExporter
        _meter_reader = _OtelPEMR(_OTLPMetricExporter(), export_interval_millis=_otel_metric_export_interval_ms)
        _meter_provider = _OtelMeterProvider(resource=_resource, metric_readers=[_meter_reader])
        _otel_metrics.set_meter_provider(_meter_provider)
        atexit.register(_meter_provider.shutdown)

        # --- LoggerProvider + LoggingHandler bridge(把 Python logging 桥接到 OTLP->Loki) ---
        # logs SDK 仍 experimental(opentelemetry.sdk._logs 带下划线),整块独立 try/except 容错:
        # 失败只 warning 不中断服务,也不影响已就绪的 Tracer/Meter provider。
        # 与 LoggingInstrumentor 分工: 后者注 trace_id 到 stdout 日志格式(下方保留),
        # LoggingHandler 负责把 log record 导出为 OTLP(SDK 自动带 active span 的 trace_id/span_id)。
        try:
            from opentelemetry import _logs as _otel_logs
            from opentelemetry.sdk._logs import LoggerProvider as _OtelLoggerProvider, LoggingHandler as _OtelLoggingHandler
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor as _OtelBLRP
            from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as _OTLPLogExporter
            _logger_provider = _OtelLoggerProvider(resource=_resource)  # 复用同一 _resource
            _logger_provider.add_log_record_processor(_OtelBLRP(_OTLPLogExporter()))  # endpoint 从 .env 读
            _otel_logs.set_logger_provider(_logger_provider)
            atexit.register(_logger_provider.shutdown)
            # 挂到 root logger,所有模块 logging.* 调用都桥接导出
            _otel_log_handler = _OtelLoggingHandler(level=logging.INFO, logger_provider=_logger_provider)
            logging.getLogger().addHandler(_otel_log_handler)
            logger.info("[otel] llm_rerank_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _HttpxInstr().instrument()  # reranker 内部走 OpenAI SDK → httpx → DeepSeek
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] llm_rerank_service instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
# ============================================================


@app.route("/rerank", methods=["POST"])
def rerank_endpoint():
    """
    POST /rerank

    请求体 JSON：
    {
        "user_history": [
            {"item_id": "...", "title": "..."},
            ...
        ],
        "candidates": [
            {"item_id": "...", "title": "...", "score": 5.93},
            ...
        ]
    }

    响应 JSON：
    {
        "success": true,
        "result": {
            "selected_item_id": "...",
            "selected_title": "...",
            "reason": "...",
            "source": "llm" | "fallback"
        }
    }
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"success": False, "error": "请求体必须是 JSON"}), 400

    user_history = data.get("user_history", [])
    candidates = data.get("candidates", [])

    if not candidates:
        return jsonify({"success": False, "error": "candidates 不能为空"}), 400

    # 基本字段检查
    for i, c in enumerate(candidates):
        if "item_id" not in c or "title" not in c:
            return jsonify({
                "success": False,
                "error": f"candidates[{i}] 缺少 item_id 或 title 字段",
            }), 400

    try:
        result = rerank(user_history, candidates)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logger.exception("rerank 处理异常")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "service": "llm_rerank_service",
    })


if __name__ == "__main__":
    port = int(os.getenv("RERANK_PORT", 5002))
    host = os.getenv("RERANK_HOST", "0.0.0.0")
    debug = os.getenv("RERANK_DEBUG", "false").lower() == "true"
    logger.info(f"LLM Rerank Service 启动: http://{host}:{port}")

    # ---- Nacos 注册 (Phase 1 + Fix) ----
    # 仅在 werkzeug reloader 的子进程或非 debug 模式下注册,
    # 避免父进程注册后被 reloader 替换导致 atexit 注销。
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not debug:
        import atexit as _atexit
        try:
            from shared.nacos_client import register_service, deregister_service
            _NACOS_SERVICE_NAME = "llm_rerank_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5002
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            logger.warning(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=debug)
