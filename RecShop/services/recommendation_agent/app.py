"""
商品推荐多Agent系统 - 独立Flask应用入口
运行方式: python app.py
默认端口: 5001
"""

from flask import Flask, send_from_directory
from flask_cors import CORS
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

from workflow import recommendation_bp

# logging.basicConfig 前移到 OTel init 之前, 使 OTel init 块内的
# logger.info("[otel] ...") 能稳定输出(本服务无 print/logger handler 配置)。
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
CORS(app)

app.register_blueprint(recommendation_bp)

# ==================== OpenTelemetry init ====================
# 镜像 llm_rerank_service / backend_api 的 bootstrap 模式(trace+metric+log 三件套)。
# 守卫差异: llm_rerank_service 用 WERKZEUG_RUN_MAIN/debug 守卫躲 reloader 双初始化,
# 但本服务下方 app.run(debug=False) 硬编码、无 reloader、无 debug 环境变量,
# 照抄那个守卫会让两个分支都为 false 导致 init 永不执行。这里只用 OTEL_ENABLED 守卫。
# OTEL_SERVICE_NAME 用 setdefault 注入,不写进 .env。
if os.environ.get("OTEL_ENABLED", "true").lower() == "true":
    os.environ.setdefault("OTEL_SERVICE_NAME", "recommendation_agent")
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
        # 可选本地 JSONL span exporter（env 门控 SPAN_FILE）：
        # 与 OTLP BSP 并存、互不影响；用 SimpleSpanProcessor（非 Batch）保证每条 span
        # 在 END 时即时 flush 落盘（过夜无人值守、按 trace_id 归窗的硬前提）。
        # 临时实例把 OTLP endpoint 指向不存在端口让 BSP 静默失败、只走本地 JSONL，
        # 不污染持久栈 Jaeger。正常持久栈不设 SPAN_FILE → 此分支不生效、行为不变。
        _span_file = os.environ.get("SPAN_FILE", "").strip()
        if _span_file:
            try:
                from opentelemetry.sdk.trace.export import SimpleSpanProcessor as _OtelSSP
                from local_span_exporter import LocalJSONLSpanExporter as _LocalJSONL
                _otel_provider.add_span_processor(_OtelSSP(_LocalJSONL(_span_file)))
                logger.info(f"[otel] local JSONL span exporter -> {_span_file}")
            except Exception as _local_e:
                logger.warning(f"[otel] local JSONL span exporter init failed: {_local_e}")
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
            logger.info("[otel] recommendation_agent log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()    # health_check + agents/tools.py 走 requests 调 SASRec
        _HttpxInstr().instrument()       # LangChain/OpenAI SDK 走 httpx 调 DeepSeek
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] recommendation_agent instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
# ============================================================

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory(app.static_folder, filename)

@app.route('/')
def index():
    """返回前端页面"""
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api')
def api_info():
    """API信息"""
    return {
        'service': '商品推荐多Agent系统',
        'version': '1.0.0',
        'endpoints': {
            'POST /recommend': '获取商品推荐',
            'POST /recommend/from_candidates': '从候选列表中选最佳推荐（离线公平评估用）',
            'GET /recommend/health': '健康检查',
            'GET /recommend/chat-messages': '获取对话消息'
        }
    }

if __name__ == '__main__':
    host = os.environ.get('RECOMMENDATION_HOST', '0.0.0.0')
    port = int(os.environ.get('RECOMMENDATION_PORT', '5001'))
    
    print("="*60)
    print("商品推荐多Agent系统启动中...")
    print("="*60)
    print(f"服务地址: http://{host}:{port}")
    print("API文档:")
    print("  POST /recommend - 获取商品推荐")
    print("  GET /recommend/health - 健康检查")
    print("="*60)
    app.run(host=host, port=port, debug=False)
