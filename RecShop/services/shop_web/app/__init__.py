from flask import Flask
from config import config

def create_app(config_name='default'):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    # ==================== OpenTelemetry init ====================
    # 必须在 db.init_app(app) 之前调用 SQLAlchemyInstrumentor().instrument(),
    # 让它先 monkey-patch sqlalchemy.create_engine,后续 db.engine 才能被自动追踪
    import os
    import logging as _logging
    # 兜底 root logger 配置,确保下面 logger.info 在 werkzeug 配置 handler 之前也能输出
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    _logger = _logging.getLogger(__name__)
    # FLASK_DEBUG: shop_web 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
    # 参考 run.py SHOPWEB_DEBUG 透传给 app.run(debug=...) 由 Flask 内部置 FLASK_DEBUG。
    # 守卫仅在 werkzeug reloader 子进程或非 debug 模式下初始化,避免父进程被 reloader 替换。
    # 默认值与 run.py 一致(FE-05 后 SHOPWEB_DEBUG 默认 'False'):未设 env 时按非 debug
    # 处理,直接在主进程注册 OTel(无 reloader 子进程)。
    # 注:此默认值 'false' 仅与 run.py 做语义对齐(cosmetic),对运行栈无行为变化——
    # 运行栈有 .env(OTEL_ENABLED=true)+ debug reloader 子进程 WERKZEUG_RUN_MAIN,注册路径不变;
    # 仅在"完全无 .env 的裸启"下,此默认值才决定主进程是否注册。
    if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
        os.environ.get("WERKZEUG_RUN_MAIN") == "true"
        or os.environ.get("SHOPWEB_DEBUG", "false").lower() != "true"
    ):
        os.environ.setdefault("OTEL_SERVICE_NAME", "shop_web")
        # M1 metric interval: validate before the optional OTel init handler.
        import sys as _otel_config_sys
        from pathlib import Path as _OtelConfigPath
        _otel_config_root = str(_OtelConfigPath(__file__).resolve().parents[3])
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
            from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor as _SAInstr
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
                _otel_log_handler = _OtelLoggingHandler(level=_logging.INFO, logger_provider=_logger_provider)
                _logging.getLogger().addHandler(_otel_log_handler)
                _logger.info("[otel] shop_web log bridge installed")
            except Exception as _otel_log_e:
                _logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

            # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
            _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
            _RequestsInstr().instrument()
            # 不传 engine,让 SQLAlchemyInstrumentor 全局 monkey-patch create_engine
            # → 紧随其后的 db.init_app(app) 创建的 engine 自动被追踪
            _SAInstr().instrument()
            _HttpxInstr().instrument()   # OpenAI SDK 走 httpx → DeepSeek 调用自动埋
            _LoggingInstr().instrument(set_logging_format=True)
            _logger.info("[otel] shop_web instrumented")
        except Exception as _otel_e:
            _logger.warning(f"[otel] init failed (ignored): {_otel_e}")
    # ============================================================

    from app.extensions import db, login_manager
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'buyer.login'
    
    @login_manager.user_loader
    def load_user(user_id):
        """根据 session 中的角色标识加载不同模型"""
        from flask import session
        from app.models import User, Merchant, Admin
        
        role = session.get('_user_role', 'buyer')
        if role == 'merchant':
            return Merchant.query.get(int(user_id))
        elif role == 'admin':
            return Admin.query.get(int(user_id))
        else:
            return User.query.get(int(user_id))

    @app.context_processor
    def inject_auth_context():
        from app.auth import get_current_role, is_admin, is_buyer, is_merchant
        from app.models import Announcement
        role = get_current_role()
        # FE-01: announcement 查询失败(如 announcements 表缺/库异常)不应拖垮
        # 所有页面(含登录页)——context_processor 抛错会让任意模板渲染 500。
        # 失败时降级为无公告横幅、页面照常渲染(模板已按可空条件渲染)。
        try:
            latest_buyer_announcement = (
                Announcement.query
                .filter(Announcement.status == 'published')
                .order_by(
                    Announcement.sort_order.desc(),
                    Announcement.published_at.desc(),
                    Announcement.id.desc(),
                )
                .first()
            )
        except Exception as _ann_e:
            latest_buyer_announcement = None
            _logger.warning("[ctx] announcement query failed (rendering without banner): %s", _ann_e)
        return {
            'current_role': role,
            'is_buyer_user': is_buyer(),
            'is_merchant_user': is_merchant(),
            'is_admin_user': is_admin(),
            'latest_buyer_announcement': latest_buyer_announcement,
        }
    
    # Register Blueprints
    from app.buyer import buyer_bp
    from app.merchant import merchant_bp
    from app.admin import admin_bp
    
    app.register_blueprint(buyer_bp)
    app.register_blueprint(merchant_bp)
    app.register_blueprint(admin_bp)

    # 健康检查（照 backend_api 模式；shop_web 用 ORM，此处不查库，纯返回，供 start_all 探活）
    @app.route('/health', methods=['GET'])
    def health_check():
        from flask import jsonify
        return jsonify({"status": "ok", "service": "shop_web"})

    return app
