"""
定价微服务 pricing_service
使用 Flask + MySQL
拥有数据: price_rules(item_id PK, markup, tax_rate, updated_at) —— 商品定价规则
深度链: GET /api/pricing/<id> 经 HTTP 调 catalog_service GET /api/items/<id> 取基价(pricing→catalog)
算价: final = base * (1 + markup) * (1 + tax_rate),无规则时 markup/tax_rate=0(用 catalog 基价)
"""
from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling
import json
import os
import logging
from pathlib import Path
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv
import requests

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 兜底 root logger 配置,确保 OTel init 块的 logger.info 在 LoggingInstrumentor
# 尚未 set_logging_format 之前也能输出(若 LoggingInstrumentor 后续调 basicConfig,
# 因 root 已有 handler 会 no-op,不冲突)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# pricing_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: pricing_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "pricing_service")
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
        from opentelemetry.instrumentation.mysql import MySQLInstrumentor as _MySQLInstr
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
            logger.info("[otel] pricing_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] pricing_service instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
        _mysql_instrumentor = None
# ============================================================
CORS(app)

# 配置 - 从环境变量读取
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', '3306')),
    'user': os.environ.get('DB_USER', 'root'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'database': os.environ.get('DB_NAME', 'shopify2')
}

if not DB_CONFIG['password']:
    raise ValueError('数据库密码未配置，请设置 DB_PASSWORD 环境变量')

# 数据库连接池
db_pool = pooling.MySQLConnectionPool(
    pool_name="pricing_pool",
    pool_size=3,
    **DB_CONFIG
)

# 下游 catalog_service 地址(深度链 pricing→catalog 取商品基价)。
# 优先用 service_discovery(Nacos)解析,失败回退 env CATALOG_SERVICE_URL,再默认 localhost:5005。
def get_catalog_service_url():
    """获取 catalog_service 的 URL(Nacos 优先,回退 env / 默认)。"""
    fb = os.environ.get('CATALOG_SERVICE_URL', 'http://127.0.0.1:5005')
    try:
        import sys as _sys
        if str(_PROJECT_ROOT) not in _sys.path:
            _sys.path.insert(0, str(_PROJECT_ROOT))
        from shared.nacos_client import get_service_url as _nacos_get_service_url
        return _nacos_get_service_url("catalog_service", fallback_url=fb) or fb
    except Exception:
        return fb

# ============================================================
# 工具函数
# ============================================================

def get_db_connection():
    """获取数据库连接

    Audit 后补丁: MySQLInstrumentor.instrument() 只 patch mysql.connector.connect(),
    pool 内部直接构造 MySQLConnection 绕开 patched connect(), 导致 SQL span 全部丢失。
    用 instrument_connection() 给 pool 拿到的每个 connection 单独包一层 (dbapi 兜底)。
    """
    conn = db_pool.get_connection()
    if _mysql_instrumentor is not None:
        try:
            conn = _mysql_instrumentor.instrument_connection(conn)
        except Exception:
            pass
    return conn

def handle_db_error(f):
    """数据库错误处理装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except HTTPException:
            # 放行 werkzeug 4xx(畸形 JSON/缺 Content-Type 触发的 400/415、abort() 等),
            # 让其按正确状态码冒泡, 不被下面兜成 500。
            raise
        except mysql.connector.Error as e:
            logger.error('Database error in %s: %s', f.__name__, e)
            return jsonify({'error': '数据库错误'}), 500
        except Exception as e:
            logger.error('Server error in %s: %s', f.__name__, e)
            return jsonify({'error': '服务器错误'}), 500
    return decorated_function


def _ensure_schema():
    """幂等建表 price_rules(本服务自有表)。

    在 __main__ 里 app.run 之前执行一次。CREATE TABLE IF NOT EXISTS 幂等,
    不动现有表;单事务 commit、finally 关连接。
    markup / tax_rate 为小数倍率(如 0.1000 表示 +10%),默认 0。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS price_rules (
                item_id      VARCHAR(64) NOT NULL,
                markup       DECIMAL(10, 4) NOT NULL DEFAULT 0,
                tax_rate     DECIMAL(10, 4) NOT NULL DEFAULT 0,
                updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (item_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        logger.info("[pricing] price_rules schema ensured")
    finally:
        cursor.close()
        conn.close()


def _fetch_base_price_from_catalog(item_id):
    """深度链 pricing→catalog: HTTP 调 catalog_service 取单品详情(含 price)取基价。

    返回 (base_price: float, found: bool)。
    下游不可用 / 商品不存在 → (None, False),由调用方决定降级(返回 502 / Item not found)。
    """
    try:
        resp = requests.get(
            f'{get_catalog_service_url()}/api/items/{item_id}',
            timeout=8,
        )
        if resp.status_code == 404:
            return None, False
        if resp.status_code != 200:
            logger.warning('[pricing] catalog_service returned %s for item %s', resp.status_code, item_id)
            return None, False
        body = resp.json() or {}
        if not body.get('success'):
            return None, False
        item = body.get('item') or {}
        price = item.get('price')
        if price is None:
            return None, False
        return float(price), True
    except requests.exceptions.RequestException as e:
        logger.warning('[pricing] catalog_service unavailable for item %s: %s', item_id, e)
        return None, None  # None found => 下游不可用(区别于商品不存在)
    except Exception as e:
        logger.warning('[pricing] catalog_service price fetch failed for %s: %s', item_id, e)
        return None, None


def _load_price_rule(item_id):
    """读自有表 price_rules,返回 (markup: float, tax_rate: float);无规则时 (0.0, 0.0)。"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT markup, tax_rate FROM price_rules WHERE item_id = %s",
            (item_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return 0.0, 0.0
        markup = float(row['markup']) if row.get('markup') is not None else 0.0
        tax_rate = float(row['tax_rate']) if row.get('tax_rate') is not None else 0.0
        return markup, tax_rate
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 定价
# ============================================================

@app.route('/api/pricing/<item_id>', methods=['GET'])
@handle_db_error
def get_pricing(item_id):
    """算价:深度链 pricing→catalog 取基价,叠加自有 price_rules 的 markup/tax。

    final = base * (1 + markup) * (1 + tax_rate)
    tax   = base * (1 + markup) * tax_rate (即不含税到含税的税额部分)
    无规则时 markup/tax_rate=0 → final == base(用 catalog 基价)。
    下游 catalog 不可用 → 502 降级;商品不存在 → 404。
    """
    # 深度链 pricing→catalog: 取基价
    base_price, found = _fetch_base_price_from_catalog(item_id)
    if found is None:
        # 下游 catalog_service 不可用 → 合理降级(502)
        return jsonify({
            'success': False,
            'message': 'Pricing upstream (catalog_service) unavailable',
        }), 502
    if not found:
        return jsonify({'success': False, 'message': 'Item not found'}), 404

    # 读自有表定价规则(无则 0)
    markup, tax_rate = _load_price_rule(item_id)

    marked_up = base_price * (1 + markup)   # 加价后(未含税)
    tax = marked_up * tax_rate              # 税额
    final = marked_up + tax                 # 最终售价(含税)

    return jsonify({
        'success': True,
        'item_id': item_id,
        'base': round(base_price, 2),
        'markup': markup,
        'tax_rate': tax_rate,
        'tax': round(tax, 2),
        'final': round(final, 2),
    })

# ============================================================
# 健康检查
# ============================================================

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    try:
        conn = get_db_connection()
        conn.close()
        db_status = 'healthy'
    except Exception:
        db_status = 'unhealthy'
    return jsonify({
        'status': 'healthy' if db_status == 'healthy' else 'degraded',
        'service': 'pricing_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('PRICING_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('PRICING_SERVICE_PORT', '5014'))

    print("=" * 60)
    print("定价微服务 pricing_service")
    print("=" * 60)
    print(f"数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"服务地址: http://{host}:{port}")
    print("=" * 60)

    # 幂等建表(仅在真正跑业务的进程执行一次,避免 reloader 父进程重复建)。
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not _DEBUG:
        try:
            _ensure_schema()
        except Exception as _schema_e:
            print(f"[pricing] _ensure_schema 异常,已忽略: {_schema_e}")

    # ---- Nacos 注册 (Phase 1 + Fix) ----
    # 仅在 werkzeug reloader 的子进程(真正跑业务那个)或非 debug 模式下注册,
    # 避免父进程注册后被 reloader 替换导致 atexit 注销。
    _debug = _DEBUG
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not _debug:
        import sys as _sys
        import atexit as _atexit
        if str(_PROJECT_ROOT) not in _sys.path:
            _sys.path.insert(0, str(_PROJECT_ROOT))
        try:
            from shared.nacos_client import register_service, deregister_service
            _NACOS_SERVICE_NAME = "pricing_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5014
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
