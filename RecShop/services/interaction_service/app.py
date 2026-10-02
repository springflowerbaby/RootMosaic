"""
交互埋点微服务 interaction_service
使用 Flask + MySQL
拥有数据: interactions(行为埋点写中枢: view/click/purchase/add_to_cart) —— 单条写入 + 用户最近交互读取
被多端 fan-in(无下游调用,直写 DB)
"""
from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling
import json
import os
import time
import logging
from pathlib import Path
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 兜底 root logger 配置,确保 OTel init 块的 logger.info 在 LoggingInstrumentor
# 尚未 set_logging_format 之前也能输出(若 LoggingInstrumentor 后续调 basicConfig,
# 因 root 已有 handler 会 no-op,不冲突)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# interaction_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: interaction_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "interaction_service")
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
            logger.info("[otel] interaction_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] interaction_service instrumented")
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
    pool_name="interaction_pool",
    pool_size=3,
    **DB_CONFIG
)

# interactions 表 interaction_type ENUM 合法值(与 schema 对齐)
_VALID_ACTIONS = ('view', 'click', 'purchase', 'rating', 'add_to_cart', 'remove_from_cart')

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

# ============================================================
# 交互埋点
# ============================================================

@app.route('/api/interactions', methods=['POST'])
@handle_db_error
def create_interaction():
    """记录一条用户交互(由 shop_web 在完成鉴权后代理调用)。

    body: {user_token, item_id, action,
           (可选)session_id, (可选)source, (可选)quantity, (可选)price}
    action 须为 interactions.interaction_type 的合法 ENUM 值。
    quantity/price 为 purchase 打点的加法可选字段:缺省时不入 INSERT 列,
    行为与旧契约完全一致(quantity 由表默认值 1 兜底)——向后兼容。
    单事务 INSERT,timestamp 由服务端按毫秒生成。
    """
    data = request.get_json() or {}
    user_token = data.get('user_token')
    item_id = data.get('item_id')
    action = data.get('action')
    session_id = data.get('session_id')
    source = data.get('source') or 'direct'
    quantity = data.get('quantity')
    price = data.get('price')

    # 校验
    if not user_token or not item_id or not action:
        return jsonify({'success': False, 'message': 'user_token, item_id, action are required'}), 400
    if action not in _VALID_ACTIONS:
        return jsonify({'success': False, 'message': f'Invalid action; must be one of {_VALID_ACTIONS}'}), 400

    ts = int(time.time() * 1000)
    columns = ['user_token', 'item_id', 'interaction_type', 'session_id', 'source', 'timestamp']
    values = [user_token, item_id, action, session_id, source, ts]
    if quantity is not None:
        columns.append('quantity')
        values.append(quantity)
    if price is not None:
        columns.append('price')
        values.append(price)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 写前校验 user_token 存在(未知 user_token → 404, 不写埋点)
        cursor.execute("SELECT 1 FROM users WHERE user_token = %s LIMIT 1", (user_token,))
        if cursor.fetchone() is None:
            return jsonify({'success': False, 'message': 'Unknown user_token'}), 404
        # 写前校验 item_id 存在(防 interactions_ibfk_2: item_id→items 触发 1452 → 500)
        cursor.execute("SELECT 1 FROM items WHERE item_id = %s LIMIT 1", (item_id,))
        if cursor.fetchone() is None:
            return jsonify({'success': False, 'message': 'Unknown item_id'}), 404
        cursor.execute(
            'INSERT INTO interactions ({cols}) VALUES ({placeholders})'.format(
                cols=', '.join(columns),
                placeholders=', '.join(['%s'] * len(columns)),
            ),
            tuple(values),
        )
        new_id = cursor.lastrowid
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Interaction recorded',
            'interaction_id': new_id,
            'timestamp': ts,
        })
    finally:
        cursor.close()
        conn.close()


@app.route('/api/interactions', methods=['GET'])
@handle_db_error
def list_interactions():
    """读取某用户最近交互(按 timestamp 倒序)。

    query params: user_token(必填)、limit(默认20,最大100)。
    """
    user_token = (request.args.get('user_token') or '').strip()
    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400
    try:
        limit = int(request.args.get('limit', 20))
    except (TypeError, ValueError):
        limit = 20
    limit = min(max(limit, 1), 100)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, user_token, item_id, interaction_type, rating, duration,
                   quantity, price, source, session_id, timestamp, created_at
            FROM interactions
            WHERE user_token = %s
            ORDER BY timestamp DESC
            LIMIT %s
            """,
            (user_token, limit),
        )
        rows = cursor.fetchall()
        interactions = []
        for r in rows:
            interactions.append({
                'id': r.get('id'),
                'user_token': r.get('user_token'),
                'item_id': r.get('item_id'),
                'action': r.get('interaction_type'),
                'interaction_type': r.get('interaction_type'),
                'rating': float(r['rating']) if r.get('rating') is not None else None,
                'duration': r.get('duration'),
                'quantity': r.get('quantity'),
                'price': float(r['price']) if r.get('price') is not None else None,
                'source': r.get('source'),
                'session_id': r.get('session_id'),
                'timestamp': r.get('timestamp'),
                'created_at': r['created_at'].isoformat() if r.get('created_at') else None,
            })
        return jsonify({'success': True, 'count': len(interactions), 'interactions': interactions})
    finally:
        cursor.close()
        conn.close()

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
        'service': 'interaction_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('INTERACTION_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('INTERACTION_SERVICE_PORT', '5020'))

    print("=" * 60)
    print("交互埋点微服务 interaction_service")
    print("=" * 60)
    print(f"数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"服务地址: http://{host}:{port}")
    print("=" * 60)

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
            _NACOS_SERVICE_NAME = "interaction_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5020
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
