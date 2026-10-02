"""
评论微服务 review_service
使用 Flask + MySQL
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

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 兜底 root logger 配置,确保 OTel init 块的 logger.info 在 LoggingInstrumentor
# 尚未 set_logging_format 之前也能输出(若 LoggingInstrumentor 后续调 basicConfig,
# 因 root 已有 handler 会 no-op,不冲突)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# review_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: review_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "review_service")
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
            logger.info("[otel] review_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] review_service instrumented")
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
    pool_name="review_pool",
    pool_size=3,
    **DB_CONFIG
)

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
# 评论
# ============================================================

def _refresh_item_rating(cursor, item_id):
    """重算 items.rating / review_count(等价 shop_web 的 refresh_item_rating,只统计 approved)。"""
    cursor.execute(
        """
        UPDATE items SET
            rating = (SELECT ROUND(AVG(rating), 2) FROM reviews WHERE item_id = %s AND status = 'approved'),
            review_count = (SELECT COUNT(*) FROM reviews WHERE item_id = %s AND status = 'approved')
        WHERE item_id = %s
        """,
        (item_id, item_id, item_id),
    )


@app.route('/api/reviews', methods=['POST'])
@handle_db_error
def create_review():
    """创建评论(由 shop_web 在完成买家鉴权/归属校验后代理调用)。"""
    data = request.get_json() or {}
    order_item_id = data.get('order_item_id')
    user_token = data.get('user_token')
    item_id = data.get('item_id')
    rating = data.get('rating')
    content = data.get('content')
    images = data.get('images')
    is_anonymous = 1 if data.get('is_anonymous') else 0

    # 校验
    if not order_item_id or not user_token or not item_id or rating is None:
        return jsonify({'success': False, 'message': 'order_item_id, user_token, item_id, rating are required'}), 400
    if isinstance(rating, bool) or not isinstance(rating, int) or rating < 1 or rating > 5:
        return jsonify({'success': False, 'message': 'Rating must be an integer between 1 and 5'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 幂等/唯一性:order_item_id 已有评论则拒绝(服务为 UNIQUE 约束的权威)
        cursor.execute("SELECT id FROM reviews WHERE order_item_id = %s", (order_item_id,))
        if cursor.fetchone() is not None:
            return jsonify({'success': False, 'message': 'This item has already been reviewed'}), 400

        content_val = content.strip() if (content and content.strip()) else None
        images_val = json.dumps(images) if images else None

        cursor.execute(
            """
            INSERT INTO reviews
                (order_item_id, user_token, item_id, rating, content, images, is_anonymous, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'approved')
            """,
            (order_item_id, user_token, item_id, rating, content_val, images_val, is_anonymous),
        )
        new_id = cursor.lastrowid
        _refresh_item_rating(cursor, item_id)
        conn.commit()
        return jsonify({'success': True, 'message': 'Review submitted successfully', 'review_id': new_id})
    finally:
        cursor.close()
        conn.close()

@app.route('/api/reviews/<int:review_id>/status', methods=['POST'])
@handle_db_error
def change_review_status(review_id):
    """管理端审核:改评论状态 + 重算评分(由 shop_web 鉴权后代理调用)。"""
    data = request.get_json() or {}
    new_status = data.get('status')
    if new_status not in ('approved', 'rejected', 'hidden'):
        return jsonify({'success': False, 'message': 'Invalid status'}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT item_id, status FROM reviews WHERE id = %s", (review_id,))
        row = cursor.fetchone()
        if row is None:
            return jsonify({'success': False, 'message': 'Review not found'}), 404
        item_id, old_status = row[0], row[1]
        # 幂等短路:同态重复审核直接返回,避免无谓 UPDATE + rating 重算
        if old_status == new_status:
            return jsonify({'success': True, 'old_status': old_status,
                            'new_status': new_status, 'item_id': item_id})
        cursor.execute("UPDATE reviews SET status = %s WHERE id = %s", (new_status, review_id))
        _refresh_item_rating(cursor, item_id)
        conn.commit()
        return jsonify({'success': True, 'old_status': old_status, 'new_status': new_status, 'item_id': item_id})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/reviews/<int:review_id>', methods=['DELETE'])
@handle_db_error
def delete_review(review_id):
    """管理端删除评论(review_replies 靠 DB 级联自动删) + 重算评分。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT item_id FROM reviews WHERE id = %s", (review_id,))
        row = cursor.fetchone()
        if row is None:
            return jsonify({'success': False, 'message': 'Review not found'}), 404
        item_id = row[0]
        cursor.execute("DELETE FROM reviews WHERE id = %s", (review_id,))
        _refresh_item_rating(cursor, item_id)
        conn.commit()
        return jsonify({'success': True, 'item_id': item_id})
    finally:
        cursor.close()
        conn.close()

@app.route('/api/reviews/<int:review_id>/reply', methods=['POST'])
@handle_db_error
def reply_review(review_id):
    """商家回复评论(由 shop_web 在完成商家归属校验后代理调用)。"""
    data = request.get_json() or {}
    merchant_id = data.get('merchant_id')
    content = (data.get('content') or '').strip()
    if not merchant_id or not content:
        return jsonify({'success': False, 'message': 'merchant_id and content are required'}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM reviews WHERE id = %s", (review_id,))
        if cursor.fetchone() is None:
            return jsonify({'success': False, 'message': 'Review not found'}), 404
        cursor.execute("SELECT id FROM review_replies WHERE review_id = %s", (review_id,))
        if cursor.fetchone() is not None:
            return jsonify({'success': False, 'message': 'Already replied'}), 400
        cursor.execute(
            "INSERT INTO review_replies (review_id, merchant_id, content) VALUES (%s, %s, %s)",
            (review_id, merchant_id, content),
        )
        new_id = cursor.lastrowid
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Reply submitted',
            'reply': {'id': new_id, 'content': content,
                      'date': datetime.now().strftime('%Y-%m-%d')},
        })
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
        'service': 'review_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('REVIEW_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('REVIEW_SERVICE_PORT', '5003'))

    print("=" * 60)
    print("评论微服务 review_service")
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
            _NACOS_SERVICE_NAME = "review_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5003
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
