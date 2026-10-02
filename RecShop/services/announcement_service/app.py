"""
公告微服务 announcement_service
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

# announcement_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: announcement_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "announcement_service")
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
            logger.info("[otel] announcement_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] announcement_service instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
        _mysql_instrumentor = None
# ============================================================
CORS(app)

# ==================== 故障注入钩子(env 门控, 默认全关) ====================
# 照 catalog_service 的范式: 三个 env 旋钮, 任一未设 = 与当前 announcement_service 字节级行为
# 一致(before_request 早 return / pool_size 仍 3)。仅供 chaos6x18 v3 的【临时 announcement
# 实例】使用; 持久 announcement@5009 不设这些 env, 零影响。
#   FAULT_DELAY_MS : 每请求进程内 sleep(ms), 抬 server span duration(application_latency)
#   FAULT_RAISE    : 请求入口抛异常 → 500(runtime_exception)
#   DB_POOL_SIZE   : env 化连接池大小(默认 3, 与现状一致; 缩到 1-2 配并发打爆 → PoolError)
import time as _time  # 故障注入 before_request 用(进程内 sleep); 仅本块新增, 不影响其它逻辑
_FAULT_DELAY_MS = int(os.environ.get("FAULT_DELAY_MS", "0") or 0)
_FAULT_RAISE = os.environ.get("FAULT_RAISE", "").strip().lower() in ("1", "true", "yes", "on")
# ============================================================

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
# pool_size 默认 3(与历史一致, 普惠零行为改变); DB_POOL_SIZE 仅临时实例传 1-2 配并发打爆。
db_pool = pooling.MySQLConnectionPool(
    pool_name="announcement_pool",
    pool_size=int(os.environ.get("DB_POOL_SIZE", "3")),
    **DB_CONFIG
)


# ============================================================
# 故障注入 before_request 钩子(env 门控, 默认零开销)
# ============================================================
@app.before_request
def _fault_inject_before_request():
    """在被 FlaskInstrumentor 计时的 server span 内注入故障(env 未设时早 return, 零开销)。

    照 catalog_service 同款语义: FAULT_DELAY_MS>0 进程内 sleep(抬 server p95);
    FAULT_RAISE 在请求入口抛 RuntimeError → Flask 默认转 500(不过 @handle_db_error)。
    env 都未设 → _FAULT_DELAY_MS=0 且 _FAULT_RAISE=False → 直接 return None, 与现状一致。
    /health 探活路由豁免, 否则 FAULT_RAISE 实例的 /health 也 500 → runner wait_health 永远超时。
    """
    if not (_FAULT_DELAY_MS > 0 or _FAULT_RAISE):
        return None  # 默认关: 零开销早 return
    if request.path == "/health":
        return None  # 探活豁免, 让 runner 能判临时实例就绪
    if _FAULT_DELAY_MS > 0:
        _time.sleep(_FAULT_DELAY_MS / 1000.0)
    if _FAULT_RAISE:
        raise RuntimeError("FAULT_RAISE injected error")

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
# 公告
# ============================================================

@app.route('/api/announcements', methods=['GET'])
@handle_db_error
def list_announcements():
    """获取已发布公告(供 buyer 只读)。可选 type 过滤,按 sort_order 倒序 + 创建时间倒序。"""
    ann_type = request.args.get('type')
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        sql = (
            "SELECT id, title, content, type, image_url, link_url, sort_order, "
            "status, published_at, created_by, created_at, updated_at "
            "FROM announcements WHERE status = 'published'"
        )
        params = []
        if ann_type:
            sql += " AND type = %s"
            params.append(ann_type)
        sql += " ORDER BY sort_order DESC, created_at DESC"
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        # datetime 列转字符串,确保 jsonify 可序列化
        for row in rows:
            for k in ('published_at', 'created_at', 'updated_at'):
                if row.get(k) is not None:
                    row[k] = row[k].strftime('%Y-%m-%d %H:%M:%S')
        return jsonify({'success': True, 'announcements': rows})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/announcements', methods=['POST'])
@handle_db_error
def create_announcement():
    """创建公告(由 shop_web 在完成管理端鉴权后代理调用),初始状态 draft。"""
    data = request.get_json() or {}
    title = (data.get('title') or '').strip()
    if not title:
        return jsonify({'success': False, 'message': 'title is required'}), 400

    content = (data.get('content') or '').strip()
    ann_type = data.get('type') or 'notice'
    if ann_type not in ('notice', 'banner', 'popup'):
        return jsonify({'success': False, 'message': 'Invalid type'}), 400
    image_url = (data.get('image_url') or '').strip() or None
    link_url = (data.get('link_url') or '').strip() or None
    sort_order = data.get('sort_order') or 0
    created_by = data.get('created_by')

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO announcements
                (title, content, type, image_url, link_url, sort_order, status, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, 'draft', %s)
            """,
            (title, content, ann_type, image_url, link_url, sort_order, created_by),
        )
        new_id = cursor.lastrowid
        conn.commit()
        return jsonify({'success': True, 'message': 'Announcement created', 'announcement_id': new_id})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/announcements/<int:ann_id>/publish', methods=['POST'])
@handle_db_error
def publish_announcement(ann_id):
    """发布公告:status -> published 并写 published_at(由 shop_web 鉴权后代理调用)。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT status FROM announcements WHERE id = %s", (ann_id,))
        row = cursor.fetchone()
        if row is None:
            return jsonify({'success': False, 'message': 'Announcement not found'}), 404
        if row[0] not in ('draft', 'published'):
            return jsonify({'success': False, 'message': f'Cannot publish from status {row[0]}'}), 409
        cursor.execute(
            "UPDATE announcements SET status = 'published', published_at = %s WHERE id = %s",
            (datetime.utcnow(), ann_id),
        )
        conn.commit()
        return jsonify({'success': True, 'status': 'published'})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/announcements/<int:ann_id>/withdraw', methods=['POST'])
@handle_db_error
def withdraw_announcement(ann_id):
    """撤回公告:status -> archived(由 shop_web 鉴权后代理调用)。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT status FROM announcements WHERE id = %s", (ann_id,))
        row = cursor.fetchone()
        if row is None:
            return jsonify({'success': False, 'message': 'Announcement not found'}), 404
        if row[0] not in ('published', 'archived'):
            return jsonify({'success': False, 'message': f'Cannot withdraw from status {row[0]}'}), 409
        cursor.execute("UPDATE announcements SET status = 'archived' WHERE id = %s", (ann_id,))
        conn.commit()
        return jsonify({'success': True, 'status': 'archived'})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/announcements/<int:ann_id>', methods=['DELETE'])
@handle_db_error
def delete_announcement(ann_id):
    """删除公告(由 shop_web 鉴权后代理调用)。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM announcements WHERE id = %s", (ann_id,))
        if cursor.fetchone() is None:
            return jsonify({'success': False, 'message': 'Announcement not found'}), 404
        cursor.execute("DELETE FROM announcements WHERE id = %s", (ann_id,))
        conn.commit()
        return jsonify({'success': True})
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
        'service': 'announcement_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('ANNOUNCEMENT_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('ANNOUNCEMENT_SERVICE_PORT', '5009'))

    print("=" * 60)
    print("公告微服务 announcement_service")
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
            _NACOS_SERVICE_NAME = "announcement_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5009
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
