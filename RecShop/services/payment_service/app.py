"""
支付微服务 payment_service
使用 Flask + MySQL
拥有数据: payments(自有新表) —— 按 order_no 查支付记录 / mock 收款
独立服务: 无下游依赖调用(按 order_no 引用订单,但不调 order 服务,避免环依赖)
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

# payment_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: payment_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "payment_service")
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
            logger.info("[otel] payment_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] payment_service instrumented")
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
    pool_name="payment_pool",
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


def _ensure_schema():
    """幂等建表: payments(本服务自有新表)。

    仅在进程启动时执行一次,CREATE TABLE IF NOT EXISTS 不动任何现有表。
    单事务 commit、finally 关连接。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_no VARCHAR(64) NOT NULL,
                amount DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
                status ENUM('pending', 'paid', 'refunded') NOT NULL DEFAULT 'pending',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                -- Z11: order_no 唯一(支付幂等 DB 层兜底,对照 reviews.uk_order_item);
                --      新建库直接带; 既有库由 scripts/_archive/sql_migrations/migrate_z11_payment_shipment_unique.sql 先去重再补。
                UNIQUE KEY uk_payments_order_no (order_no)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        logger.info("[schema] payments table ensured")
    finally:
        cursor.close()
        conn.close()


def _row_to_payment_dict(row):
    """把 payments 表的一行(dict cursor) 序列化为前端友好的 JSON。"""
    amount = row.get('amount')
    created_at = row.get('created_at')
    return {
        'id': row.get('id'),
        'order_no': row.get('order_no'),
        'amount': float(amount) if amount is not None else 0.0,
        'status': row.get('status'),
        'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(created_at, datetime) else None,
    }

# ============================================================
# 支付记录查询 / mock 收款
# ============================================================

@app.route('/api/payments/<order_no>', methods=['GET'])
@handle_db_error
def get_payments(order_no):
    """查该订单的支付记录(按 order_no),无则返回空列表。"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, order_no, amount, status, created_at
            FROM payments
            WHERE order_no = %s
            ORDER BY id DESC
            """,
            (order_no,),
        )
        rows = cursor.fetchall()
        payments = [_row_to_payment_dict(r) for r in rows]
        return jsonify({'success': True, 'order_no': order_no, 'payments': payments})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/payments', methods=['POST'])
@handle_db_error
def create_payment():
    """mock 收款: 插一条 status=paid 的支付记录。

    body: {order_no, amount}。不调订单服务,仅按 order_no 引用(避免环依赖)。
    """
    data = request.get_json() or {}
    order_no = (data.get('order_no') or '').strip()
    amount = data.get('amount')

    if not order_no:
        return jsonify({'success': False, 'message': 'order_no is required'}), 400
    try:
        amount_val = float(amount)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'message': 'amount must be a number'}), 400
    if amount_val < 0:
        return jsonify({'success': False, 'message': 'amount must be non-negative'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 幂等: 同一 order_no 已有支付记录则直接返回, 避免并发/重试产生重复扣款行
        # (与 payments.order_no UNIQUE 约束配套, 应用层查重为第一道防线)
        cursor_q = conn.cursor(dictionary=True)
        cursor_q.execute(
            "SELECT id, order_no, amount, status FROM payments WHERE order_no = %s ORDER BY id ASC LIMIT 1",
            (order_no,),
        )
        existing = cursor_q.fetchone()
        cursor_q.close()
        if existing is not None:
            existing_amount = existing.get('amount')
            # 不在此手动 close cursor/conn —— 交由下方 finally 统一归还连接池;
            # 池化连接被 double-close(此处 + finally)会触发 PoolError(queue is full)→ 500。
            return jsonify({
                'success': True,
                'message': 'Payment already recorded',
                'payment': {
                    'id': existing.get('id'),
                    'order_no': existing.get('order_no'),
                    'amount': float(existing_amount) if existing_amount is not None else 0.0,
                    'status': existing.get('status'),
                },
            })

        cursor.execute(
            """
            INSERT INTO payments (order_no, amount, status)
            VALUES (%s, %s, 'paid')
            """,
            (order_no, amount_val),
        )
        new_id = cursor.lastrowid
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Payment recorded',
            'payment': {
                'id': new_id,
                'order_no': order_no,
                'amount': amount_val,
                'status': 'paid',
            },
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
        'service': 'payment_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('PAYMENT_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('PAYMENT_SERVICE_PORT', '5012'))

    print("=" * 60)
    print("支付微服务 payment_service")
    print("=" * 60)
    print(f"数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"服务地址: http://{host}:{port}")
    print("=" * 60)

    # ---- 幂等建表(自有新表 payments),app.run 之前执行一次 ----
    # 仅在 werkzeug reloader 的子进程或非 debug 模式下建表,避免 reloader 父进程重复执行。
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not _DEBUG:
        try:
            _ensure_schema()
        except Exception as _schema_e:
            print(f"[schema] 建表流程异常,已忽略: {_schema_e}")
    # ------------------------------------------------------------

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
            _NACOS_SERVICE_NAME = "payment_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5012
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
