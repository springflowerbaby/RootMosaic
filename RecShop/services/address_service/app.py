"""
地址微服务 address_service
使用 Flask + MySQL

拥有数据: user_addresses (UserAddress)。从 shop_web buyer 端拆出。
shop_web 在完成 @login_required 鉴权后,带 user_token 代理调用本服务;
本服务以 user_token 作权威归属校验,负责 INSERT/UPDATE/DELETE 与 is_default 互斥。
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

# address_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: address_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "address_service")
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
            logger.info("[otel] address_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] address_service instrumented")
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
    pool_name="address_pool",
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
# 地址
# ============================================================

def _address_row_to_dict(row):
    """把 user_addresses 行(dict cursor)转成与 shop_web UserAddress.to_dict 一致的结构。"""
    province = row.get('province') or ''
    city = row.get('city') or ''
    district = row.get('district') or ''
    address = row.get('address')
    full = ' '.join(p for p in [province, city, district, address] if p)
    return {
        'id': row.get('id'),
        'receiver_name': row.get('receiver_name'),
        'receiver_phone': row.get('receiver_phone'),
        'province': province,
        'city': city,
        'district': district,
        'address': address,
        'full_address': full,
        'is_default': row.get('is_default'),
    }


@app.route('/api/address', methods=['GET'])
@handle_db_error
def list_addresses():
    """列出某用户的地址(供冒烟只读)。鉴权/归属由 shop_web 保证,本服务以 user_token 过滤。"""
    user_token = request.args.get('user_token')
    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, user_token, receiver_name, receiver_phone,
                   province, city, district, address, is_default
            FROM user_addresses
            WHERE user_token = %s
            ORDER BY is_default DESC, id DESC
            """,
            (user_token,),
        )
        rows = cursor.fetchall()
        return jsonify({'success': True, 'addresses': [_address_row_to_dict(r) for r in rows]})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/address/save', methods=['POST'])
@handle_db_error
def save_address():
    """新增或更新一条收货地址(由 shop_web 在完成买家鉴权后代理调用)。"""
    data = request.get_json() or {}
    user_token = data.get('user_token')
    receiver_name = (data.get('receiver_name') or '').strip()
    receiver_phone = (data.get('receiver_phone') or '').strip()
    province = (data.get('province') or '').strip()
    city = (data.get('city') or '').strip()
    district = (data.get('district') or '').strip()
    address = (data.get('address') or '').strip()
    is_default = 1 if data.get('is_default') else 0
    address_id = data.get('address_id')

    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400
    if not receiver_name or not receiver_phone or not address:
        return jsonify({'success': False, 'message': 'Name, phone and address are required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 若设为默认,先把该用户其它地址的 is_default 清零(同事务,保证互斥)
        if is_default:
            cursor.execute(
                "UPDATE user_addresses SET is_default = 0 WHERE user_token = %s AND is_default = 1",
                (user_token,),
            )

        if address_id:
            # 更新前以 (id, user_token) 做权威归属校验
            cursor.execute(
                "SELECT id FROM user_addresses WHERE id = %s AND user_token = %s",
                (address_id, user_token),
            )
            if cursor.fetchone() is None:
                return jsonify({'success': False, 'message': 'Address not found'}), 404
            cursor.execute(
                """
                UPDATE user_addresses SET
                    receiver_name = %s, receiver_phone = %s, province = %s,
                    city = %s, district = %s, address = %s, is_default = %s
                WHERE id = %s AND user_token = %s
                """,
                (receiver_name, receiver_phone, province, city, district,
                 address, is_default, address_id, user_token),
            )
            saved_id = address_id
        else:
            # 该用户首条地址强制设为默认(与原 shop_web 行为一致)
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM user_addresses WHERE user_token = %s",
                (user_token,),
            )
            existing_count = cursor.fetchone()['cnt']
            effective_default = 1 if existing_count == 0 else is_default
            cursor.execute(
                """
                INSERT INTO user_addresses
                    (user_token, receiver_name, receiver_phone, province,
                     city, district, address, is_default)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (user_token, receiver_name, receiver_phone, province,
                 city, district, address, effective_default),
            )
            saved_id = cursor.lastrowid

        # 回读保存后的整行,返回与 shop_web 一致的 address dict
        cursor.execute(
            """
            SELECT id, user_token, receiver_name, receiver_phone,
                   province, city, district, address, is_default
            FROM user_addresses WHERE id = %s
            """,
            (saved_id,),
        )
        row = cursor.fetchone()
        conn.commit()
        return jsonify({'success': True, 'address': _address_row_to_dict(row)})
    finally:
        cursor.close()
        conn.close()


@app.route('/api/address/delete', methods=['POST'])
@handle_db_error
def delete_address():
    """删除一条收货地址(以 user_token 做权威归属校验)。"""
    data = request.get_json() or {}
    user_token = data.get('user_token')
    address_id = data.get('address_id')
    if not user_token or not address_id:
        return jsonify({'success': False, 'message': 'user_token and address_id are required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id FROM user_addresses WHERE id = %s AND user_token = %s",
            (address_id, user_token),
        )
        if cursor.fetchone() is None:
            return jsonify({'success': False, 'message': 'Address not found'}), 404
        try:
            cursor.execute(
                "DELETE FROM user_addresses WHERE id = %s AND user_token = %s",
                (address_id, user_token),
            )
            conn.commit()
            return jsonify({'success': True})
        except mysql.connector.Error as e:
            if getattr(e, 'errno', None) == 1451:
                conn.rollback()
                return jsonify({'success': False,
                                'message': '该地址已被订单引用,无法删除'}), 409
            raise
    finally:
        cursor.close()
        conn.close()


@app.route('/api/address/default', methods=['POST'])
@handle_db_error
def set_default_address():
    """把某地址设为当前用户默认地址(以 user_token 做权威归属校验,单事务互斥)。"""
    data = request.get_json() or {}
    user_token = data.get('user_token')
    address_id = data.get('address_id')
    if not user_token or not address_id:
        return jsonify({'success': False, 'message': 'user_token and address_id are required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT id FROM user_addresses WHERE id = %s AND user_token = %s",
            (address_id, user_token),
        )
        if cursor.fetchone() is None:
            return jsonify({'success': False, 'message': 'Address not found'}), 404
        cursor.execute(
            "UPDATE user_addresses SET is_default = 0 WHERE user_token = %s",
            (user_token,),
        )
        cursor.execute(
            "UPDATE user_addresses SET is_default = 1 WHERE id = %s AND user_token = %s",
            (address_id, user_token),
        )
        conn.commit()
        return jsonify({'success': True, 'message': 'Default address updated'})
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
        'service': 'address_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('ADDRESS_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('ADDRESS_SERVICE_PORT', '5007'))

    print("=" * 60)
    print("地址微服务 address_service")
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
            _NACOS_SERVICE_NAME = "address_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5007
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
