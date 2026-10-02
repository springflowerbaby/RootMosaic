"""
购物车微服务 cart_service
使用 Flask + MySQL
拥有数据: CartItem —— 加入购物车 / 改数量 / 移除 / 计数
深度链: add 时经 HTTP 调 catalog_service GET /api/items/<id> 取商品价格(cart→catalog)
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
import requests

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 兜底 root logger 配置,确保 OTel init 块的 logger.info 在 LoggingInstrumentor
# 尚未 set_logging_format 之前也能输出(若 LoggingInstrumentor 后续调 basicConfig,
# 因 root 已有 handler 会 no-op,不冲突)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# cart_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: cart_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "cart_service")
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
            logger.info("[otel] cart_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] cart_service instrumented")
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
    pool_name="cart_pool",
    pool_size=3,
    **DB_CONFIG
)

# 下游 catalog_service 地址(深度链 cart→catalog 取商品价格)。
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


def _fetch_item_from_catalog(item_id):
    """深度链 cart→catalog: HTTP 调 catalog_service 取单品详情(含 price)。

    返回商品 dict(catalog 的 item JSON)或 None(不存在/下游不可用)。
    异常被吞掉返回 None,由调用方决定是否继续(价格仅用于行为日志,失败不阻塞加购)。
    """
    try:
        resp = requests.get(
            f'{get_catalog_service_url()}/api/items/{item_id}',
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        body = resp.json() or {}
        if not body.get('success'):
            return None
        return body.get('item')
    except requests.exceptions.RequestException as e:
        logger.warning('[cart] catalog_service unavailable for item %s: %s', item_id, e)
        return None
    except Exception as e:
        logger.warning('[cart] catalog_service item fetch failed for %s: %s', item_id, e)
        return None

# ============================================================
# 购物车
# ============================================================

@app.route('/api/cart/add', methods=['POST'])
@handle_db_error
def add_to_cart():
    """加入购物车(由 shop_web 在完成买家鉴权后代理调用)。

    深度链: 先 HTTP 调 catalog_service 取商品价格(校验商品存在 + 行为日志带价),
    再 UPSERT cart_items,并尽力写一条 add_to_cart 交互(失败不阻塞)。单事务 commit。
    """
    data = request.get_json() or {}
    user_token = data.get('user_token')
    item_id = data.get('item_id')
    try:
        quantity = int(data.get('quantity', 1))
    except (TypeError, ValueError):
        quantity = 0

    if not user_token or not item_id or quantity <= 0:
        return jsonify({'success': False, 'message': 'Invalid parameters'}), 400

    # 深度链 cart→catalog: 取商品(存在性校验 + 取价格用于行为日志)
    item = _fetch_item_from_catalog(item_id)
    if item is None:
        return jsonify({'success': False, 'message': 'Product not found'}), 404
    price = item.get('price')

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # UPSERT: 已有则累加数量,否则新增
        cursor.execute(
            "SELECT id, quantity FROM cart_items WHERE user_token = %s AND item_id = %s",
            (user_token, item_id),
        )
        row = cursor.fetchone()
        if row is not None:
            cursor.execute(
                "UPDATE cart_items SET quantity = quantity + %s WHERE id = %s",
                (quantity, row[0]),
            )
        else:
            cursor.execute(
                "INSERT INTO cart_items (user_token, item_id, quantity) VALUES (%s, %s, %s)",
                (user_token, item_id, quantity),
            )

        # 尽力记录 add_to_cart 交互(失败不阻塞加购,仅 warning)
        try:
            cursor.execute(
                """
                INSERT INTO interactions
                    (user_token, item_id, interaction_type, quantity, price, timestamp)
                VALUES (%s, %s, 'add_to_cart', %s, %s, %s)
                """,
                (user_token, item_id, quantity, price, int(time.time() * 1000)),
            )
        except mysql.connector.Error as e:
            logger.warning('[cart] failed to log add_to_cart interaction: %s', e)

        # 购物车计数
        cursor.execute(
            "SELECT COUNT(*) FROM cart_items WHERE user_token = %s", (user_token,)
        )
        cart_count = cursor.fetchone()[0]

        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Added to cart successfully',
            'cart_count': cart_count,
        })
    finally:
        cursor.close()
        conn.close()


@app.route('/api/cart/update/<int:cart_id>', methods=['POST'])
@handle_db_error
def update_cart_item(cart_id):
    """改购物车某行数量(由 shop_web 在完成买家归属校验后代理调用)。"""
    data = request.get_json() or {}
    user_token = data.get('user_token')
    try:
        quantity = int(data.get('quantity', 1))
    except (TypeError, ValueError):
        quantity = 0

    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400
    if quantity <= 0:
        return jsonify({'success': False, 'message': 'Quantity must be greater than 0'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 归属校验也在服务侧做一遍(权威): 行不存在或不属于该用户 → 404
        cursor.execute(
            "SELECT item_id FROM cart_items WHERE id = %s AND user_token = %s",
            (cart_id, user_token),
        )
        row = cursor.fetchone()
        if row is None:
            return jsonify({'success': False, 'message': 'Cart item not found'}), 404
        item_id = row[0]

        cursor.execute(
            "UPDATE cart_items SET quantity = %s WHERE id = %s", (quantity, cart_id)
        )
        conn.commit()

        # 取价格算小计(下游 catalog 不可用时 price 缺省 0)
        item = _fetch_item_from_catalog(item_id)
        price = item.get('price') if item else None
        subtotal = float(price) * quantity if price else 0.0

        return jsonify({
            'success': True,
            'message': 'Cart updated successfully',
            'subtotal': subtotal,
        })
    finally:
        cursor.close()
        conn.close()


@app.route('/api/cart/remove/<int:cart_id>', methods=['POST'])
@handle_db_error
def remove_from_cart(cart_id):
    """移除购物车某行(由 shop_web 在完成买家归属校验后代理调用)。

    删除前记录一条 remove_from_cart 交互(失败不阻塞)。单事务 commit。
    """
    data = request.get_json() or {}
    user_token = data.get('user_token')
    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT item_id, quantity FROM cart_items WHERE id = %s AND user_token = %s",
            (cart_id, user_token),
        )
        row = cursor.fetchone()
        if row is None:
            return jsonify({'success': False, 'message': 'Cart item not found'}), 404
        item_id, qty = row[0], row[1]

        # 尽力记录 remove_from_cart 交互(失败不阻塞移除,仅 warning)
        try:
            cursor.execute(
                """
                INSERT INTO interactions
                    (user_token, item_id, interaction_type, quantity, timestamp)
                VALUES (%s, %s, 'remove_from_cart', %s, %s)
                """,
                (user_token, item_id, qty, int(time.time() * 1000)),
            )
        except mysql.connector.Error as e:
            logger.warning('[cart] failed to log remove_from_cart interaction: %s', e)

        cursor.execute("DELETE FROM cart_items WHERE id = %s", (cart_id,))

        cursor.execute(
            "SELECT COUNT(*) FROM cart_items WHERE user_token = %s", (user_token,)
        )
        cart_count = cursor.fetchone()[0]

        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Item removed from cart',
            'cart_count': cart_count,
        })
    finally:
        cursor.close()
        conn.close()


@app.route('/api/cart/count', methods=['GET'])
@handle_db_error
def get_cart_count():
    """购物车计数(由 shop_web 在完成买家鉴权后带 user_token 代理调用)。"""
    user_token = request.args.get('user_token')
    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM cart_items WHERE user_token = %s", (user_token,)
        )
        count = cursor.fetchone()[0]
        return jsonify({'success': True, 'count': count})
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
        'service': 'cart_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('CART_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('CART_SERVICE_PORT', '5006'))

    print("=" * 60)
    print("购物车微服务 cart_service")
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
            _NACOS_SERVICE_NAME = "cart_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5006
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
