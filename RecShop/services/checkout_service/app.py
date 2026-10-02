"""
结账编排微服务 checkout_service
使用 Flask + MySQL
拥有数据: 无表(纯编排 / Boutique 扇出范式)。复用现有 cart_items 表做只读取购物车,
          其余信息全部经 HTTP 向下游微服务扇出获取,自身不建表、不做写。

核心扇出(★★ 症状漂移源头): GET /api/checkout/preview 一次结账预览会三路 fan-out:
    checkout -> cart_service(5005? 实际 5006)  取购物车计数(checkout→cart 边)
    checkout -> pricing_service(5014)           对每个购物车商品算含税价(checkout→pricing 边)
    checkout -> inventory_service(5013)         对每个购物车商品查可用库存(checkout→inventory 边)
任一下游慢/错都会让 preview 整体劣化(故障在调用图上向上游漂移),便于做根因定位演练。
只读不下单:不扣库存、不建订单、不写任何表。
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

# checkout_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: checkout_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "checkout_service")
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
            logger.info("[otel] checkout_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] checkout_service instrumented")
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
# 说明: checkout_service 自身不建表;但需对现有 cart_items 表做只读查询(取该用户购物车行),
# 故仍持一个连接池。所有读用 get_db_connection() + cursor,finally 关连接,绝不做写。
db_pool = pooling.MySQLConnectionPool(
    pool_name="checkout_pool",
    pool_size=3,
    **DB_CONFIG
)

# requests 统一超时(秒):下游慢时不至于无限阻塞,触发降级。
# env 化(默认 8 = 现状, 字节级等价): chaos6x18 timeout_misconfiguration 故障经临时实例注
# CHECKOUT_HTTP_TIMEOUT=0.05 制造误配超时阈 → 三路扇出全 timeout → 全降级。默认值不变。
_HTTP_TIMEOUT = float(os.environ.get("CHECKOUT_HTTP_TIMEOUT", "8"))


# ---- 下游服务地址解析(Nacos 优先,回退 env / 默认本地端口)----
# 模板已把 _PROJECT_ROOT 加进 sys.path;get_service_url 内置 TCP 探活 + 熔断,
# Nacos 不可达会快速回退 fallback_url,不卡 ~2s。

def _resolve_service_url(service_name, env_key, default_url):
    """通用解析:Nacos 优先,失败回退 env[env_key],再回退 default_url。"""
    fb = os.environ.get(env_key, default_url)
    try:
        import sys as _sys
        if str(_PROJECT_ROOT) not in _sys.path:
            _sys.path.insert(0, str(_PROJECT_ROOT))
        from shared.nacos_client import get_service_url as _nacos_get_service_url
        return _nacos_get_service_url(service_name, fallback_url=fb) or fb
    except Exception:
        return fb


def get_cart_service_url():
    """下游 cart_service 地址(checkout→cart 扇出边)。"""
    return _resolve_service_url('cart_service', 'CART_SERVICE_URL', 'http://127.0.0.1:5006')


def get_pricing_service_url():
    """下游 pricing_service 地址(checkout→pricing 扇出边)。"""
    return _resolve_service_url('pricing_service', 'PRICING_SERVICE_URL', 'http://127.0.0.1:5014')


def get_inventory_service_url():
    """下游 inventory_service 地址(checkout→inventory 扇出边)。"""
    return _resolve_service_url('inventory_service', 'INVENTORY_SERVICE_URL', 'http://127.0.0.1:5013')

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
# 下游扇出辅助
# ============================================================

def _load_cart_rows(user_token):
    """读现有 cart_items 表取该用户购物车行(只读,checkout 不拥有该表)。

    返回 [{'id', 'item_id', 'quantity'}, ...]。cart_items 由 cart_service 拥有,
    这里只做读取以驱动后续 pricing/inventory 扇出(纯编排,不做任何写)。
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT id, item_id, quantity FROM cart_items WHERE user_token = %s ORDER BY id",
            (user_token,),
        )
        rows = cursor.fetchall()
        return [
            {'id': r['id'], 'item_id': r['item_id'], 'quantity': int(r['quantity'] or 0)}
            for r in rows
        ]
    finally:
        cursor.close()
        conn.close()


def _fetch_cart_count(user_token):
    """checkout→cart 扇出边: HTTP 调 cart_service 取购物车计数。

    返回 (count: int|None, degraded: bool)。下游不可用 → (None, True)(降级,不阻塞预览)。
    """
    try:
        resp = requests.get(
            f'{get_cart_service_url()}/api/cart/count',
            params={'user_token': user_token},
            timeout=_HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning('[checkout] cart_service count returned %s', resp.status_code)
            return None, True
        body = resp.json() or {}
        if not body.get('success'):
            return None, True
        return int(body.get('count') or 0), False
    except requests.exceptions.RequestException as e:
        logger.warning('[checkout] cart_service unavailable: %s', e)
        return None, True
    except Exception as e:
        logger.warning('[checkout] cart_service count failed: %s', e)
        return None, True


def _fetch_pricing(item_id):
    """checkout→pricing 扇出边: HTTP 调 pricing_service 取含税单价。

    返回 (unit_price: float, degraded: bool)。下游不可用/出错 → (None, True),
    由调用方用 base 兜底(降级,不阻塞预览)。
    """
    try:
        resp = requests.get(
            f'{get_pricing_service_url()}/api/pricing/{item_id}',
            timeout=_HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning('[checkout] pricing_service returned %s for %s', resp.status_code, item_id)
            return None, True
        body = resp.json() or {}
        if not body.get('success'):
            return None, True
        final = body.get('final')
        if final is None:
            return None, True
        return float(final), False
    except requests.exceptions.RequestException as e:
        logger.warning('[checkout] pricing_service unavailable for %s: %s', item_id, e)
        return None, True
    except Exception as e:
        logger.warning('[checkout] pricing_service failed for %s: %s', item_id, e)
        return None, True


def _fetch_inventory(item_id):
    """checkout→inventory 扇出边: HTTP 调 inventory_service 查可用库存。

    返回 (available: int, degraded: bool)。下游不可用 → (0, True)(降级为零库存,不阻塞预览)。
    """
    try:
        resp = requests.get(
            f'{get_inventory_service_url()}/api/inventory/{item_id}',
            timeout=_HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning('[checkout] inventory_service returned %s for %s', resp.status_code, item_id)
            return 0, True
        body = resp.json() or {}
        if not body.get('success'):
            return 0, True
        return int(body.get('available') or 0), False
    except requests.exceptions.RequestException as e:
        logger.warning('[checkout] inventory_service unavailable for %s: %s', item_id, e)
        return 0, True
    except Exception as e:
        logger.warning('[checkout] inventory_service failed for %s: %s', item_id, e)
        return 0, True

# ============================================================
# 结账预览(核心三路扇出)
# ============================================================

@app.route('/api/checkout/preview', methods=['GET'])
@handle_db_error
def checkout_preview():
    """结账预览(只读不下单):三路扇出汇总购物车每项的价与库存。

    1) checkout→cart: 取购物车(读 cart_items 行) + 调 cart_service 取计数(扇出边)
    2) checkout→pricing: 对每行商品调 pricing_service 算含税单价(下游不可用则用 0/降级)
    3) checkout→inventory: 对每行商品调 inventory_service 查可用库存(下游不可用则当 0)
    汇总返回 items(每项含 unit_price/subtotal/available/in_stock) + total + availability。
    任一下游慢/错只降级当前维度,不让整个预览失败(故障沿调用图向上游漂移)。
    """
    user_token = request.args.get('user_token')
    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400

    # ---- 取购物车行(只读 cart_items) ----
    cart_rows = _load_cart_rows(user_token)

    # ---- checkout→cart 扇出边:调 cart_service 取计数(降级不阻塞) ----
    cart_count, cart_degraded = _fetch_cart_count(user_token)

    if not cart_rows:
        return jsonify({
            'success': True,
            'user_token': user_token,
            'items': [],
            'item_count': 0,
            'total': 0.0,
            'all_available': True,
            'cart_count': cart_count,
            'degraded': {'cart': cart_degraded, 'pricing': False, 'inventory': False},
            'message': 'Cart is empty',
        })

    items = []
    total = 0.0
    all_available = True
    pricing_degraded = False
    inventory_degraded = False

    for row in cart_rows:
        item_id = row['item_id']
        quantity = row['quantity']

        # checkout→pricing 扇出边
        unit_price, p_deg = _fetch_pricing(item_id)
        if p_deg:
            pricing_degraded = True
        unit_price_val = unit_price if unit_price is not None else 0.0

        # checkout→inventory 扇出边
        available, i_deg = _fetch_inventory(item_id)
        if i_deg:
            inventory_degraded = True

        subtotal = round(unit_price_val * quantity, 2)
        total += subtotal
        in_stock = available >= quantity
        if not in_stock:
            all_available = False

        items.append({
            'cart_id': row['id'],
            'item_id': item_id,
            'quantity': quantity,
            'unit_price': round(unit_price_val, 2),
            'subtotal': subtotal,
            'available': available,
            'in_stock': in_stock,
            'price_degraded': p_deg,
            'inventory_degraded': i_deg,
        })

    return jsonify({
        'success': True,
        'user_token': user_token,
        'items': items,
        'item_count': sum(it['quantity'] for it in items),
        'total': round(total, 2),
        'all_available': all_available,
        'cart_count': cart_count,
        'degraded': {
            'cart': cart_degraded,
            'pricing': pricing_degraded,
            'inventory': inventory_degraded,
        },
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
        'service': 'checkout_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('CHECKOUT_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('CHECKOUT_SERVICE_PORT', '5011'))

    print("=" * 60)
    print("结账编排微服务 checkout_service")
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
            _NACOS_SERVICE_NAME = "checkout_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5011
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
