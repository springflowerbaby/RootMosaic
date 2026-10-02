"""
订单微服务 order_service
使用 Flask + MySQL
拥有数据: orders / order_items(读为主,复用现有表,不建新表)
端点:
    GET /health
    GET /api/orders?user_token=         订单列表(JSON)
    GET /api/orders/<order_no>           订单详情 + 订单行(JSON)
深度链(可选): GET /api/orders/<order_no>?enrich=1 时,逐个订单行经 HTTP 调
    catalog_service GET /api/items/<id> 补当前商品信息(order→catalog);
    下游不可用/商品不存在时降级,不影响订单本身返回。不做写、不调 payment/inventory。
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

# order_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: order_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "order_service")
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
            logger.info("[otel] order_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] order_service instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
        _mysql_instrumentor = None
# ============================================================
CORS(app)

# ==================== 故障注入钩子(env 门控, 默认全关) ====================
# 照 catalog_service 的范式: 三个 env 旋钮, 任一未设 = 与当前 order_service 字节级行为一致
# (before_request 早 return / pool_size 仍 3)。仅供 chaos6x18 v3 的【临时 order 实例】使用;
# 持久 order@5010 不设这些 env, 零影响。
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
    pool_name="order_pool",
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


# 下游 catalog_service 地址(深度链 order→catalog 补当前商品信息,可选)。
# 优先用 Nacos 解析,失败回退 env CATALOG_SERVICE_URL,再默认 localhost:5005。
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


# 占位图地址(与 shop_web OrderItem.to_dict() 同源,前端无图时兜底)
_PLACEHOLDER_IMAGE = 'https://placehold.co/400x400/f5f5f5/333333?text=No+Image'

# 订单状态中文/英文文案(与 shop_web Order.to_dict() 的 status_text 对齐)
_STATUS_TEXT = {
    'pending': 'Pending Payment',
    'paid': 'Paid',
    'shipped': 'Shipped',
    'completed': 'Completed',
    'cancelled': 'Cancelled',
}

# 合法的订单状态过滤值(与 shop_web orders 页面同源)
_VALID_STATUS = ('pending', 'paid', 'shipped', 'completed', 'cancelled')


def _fmt_ts(value):
    """把 datetime / None 序列化成字符串(与 shop_web Order.to_dict() 同格式)。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)


def _row_to_order_dict(row, items=None):
    """把 orders 表一行(dict cursor)序列化为前端友好的订单 JSON。

    字段对齐 shop_web 的 Order.to_dict();items 传入时附带订单行列表。
    """
    total = row.get('total_amount')
    status = row.get('status')
    return {
        'id': row.get('id'),
        'order_no': row.get('order_no'),
        'user_token': row.get('user_token'),
        'snapshot_receiver_name': row.get('snapshot_receiver_name'),
        'snapshot_receiver_phone': row.get('snapshot_receiver_phone'),
        'snapshot_address': row.get('snapshot_address'),
        'total_amount': float(total) if total is not None else 0.0,
        'item_count': row.get('item_count') or 0,
        'status': status,
        'status_text': _STATUS_TEXT.get(status, status),
        'remark': row.get('remark'),
        'cancel_reason': row.get('cancel_reason'),
        'created_at': _fmt_ts(row.get('created_at')),
        'paid_at': _fmt_ts(row.get('paid_at')),
        'shipped_at': _fmt_ts(row.get('shipped_at')),
        'completed_at': _fmt_ts(row.get('completed_at')),
        'cancelled_at': _fmt_ts(row.get('cancelled_at')),
        'items': items if items is not None else [],
    }


def _row_to_order_item_dict(row):
    """把 order_items 表一行(dict cursor)序列化为订单行 JSON(对齐 shop_web OrderItem.to_dict())。"""
    price = row.get('item_price')
    subtotal = row.get('subtotal')
    return {
        'id': row.get('id'),
        'item_id': row.get('item_id'),
        'item_title': row.get('item_title'),
        'item_image': row.get('item_image') or _PLACEHOLDER_IMAGE,
        'item_price': float(price) if price is not None else 0.0,
        'quantity': row.get('quantity') or 0,
        'subtotal': float(subtotal) if subtotal is not None else 0.0,
    }


def _fetch_current_item_from_catalog(item_id):
    """深度链 order→catalog: HTTP 调 catalog_service 取单品当前详情(可选增强)。

    返回当前商品信息 dict(name/price/image/rating 等)或 None。
    下游不可用 / 商品不存在 / 异常 → None(调用方降级:仅用订单快照,不报错)。
    """
    try:
        # 超时阈 env 化(默认 8 = 现状, 字节级等价): chaos6x18 v3 timeout_misconfiguration 故障的
        # 第 2 位点经临时 order 实例注 ORDER_HTTP_TIMEOUT=0.05 制造误配超时阈 → order→catalog
        # enrich 请求 timeout → 降级(current_available=false)。默认值不变(mirror checkout)。
        resp = requests.get(
            f'{get_catalog_service_url()}/api/items/{item_id}',
            timeout=float(os.environ.get("ORDER_HTTP_TIMEOUT", "8")),
        )
        if resp.status_code != 200:
            return None
        body = resp.json() or {}
        if not body.get('success'):
            return None
        return body.get('item') or None
    except requests.exceptions.RequestException as e:
        logger.warning('[order] catalog_service unavailable for item %s: %s', item_id, e)
        return None
    except Exception as e:
        logger.warning('[order] catalog enrich failed for item %s: %s', item_id, e)
        return None

# ============================================================
# 订单列表
# ============================================================

@app.route('/api/orders', methods=['GET'])
@handle_db_error
def list_orders():
    """订单列表(按 user_token 权威过滤),支持 status 过滤 + 分页。

    query params:
        user_token(必填) —— 由 shop_web 鉴权后透传,作为归属过滤条件
        status(可选,pending/paid/shipped/completed/cancelled)
        page(默认1)、per_page(默认10,最大50)
    返回每个订单及其订单行;另带各状态计数(供前端 tab 展示)。
    """
    user_token = (request.args.get('user_token') or '').strip()
    if not user_token:
        return jsonify({'success': False, 'message': 'user_token is required'}), 400

    status = (request.args.get('status') or '').strip()
    try:
        page = int(request.args.get('page', 1))
    except (TypeError, ValueError):
        page = 1
    try:
        per_page = int(request.args.get('per_page', 10))
    except (TypeError, ValueError):
        per_page = 10
    page = max(page, 1)
    per_page = min(max(per_page, 1), 50)
    offset = (page - 1) * per_page

    where = ["user_token = %s"]
    params = [user_token]
    if status and status in _VALID_STATUS:
        where.append("status = %s")
        params.append(status)
    where_sql = " AND ".join(where)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 总数(分页元信息)
        cursor.execute(f"SELECT COUNT(id) AS total FROM orders WHERE {where_sql}", tuple(params))
        total = cursor.fetchone()['total']

        # 各状态计数(供前端 tab),始终基于该用户全部订单
        cursor.execute(
            "SELECT status, COUNT(id) AS cnt FROM orders WHERE user_token = %s GROUP BY status",
            (user_token,),
        )
        by_status = {r['status']: r['cnt'] for r in cursor.fetchall()}
        status_counts = {'all': sum(by_status.values())}
        for s in _VALID_STATUS:
            status_counts[s] = by_status.get(s, 0)

        # 当前分页的订单
        cursor.execute(
            f"""
            SELECT id, order_no, user_token, snapshot_receiver_name, snapshot_receiver_phone,
                   snapshot_address, total_amount, item_count, status, remark, cancel_reason,
                   created_at, paid_at, shipped_at, completed_at, cancelled_at
            FROM orders
            WHERE {where_sql}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (per_page, offset),
        )
        order_rows = cursor.fetchall()

        # 批量取这些订单的订单行(一次查询,按 order_id 归组)
        items_by_order = {}
        order_ids = [r['id'] for r in order_rows]
        if order_ids:
            placeholders = ", ".join(["%s"] * len(order_ids))
            cursor.execute(
                f"""
                SELECT id, order_id, item_id, item_title, item_image, item_price, quantity, subtotal
                FROM order_items
                WHERE order_id IN ({placeholders})
                ORDER BY id ASC
                """,
                tuple(order_ids),
            )
            for ir in cursor.fetchall():
                items_by_order.setdefault(ir['order_id'], []).append(_row_to_order_item_dict(ir))

        orders = [
            _row_to_order_dict(r, items=items_by_order.get(r['id'], []))
            for r in order_rows
        ]
        total_pages = (total + per_page - 1) // per_page if per_page else 0
        return jsonify({
            'success': True,
            'orders': orders,
            'page': page,
            'per_page': per_page,
            'total': total,
            'total_pages': total_pages,
            'status_counts': status_counts,
        })
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 订单详情
# ============================================================

@app.route('/api/orders/<order_no>', methods=['GET'])
@handle_db_error
def get_order(order_no):
    """订单详情 + 订单行(JSON)。

    query params:
        user_token(可选) —— 传入时做归属校验(不匹配返回 404,避免越权)
        enrich(可选,1/true) —— 深度链 order→catalog:逐行经 HTTP 补当前商品信息
                               (下游不可用/商品不存在时降级,不影响订单返回)
    """
    user_token = (request.args.get('user_token') or '').strip()
    enrich = (request.args.get('enrich') or '').strip().lower() in ('1', 'true', 'yes')

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, order_no, user_token, snapshot_receiver_name, snapshot_receiver_phone,
                   snapshot_address, total_amount, item_count, status, remark, cancel_reason,
                   created_at, paid_at, shipped_at, completed_at, cancelled_at
            FROM orders
            WHERE order_no = %s
            """,
            (order_no,),
        )
        order_row = cursor.fetchone()
        if order_row is None:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        # 归属校验:传了 user_token 且不匹配 → 当作不存在处理(防越权枚举)
        if user_token and order_row.get('user_token') != user_token:
            return jsonify({'success': False, 'message': 'Order not found'}), 404

        cursor.execute(
            """
            SELECT id, order_id, item_id, item_title, item_image, item_price, quantity, subtotal
            FROM order_items
            WHERE order_id = %s
            ORDER BY id ASC
            """,
            (order_row['id'],),
        )
        item_dicts = [_row_to_order_item_dict(ir) for ir in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

    # 深度链 order→catalog(可选): 用当前 catalog 信息补订单行(库存/现价/现图等)。
    # 失败仅降级:订单快照照常返回,current_* 字段缺省。不阻塞、不报错。
    if enrich and item_dicts:
        catalog_unavailable = False
        for it in item_dicts:
            current = _fetch_current_item_from_catalog(it['item_id'])
            if current is None:
                catalog_unavailable = True
                it['current_available'] = False
                continue
            it['current_available'] = True
            it['current_price'] = current.get('price')
            it['current_name'] = current.get('name')
            it['current_image'] = current.get('image')
            it['current_rating'] = current.get('rating')
        if catalog_unavailable:
            logger.info('[order] enrich partial/degraded for order %s (catalog unavailable)', order_no)

    return jsonify({'success': True, 'order': _row_to_order_dict(order_row, items=item_dicts)})

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
        'service': 'order_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('ORDER_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('ORDER_SERVICE_PORT', '5010'))

    print("=" * 60)
    print("订单微服务 order_service")
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
            _NACOS_SERVICE_NAME = "order_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5010
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
