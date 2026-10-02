"""
库存微服务 inventory_service
使用 Flask + MySQL
拥有数据: inventory(item_id 主键 / stock / reserved / updated_at) —— 库存查询 / 库存预留

深度链: reserve 时 HTTP 调 catalog_service 校验商品存在 (inventory -> catalog)。
"""
from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling
import json
import os
import logging
import requests
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

# inventory_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: inventory_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "inventory_service")
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
            logger.info("[otel] inventory_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] inventory_service instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
        _mysql_instrumentor = None
# ============================================================
CORS(app)

# ==================== 故障注入钩子(env 门控, 默认全关) ====================
# 照 catalog_service 的范式: 三个 env 旋钮, 任一未设 = 与当前 inventory_service 字节级行为一致
# (before_request 早 return / pool_size 仍 3)。仅供 chaos6x18 v3 的【临时 inventory 实例】使用;
# 持久 inventory@5013 不设这些 env, 零影响。
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
    pool_name="inventory_pool",
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

# 下游 catalog_service 地址(深度链 inventory→catalog 校验商品存在)。
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


# 建表语句(幂等):inventory 表为本服务私有数据,首次启动自动创建,不动现有表。
_INVENTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS inventory (
    item_id    VARCHAR(64) NOT NULL,
    stock      INT NOT NULL DEFAULT 0,
    reserved   INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def _ensure_schema():
    """幂等建表:确保 inventory 表存在(CREATE TABLE IF NOT EXISTS)。在 app.run 前执行一次。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(_INVENTORY_SCHEMA)
        conn.commit()
        logger.info("[inventory] schema ensured (inventory table ready)")
    finally:
        cursor.close()
        conn.close()


def _check_item_in_catalog(item_id):
    """深度链 inventory→catalog: HTTP 调 catalog_service 校验商品是否存在。

    返回:
        True  —— 商品存在
        False —— 商品明确不存在(catalog 返回 404 / success=False)
        None  —— 下游不可用 / 超时 / 异常(由调用方决定降级策略)
    """
    try:
        resp = requests.get(
            f'{get_catalog_service_url()}/api/items/{item_id}',
            timeout=8,
        )
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            return None
        body = resp.json() or {}
        return bool(body.get('success'))
    except requests.exceptions.RequestException as e:
        logger.warning('[inventory] catalog_service unavailable for item %s: %s', item_id, e)
        return None
    except Exception as e:
        logger.warning('[inventory] catalog_service check failed for %s: %s', item_id, e)
        return None

# ============================================================
# 库存查询
# ============================================================

@app.route('/api/inventory/<item_id>', methods=['GET'])
@handle_db_error
def get_inventory(item_id):
    """查询某商品库存。不存在时返回 stock=0(不报错),available = stock - reserved。"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT item_id, stock, reserved, updated_at FROM inventory WHERE item_id = %s",
            (item_id,),
        )
        row = cursor.fetchone()
        if row is None:
            # 不存在视为零库存(而非 404),便于上游统一处理
            return jsonify({
                'success': True,
                'item_id': item_id,
                'stock': 0,
                'reserved': 0,
                'available': 0,
                'exists': False,
            })
        stock = int(row['stock'] or 0)
        reserved = int(row['reserved'] or 0)
        return jsonify({
            'success': True,
            'item_id': row['item_id'],
            'stock': stock,
            'reserved': reserved,
            'available': max(stock - reserved, 0),
            'exists': True,
            'updated_at': row['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if row.get('updated_at') else None,
        })
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 库存预留
# ============================================================

@app.route('/api/inventory/<item_id>/reserve', methods=['POST'])
@handle_db_error
def reserve_inventory(item_id):
    """预留库存:先调 catalog 校验商品存在,再扣减(reserved += quantity)。单事务。

    深度链 inventory→catalog:HTTP 调 catalog_service GET /api/items/<id> 校验商品。
      - catalog 明确返回商品不存在 → 400 拒绝
      - catalog 不可用/超时 → 降级放行(不阻塞预留),仅记录 warning
    库存不足(available < quantity)→ 409 拒绝,不改库存。
    """
    data = request.get_json() or {}

    # Z3-BE-OBS-02: reserve 是显式库存预留动作,quantity 必填——缺省时返 400,
    # 不再静默按 1 预留(默默占 1 件库存是隐式契约,易致超卖/漏占;接口契约应显式)。
    if 'quantity' not in data:
        return jsonify({'success': False, 'message': 'quantity is required'}), 400
    quantity = data.get('quantity')

    # 校验入参
    try:
        quantity = int(quantity)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'message': 'quantity must be an integer'}), 400
    if quantity <= 0:
        return jsonify({'success': False, 'message': 'quantity must be a positive integer'}), 400

    # 深度链:先 HTTP 调 catalog_service 校验商品存在(下游不可用则降级放行)
    exists = _check_item_in_catalog(item_id)
    if exists is False:
        return jsonify({'success': False, 'message': 'Item not found in catalog', 'item_id': item_id}), 400
    catalog_degraded = exists is None  # 下游不可用,降级标记(响应里告知上游)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 取当前库存(行锁,保证并发预留不超卖);不存在则视为零库存
        cursor.execute(
            "SELECT stock, reserved FROM inventory WHERE item_id = %s FOR UPDATE",
            (item_id,),
        )
        row = cursor.fetchone()
        if row is None:
            stock, reserved = 0, 0
        else:
            stock, reserved = int(row[0] or 0), int(row[1] or 0)

        available = max(stock - reserved, 0)
        if quantity > available:
            conn.rollback()
            return jsonify({
                'success': False,
                'message': 'Insufficient stock',
                'item_id': item_id,
                'requested': quantity,
                'available': available,
            }), 409

        new_reserved = reserved + quantity
        # UPSERT:存在则只更新 reserved,不存在则建一条(stock=0 时上面已挡掉,故走到这必有记录,
        # 但仍用 ON DUPLICATE 兜底并发首次插入的竞态)
        cursor.execute(
            """
            INSERT INTO inventory (item_id, stock, reserved)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE reserved = VALUES(reserved)
            """,
            (item_id, stock, new_reserved),
        )
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Reserved',
            'item_id': item_id,
            'reserved_now': quantity,
            'stock': stock,
            'reserved': new_reserved,
            'available': max(stock - new_reserved, 0),
            'catalog_degraded': catalog_degraded,
        })
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@app.route('/api/inventory/<item_id>/release', methods=['POST'])
@handle_db_error
def release_inventory(item_id):
    """释放(取消)已预留库存:reserved -= quantity(非负钳制)。单事务,幂等。

    用途:上游(如 shop_web create_order)在 best-effort 预留后若下单失败回滚,
    需把本次已占的 reserved 还回去,避免失败下单永久漏占库存(Z-OBS-01)。
      - 不调 catalog(释放无需校验商品存在);不碰 stock(只动 reserved)。
      - item 不存在 / reserved 已为 0 → no-op,返回 success(幂等)。
      - reserved - quantity < 0 → 钳制为 0(不为负),避免错误扣减他人预留。
    """
    data = request.get_json() or {}
    quantity = data.get('quantity', 1)

    # 校验入参(与 reserve 一致)
    try:
        quantity = int(quantity)
    except (TypeError, ValueError):
        return jsonify({'success': False, 'message': 'quantity must be an integer'}), 400
    if quantity <= 0:
        return jsonify({'success': False, 'message': 'quantity must be a positive integer'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 取当前 reserved(行锁,保证并发释放/预留不竞态);不存在则视为零、no-op
        cursor.execute(
            "SELECT stock, reserved FROM inventory WHERE item_id = %s FOR UPDATE",
            (item_id,),
        )
        row = cursor.fetchone()
        if row is None:
            conn.commit()  # 释放行锁(虽无更新);item 不存在视为已释放
            return jsonify({
                'success': True,
                'message': 'No reservation to release (item not tracked)',
                'item_id': item_id,
                'released_now': 0,
                'reserved': 0,
            })

        stock, reserved = int(row[0] or 0), int(row[1] or 0)
        new_reserved = max(reserved - quantity, 0)  # 非负钳制
        released_now = reserved - new_reserved      # 实际释放量(<= 请求量)

        if released_now > 0:
            cursor.execute(
                "UPDATE inventory SET reserved = %s WHERE item_id = %s",
                (new_reserved, item_id),
            )
        conn.commit()
        return jsonify({
            'success': True,
            'message': 'Released',
            'item_id': item_id,
            'released_now': released_now,
            'stock': stock,
            'reserved': new_reserved,
            'available': max(stock - new_reserved, 0),
        })
    except Exception:
        conn.rollback()
        raise
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
        'service': 'inventory_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('INVENTORY_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('INVENTORY_SERVICE_PORT', '5013'))

    print("=" * 60)
    print("库存微服务 inventory_service")
    print("=" * 60)
    print(f"数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"服务地址: http://{host}:{port}")
    print("=" * 60)

    # 幂等建表:确保 inventory 表存在(首启自动创建,不动现有表)
    try:
        _ensure_schema()
    except Exception as _e:
        print(f"[inventory] 建表流程异常,已忽略: {_e}")

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
            _NACOS_SERVICE_NAME = "inventory_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5013
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
