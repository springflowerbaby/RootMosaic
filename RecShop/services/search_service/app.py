"""
搜索微服务 search_service
使用 Flask + MySQL
拥有数据: Item(独立读模型) —— 按标题/关键字检索在售商品(thin-slice 直查 items 表 LIKE title)
深度(可选): 调 catalog_service 补单品详情(下游不可用时优雅降级,直查 items 兜底)
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

# search_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: search_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "search_service")
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
            logger.info("[otel] search_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] search_service instrumented")
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
    pool_name="search_pool",
    pool_size=3,
    **DB_CONFIG
)

# 下游服务发现:把项目根加进 sys.path 后用 shared.nacos_client.get_service_url。
# Nacos 容错已修(不可达自动回退到下方 fallback URL)。
import sys as _sys
if str(_PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PROJECT_ROOT))
try:
    from shared.nacos_client import get_service_url as _get_service_url
except Exception:
    _get_service_url = None


def _catalog_base_url():
    """catalog_service 基址(Nacos 优先,回退 env CATALOG_SERVICE_URL)。"""
    fb = os.environ.get('CATALOG_SERVICE_URL', 'http://127.0.0.1:5005')
    if _get_service_url is None:
        return fb
    try:
        return _get_service_url("catalog_service", fallback_url=fb) or fb
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


# 占位图地址(与 shop_web Item.to_dict() / catalog_service 同源,前端无图时兜底)
_PLACEHOLDER_IMAGE = 'https://placehold.co/400x400/f5f5f5/333333?text=No+Image'


def _row_to_item_dict(row):
    """把 items 表的一行(dict cursor) 序列化为前端友好的商品 JSON。

    字段对齐 shop_web 的 Item.to_dict():name=title, image=image_url, 价格/评分转 float。
    """
    price = row.get('price')
    rating = row.get('rating')
    return {
        'id': row.get('id'),
        'item_id': row.get('item_id'),
        'name': row.get('title'),
        'category': row.get('category') or '商品',
        'brand': row.get('brand'),
        'price': float(price) if price is not None else 0.0,
        'image': row.get('image_url') or _PLACEHOLDER_IMAGE,
        'description': row.get('description'),
        'rating': float(rating) if rating is not None else 0.0,
        'review_count': row.get('review_count') or 0,
    }


def _enrich_from_catalog(item_id):
    """深度(可选): 调 catalog_service 取单品详情, 补充检索结果。

    下游不可用 / 超时 / 非 200 一律优雅降级(返回 None, 不抛错、不阻断检索主流程)。
    """
    try:
        url = f"{_catalog_base_url()}/api/items/{item_id}"
        resp = requests.get(url, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('success') and data.get('item'):
                return data['item']
    except requests.exceptions.RequestException as e:
        logger.warning("[search] catalog_service unavailable, degrade: %s", e)
    except Exception as e:
        logger.warning("[search] catalog enrich failed, degrade: %s", e)
    return None

# ============================================================
# 商品检索
# ============================================================

@app.route('/api/search', methods=['GET'])
@handle_db_error
def search_items():
    """按标题/关键字检索在售商品(独立读模型, 直查 items 表 LIKE title)。

    query params:
      q(标题/品牌/分类 模糊关键字, 空则返回在售商品列表)、page(默认1)、
      per_page(默认12, 最大50)、enrich(=1 时对首条结果调 catalog_service 补详情)。
    返回: 匹配列表 items + 总数 total + 分页元信息。
    """
    q = (request.args.get('q') or '').strip()
    try:
        page = int(request.args.get('page', 1))
    except (TypeError, ValueError):
        page = 1
    try:
        per_page = int(request.args.get('per_page', 12))
    except (TypeError, ValueError):
        per_page = 12
    page = max(page, 1)
    per_page = min(max(per_page, 1), 50)
    offset = (page - 1) * per_page

    where = ["status = 'active'", "price IS NOT NULL"]
    params = []
    if q:
        # CJK 友好:用 LIKE 在 标题/品牌/分类 上做模糊匹配(MySQL FULLTEXT 默认分词器无法分中文)
        # 先转义用户输入中的 LIKE 通配符(! % _)为字面量(顺序: 先 ! 再 % _),配 ESCAPE '!' 子句。
        # 用 ! 作转义符(而非反斜杠), 避免 Python 源 → SQL 文本反斜杠双重转义陷阱(单 \ 会触发 MySQL 1064)。
        like_q = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        where.append("(title LIKE %s ESCAPE '!' OR brand LIKE %s ESCAPE '!' OR category LIKE %s ESCAPE '!')")
        like = f"%{like_q}%"
        params.extend([like, like, like])
    where_sql = " AND ".join(where)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 先取总数(分页元信息)
        cursor.execute(f"SELECT COUNT(id) AS total FROM items WHERE {where_sql}", tuple(params))
        total = cursor.fetchone()['total']

        cursor.execute(
            f"""
            SELECT id, item_id, title, category, brand, price, image_url,
                   description, rating, review_count, status
            FROM items
            WHERE {where_sql}
            ORDER BY id
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (per_page, offset),
        )
        rows = cursor.fetchall()
        items = [_row_to_item_dict(r) for r in rows]
        total_pages = (total + per_page - 1) // per_page if per_page else 0

        result = {
            'success': True,
            'query': q,
            'items': items,
            'page': page,
            'per_page': per_page,
            'total': total,
            'total_pages': total_pages,
        }

        # 深度(可选):enrich=1 时对首条结果调 catalog_service 补详情(降级不阻断)
        if request.args.get('enrich') == '1' and items:
            detail = _enrich_from_catalog(items[0].get('item_id'))
            result['enriched'] = detail is not None
            if detail is not None:
                result['top_detail'] = detail

        return jsonify(result)
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
        'service': 'search_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('SEARCH_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('SEARCH_SERVICE_PORT', '5017'))

    print("=" * 60)
    print("搜索微服务 search_service")
    print("=" * 60)
    print(f"数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"服务地址: http://{host}:{port}")
    print("=" * 60)

    # ---- Nacos 注册 (Phase 1 + Fix) ----
    # 仅在 werkzeug reloader 的子进程(真正跑业务那个)或非 debug 模式下注册,
    # 避免父进程注册后被 reloader 替换导致 atexit 注销。
    _debug = _DEBUG
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not _debug:
        import sys as _sys2
        import atexit as _atexit
        if str(_PROJECT_ROOT) not in _sys2.path:
            _sys2.path.insert(0, str(_PROJECT_ROOT))
        try:
            from shared.nacos_client import register_service, deregister_service
            _NACOS_SERVICE_NAME = "search_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5017
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
