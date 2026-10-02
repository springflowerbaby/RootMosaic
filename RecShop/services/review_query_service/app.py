"""
评论读微服务 review_query_service
使用 Flask + MySQL
拥有数据(读路径): reviews / review_replies(与 review_service 写者配对,共享同一组表)

端点:
    GET /health
    GET /api/reviews?item_id=&status=approved&page=&per_page=   商品评论列表(只读,带回复/用户名/头像)
    GET /api/reviews/summary?item_id=                            评论数 + 均分(只读)

深度链(可选): ?enrich=1 时经 HTTP 调 catalog_service 补商品名/评分用于展示
    (review_query → catalog, 下游不可用时优雅降级, 不崩不阻断主读路径)
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
from functools import wraps
from dotenv import load_dotenv

import requests

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 模板已把项目根加进 sys.path,供 shared.nacos_client 导入(get_service_url 容错已修)
import sys as _sys
if str(_PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PROJECT_ROOT))
try:
    from shared.nacos_client import get_service_url
except Exception:
    get_service_url = None

# 兜底 root logger 配置,确保 OTel init 块的 logger.info 在 LoggingInstrumentor
# 尚未 set_logging_format 之前也能输出(若 LoggingInstrumentor 后续调 basicConfig,
# 因 root 已有 handler 会 no-op,不冲突)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# review_query_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: review_query_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "review_query_service")
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
            logger.info("[otel] review_query_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] review_query_service instrumented")
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

# 数据库连接池(独立 pool_name,与其它服务隔离)
db_pool = pooling.MySQLConnectionPool(
    pool_name="review_query_pool",
    pool_size=3,
    **DB_CONFIG
)

# 占位头像(与 shop_web Review.to_dict() 同源)
_AVATAR_ANON = 'https://placehold.co/40x40/cccccc/333333?text=A'
_AVATAR_USER_DEFAULT = 'https://placehold.co/40x40/cccccc/333333?text=User'
_AVATAR_FALLBACK = 'https://placehold.co/40x40/cccccc/333333?text=U'

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


def _get_catalog_service_url() -> str:
    """解析 catalog_service URL(Nacos 优先,回退 env / localhost:5005)。"""
    fb = os.environ.get('CATALOG_SERVICE_URL', 'http://127.0.0.1:5005')
    if get_service_url is None:
        return fb
    try:
        return get_service_url("catalog_service", fallback_url=fb) or fb
    except Exception:
        return fb


def _fetch_item_from_catalog(item_id):
    """深度链 review_query→catalog: HTTP 调 catalog_service 取单品(商品名/评分用于展示)。

    返回 catalog 的 item dict 或 None(不存在/下游不可用)。
    所有异常被吞掉返回 None,主读路径不受影响(优雅降级)。
    """
    try:
        resp = requests.get(
            f'{_get_catalog_service_url()}/api/items/{item_id}',
            timeout=8,
        )
        if resp.status_code != 200:
            return None
        body = resp.json() or {}
        if not body.get('success'):
            return None
        return body.get('item')
    except requests.exceptions.RequestException as e:
        logger.warning('[review_query] catalog_service unavailable for item %s: %s', item_id, e)
        return None
    except Exception as e:
        logger.warning('[review_query] catalog_service item fetch failed for %s: %s', item_id, e)
        return None


def _row_to_review_dict(row):
    """把一行(LEFT JOIN users / review_replies 的 dict cursor)序列化为前端友好 JSON。

    字段/兜底逻辑对齐 shop_web 的 Review.to_dict():匿名遮蔽用户名/头像、reply 嵌套。
    """
    is_anonymous = int(row.get('is_anonymous') or 0)
    username = row.get('username')
    avatar_url = row.get('avatar_url')
    if is_anonymous:
        display_name = 'Anonymous'
        avatar = _AVATAR_ANON
    else:
        display_name = username if username else 'User'
        avatar = avatar_url if avatar_url else (_AVATAR_USER_DEFAULT if username is not None else _AVATAR_FALLBACK)

    created_at = row.get('created_at')
    date_str = created_at.strftime('%Y-%m-%d') if created_at else ''

    reply = None
    if row.get('reply_id') is not None:
        reply_created = row.get('reply_created_at')
        reply = {
            'id': row.get('reply_id'),
            'content': row.get('reply_content'),
            'date': reply_created.strftime('%Y-%m-%d') if reply_created else '',
        }

    # reviews.images 是 JSON 列, mysql-connector 对 JSON 列返回 str —
    # 服务端统一 json.loads 反序列化(解析失败/非 list 兜底 []),所有消费方受益,
    # 避免下游 Jinja 首屏 {% for img in review.images %} 把 str 逐字符迭代。
    images = row.get('images')
    if isinstance(images, str):
        try:
            images = json.loads(images)
        except (ValueError, TypeError):
            images = []
    if not isinstance(images, list):
        images = []

    return {
        'id': row.get('id'),
        'username': display_name,
        'avatar': avatar,
        'rating': row.get('rating'),
        'content': row.get('content') or '',
        'images': images,
        'date': date_str,
        'is_anonymous': is_anonymous,
        'reply': reply,
    }

# ============================================================
# 评论读
# ============================================================

@app.route('/api/reviews', methods=['GET'])
@handle_db_error
def list_reviews():
    """商品评论列表(只读,默认仅 approved),支持分页 + 可选 catalog 富化。

    query params:
        item_id  (必填)
        status   (默认 approved;传 all 则不过滤状态)
        page     (默认 1)
        per_page (默认 10,最大 50)
        enrich   (=1 时附带 catalog 商品名/评分,下游不可用则降级省略)
    """
    item_id = (request.args.get('item_id') or '').strip()
    if not item_id:
        return jsonify({'success': False, 'message': 'item_id is required'}), 400

    status = (request.args.get('status') or 'approved').strip()
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
    enrich = request.args.get('enrich') in ('1', 'true', 'True')

    where = ["r.item_id = %s"]
    params = [item_id]
    if status and status.lower() != 'all':
        where.append("r.status = %s")
        params.append(status)
    where_sql = " AND ".join(where)

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 总数(分页元信息)
        cursor.execute(f"SELECT COUNT(r.id) AS total FROM reviews r WHERE {where_sql}", tuple(params))
        total = cursor.fetchone()['total']

        # LEFT JOIN users(用户名/头像) + review_replies(商家回复) 一次取齐
        cursor.execute(
            f"""
            SELECT
                r.id, r.item_id, r.rating, r.content, r.images,
                r.is_anonymous, r.status, r.created_at,
                u.username AS username, u.avatar_url AS avatar_url,
                rr.id AS reply_id, rr.content AS reply_content, rr.created_at AS reply_created_at
            FROM reviews r
            LEFT JOIN users u ON r.user_token = u.user_token
            LEFT JOIN review_replies rr ON r.id = rr.review_id
            WHERE {where_sql}
            ORDER BY r.created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (per_page, offset),
        )
        rows = cursor.fetchall()
        reviews = [_row_to_review_dict(r) for r in rows]
        total_pages = (total + per_page - 1) // per_page if per_page else 0

        result = {
            'success': True,
            'reviews': reviews,
            'page': page,
            'per_page': per_page,
            'total': total,
            'total_pages': total_pages,
        }

        # 可选深度链: 富化商品展示信息(下游不可用优雅降级,标记 degraded)
        if enrich:
            item = _fetch_item_from_catalog(item_id)
            if item is not None:
                result['item'] = {
                    'name': item.get('name'),
                    'rating': item.get('rating'),
                    'review_count': item.get('review_count'),
                }
            else:
                result['item'] = None
                result['catalog_degraded'] = True

        return jsonify(result)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/reviews/summary', methods=['GET'])
@handle_db_error
def reviews_summary():
    """评论概览:某商品的 approved 评论数 + 均分(只读)。

    query params: item_id(必填)。返回 {total, avg_rating}。
    """
    item_id = (request.args.get('item_id') or '').strip()
    if not item_id:
        return jsonify({'success': False, 'message': 'item_id is required'}), 400

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT COUNT(id) AS total, AVG(rating) AS avg_rating
            FROM reviews
            WHERE item_id = %s AND status = 'approved'
            """,
            (item_id,),
        )
        row = cursor.fetchone() or {}
        total = row.get('total') or 0
        avg = row.get('avg_rating')
        return jsonify({
            'success': True,
            'item_id': item_id,
            'total': total,
            'avg_rating': round(float(avg), 1) if avg is not None else 0.0,
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
        'service': 'review_query_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('REVIEW_QUERY_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('REVIEW_QUERY_SERVICE_PORT', '5018'))

    print("=" * 60)
    print("评论读微服务 review_query_service")
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
            _NACOS_SERVICE_NAME = "review_query_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5018
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
