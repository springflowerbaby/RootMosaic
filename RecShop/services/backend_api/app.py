"""
SASRec Demo 后端 API 服务
使用 Flask + MySQL
"""
from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling
import requests
import json
import os
import logging
from pathlib import Path
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv
from opentelemetry import trace as _otel_trace_api

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# 兜底 root logger 配置,确保 OTel init 块的 logger.info 在 LoggingInstrumentor
# 尚未 set_logging_format 之前也能输出(若 LoggingInstrumentor 后续调 basicConfig,
# 因 root 已有 handler 会 no-op,不冲突)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# backend 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: backend_api 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与 line 535 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "backend_api")
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
            logger.info("[otel] backend_api log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] backend_api instrumented")
    except Exception as _otel_e:
        logger.warning(f"[otel] init failed (ignored): {_otel_e}")
        _mysql_instrumentor = None
# ============================================================

# ==================== 业务 metric instrument ====================
# 模块级 meter + instrument, 供 get_recommendations() 埋点。OTel 未就绪时降级为 None。
_RECOMMEND_COUNT_HIST = None
try:
    from opentelemetry import metrics as _otel_metrics_api
    _backend_meter = _otel_metrics_api.get_meter(__name__)
    _RECOMMEND_COUNT_HIST = _backend_meter.create_histogram(
        name="recweb_recommend_count",
        unit="1",
        description="backend_api 单次推荐返回条数分布",
    )
except Exception as _m_e:
    logger.warning(f"[otel] backend metric init failed (ignored): {_m_e}")
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

SASREC_API_URL = os.environ.get('SASREC_API_URL', 'http://127.0.0.1:8200')

# Phase 2: 动态服务发现(失败时 fallback 到上面的 SASREC_API_URL)
from service_discovery import get_sasrec_api_url

# 数据库连接池
db_pool = pooling.MySQLConnectionPool(
    pool_name="sasrec_pool",
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
# 用户相关 API
# ============================================================

@app.route('/api/users/<user_token>', methods=['GET'])
@handle_db_error
def get_user(user_token):
    """获取用户信息"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT id, user_token, username, email, avatar_url, status, created_at, updated_at "
            "FROM users WHERE user_token = %s",
            (user_token,)
        )
        user = cursor.fetchone()

        if user:
            return jsonify(user)
        else:
            return jsonify({'error': '用户不存在'}), 404
    finally:
        cursor.close()
        conn.close()

@app.route('/api/users', methods=['POST'])
@handle_db_error
def create_user():
    """创建用户"""
    data = request.get_json(silent=True) or {}
    user_token = data.get('user_token')
    email = data.get('email')

    if not user_token:
        return jsonify({'error': 'user_token 必填'}), 400

    username = data.get('username', f'User_{user_token[:8]}')

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO users (user_token, username, email) VALUES (%s, %s, %s)",
            (user_token, username, email)
        )
        conn.commit()
        user_id = cursor.lastrowid

        return jsonify({'id': user_id, 'user_token': user_token, 'username': username}), 201
    finally:
        cursor.close()
        conn.close()

@app.route('/api/users/<user_token>/history', methods=['GET'])
@handle_db_error
def get_user_history(user_token):
    """获取用户历史交互"""
    limit = request.args.get('limit', 50, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = """
            SELECT
                i.id,
                i.item_id,
                it.title,
                it.image_url,
                it.category,
                it.price,
                i.interaction_type,
                i.timestamp,
                FROM_UNIXTIME(i.timestamp/1000) as interaction_time
            FROM interactions i
            JOIN items it ON i.item_id = it.item_id
            WHERE i.user_token = %s
            ORDER BY i.timestamp DESC
            LIMIT %s
        """

        cursor.execute(query, (user_token, limit))
        history = cursor.fetchall()

        return jsonify(history)
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 商品相关 API
# ============================================================

@app.route('/api/items', methods=['GET'])
@handle_db_error
def get_items():
    """获取商品列表（分页）"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    category = request.args.get('category')

    page = max(page, 1)
    per_page = min(max(per_page, 1), 100)

    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 构建查询
        # 注意: 每次 cursor.execute() 后必须立即 fetchall/fetchone 消费结果,
        # 否则 mysql-connector 会抛 "Unread result found"。
        if category:
            # category 含 idx_category 单列索引, 但 leading-% 的 '%X%' 使谓词不可 sargable
            # (data 全表扫 type=ALL, count 全索引扫), 改用前缀 'X%' 走 idx_category range 扫描。
            # 先转义用户输入中的 LIKE 通配符(\ % _)为字面量(顺序: 先 \ 再 % _),
            # 仅保留尾部未转义的 % 作前缀通配符, 防止用户传 '%abc' 再次退化为 leading-% 全表扫。
            like_prefix = category.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
            query = ("SELECT id, item_id, title, category, brand, price, image_url, description, "
                     "rating, review_count, merchant_id, status, created_at, updated_at "
                     "FROM items WHERE category LIKE %s LIMIT %s OFFSET %s")
            cursor.execute(query, (like_prefix, per_page, offset))
            items = cursor.fetchall()

            count_query = "SELECT COUNT(*) as total FROM items WHERE category LIKE %s"
            cursor.execute(count_query, (like_prefix,))
            total = cursor.fetchone()['total']
        else:
            query = ("SELECT id, item_id, title, category, brand, price, image_url, description, "
                     "rating, review_count, merchant_id, status, created_at, updated_at "
                     "FROM items LIMIT %s OFFSET %s")
            cursor.execute(query, (per_page, offset))
            items = cursor.fetchall()

            # 全表无过滤总数仅作分页量级提示, items 表(38万行/165MB)超出
            # innodb_buffer_pool_size(默认128MB), 精确 COUNT(*) 必扫全聚簇索引且打盘(~23s),
            # 改用 information_schema 的 InnoDB 行数估算(~11ms), 契约不变(仍是量级数字)。
            count_query = ("SELECT table_rows as total FROM information_schema.tables "
                           "WHERE table_schema = %s AND table_name = 'items'")
            cursor.execute(count_query, (DB_CONFIG['database'],))
            row = cursor.fetchone()
            # table_rows 对 InnoDB 始终是估算, 统计未初始化的极端情况可能为 None, 兜底为 0
            total = (row['total'] if row and row['total'] is not None else 0)

        return jsonify({
            'items': items,
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/items/<item_id>', methods=['GET'])
@handle_db_error
def get_item(item_id):
    """获取商品详情"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT id, item_id, title, category, brand, price, image_url, description, "
            "rating, review_count, merchant_id, status, created_at, updated_at "
            "FROM items WHERE item_id = %s",
            (item_id,)
        )
        item = cursor.fetchone()

        if item:
            return jsonify(item)
        else:
            return jsonify({'error': '商品不存在'}), 404
    finally:
        cursor.close()
        conn.close()

@app.route('/api/items/search', methods=['GET'])
@handle_db_error
def search_items():
    """搜索商品"""
    query = request.args.get('q', '')
    limit = request.args.get('limit', 20, type=int)
    
    if not query:
        return jsonify({'error': '搜索关键词不能为空'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        search_query = """
            SELECT id, item_id, title, category, brand, price, image_url, description,
                   rating, review_count, merchant_id, status, created_at, updated_at
            FROM items
            WHERE title LIKE %s OR category LIKE %s OR brand LIKE %s
            LIMIT %s
        """
        search_pattern = f'%{query}%'
        cursor.execute(search_query, (search_pattern, search_pattern, search_pattern, limit))
        items = cursor.fetchall()

        return jsonify(items)
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 交互相关 API
# ============================================================

@app.route('/api/interactions', methods=['POST'])
@handle_db_error
def record_interaction():
    """记录用户交互"""
    data = request.get_json(silent=True) or {}
    user_token = data.get('user_token')
    item_id = data.get('item_id')
    interaction_type = data.get('interaction_type', 'view')
    rating = data.get('rating')

    if not user_token or not item_id:
        return jsonify({'error': 'user_token 和 item_id 必填'}), 400

    timestamp = int(datetime.now().timestamp() * 1000)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO interactions (user_token, item_id, interaction_type, rating, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_token, item_id, interaction_type, rating, timestamp))

        conn.commit()
        interaction_id = cursor.lastrowid

        return jsonify({'id': interaction_id, 'timestamp': timestamp}), 201
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 推荐相关 API
# ============================================================

@app.route('/api/recommend', methods=['POST'])
@handle_db_error
def get_recommendations():
    """获取推荐（调用 SASRec API）"""
    data = request.get_json(silent=True) or {}
    user_token = data.get('user_token')
    top_k = data.get('top_k', 10)

    if not user_token:
        return jsonify({'error': 'user_token 必填'}), 400

    # 1. 获取用户历史交互
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT item_id FROM interactions
            WHERE user_token = %s
            ORDER BY timestamp DESC
            LIMIT 50
        """, (user_token,))

        history = cursor.fetchall()
        item_sequence = [h['item_id'] for h in history]

        if not item_sequence:
            return jsonify({'error': '用户没有历史交互记录'}), 400

        # 2. 调用 SASRec API
        try:
            response = requests.post(f'{get_sasrec_api_url()}/recommend', json={
                'item_sequence': item_sequence,
                'top_k': top_k,
                'exclude_history': True
            }, timeout=10)

            if response.status_code != 200:
                return jsonify({'error': '推荐服务错误', 'details': response.text}), 500

            result = response.json()
            recommendations = result['recommendations']
            inference_time = result['inference_time']

        except requests.exceptions.RequestException as e:
            return jsonify({'error': f'无法连接推荐服务: {str(e)}'}), 503

        # 3. 保存推荐记录
        for rec in recommendations:
            cursor.execute("""
                INSERT INTO recommendations
                (user_token, item_id, score, `rank`, input_sequence, inference_time)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                user_token,
                rec['item_id'],
                rec['score'],
                rec['rank'],
                json.dumps(item_sequence[:10]),  # 只保存前10个
                inference_time
            ))

        conn.commit()

        # OTel: 给当前 Flask server span(POST /api/recommend)补推荐条数
        _otel_trace_api.get_current_span().set_attribute("recweb.recommend.count", len(recommendations))
        # OTel metric: 推荐返回条数分布(无 label, 全局聚合)
        if _RECOMMEND_COUNT_HIST is not None:
            _RECOMMEND_COUNT_HIST.record(int(len(recommendations)))

        # 4. 获取推荐商品的详细信息
        item_ids = [rec['item_id'] for rec in recommendations]
        placeholders = ','.join(['%s'] * len(item_ids))

        cursor.execute(f"""
            SELECT id, item_id, title, category, brand, price, image_url, description,
                   rating, review_count, merchant_id, status, created_at, updated_at
            FROM items WHERE item_id IN ({placeholders})
        """, item_ids)

        items = cursor.fetchall()
        items_dict = {item['item_id']: item for item in items}
    finally:
        cursor.close()
        conn.close()

    # 5. 组合推荐结果和商品信息
    result_items = []
    for rec in recommendations:
        item = items_dict.get(rec['item_id'])
        if item:
            result_items.append({
                **item,
                'recommendation_score': rec['score'],
                'recommendation_rank': rec['rank']
            })

    return jsonify({
        'recommendations': result_items,
        'inference_time': inference_time,
        'input_sequence_length': len(item_sequence)
    })

@app.route('/api/recommend/feedback', methods=['POST'])
@handle_db_error
def recommendation_feedback():
    """反馈推荐效果"""
    data = request.get_json(silent=True) or {}
    user_token = data.get('user_token')
    item_id = data.get('item_id')
    feedback_type = data.get('type')  # 'click' or 'purchase'

    if not all([user_token, item_id, feedback_type]):
        return jsonify({'error': '参数不完整'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if feedback_type == 'click':
            cursor.execute("""
                UPDATE recommendations
                SET is_clicked = TRUE
                WHERE user_token = %s AND item_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (user_token, item_id))
        elif feedback_type == 'purchase':
            cursor.execute("""
                UPDATE recommendations
                SET is_purchased = TRUE
                WHERE user_token = %s AND item_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (user_token, item_id))

        conn.commit()

        return jsonify({'success': True})
    finally:
        cursor.close()
        conn.close()

# ============================================================
# 统计相关 API
# ============================================================

@app.route('/api/stats/model', methods=['GET'])
@handle_db_error
def get_model_stats():
    """获取模型性能指标"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT * FROM model_metrics
            ORDER BY created_at DESC
        """)
        metrics = cursor.fetchall()

        return jsonify(metrics)
    finally:
        cursor.close()
        conn.close()

@app.route('/api/stats/recommendations', methods=['GET'])
@handle_db_error
def get_recommendation_stats():
    """获取推荐效果统计"""
    days = request.args.get('days', 7, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT * FROM recommendation_performance
            ORDER BY date DESC
            LIMIT %s
        """, (days,))

        stats = cursor.fetchall()

        return jsonify(stats)
    finally:
        cursor.close()
        conn.close()

@app.route('/api/stats/popular-items', methods=['GET'])
@handle_db_error
def get_popular_items():
    """获取热门商品"""
    limit = request.args.get('limit', 10, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT * FROM item_popularity_stats
            ORDER BY interaction_count DESC
            LIMIT %s
        """, (limit,))

        items = cursor.fetchall()

        return jsonify(items)
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
        # 检查数据库连接
        conn = get_db_connection()
        conn.close()
        db_status = 'healthy'
    except:
        db_status = 'unhealthy'
    
    try:
        # 检查 SASRec API
        response = requests.get(f'{get_sasrec_api_url()}/health', timeout=5)
        sasrec_status = 'healthy' if response.status_code == 200 else 'unhealthy'
    except:
        sasrec_status = 'unhealthy'
    
    return jsonify({
        'status': 'healthy' if db_status == 'healthy' and sasrec_status == 'healthy' else 'degraded',
        'database': db_status,
        'sasrec_api': sasrec_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('BACKEND_HOST', '0.0.0.0')
    port = int(os.environ.get('BACKEND_PORT', '5000'))

    print("=" * 60)
    print("SASRec Demo 后端服务")
    print("=" * 60)
    print(f"数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"SASRec API: {SASREC_API_URL}")
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
            _NACOS_SERVICE_NAME = "backend_api"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5000
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
