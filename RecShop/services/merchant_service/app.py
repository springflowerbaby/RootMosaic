"""
商家资料微服务 merchant_service
使用 Flask + MySQL
拥有数据: merchants(商家资料) + shops(店铺) —— 商家资料/店铺读 + 部分更新
端点:
  GET  /health
  GET  /api/merchants/<id>      商家资料 + 其店铺
  POST /api/merchants/<id>      更新资料/店铺字段(单事务,部分更新)
无下游调用。
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

# merchant_service 固定 debug=False(关 reloader: 单进程, 连接不翻倍, 注入窗口更干净, 关 Werkzeug 调试器), 与下方 __main__ 的 app.run(debug=) 一致(复用同一常量)
_DEBUG = False

app = Flask(__name__)
# ==================== OpenTelemetry init ====================
# FLASK_DEBUG: merchant_service 用 Flask 默认的 FLASK_DEBUG 控制 debug 模式,
# 守卫与下方 Nacos 守卫同款(WERKZEUG_RUN_MAIN 子进程 或 非 debug)。
# 避免父进程残留 BatchSpanProcessor 后台线程被 reloader fork 后污染子进程。
_mysql_instrumentor = None  # Audit 后补丁: 暴露给 get_db_connection() 用 instrument_connection 包 pool 连接
if os.environ.get("OTEL_ENABLED", "true").lower() == "true" and (
    os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    or not _DEBUG
):
    os.environ.setdefault("OTEL_SERVICE_NAME", "merchant_service")
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
            logger.info("[otel] merchant_service log bridge installed")
        except Exception as _otel_log_e:
            logger.warning(f"[otel] log bridge init failed (ignored): {_otel_log_e}")

        # meter_provider= 显式传给 FlaskInstrumentor → 自动产 http.server.* RED 指标
        _FlaskInstr().instrument_app(app, meter_provider=_meter_provider)
        _RequestsInstr().instrument()
        _mysql_instrumentor = _MySQLInstr()
        _mysql_instrumentor.instrument()
        _LoggingInstr().instrument(set_logging_format=True)
        logger.info("[otel] merchant_service instrumented")
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
    pool_name="merchant_pool",
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


def _merchant_to_dict(row):
    """把 merchants 表的一行(dict cursor) 序列化为前端友好的商家资料 JSON。"""
    created = row.get('created_at')
    updated = row.get('updated_at')
    return {
        'id': row.get('id'),
        'merchant_token': row.get('merchant_token'),
        'username': row.get('username'),
        'email': row.get('email'),
        'phone': row.get('phone'),
        'status': row.get('status'),
        'created_at': created.strftime('%Y-%m-%d %H:%M:%S') if isinstance(created, datetime) else created,
        'updated_at': updated.strftime('%Y-%m-%d %H:%M:%S') if isinstance(updated, datetime) else updated,
    }


def _shop_to_dict(row):
    """把 shops 表的一行(dict cursor) 序列化为店铺 JSON。"""
    return {
        'id': row.get('id'),
        'merchant_id': row.get('merchant_id'),
        'name': row.get('name'),
        'description': row.get('description'),
        'logo_url': row.get('logo_url'),
        'status': row.get('status'),
    }

# ============================================================
# 商家资料 / 店铺
# ============================================================

@app.route('/api/merchants/<int:merchant_id>', methods=['GET'])
@handle_db_error
def get_merchant(merchant_id):
    """商家资料 + 其店铺:按 merchants.id 查 merchants 与 shops。"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT id, merchant_token, username, email, phone, status,
                   created_at, updated_at
            FROM merchants
            WHERE id = %s
            """,
            (merchant_id,),
        )
        m_row = cursor.fetchone()
        if m_row is None:
            return jsonify({'success': False, 'message': 'Merchant not found'}), 404

        cursor.execute(
            """
            SELECT id, merchant_id, name, description, logo_url, status
            FROM shops
            WHERE merchant_id = %s
            ORDER BY id ASC
            LIMIT 1
            """,
            (merchant_id,),
        )
        s_row = cursor.fetchone()
        return jsonify({
            'success': True,
            'merchant': _merchant_to_dict(m_row),
            'shop': _shop_to_dict(s_row) if s_row else None,
        })
    finally:
        cursor.close()
        conn.close()


@app.route('/api/merchants/<int:merchant_id>', methods=['POST'])
@handle_db_error
def update_merchant(merchant_id):
    """更新商家资料 / 店铺字段(单事务,部分更新)。

    由 shop_web 在完成商家鉴权 / 归属校验后代理调用。请求体(均可选,部分更新):
      merchant 资料:  username, phone
      店铺字段:        shop_name, shop_description, shop_logo_url, shop_status
    店铺不存在时,若带任一 shop_* 字段则自动建店(name 兜底 username 或 '我的店铺')。
    全部在同一连接 / 同一事务内完成,失败回滚。
    """
    data = request.get_json() or {}

    # --- 收集 merchant 资料的部分更新(只更新请求里出现的键) ---
    merchant_sets = []
    merchant_params = []
    if 'username' in data:
        username = (data.get('username') or '').strip()
        if username:
            merchant_sets.append('username = %s')
            merchant_params.append(username)
    if 'phone' in data:
        phone = (data.get('phone') or '').strip()
        merchant_sets.append('phone = %s')
        merchant_params.append(phone if phone else None)

    # --- 收集店铺字段的部分更新 ---
    has_shop_update = any(k in data for k in ('shop_name', 'shop_description', 'shop_logo_url', 'shop_status'))
    shop_status = data.get('shop_status')
    if shop_status is not None and shop_status not in ('active', 'closed'):
        return jsonify({'success': False, 'message': 'Invalid shop_status'}), 400

    if not merchant_sets and not has_shop_update:
        return jsonify({'success': False, 'message': 'No updatable fields provided'}), 400

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 归属/存在性:目标商家必须存在(shop_web 已做归属校验,这里再兜一层存在性)
        cursor.execute(
            "SELECT id, username FROM merchants WHERE id = %s",
            (merchant_id,),
        )
        m_row = cursor.fetchone()
        if m_row is None:
            return jsonify({'success': False, 'message': 'Merchant not found'}), 404

        # 1) 更新 merchant 资料
        if merchant_sets:
            cursor.execute(
                f"UPDATE merchants SET {', '.join(merchant_sets)} WHERE id = %s",
                tuple(merchant_params) + (merchant_id,),
            )

        # 2) 更新 / 新建店铺
        if has_shop_update:
            cursor.execute(
                "SELECT id FROM shops WHERE merchant_id = %s ORDER BY id ASC LIMIT 1",
                (merchant_id,),
            )
            shop_row = cursor.fetchone()

            shop_name = (data.get('shop_name') or '').strip() if data.get('shop_name') is not None else None
            shop_desc = data.get('shop_description')
            shop_desc = shop_desc.strip() if isinstance(shop_desc, str) else shop_desc
            shop_logo = data.get('shop_logo_url')
            shop_logo = (shop_logo.strip() or None) if isinstance(shop_logo, str) else shop_logo

            if shop_row is None:
                # 自动建店(照 shop_web shop_settings 的兜底:name 用 shop_name -> username -> '我的店铺')
                fallback_name = shop_name or m_row.get('username') or '我的店铺'
                cursor.execute(
                    """
                    INSERT INTO shops (merchant_id, name, description, logo_url, status)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        merchant_id,
                        fallback_name,
                        shop_desc,
                        shop_logo,
                        shop_status or 'active',
                    ),
                )
            else:
                shop_sets = []
                shop_params = []
                if 'shop_name' in data and shop_name:
                    shop_sets.append('name = %s')
                    shop_params.append(shop_name)
                if 'shop_description' in data:
                    shop_sets.append('description = %s')
                    shop_params.append(shop_desc)
                if 'shop_logo_url' in data:
                    shop_sets.append('logo_url = %s')
                    shop_params.append(shop_logo)
                if 'shop_status' in data and shop_status is not None:
                    shop_sets.append('status = %s')
                    shop_params.append(shop_status)
                if shop_sets:
                    cursor.execute(
                        f"UPDATE shops SET {', '.join(shop_sets)} WHERE id = %s",
                        tuple(shop_params) + (shop_row['id'],),
                    )

        conn.commit()

        # 回读最新资料 + 店铺返回(同连接)
        cursor.execute(
            """
            SELECT id, merchant_token, username, email, phone, status,
                   created_at, updated_at
            FROM merchants WHERE id = %s
            """,
            (merchant_id,),
        )
        m_final = cursor.fetchone()
        cursor.execute(
            """
            SELECT id, merchant_id, name, description, logo_url, status
            FROM shops WHERE merchant_id = %s ORDER BY id ASC LIMIT 1
            """,
            (merchant_id,),
        )
        s_final = cursor.fetchone()
        return jsonify({
            'success': True,
            'message': 'Merchant updated successfully',
            'merchant': _merchant_to_dict(m_final) if m_final else None,
            'shop': _shop_to_dict(s_final) if s_final else None,
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
        'service': 'merchant_service',
        'database': db_status
    })

# ============================================================
# 主程序
# ============================================================

if __name__ == '__main__':
    host = os.environ.get('MERCHANT_SERVICE_HOST', '0.0.0.0')
    port = int(os.environ.get('MERCHANT_SERVICE_PORT', '5019'))

    print("=" * 60)
    print("商家资料微服务 merchant_service")
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
            _NACOS_SERVICE_NAME = "merchant_service"
            _NACOS_IP = "127.0.0.1"
            _NACOS_PORT = 5019
            register_service(_NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
            _atexit.register(deregister_service, _NACOS_SERVICE_NAME, _NACOS_IP, _NACOS_PORT)
        except Exception as _e:
            print(f"[nacos] 注册流程异常,已忽略: {_e}")
    # --------------------------------

    app.run(host=host, port=port, debug=_debug)
