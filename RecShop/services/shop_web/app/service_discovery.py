"""
ShopWeb 的服务发现包装层(Phase 2)

把 `shared.nacos_client.get_service_url` 按服务名封装成 get_backend_api_url /
get_rerank_service_url,并把原 routes.py 顶层定义的模块级常量作为 fallback
传入。Nacos 不可达或关闭时,行为与 Phase 1 前完全一致。

- 每次 HTTP 调用前实时查询,不做缓存
- 任何异常都被 nacos_client 内部吞掉,这里只透传返回值
"""

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from shared.nacos_client import get_service_url as _nacos_get_service_url
except Exception:
    _nacos_get_service_url = None


# 最后一道防线:与 routes.py 中模块级常量保持同源,env 缺失时回退到硬编码默认值
def _fallback_backend_api_url() -> str:
    return os.environ.get('BACKEND_API_URL', 'http://127.0.0.1:5000')


def _fallback_rerank_service_url() -> str:
    return os.environ.get('RERANK_SERVICE_URL', 'http://127.0.0.1:5002')


def _fallback_review_service_url() -> str:
    return os.environ.get('REVIEW_SERVICE_URL', 'http://127.0.0.1:5003')


def _fallback_review_query_service_url() -> str:
    return os.environ.get('REVIEW_QUERY_SERVICE_URL', 'http://127.0.0.1:5018')


def _fallback_catalog_service_url() -> str:
    return os.environ.get('CATALOG_SERVICE_URL', 'http://127.0.0.1:5005')


def _fallback_cart_service_url() -> str:
    return os.environ.get('CART_SERVICE_URL', 'http://127.0.0.1:5006')


def _fallback_user_service_url() -> str:
    return os.environ.get('USER_SERVICE_URL', 'http://127.0.0.1:5004')


def _fallback_address_service_url() -> str:
    return os.environ.get('ADDRESS_SERVICE_URL', 'http://127.0.0.1:5007')


def _fallback_ai_memory_service_url() -> str:
    return os.environ.get('AI_MEMORY_SERVICE_URL', 'http://127.0.0.1:5008')


def _fallback_announcement_service_url() -> str:
    return os.environ.get('ANNOUNCEMENT_SERVICE_URL', 'http://127.0.0.1:5009')


def _fallback_order_service_url() -> str:
    return os.environ.get('ORDER_SERVICE_URL', 'http://127.0.0.1:5010')


def _fallback_checkout_service_url() -> str:
    return os.environ.get('CHECKOUT_SERVICE_URL', 'http://127.0.0.1:5011')


def _fallback_payment_service_url() -> str:
    return os.environ.get('PAYMENT_SERVICE_URL', 'http://127.0.0.1:5012')


def _fallback_promotion_service_url() -> str:
    return os.environ.get('PROMOTION_SERVICE_URL', 'http://127.0.0.1:5015')


def _fallback_inventory_service_url() -> str:
    return os.environ.get('INVENTORY_SERVICE_URL', 'http://127.0.0.1:5013')


def _fallback_interaction_service_url() -> str:
    return os.environ.get('INTERACTION_SERVICE_URL', 'http://127.0.0.1:5020')


def _fallback_merchant_service_url() -> str:
    return os.environ.get('MERCHANT_SERVICE_URL', 'http://127.0.0.1:5019')


def _fallback_admin_audit_service_url() -> str:
    return os.environ.get('ADMIN_AUDIT_SERVICE_URL', 'http://127.0.0.1:5022')


def _fallback_search_service_url() -> str:
    return os.environ.get('SEARCH_SERVICE_URL', 'http://127.0.0.1:5017')


def _fallback_shipping_service_url() -> str:
    return os.environ.get('SHIPPING_SERVICE_URL', 'http://127.0.0.1:5016')


def _fallback_notification_service_url() -> str:
    return os.environ.get('NOTIFICATION_SERVICE_URL', 'http://127.0.0.1:5021')


def get_backend_api_url() -> str:
    """获取 Backend API 的 URL。失败时 fallback 到 env `BACKEND_API_URL`。"""
    fb = _fallback_backend_api_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("backend_api", fallback_url=fb) or fb


def get_rerank_service_url() -> str:
    """获取 LLM Rerank 服务的 URL。失败时 fallback 到 env `RERANK_SERVICE_URL`。"""
    fb = _fallback_rerank_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("llm_rerank_service", fallback_url=fb) or fb


def get_review_service_url() -> str:
    """获取 Review Service 的 URL。失败时 fallback 到 env `REVIEW_SERVICE_URL`。"""
    fb = _fallback_review_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("review_service", fallback_url=fb) or fb


def get_review_query_service_url() -> str:
    """获取 Review Query Service 的 URL。失败时 fallback 到 env `REVIEW_QUERY_SERVICE_URL`。"""
    fb = _fallback_review_query_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("review_query_service", fallback_url=fb) or fb


def get_catalog_service_url() -> str:
    """获取 Catalog Service 的 URL。失败时 fallback 到 env `CATALOG_SERVICE_URL`。"""
    fb = _fallback_catalog_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("catalog_service", fallback_url=fb) or fb


def get_cart_service_url() -> str:
    """获取 Cart Service 的 URL。失败时 fallback 到 env `CART_SERVICE_URL`。"""
    fb = _fallback_cart_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("cart_service", fallback_url=fb) or fb


def get_user_service_url() -> str:
    """获取 User Service 的 URL。失败时 fallback 到 env `USER_SERVICE_URL`。"""
    fb = _fallback_user_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("user_service", fallback_url=fb) or fb


def get_address_service_url() -> str:
    """获取 Address Service 的 URL。失败时 fallback 到 env `ADDRESS_SERVICE_URL`。"""
    fb = _fallback_address_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("address_service", fallback_url=fb) or fb


def get_ai_memory_service_url() -> str:
    """获取 AI Memory Service 的 URL。失败时 fallback 到 env `AI_MEMORY_SERVICE_URL`。"""
    fb = _fallback_ai_memory_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("ai_memory_service", fallback_url=fb) or fb


def get_announcement_service_url() -> str:
    """获取 Announcement Service 的 URL。失败时 fallback 到 env `ANNOUNCEMENT_SERVICE_URL`。"""
    fb = _fallback_announcement_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("announcement_service", fallback_url=fb) or fb


def get_order_service_url() -> str:
    """获取 Order Service 的 URL。失败时 fallback 到 env `ORDER_SERVICE_URL`。"""
    fb = _fallback_order_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("order_service", fallback_url=fb) or fb


def get_checkout_service_url() -> str:
    """获取 Checkout Service 的 URL。失败时 fallback 到 env `CHECKOUT_SERVICE_URL`。"""
    fb = _fallback_checkout_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("checkout_service", fallback_url=fb) or fb


def get_payment_service_url() -> str:
    """获取 Payment Service 的 URL。失败时 fallback 到 env `PAYMENT_SERVICE_URL`。"""
    fb = _fallback_payment_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("payment_service", fallback_url=fb) or fb


def get_promotion_service_url() -> str:
    """获取 Promotion Service 的 URL。失败时 fallback 到 env `PROMOTION_SERVICE_URL`。"""
    fb = _fallback_promotion_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("promotion_service", fallback_url=fb) or fb


def get_inventory_service_url() -> str:
    """获取 Inventory Service 的 URL。失败时 fallback 到 env `INVENTORY_SERVICE_URL`。"""
    fb = _fallback_inventory_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("inventory_service", fallback_url=fb) or fb


def get_interaction_service_url() -> str:
    """获取 Interaction Service 的 URL。失败时 fallback 到 env `INTERACTION_SERVICE_URL`。"""
    fb = _fallback_interaction_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("interaction_service", fallback_url=fb) or fb


def get_merchant_service_url() -> str:
    """获取 Merchant Service 的 URL。失败时 fallback 到 env `MERCHANT_SERVICE_URL`。"""
    fb = _fallback_merchant_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("merchant_service", fallback_url=fb) or fb


def get_admin_audit_service_url() -> str:
    """获取 Admin Audit Service 的 URL。失败时 fallback 到 env `ADMIN_AUDIT_SERVICE_URL`。"""
    fb = _fallback_admin_audit_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("admin_audit_service", fallback_url=fb) or fb


def get_search_service_url() -> str:
    """获取 Search Service 的 URL。失败时 fallback 到 env `SEARCH_SERVICE_URL`。"""
    fb = _fallback_search_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("search_service", fallback_url=fb) or fb


def get_shipping_service_url() -> str:
    """获取 Shipping Service 的 URL。失败时 fallback 到 env `SHIPPING_SERVICE_URL`。"""
    fb = _fallback_shipping_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("shipping_service", fallback_url=fb) or fb


def get_notification_service_url() -> str:
    """获取 Notification Service 的 URL。失败时 fallback 到 env `NOTIFICATION_SERVICE_URL`。"""
    fb = _fallback_notification_service_url()
    if _nacos_get_service_url is None:
        return fb
    return _nacos_get_service_url("notification_service", fallback_url=fb) or fb
