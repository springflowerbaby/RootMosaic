from flask import Blueprint, request
from flask_login import current_user
from app.auth import handle_non_buyer_access, is_buyer

buyer_bp = Blueprint('buyer', __name__, url_prefix='/')

_BUYER_ONLY_ENDPOINTS = {
    'buyer.ai_picks',
    'buyer.profile',
    'buyer.account',
    'buyer.user_center',
    'buyer.chat_assistant',
    'buyer.extract_memory',
    'buyer.get_memories',
    'buyer.delete_memory',
    'buyer.cart',
    'buyer.add_to_cart',
    'buyer.update_cart_item',
    'buyer.remove_from_cart',
    'buyer.get_cart_count',
    'buyer.checkout',
    'buyer.save_address',
    'buyer.delete_address',
    'buyer.set_default_address',
    'buyer.create_order',
    'buyer.orders',
    'buyer.order_detail',
    'buyer.pay_order',
    'buyer.cancel_order',
    'buyer.complete_order',
    'buyer.submit_review',
}


@buyer_bp.before_request
def ensure_buyer_routes_are_buyer_only():
    endpoint = request.endpoint or ''
    if endpoint not in _BUYER_ONLY_ENDPOINTS:
        return None
    if not current_user.is_authenticated:
        return None
    if is_buyer():
        return None
    return handle_non_buyer_access()

from app.buyer import routes  # noqa: E402, F401
