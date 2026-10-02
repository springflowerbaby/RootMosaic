from functools import wraps
from flask import abort, g, redirect, url_for
from flask_login import current_user
from app.auth import ROLE_MERCHANT, get_current_role


def merchant_required(f):
    """确保当前用户是已审核通过的商家"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or get_current_role() != ROLE_MERCHANT:
            return redirect(url_for('merchant.login'))
        if current_user.status != 'approved':
            abort(403)
        return f(*args, **kwargs)
    return decorated


def own_product_required(f):
    """确保商家只能操作自己的商品，查询结果缓存在 g.product 避免路由重复查询"""
    @wraps(f)
    def decorated(product_id, *args, **kwargs):
        from app.models import Item
        product = Item.query.get_or_404(product_id)
        if product.merchant_id != current_user.id:
            abort(403)
        g.product = product
        return f(product_id, *args, **kwargs)
    return decorated
