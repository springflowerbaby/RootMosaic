from flask import render_template, request, redirect, url_for, flash, session, jsonify, g
from flask_login import login_user, logout_user, current_user
from app.extensions import db
from app.models import Merchant, Shop, Item, Order, OrderItem, Review
from app.merchant import merchant_bp as bp
from app.merchant.decorators import merchant_required, own_product_required
from app.auth import ROLE_MERCHANT, get_current_role
from app.service_discovery import get_review_service_url, get_merchant_service_url, get_shipping_service_url
from opentelemetry import trace as _otel_trace_api
import requests
import uuid
import os
import logging
from types import SimpleNamespace
from sqlalchemy import func
from datetime import datetime, timedelta
from werkzeug.exceptions import HTTPException

logger = logging.getLogger(__name__)
_tracer = _otel_trace_api.get_tracer(__name__)

AUTO_APPROVE = os.environ.get('AUTO_APPROVE_MERCHANT', '').lower() in ('true', '1', 'yes')

_NULL_LIKE_VALUES = {'', 'none', 'null', 'nil', 'n/a', 'na', '-'}


def _normalize_nullable_text(value):
    """Normalize null-like text values from forms to None."""
    text = (value or '').strip()
    return None if text.lower() in _NULL_LIKE_VALUES else text


def _normalize_nullable_price(value):
    """Convert null-like or empty price inputs to None."""
    text = _normalize_nullable_text(value)
    if text is None:
        return None
    return float(text)


# ============================================================
# 认证路由
# ============================================================

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated and get_current_role() == ROLE_MERCHANT:
        return redirect(url_for('merchant.dashboard'))
    
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        
        merchant = Merchant.query.filter_by(email=email).first()
        
        if merchant and merchant.check_password(password):
            if merchant.status == 'pending':
                flash('Your account is under review, please wait', 'warning')
                return render_template('merchant/login.html')
            if merchant.status == 'rejected':
                flash('Your application has been rejected', 'error')
                return render_template('merchant/login.html')
            if merchant.status == 'banned':
                flash('Your account has been banned', 'error')
                return render_template('merchant/login.html')
            
            login_user(merchant)
            session['_user_role'] = 'merchant'
            return redirect(url_for('merchant.dashboard'))
        else:
            flash('Invalid email or password', 'error')
    
    return render_template('merchant/login.html')


@bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated and get_current_role() == ROLE_MERCHANT:
        return redirect(url_for('merchant.dashboard'))
    
    if request.method == 'POST':
        shop_name = request.form.get('shop_name', '').strip()
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        phone = request.form.get('phone', '').strip()
        
        if not shop_name or not email or not password:
            flash('All required fields must be filled', 'error')
            return render_template('merchant/register.html')
        
        if password != confirm_password:
            flash('Passwords do not match', 'error')
            return render_template('merchant/register.html')
        
        if len(password) < 8:
            flash('Password must be at least 8 characters', 'error')
            return render_template('merchant/register.html')
        
        if Merchant.query.filter_by(email=email).first():
            flash('Email already registered', 'error')
            return render_template('merchant/register.html')
        
        try:
            merchant = Merchant()
            merchant.merchant_token = str(uuid.uuid4())
            merchant.username = shop_name
            merchant.email = email
            merchant.set_password(password)
            merchant.phone = phone
            merchant.status = 'approved' if AUTO_APPROVE else 'pending'
            
            db.session.add(merchant)
            db.session.flush()
            
            shop = Shop(
                merchant_id=merchant.id,
                name=shop_name
            )
            db.session.add(shop)
            db.session.commit()
            
            if AUTO_APPROVE:
                flash('Registration successful! Please login.', 'success')
            else:
                flash('Registration successful! Please wait for admin approval.', 'success')
            return redirect(url_for('merchant.login'))
        except Exception as e:
            db.session.rollback()
            print(f"[Merchant] Registration error: {e}")
            flash('Registration failed, please try again', 'error')
            return render_template('merchant/register.html')
    
    return render_template('merchant/register.html')


@bp.route('/logout')
def logout():
    logout_user()
    session.pop('_user_role', None)
    return redirect(url_for('merchant.login'))


# ============================================================
# 仪表盘
# ============================================================

@bp.route('/')
@merchant_required
def dashboard():
    merchant_id = current_user.id
    
    total_products = Item.query.filter_by(merchant_id=merchant_id).count()
    active_products = Item.query.filter_by(merchant_id=merchant_id, status='active').count()
    
    pending_orders = Order.query.join(OrderItem).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(Item.merchant_id == merchant_id, Order.status == 'paid').distinct().count()
    
    total_sales_result = db.session.query(
        func.coalesce(func.sum(OrderItem.subtotal), 0)
    ).join(Item, OrderItem.item_id == Item.item_id).join(
        Order, OrderItem.order_id == Order.id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status == 'completed'
    ).scalar()
    total_sales = float(total_sales_result) if total_sales_result else 0.0
    
    pending_reviews = Review.query.join(Item, Review.item_id == Item.item_id).filter(
        Item.merchant_id == merchant_id,
        Review.status == 'approved',
        ~Review.reply.has()
    ).count()

    return render_template('merchant/dashboard.html',
        total_products=total_products,
        active_products=active_products,
        pending_orders=pending_orders,
        total_sales=total_sales,
        pending_reviews=pending_reviews
    )


# ============================================================
# 商品管理
# ============================================================

@bp.route('/products')
@merchant_required
def products():
    page = min(max(request.args.get('page', 1, type=int), 1), 100000)
    status_filter = request.args.get('status', '')
    q = request.args.get('q', '').strip()
    per_page = 15
    
    query = Item.query.filter_by(merchant_id=current_user.id)
    
    if status_filter and status_filter in ('draft', 'pending', 'active', 'rejected', 'removed'):
        query = query.filter_by(status=status_filter)
    else:
        query = query.filter(Item.status != 'removed')
    
    if q:
        like_q = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        query = query.filter(Item.title.ilike(f'%{like_q}%', escape='!'))
    
    query = query.order_by(Item.updated_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    status_counts = {}
    base_query = Item.query.filter_by(merchant_id=current_user.id)
    status_counts['all'] = base_query.filter(Item.status != 'removed').count()
    for s in ('active', 'draft', 'pending', 'rejected', 'removed'):
        status_counts[s] = base_query.filter_by(status=s).count()
    
    return render_template('merchant/products.html',
        products=pagination.items,
        page=page,
        total_pages=pagination.pages,
        total_results=pagination.total,
        status_filter=status_filter,
        status_counts=status_counts,
        query=q
    )


@bp.route('/products/new', methods=['GET', 'POST'])
@merchant_required
def product_new():
    if request.method == 'POST':
        try:
            item = Item()
            item.item_id = f"M{current_user.id}-{uuid.uuid4().hex[:8]}"
            item.title = request.form.get('title', '').strip()
            item.category = request.form.get('category', '').strip()
            item.brand = request.form.get('brand', '').strip()
            item.price = _normalize_nullable_price(request.form.get('price'))
            item.image_url = _normalize_nullable_text(request.form.get('image_url'))
            item.description = request.form.get('description', '').strip()
            item.merchant_id = current_user.id
            item.status = 'active'
            
            if not item.title:
                flash('Product title is required', 'error')
                return render_template('merchant/product_form.html', product=None)
            
            db.session.add(item)
            db.session.commit()
            flash('Product created successfully', 'success')
            return redirect(url_for('merchant.products'))
        except Exception as e:
            db.session.rollback()
            print(f"[Merchant] Product create error: {e}")
            flash('Failed to create product, please try again', 'error')
    
    return render_template('merchant/product_form.html', product=None)


@bp.route('/products/<int:product_id>/edit', methods=['GET', 'POST'])
@merchant_required
@own_product_required
def product_edit(product_id):
    product = g.product
    
    if request.method == 'POST':
        try:
            product.title = request.form.get('title', '').strip()
            product.category = request.form.get('category', '').strip()
            product.brand = request.form.get('brand', '').strip()
            product.price = _normalize_nullable_price(request.form.get('price'))
            product.image_url = _normalize_nullable_text(request.form.get('image_url'))
            product.description = request.form.get('description', '').strip()
            
            if not product.title:
                flash('Product title is required', 'error')
                return render_template('merchant/product_form.html', product=product)
            
            db.session.commit()
            flash('Product updated successfully', 'success')
            return redirect(url_for('merchant.products'))
        except Exception as e:
            db.session.rollback()
            print(f"[Merchant] Product update error: {e}")
            flash('Failed to update product, please try again', 'error')
    
    return render_template('merchant/product_form.html', product=product)


@bp.route('/api/products/<int:product_id>/toggle-status', methods=['POST'])
@merchant_required
@own_product_required
def toggle_product_status(product_id):
    try:
        product = g.product
        if product.status == 'active':
            product.status = 'draft'
            msg = '商品已下架'
        elif product.status == 'draft':
            product.status = 'active'
            msg = '商品已上架'
        else:
            return jsonify({'success': False, 'message': 'This product is under review and cannot be changed by the merchant'}), 403
        db.session.commit()
        return jsonify({'success': True, 'message': msg, 'new_status': product.status})
    except Exception as e:
        db.session.rollback()
        logger.warning('[Product] Error toggling product status: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


@bp.route('/api/products/<int:product_id>/delete', methods=['POST'])
@merchant_required
@own_product_required
def delete_product(product_id):
    product = g.product
    try:
        # Preferred path: soft-delete for auditability.
        product.status = 'removed'
        db.session.commit()
        return jsonify({'success': True, 'message': '商品已删除'})
    except Exception as e:
        # Fallback for older schemas that may not support `removed` status.
        db.session.rollback()
        try:
            db.session.delete(product)
            db.session.commit()
            return jsonify({'success': True, 'message': '商品已删除'})
        except Exception as inner_e:
            db.session.rollback()
            logger.warning('[Product] delete_product failed (primary: %s; fallback: %s)', e, inner_e)
            return jsonify({'success': False, 'message': 'Internal server error'}), 500


# ============================================================
# 订单管理
# ============================================================

@bp.route('/orders')
@merchant_required
def orders():
    page = min(max(request.args.get('page', 1, type=int), 1), 100000)
    status_filter = request.args.get('status', '')
    per_page = 15
    
    query = Order.query.join(OrderItem).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(Item.merchant_id == current_user.id)
    
    if status_filter and status_filter in ('pending', 'paid', 'shipped', 'completed', 'cancelled'):
        query = query.filter(Order.status == status_filter)
    
    query = query.distinct().order_by(Order.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    base_query = Order.query.join(OrderItem).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(Item.merchant_id == current_user.id)
    
    status_counts = {'all': base_query.distinct().count()}
    for s in ('pending', 'paid', 'shipped', 'completed', 'cancelled'):
        status_counts[s] = base_query.filter(Order.status == s).distinct().count()
    
    return render_template('merchant/orders.html',
        orders=pagination.items,
        page=page,
        total_pages=pagination.pages,
        total_results=pagination.total,
        status_filter=status_filter,
        status_counts=status_counts
    )


@bp.route('/orders/<order_no>')
@merchant_required
def order_detail(order_no):
    order = Order.query.join(OrderItem).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(
        Item.merchant_id == current_user.id,
        Order.order_no == order_no
    ).first_or_404()
    
    my_item_ids = {i.item_id for i in Item.query.filter(
        Item.merchant_id == current_user.id,
        Item.item_id.in_([oi.item_id for oi in order.items])
    ).all()}
    my_items = [oi for oi in order.items if oi.item_id in my_item_ids]
    
    return render_template('merchant/order_detail.html', order=order, my_items=my_items)


@bp.route('/api/orders/<order_no>/ship', methods=['POST'])
@merchant_required
def ship_order(order_no):
    try:
        order = Order.query.join(OrderItem).join(
            Item, OrderItem.item_id == Item.item_id
        ).filter(
            Item.merchant_id == current_user.id,
            Order.order_no == order_no
        ).with_for_update().first()

        if not order:
            return jsonify({'success': False, 'message': '订单不存在'}), 404
        if order.status != 'paid':
            return jsonify({'success': False, 'message': '仅已付款订单可发货'}), 400
        
        order.status = 'shipped'
        order.shipped_at = db.func.current_timestamp()
        db.session.commit()

        # 事后 best-effort 代理 shipping_service 建发货单(补 shop_web→shipping 这条边)。
        # 保留上方鉴权/归属校验;shipping_service 不可用/异常仅记日志,不阻断原发货成功。
        try:
            data = request.get_json(silent=True) or {}
            carrier = (data.get('carrier') or '').strip() or None
            resp = requests.post(
                f'{get_shipping_service_url()}/api/shipments',
                json={
                    'order_no': order_no,
                    'carrier': carrier,
                    # 买家真实 token,供 shipping→notification 深链尾正确写通知(修通知错键)
                    'user_token': order.user_token,
                },
                timeout=8,
            )
            if resp.status_code != 200:
                logger.warning('[Ship] shipping_service returned %s for order %s', resp.status_code, order_no)
        except requests.exceptions.RequestException as e:
            logger.warning('[Ship] shipping_service unavailable for order %s: %s', order_no, e)
        except Exception as e:
            logger.warning('[Ship] shipment record failed for order %s: %s', order_no, e)

        return jsonify({'success': True, 'message': '发货成功'})
    except Exception as e:
        db.session.rollback()
        logger.warning('[Ship] Error shipping order: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


# ============================================================
# 商家平台 P1 运营功能
# ============================================================

@bp.route('/stats')
@merchant_required
def stats():
    merchant_id = current_user.id
    days = request.args.get('days', 7, type=int)
    if days not in (7, 14, 30):
        days = 7

    since = datetime.utcnow() - timedelta(days=days)
    trend_rows = db.session.query(
        func.date(Order.created_at).label('d'),
        func.count(func.distinct(Order.id)).label('orders'),
        func.coalesce(func.sum(OrderItem.subtotal), 0).label('sales')
    ).join(OrderItem, Order.id == OrderItem.order_id).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status.in_(['paid', 'shipped', 'completed']),
        Order.created_at >= since
    ).group_by(func.date(Order.created_at)).order_by(func.date(Order.created_at).asc()).all()

    top_rows = db.session.query(
        Item.item_id,
        Item.title,
        func.coalesce(func.sum(OrderItem.quantity), 0).label('sold_qty'),
        func.coalesce(func.sum(OrderItem.subtotal), 0).label('sales')
    ).join(OrderItem, Item.item_id == OrderItem.item_id).join(
        Order, Order.id == OrderItem.order_id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status.in_(['paid', 'shipped', 'completed'])
    ).group_by(Item.item_id, Item.title).order_by(func.sum(OrderItem.subtotal).desc()).limit(10).all()

    return render_template('merchant/stats.html', days=days, trend_rows=trend_rows, top_rows=top_rows)


@bp.route('/shop', methods=['GET', 'POST'])
@merchant_required
def shop_settings():
    shop = Shop.query.filter_by(merchant_id=current_user.id).first()
    if not shop:
        shop = Shop(merchant_id=current_user.id, name=current_user.username or '我的店铺')
        db.session.add(shop)
        db.session.commit()

    if request.method == 'POST':
        # 写路径代理到 merchant_service(保留 @merchant_required + current_user.id 归属作用域)。
        # merchant_service 做实际 UPDATE/建店并重算;不可用时降级回本地 ORM 写,保证页面可用。
        payload = {
            'shop_name': request.form.get('name', '').strip() or shop.name,
            'shop_description': request.form.get('description', '').strip(),
            'shop_logo_url': request.form.get('logo_url', '').strip(),
        }
        proxied = False
        try:
            resp = requests.post(
                f'{get_merchant_service_url()}/api/merchants/{current_user.id}',
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200 and (resp.json() or {}).get('success'):
                proxied = True
            else:
                logger.warning('[Merchant] merchant_service shop update non-ok: %s %s',
                               resp.status_code, resp.text[:200])
        except requests.exceptions.RequestException as e:
            logger.warning('[Merchant] merchant_service unavailable, fallback to ORM: %s', e)

        if not proxied:
            # 优雅降级:merchant_service 不可用 / 返回失败时,沿用原本地 ORM 写
            shop.name = payload['shop_name']
            shop.description = payload['shop_description']
            shop.logo_url = payload['shop_logo_url'] or None
            db.session.commit()

        flash('Shop info updated successfully', 'success')
        return redirect(url_for('merchant.shop_settings'))

    # 读路径增强:GET 店铺资料优先从 merchant_service 读(shop_web→merchant_service 读边)。
    # 失败 / 返回失败 / shop 为 null 时降级使用上方 ORM 查询(含自动建店)的结果,页面渲染不受影响。
    shop_view = shop
    try:
        resp = requests.get(
            f'{get_merchant_service_url()}/api/merchants/{current_user.id}',
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json() or {}
            remote_shop = data.get('shop') if data.get('success') else None
            if remote_shop:
                shop_view = SimpleNamespace(
                    name=remote_shop.get('name'),
                    description=remote_shop.get('description'),
                    logo_url=remote_shop.get('logo_url'),
                )
            else:
                logger.warning('[Merchant] merchant_service shop read no usable shop, fallback to ORM: %s',
                               resp.text[:200])
        else:
            logger.warning('[Merchant] merchant_service shop read non-ok: %s %s',
                           resp.status_code, resp.text[:200])
    except (requests.exceptions.RequestException, ValueError) as e:
        logger.warning('[Merchant] merchant_service unavailable for shop read, fallback to ORM: %s', e)

    return render_template('merchant/shop.html', shop=shop_view)


@bp.route('/settings', methods=['GET', 'POST'])
@merchant_required
def settings():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'profile':
            # 资料写路径代理到 merchant_service(保留 @merchant_required + current_user.id 归属)。
            # 不可用 / 返回失败时降级回本地 ORM 写,保证账户信息仍可更新。
            new_username = request.form.get('username', '').strip() or current_user.username
            new_phone = request.form.get('phone', '').strip()
            proxied = False
            try:
                resp = requests.post(
                    f'{get_merchant_service_url()}/api/merchants/{current_user.id}',
                    json={'username': new_username, 'phone': new_phone},
                    timeout=10,
                )
                if resp.status_code == 200 and (resp.json() or {}).get('success'):
                    proxied = True
                else:
                    logger.warning('[Merchant] merchant_service profile update non-ok: %s %s',
                                   resp.status_code, resp.text[:200])
            except requests.exceptions.RequestException as e:
                logger.warning('[Merchant] merchant_service unavailable, fallback to ORM: %s', e)

            if not proxied:
                current_user.username = new_username
                current_user.phone = new_phone
                db.session.commit()
            flash('Account info updated successfully', 'success')
            return redirect(url_for('merchant.settings'))
        if action == 'password':
            old_password = request.form.get('old_password', '')
            new_password = request.form.get('new_password', '')
            confirm_password = request.form.get('confirm_password', '')
            if not current_user.check_password(old_password):
                flash('Current password is incorrect', 'error')
                return redirect(url_for('merchant.settings'))
            if len(new_password) < 8:
                flash('New password must be at least 8 characters', 'error')
                return redirect(url_for('merchant.settings'))
            if new_password != confirm_password:
                flash('New passwords do not match', 'error')
                return redirect(url_for('merchant.settings'))
            current_user.set_password(new_password)
            db.session.commit()
            flash('Password updated successfully', 'success')
            return redirect(url_for('merchant.settings'))

    return render_template('merchant/settings.html')


@bp.route('/finance')
@merchant_required
def finance():
    merchant_id = current_user.id
    confirmed_sales = db.session.query(
        func.coalesce(func.sum(OrderItem.subtotal), 0)
    ).join(Item, OrderItem.item_id == Item.item_id).join(
        Order, Order.id == OrderItem.order_id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status == 'completed'
    ).scalar()
    in_transit_sales = db.session.query(
        func.coalesce(func.sum(OrderItem.subtotal), 0)
    ).join(Item, OrderItem.item_id == Item.item_id).join(
        Order, Order.id == OrderItem.order_id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status.in_(['paid', 'shipped'])
    ).scalar()

    platform_rate = 0.05
    estimated_settlement = float(confirmed_sales or 0) * (1 - platform_rate)

    return render_template(
        'merchant/finance.html',
        confirmed_sales=float(confirmed_sales or 0),
        in_transit_sales=float(in_transit_sales or 0),
        platform_rate=platform_rate,
        estimated_settlement=estimated_settlement
    )


# ============================================================
# 评论管理
# ============================================================

@bp.route('/reviews')
@merchant_required
def reviews():
    page = min(max(request.args.get('page', 1, type=int), 1), 100000)
    reply_filter = request.args.get('filter', '')  # '', 'pending', 'replied'
    per_page = 15

    # All reviews for this merchant's items
    query = Review.query.join(Item, Review.item_id == Item.item_id).filter(
        Item.merchant_id == current_user.id,
        Review.status == 'approved'
    )

    if reply_filter == 'pending':
        query = query.filter(~Review.reply.has())
    elif reply_filter == 'replied':
        query = query.filter(Review.reply.has())

    query = query.order_by(Review.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    # Counts
    base = Review.query.join(Item, Review.item_id == Item.item_id).filter(
        Item.merchant_id == current_user.id, Review.status == 'approved'
    )
    total_count = base.count()
    pending_reply = base.filter(~Review.reply.has()).count()
    replied_count = base.filter(Review.reply.has()).count()

    return render_template('merchant/reviews.html',
        reviews=pagination.items,
        page=page,
        total_pages=pagination.pages,
        total_results=pagination.total,
        reply_filter=reply_filter,
        total_count=total_count,
        pending_reply=pending_reply,
        replied_count=replied_count,
    )


@bp.route('/api/reviews/<int:review_id>/reply', methods=['POST'])
@merchant_required
def reply_review(review_id):
    try:
        review = Review.query.get_or_404(review_id)

        # Verify the review belongs to this merchant's item
        item = Item.query.filter_by(item_id=review.item_id).first()
        if not item or item.merchant_id != current_user.id:
            return jsonify({'success': False, 'message': 'No permission'}), 403

        if review.reply:
            return jsonify({'success': False, 'message': 'Already replied'}), 400

        data = request.get_json() or {}
        content = (data.get('content') or '').strip()
        if not content:
            return jsonify({'success': False, 'message': 'Reply content cannot be empty'}), 400

        try:
            resp = requests.post(
                f'{get_review_service_url()}/api/reviews/{review_id}/reply',
                json={'merchant_id': current_user.id, 'content': content},
                timeout=10,
            )
            return jsonify(resp.json()), resp.status_code
        except requests.exceptions.RequestException as e:
            logger.warning('[Review] review_service unavailable: %s', e)
            return jsonify({'success': False, 'message': 'Review service unavailable'}), 503
    except HTTPException:
        raise
    except Exception as e:
        db.session.rollback()
        logger.warning('[Review] Error replying to review: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


# ============================================================
# AI 经营助手
# ============================================================

def _gather_merchant_context(merchant_id):
    """Gather recent business data for the merchant AI assistant context."""
    now = datetime.utcnow()
    seven_days_ago = now - timedelta(days=7)
    thirty_days_ago = now - timedelta(days=30)

    # --- Product stats ---
    product_total = Item.query.filter_by(merchant_id=merchant_id).count()
    product_active = Item.query.filter_by(merchant_id=merchant_id, status='active').count()
    product_draft = Item.query.filter_by(merchant_id=merchant_id, status='draft').count()
    product_pending = Item.query.filter_by(merchant_id=merchant_id, status='pending').count()
    product_rejected = Item.query.filter_by(merchant_id=merchant_id, status='rejected').count()

    # --- Order stats (7d & 30d) ---
    def _order_stats(since):
        base = db.session.query(
            func.count(func.distinct(Order.id)),
            func.coalesce(func.sum(OrderItem.subtotal), 0),
        ).join(OrderItem, Order.id == OrderItem.order_id).join(
            Item, OrderItem.item_id == Item.item_id
        ).filter(
            Item.merchant_id == merchant_id,
            Order.status.in_(['paid', 'shipped', 'completed']),
            Order.created_at >= since,
        ).first()
        return int(base[0] or 0), float(base[1] or 0)

    orders_7d, sales_7d = _order_stats(seven_days_ago)
    orders_30d, sales_30d = _order_stats(thirty_days_ago)

    pending_ship = Order.query.join(OrderItem).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(Item.merchant_id == merchant_id, Order.status == 'paid').distinct().count()

    completed_sales = float(db.session.query(
        func.coalesce(func.sum(OrderItem.subtotal), 0)
    ).join(Item, OrderItem.item_id == Item.item_id).join(
        Order, Order.id == OrderItem.order_id
    ).filter(
        Item.merchant_id == merchant_id, Order.status == 'completed'
    ).scalar() or 0)

    # --- Recent orders (last 10) ---
    recent_orders = Order.query.join(OrderItem).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(Item.merchant_id == merchant_id).distinct().order_by(
        Order.created_at.desc()
    ).limit(10).all()

    recent_orders_desc = []
    for o in recent_orders:
        my_items = [oi for oi in o.items
                     if Item.query.filter_by(item_id=oi.item_id, merchant_id=merchant_id).first()]
        item_names = ', '.join(oi.item_title for oi in my_items[:3])
        if len(my_items) > 3:
            item_names += f' ... (+{len(my_items) - 3})'
        recent_orders_desc.append(
            f"  - {o.order_no} | {o.status} | ${o.total_amount:.2f} | {o.created_at.strftime('%Y-%m-%d %H:%M')} | Items: {item_names}"
        )

    # --- Daily sales trend (last 7 days) ---
    trend_rows = db.session.query(
        func.date(Order.created_at).label('d'),
        func.count(func.distinct(Order.id)).label('orders'),
        func.coalesce(func.sum(OrderItem.subtotal), 0).label('sales')
    ).join(OrderItem, Order.id == OrderItem.order_id).join(
        Item, OrderItem.item_id == Item.item_id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status.in_(['paid', 'shipped', 'completed']),
        Order.created_at >= seven_days_ago
    ).group_by(func.date(Order.created_at)).order_by(func.date(Order.created_at).asc()).all()

    trend_desc = []
    for row in trend_rows:
        trend_desc.append(f"  - {row.d}: {row.orders} orders, ${float(row.sales):.2f}")

    # --- Top 5 products by sales ---
    top_rows = db.session.query(
        Item.title,
        func.coalesce(func.sum(OrderItem.quantity), 0).label('sold'),
        func.coalesce(func.sum(OrderItem.subtotal), 0).label('sales')
    ).join(OrderItem, Item.item_id == OrderItem.item_id).join(
        Order, Order.id == OrderItem.order_id
    ).filter(
        Item.merchant_id == merchant_id,
        Order.status.in_(['paid', 'shipped', 'completed'])
    ).group_by(Item.item_id, Item.title).order_by(
        func.sum(OrderItem.subtotal).desc()
    ).limit(5).all()

    top_desc = []
    for row in top_rows:
        top_desc.append(f"  - {row.title}: {int(row.sold)} sold, ${float(row.sales):.2f}")

    # --- Review stats ---
    review_base = Review.query.join(Item, Review.item_id == Item.item_id).filter(
        Item.merchant_id == merchant_id, Review.status == 'approved'
    )
    total_reviews = review_base.count()
    pending_reply = review_base.filter(~Review.reply.has()).count()

    avg_rating_result = db.session.query(
        func.avg(Review.rating)
    ).join(Item, Review.item_id == Item.item_id).filter(
        Item.merchant_id == merchant_id, Review.status == 'approved'
    ).scalar()
    avg_rating = round(float(avg_rating_result), 2) if avg_rating_result else 0.0

    # Recent reviews (last 5 pending reply)
    recent_reviews = review_base.filter(~Review.reply.has()).order_by(
        Review.created_at.desc()
    ).limit(5).all()
    reviews_desc = []
    for r in recent_reviews:
        item = Item.query.filter_by(item_id=r.item_id).first()
        pname = item.title if item else r.item_id
        stars = '★' * r.rating + '☆' * (5 - r.rating)
        content_preview = (r.content or '')[:80]
        reviews_desc.append(f"  - {stars} on [{pname}]: \"{content_preview}\"")

    # --- Build context string ---
    shop = Shop.query.filter_by(merchant_id=merchant_id).first()
    shop_name = shop.name if shop else 'Unknown'

    lines = [
        f"[Shop Name] {shop_name}",
        "",
        "[Product Overview]",
        f"  Total: {product_total}, Active: {product_active}, Draft: {product_draft}, Pending Review: {product_pending}, Rejected: {product_rejected}",
        "",
        "[Order Overview]",
        f"  Last 7 days: {orders_7d} orders, ${sales_7d:.2f} sales",
        f"  Last 30 days: {orders_30d} orders, ${sales_30d:.2f} sales",
        f"  All-time completed sales: ${completed_sales:.2f}",
        f"  Pending shipment: {pending_ship} orders",
        "",
        "[Daily Sales Trend - Last 7 Days]",
        *(trend_desc if trend_desc else ["  No sales data"]),
        "",
        "[Top 5 Best-Selling Products]",
        *(top_desc if top_desc else ["  No sales data"]),
        "",
        "[Review Overview]",
        f"  Total reviews: {total_reviews}, Average rating: {avg_rating}/5, Pending reply: {pending_reply}",
        "",
        "[Recent Reviews Awaiting Reply]",
        *(reviews_desc if reviews_desc else ["  None"]),
        "",
        "[Recent Orders (last 10)]",
        *(recent_orders_desc if recent_orders_desc else ["  None"]),
    ]
    return "\n".join(lines)


@bp.route('/api/chat', methods=['POST'])
@merchant_required
def merchant_chat():
    """AI business assistant chat endpoint for merchants."""
    try:
        from openai import OpenAI

        data = request.get_json(silent=True) or {}
        messages = data.get('messages', [])

        if not messages or not isinstance(messages, list):
            return jsonify({'error': 'Messages cannot be empty'}), 400

        api_key = os.environ.get('DEEPSEEK_API_KEY', '')
        base_url = os.environ.get('DEEPSEEK_API_BASE', 'https://api.deepseek.com/v1')
        model = os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat')

        if not api_key or api_key == 'your_deepseek_api_key_here':
            return jsonify({
                'error': 'Please configure API Key',
                'message': 'Please set DEEPSEEK_API_KEY in .env file'
            }), 500

        client = OpenAI(api_key=api_key, base_url=base_url)

        # Gather merchant business context
        merchant_context = _gather_merchant_context(current_user.id)

        system_message = {
            "role": "system",
            "content": f"""You are a professional e-commerce business analyst assistant serving a merchant on our platform.
Your job is to help the merchant understand their store performance, identify trends, and provide actionable suggestions.

{merchant_context}

[Important Rules]
1. Analyze the merchant's business data and provide clear, actionable insights
2. When discussing sales trends, compare periods and highlight changes
3. If the merchant has pending shipment orders, proactively remind them
4. If there are reviews awaiting reply, suggest the merchant respond promptly to improve customer satisfaction
5. Provide specific, data-backed suggestions — avoid vague advice
6. Keep responses concise and professional, like a business consultant
7. If asked about something not in the data, honestly say the information is not available
8. Respond in the same language the merchant uses (Chinese or English)"""
        }

        full_messages = [system_message] + messages

        with _tracer.start_as_current_span(f"chat {model}") as _span:
            _span.set_attribute("gen_ai.operation.name", "chat")
            _span.set_attribute("gen_ai.system", "deepseek")
            _span.set_attribute("gen_ai.request.model", model)
            _span.set_attribute("gen_ai.request.temperature", 0.7)
            _span.set_attribute("gen_ai.request.max_tokens", 800)
            _span.set_attribute("recweb.ai.role", "merchant")
            _span.set_attribute("recweb.ai.purpose", "chat")
            response = client.chat.completions.create(
                model=model,
                messages=full_messages,
                temperature=0.7,
                max_tokens=800
            )
            if response.usage:
                _span.set_attribute("gen_ai.response.model", response.model)
                _span.set_attribute("gen_ai.response.id", response.id)
                _span.set_attribute("gen_ai.usage.input_tokens", response.usage.prompt_tokens)
                _span.set_attribute("gen_ai.usage.output_tokens", response.usage.completion_tokens)

        assistant_message = response.choices[0].message.content

        return jsonify({
            'success': True,
            'message': assistant_message,
            'usage': {
                'prompt_tokens': response.usage.prompt_tokens,
                'completion_tokens': response.usage.completion_tokens,
                'total_tokens': response.usage.total_tokens
            }
        })

    except HTTPException:
        raise
    except ImportError:
        return jsonify({
            'error': 'Missing openai package',
            'message': 'Please run: pip install openai'
        }), 500
    except Exception as e:
        logger.warning('[MerchantChat] Error: %s', e)
        return jsonify({
            'error': 'AI service call failed',
            'message': 'AI service is temporarily unavailable, please try again later'
        }), 500
