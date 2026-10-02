from datetime import datetime, timedelta
import logging
import os
import uuid

import requests
from flask import flash, jsonify, redirect, render_template, request, session, url_for
from flask_login import current_user, login_user, logout_user
from sqlalchemy import func, or_, text

from app.admin import admin_bp as bp
from app.admin.decorators import admin_required, role_required
from app.auth import ROLE_ADMIN, get_current_role
from app.extensions import db
from app.models import Admin, AdminLog, Announcement, Item, Merchant, Order, OrderItem, User, Review
from app.service_discovery import get_announcement_service_url, get_review_service_url
from opentelemetry import trace as _otel_trace_api

logger = logging.getLogger(__name__)
_tracer = _otel_trace_api.get_tracer(__name__)


def _parse_page():
    return min(max(request.args.get("page", 1, type=int), 1), 100000)


def _parse_per_page(default=15):
    return min(max(request.args.get("per_page", default, type=int), 1), 100)


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated and get_current_role() == ROLE_ADMIN:
        return redirect(url_for("admin.dashboard"))

    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        admin = Admin.query.filter_by(email=email).first()

        if not admin or not admin.check_password(password):
            flash("用户名或密码错误", "error")
            return render_template("admin/login.html")
        if admin.status != "active":
            flash("管理员账号已禁用", "error")
            return render_template("admin/login.html")

        login_user(admin)
        session["_user_role"] = "admin"
        # FE-02: 更新 last_login_at + 写登录审计是 best-effort 副作用。
        # 审计写失败(如 admin_logs 表缺/库异常/下游不可用)不应让登录本身 500——
        # 登录已经成功(login_user 已建立 session),失败时回滚副作用 + warning,照常跳转。
        try:
            admin.last_login_at = db.func.current_timestamp()
            AdminLog.log("admin_login", target_type="admin", target_id=admin.id, detail={"username": admin.username})
            db.session.commit()
        except Exception as _audit_e:
            db.session.rollback()
            logger.warning("[admin.login] last_login_at/audit write failed (login still succeeds): %s", _audit_e)
        return redirect(url_for("admin.dashboard"))

    return render_template("admin/login.html")


@bp.route("/logout")
def logout():
    logout_user()
    session.pop("_user_role", None)
    return redirect(url_for("admin.login"))


@bp.route("/")
@admin_required
def dashboard():
    total_users = User.query.count()
    total_merchants = Merchant.query.count()
    total_orders = Order.query.count()
    total_gmv = (
        db.session.query(func.coalesce(func.sum(Order.total_amount), 0))
        .filter(Order.status == "completed")
        .scalar()
    )
    pending_merchants = Merchant.query.filter_by(status="pending").count()
    pending_items = Item.query.filter_by(status="pending").count()
    pending_reviews = Review.query.filter_by(status="pending").count()
    total_reviews = Review.query.count()

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        total_merchants=total_merchants,
        total_orders=total_orders,
        total_gmv=float(total_gmv or 0),
        pending_merchants=pending_merchants,
        pending_items=pending_items,
        pending_reviews=pending_reviews,
        total_reviews=total_reviews,
    )


@bp.route("/users")
@admin_required
def users():
    page = _parse_page()
    per_page = _parse_per_page()
    q = request.args.get("q", "").strip()

    query = User.query
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like = f"%{esc}%"
        query = query.filter(or_(User.user_token.ilike(like, escape='!'), User.username.ilike(like, escape='!'), User.email.ilike(like, escape='!')))

    pagination = query.order_by(User.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return render_template("admin/users.html", users=pagination.items, page=page, total_pages=pagination.pages, total=pagination.total, q=q)


@bp.route("/users/<user_token>")
@admin_required
def user_detail(user_token):
    user = User.query.filter_by(user_token=user_token).first_or_404()
    recent_orders = Order.query.filter_by(user_token=user_token).order_by(Order.created_at.desc()).limit(10).all()
    return render_template("admin/user_detail.html", user=user, recent_orders=recent_orders)


@bp.route("/api/users/<user_token>/toggle-ban", methods=["POST"])
@admin_required
def toggle_user_ban(user_token):
    user = User.query.filter_by(user_token=user_token).first_or_404()
    user.status = "active" if user.status == "banned" else "banned"
    AdminLog.log("toggle_user_ban", target_type="user", target_id=user_token, detail={"new_status": user.status})
    db.session.commit()
    return jsonify({"success": True, "message": "操作成功", "new_status": user.status})


@bp.route("/merchants")
@admin_required
def merchants():
    page = _parse_page()
    per_page = _parse_per_page()
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()

    query = Merchant.query
    if status in ("pending", "approved", "rejected", "banned"):
        query = query.filter(Merchant.status == status)
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like = f"%{esc}%"
        query = query.filter(or_(Merchant.username.ilike(like, escape='!'), Merchant.email.ilike(like, escape='!'), Merchant.phone.ilike(like, escape='!')))

    pagination = query.order_by(Merchant.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    status_counts = {"all": Merchant.query.count()}
    for s in ("pending", "approved", "rejected", "banned"):
        status_counts[s] = Merchant.query.filter_by(status=s).count()

    return render_template(
        "admin/merchants.html",
        merchants=pagination.items,
        page=page,
        total_pages=pagination.pages,
        total=pagination.total,
        status=status,
        q=q,
        status_counts=status_counts,
    )


@bp.route("/merchants/<int:merchant_id>")
@admin_required
def merchant_detail(merchant_id):
    merchant = Merchant.query.get_or_404(merchant_id)
    items = Item.query.filter_by(merchant_id=merchant_id).order_by(Item.created_at.desc()).limit(20).all()
    return render_template("admin/merchant_detail.html", merchant=merchant, items=items)


def _change_merchant_status(merchant_id, action):
    merchant = Merchant.query.get_or_404(merchant_id)
    # 源状态守卫: 拦非法状态流转, 非法直接 400 不写库。
    if action == "approve":
        if merchant.status != "pending":
            return jsonify({"success": False, "message": f"不允许从 {merchant.status} 审批通过"}), 400
        merchant.status = "approved"
    elif action == "reject":
        if merchant.status != "pending":
            return jsonify({"success": False, "message": f"不允许从 {merchant.status} 驳回"}), 400
        merchant.status = "rejected"
    elif action == "ban":
        if merchant.status not in ("approved", "banned"):
            return jsonify({"success": False, "message": f"不允许从 {merchant.status} 封禁"}), 400
        merchant.status = "banned"
        Item.query.filter_by(merchant_id=merchant_id).update({"status": "removed"}, synchronize_session=False)
    elif action == "unban":
        if merchant.status != "banned":
            return jsonify({"success": False, "message": f"不允许从 {merchant.status} 解封"}), 400
        merchant.status = "approved"
    else:
        return jsonify({"success": False, "message": "不支持的操作"}), 400

    AdminLog.log(f"merchant_{action}", target_type="merchant", target_id=merchant_id, detail={"new_status": merchant.status})
    db.session.commit()
    return jsonify({"success": True, "message": "操作成功", "new_status": merchant.status})


@bp.route("/api/merchants/<int:merchant_id>/approve", methods=["POST"])
@admin_required
def approve_merchant(merchant_id):
    return _change_merchant_status(merchant_id, "approve")


@bp.route("/api/merchants/<int:merchant_id>/reject", methods=["POST"])
@admin_required
def reject_merchant(merchant_id):
    return _change_merchant_status(merchant_id, "reject")


@bp.route("/api/merchants/<int:merchant_id>/ban", methods=["POST"])
@admin_required
def ban_merchant(merchant_id):
    return _change_merchant_status(merchant_id, "ban")


@bp.route("/api/merchants/<int:merchant_id>/unban", methods=["POST"])
@admin_required
def unban_merchant(merchant_id):
    return _change_merchant_status(merchant_id, "unban")


@bp.route("/items")
@admin_required
def items():
    page = _parse_page()
    per_page = _parse_per_page()
    status = request.args.get("status", "").strip()
    merchant_id = request.args.get("merchant_id", type=int)
    q = request.args.get("q", "").strip()

    query = Item.query
    if status in ("draft", "pending", "active", "rejected", "removed"):
        query = query.filter(Item.status == status)
    if merchant_id:
        query = query.filter(Item.merchant_id == merchant_id)
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        query = query.filter(Item.title.ilike(f"%{esc}%", escape='!'))

    # Avoid COUNT(*) on very large item tables to keep admin page responsive.
    rows = (
        query.order_by(Item.updated_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page + 1)
        .all()
    )
    has_next = len(rows) > per_page
    page_items = rows[:per_page]
    total_pages = page + 1 if has_next else page
    merchants = Merchant.query.order_by(Merchant.username.asc()).limit(500).all()
    return render_template(
        "admin/items.html",
        items=page_items,
        merchants=merchants,
        page=page,
        total_pages=total_pages,
        total=(page - 1) * per_page + len(page_items),
        status=status,
        merchant_id=merchant_id,
        q=q,
    )


def _change_item_status(item_id, new_status):
    item = Item.query.filter_by(item_id=item_id).first_or_404()
    # 源状态守卫: 拦非法状态流转 (draft→pending→active/rejected→removed)。
    # 默认【不放开 rejected→active】; 如需"拒绝后改判通过"须显式批准。
    _ALLOWED = {
        'active':   ('pending', 'active'),
        'rejected': ('pending', 'rejected'),
        'removed':  ('draft', 'pending', 'active', 'rejected', 'removed'),
    }
    if new_status in _ALLOWED and item.status not in _ALLOWED[new_status]:
        return jsonify({"success": False, "message": f"不允许从 {item.status} 流转到 {new_status}"}), 400
    item.status = new_status
    AdminLog.log("review_item", target_type="item", target_id=item_id, detail={"new_status": new_status})
    db.session.commit()
    return jsonify({"success": True, "message": "操作成功", "new_status": item.status})


@bp.route("/api/items/<item_id>/approve", methods=["POST"])
@admin_required
def approve_item(item_id):
    return _change_item_status(item_id, "active")


@bp.route("/api/items/<item_id>/reject", methods=["POST"])
@admin_required
def reject_item(item_id):
    return _change_item_status(item_id, "rejected")


@bp.route("/api/items/<item_id>/remove", methods=["POST"])
@admin_required
def remove_item(item_id):
    return _change_item_status(item_id, "removed")


@bp.route("/orders")
@admin_required
def orders():
    page = _parse_page()
    per_page = _parse_per_page()
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()

    query = Order.query
    if status in ("pending", "paid", "shipped", "completed", "cancelled"):
        query = query.filter(Order.status == status)
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like = f"%{esc}%"
        query = query.filter(or_(Order.order_no.ilike(like, escape='!'), Order.user_token.ilike(like, escape='!')))

    pagination = query.order_by(Order.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return render_template(
        "admin/orders.html",
        orders=pagination.items,
        page=page,
        total_pages=pagination.pages,
        total=pagination.total,
        status=status,
        q=q,
    )


@bp.route("/orders/<order_no>")
@admin_required
def order_detail(order_no):
    order = Order.query.filter_by(order_no=order_no).first_or_404()
    return render_template("admin/order_detail.html", order=order)


@bp.route("/api/orders/<order_no>/force-cancel", methods=["POST"])
@role_required("super_admin")
def force_cancel_order(order_no):
    # Lock the row (P0-1): close the cross-handler race with buyer pay/cancel.
    order = Order.query.filter_by(order_no=order_no).with_for_update().first_or_404()
    if order.status in ("completed", "cancelled"):
        return jsonify({"success": False, "message": "当前状态不允许强制取消"}), 400

    old_status = order.status
    order.status = "cancelled"
    order.cancel_reason = "管理员强制取消"
    order.cancelled_at = datetime.utcnow()
    AdminLog.log("force_cancel_order", target_type="order", target_id=order_no, detail={"old_status": old_status})
    db.session.commit()
    return jsonify({"success": True, "message": "订单已强制取消"})


# ============================================================
# 管理员平台 P1 运营功能
# ============================================================

@bp.route("/stats")
@admin_required
def stats():
    user_growth = db.session.execute(
        text(
            """
            SELECT DATE(created_at) AS d, COUNT(*) AS c
            FROM users
            GROUP BY DATE(created_at)
            ORDER BY d DESC
            LIMIT 15
            """
        )
    ).fetchall()
    order_trend = db.session.execute(
        text(
            """
            SELECT DATE(created_at) AS d, COUNT(*) AS c, COALESCE(SUM(total_amount), 0) AS amount
            FROM orders
            GROUP BY DATE(created_at)
            ORDER BY d DESC
            LIMIT 15
            """
        )
    ).fetchall()
    rec_perf = db.session.execute(
        text(
            """
            SELECT DATE(created_at) AS d,
                   COUNT(*) AS total_recommendations
            FROM recommendations
            GROUP BY DATE(created_at)
            ORDER BY d DESC
            LIMIT 15
            """
        )
    ).fetchall()
    return render_template("admin/stats.html", user_growth=user_growth, order_trend=order_trend, rec_perf=rec_perf)


@bp.route("/announcements")
@admin_required
def announcements():
    page = _parse_page()
    per_page = _parse_per_page()
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()

    query = Announcement.query
    if status in ("draft", "published", "archived"):
        query = query.filter(Announcement.status == status)
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        query = query.filter(Announcement.title.ilike(f"%{esc}%", escape='!'))
    pagination = query.order_by(Announcement.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return render_template(
        "admin/announcements.html",
        announcements=pagination.items,
        page=page,
        total_pages=pagination.pages,
        status=status,
        q=q,
    )


@bp.route("/announcements/new", methods=["POST"])
@admin_required
def announcement_new():
    title = request.form.get("title", "").strip()
    if not title:
        flash("标题不能为空", "error")
        return redirect(url_for("admin.announcements"))
    payload = {
        "title": title,
        "content": request.form.get("content", "").strip(),
        "type": request.form.get("type", "notice"),
        "image_url": request.form.get("image_url", "").strip() or None,
        "link_url": request.form.get("link_url", "").strip() or None,
        "sort_order": request.form.get("sort_order", 0, type=int),
        "created_by": current_user.id,
    }
    try:
        resp = requests.post(
            f'{get_announcement_service_url()}/api/announcements',
            json=payload,
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.warning("[Announcement] announcement_service unavailable: %s", e)
        flash("公告服务暂不可用", "error")
        return redirect(url_for("admin.announcements"))
    body = resp.json()
    if resp.status_code == 200 and body.get("success"):
        AdminLog.log(
            "announcement_create",
            target_type="announcement",
            target_id=body.get("announcement_id"),
            detail={"title": title},
        )
        db.session.commit()
        flash("公告已创建", "success")
    else:
        flash(body.get("message", "公告创建失败"), "error")
    return redirect(url_for("admin.announcements"))


@bp.route("/api/announcements/<int:ann_id>/publish", methods=["POST"])
@admin_required
def announcement_publish(ann_id):
    try:
        resp = requests.post(
            f'{get_announcement_service_url()}/api/announcements/{ann_id}/publish',
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.warning("[Announcement] announcement_service unavailable: %s", e)
        return jsonify({"success": False, "message": "Announcement service unavailable"}), 503
    body = resp.json()
    if resp.status_code == 200 and body.get("success"):
        AdminLog.log("announcement_publish", target_type="announcement", target_id=ann_id)
        db.session.commit()
        return jsonify({"success": True, "status": body.get("status", "published")}), 200
    return jsonify(body), resp.status_code


@bp.route("/api/announcements/<int:ann_id>/withdraw", methods=["POST"])
@admin_required
def announcement_withdraw(ann_id):
    try:
        resp = requests.post(
            f'{get_announcement_service_url()}/api/announcements/{ann_id}/withdraw',
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.warning("[Announcement] announcement_service unavailable: %s", e)
        return jsonify({"success": False, "message": "Announcement service unavailable"}), 503
    body = resp.json()
    if resp.status_code == 200 and body.get("success"):
        AdminLog.log("announcement_withdraw", target_type="announcement", target_id=ann_id)
        db.session.commit()
        return jsonify({"success": True, "status": body.get("status", "archived")}), 200
    return jsonify(body), resp.status_code


@bp.route("/api/announcements/<int:ann_id>/delete", methods=["POST"])
@admin_required
def announcement_delete(ann_id):
    try:
        resp = requests.delete(
            f'{get_announcement_service_url()}/api/announcements/{ann_id}',
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.warning("[Announcement] announcement_service unavailable: %s", e)
        return jsonify({"success": False, "message": "Announcement service unavailable"}), 503
    body = resp.json()
    if resp.status_code == 200 and body.get("success"):
        AdminLog.log("announcement_delete", target_type="announcement", target_id=ann_id)
        db.session.commit()
        return jsonify({"success": True}), 200
    return jsonify(body), resp.status_code


@bp.route("/logs")
@admin_required
def logs():
    page = _parse_page()
    per_page = _parse_per_page()
    action = request.args.get("action", "").strip()
    target_type = request.args.get("target_type", "").strip()
    query = AdminLog.query
    if action:
        esc = action.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        query = query.filter(AdminLog.action.ilike(f"%{esc}%", escape='!'))
    if target_type:
        query = query.filter(AdminLog.target_type == target_type)
    pagination = query.order_by(AdminLog.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return render_template("admin/logs.html", logs=pagination.items, page=page, total_pages=pagination.pages, action=action, target_type=target_type)


@bp.route("/admins")
@role_required("super_admin")
def admins():
    page = _parse_page()
    per_page = _parse_per_page()
    q = request.args.get("q", "").strip()
    query = Admin.query
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like = f"%{esc}%"
        query = query.filter(or_(Admin.username.ilike(like, escape='!'), Admin.email.ilike(like, escape='!')))
    pagination = query.order_by(Admin.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return render_template("admin/admins.html", admins=pagination.items, page=page, total_pages=pagination.pages, q=q)


@bp.route("/admins/new", methods=["POST"])
@role_required("super_admin")
def admin_create():
    username = request.form.get("username", "").strip()
    email = request.form.get("email", "").strip() or None
    role = request.form.get("role", "operation")
    if not username:
        flash("用户名不能为空", "error")
        return redirect(url_for("admin.admins"))
    if Admin.query.filter_by(username=username).first():
        flash("用户名已存在", "error")
        return redirect(url_for("admin.admins"))
    if role not in ('super_admin', 'operation', 'finance', 'customer_service'):
        flash("无效的管理员角色", "error")
        return redirect(url_for("admin.admins"))

    temp_password = f"Admin@{uuid.uuid4().hex[:8]}"
    admin = Admin(username=username, email=email, role=role, status="active")
    admin.set_password(temp_password)
    db.session.add(admin)
    db.session.flush()
    AdminLog.log("admin_create", target_type="admin", target_id=admin.id, detail={"username": username, "role": role})
    db.session.commit()
    flash(f"管理员创建成功，初始密码：{temp_password}", "success")
    return redirect(url_for("admin.admins"))


@bp.route("/api/admins/<int:admin_id>/toggle-status", methods=["POST"])
@role_required("super_admin")
def admin_toggle_status(admin_id):
    admin = Admin.query.get_or_404(admin_id)
    if admin.id == current_user.id:
        return jsonify({"success": False, "message": "不能禁用自己"}), 400
    admin.status = "disabled" if admin.status == "active" else "active"
    AdminLog.log("admin_toggle_status", target_type="admin", target_id=admin_id, detail={"new_status": admin.status})
    db.session.commit()
    return jsonify({"success": True, "new_status": admin.status})


@bp.route("/api/admins/<int:admin_id>/reset-password", methods=["POST"])
@role_required("super_admin")
def admin_reset_password(admin_id):
    admin = Admin.query.get_or_404(admin_id)
    new_password = f"Reset@{uuid.uuid4().hex[:8]}"
    admin.set_password(new_password)
    AdminLog.log("admin_reset_password", target_type="admin", target_id=admin_id)
    db.session.commit()
    return jsonify({"success": True, "new_password": new_password})


# ============================================================
# 评论审核管理
# ============================================================

@bp.route("/reviews")
@admin_required
def reviews():
    page = _parse_page()
    per_page = _parse_per_page()
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()

    query = Review.query
    if status in ("pending", "approved", "rejected", "hidden"):
        query = query.filter(Review.status == status)
    if q:
        esc = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like = f"%{esc}%"
        query = query.join(Item, Review.item_id == Item.item_id).filter(
            or_(Item.title.ilike(like, escape='!'), Review.user_token.ilike(like, escape='!'))
        )

    pagination = query.order_by(Review.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)

    status_counts = {"all": Review.query.count()}
    for s in ("pending", "approved", "rejected", "hidden"):
        status_counts[s] = Review.query.filter_by(status=s).count()

    return render_template(
        "admin/reviews.html",
        reviews=pagination.items,
        page=page,
        total_pages=pagination.pages,
        total=pagination.total,
        status=status,
        q=q,
        status_counts=status_counts,
    )


def _change_review_status(review_id, new_status):
    try:
        resp = requests.post(
            f'{get_review_service_url()}/api/reviews/{review_id}/status',
            json={"status": new_status},
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.warning("[Review] review_service unavailable: %s", e)
        return jsonify({"success": False, "message": "Review service unavailable"}), 503
    body = resp.json()
    if resp.status_code == 200 and body.get("success"):
        AdminLog.log(
            f"review_{new_status}",
            target_type="review",
            target_id=review_id,
            detail={"old_status": body.get("old_status"), "new_status": new_status},
        )
        db.session.commit()
        return jsonify({"success": True, "message": "操作成功", "new_status": body.get("new_status", new_status)}), 200
    return jsonify(body), resp.status_code


@bp.route("/api/reviews/<int:review_id>/approve", methods=["POST"])
@admin_required
def approve_review(review_id):
    return _change_review_status(review_id, "approved")


@bp.route("/api/reviews/<int:review_id>/reject", methods=["POST"])
@admin_required
def reject_review(review_id):
    return _change_review_status(review_id, "rejected")


@bp.route("/api/reviews/<int:review_id>/hide", methods=["POST"])
@admin_required
def hide_review(review_id):
    return _change_review_status(review_id, "hidden")


@bp.route("/api/reviews/<int:review_id>/delete", methods=["POST"])
@role_required("super_admin")
def delete_review(review_id):
    try:
        resp = requests.delete(
            f'{get_review_service_url()}/api/reviews/{review_id}',
            timeout=10,
        )
    except requests.exceptions.RequestException as e:
        logger.warning("[Review] review_service unavailable: %s", e)
        return jsonify({"success": False, "message": "Review service unavailable"}), 503
    body = resp.json()
    if resp.status_code == 200 and body.get("success"):
        AdminLog.log("review_delete", target_type="review", target_id=review_id)
        db.session.commit()
        return jsonify({"success": True, "message": "评论已删除"}), 200
    return jsonify(body), resp.status_code


# ============================================================
# AI 平台运营助手
# ============================================================

def _gather_admin_context():
    """Gather platform-wide operational data for the admin AI assistant."""
    now = datetime.utcnow()
    seven_days_ago = now - timedelta(days=7)
    thirty_days_ago = now - timedelta(days=30)

    # --- Platform overview ---
    total_users = User.query.count()
    total_merchants = Merchant.query.count()
    total_orders = Order.query.count()
    total_items = Item.query.count()
    total_reviews = Review.query.count()

    total_gmv = float(db.session.query(
        func.coalesce(func.sum(Order.total_amount), 0)
    ).filter(Order.status == 'completed').scalar() or 0)

    # --- Pending actions ---
    pending_merchants = Merchant.query.filter_by(status='pending').count()
    pending_items = Item.query.filter_by(status='pending').count()
    pending_reviews = Review.query.filter_by(status='pending').count()

    # --- Merchant status distribution ---
    merchant_statuses = {}
    for s in ('pending', 'approved', 'rejected', 'banned'):
        merchant_statuses[s] = Merchant.query.filter_by(status=s).count()

    # --- Order status distribution ---
    order_statuses = {}
    for s in ('pending', 'paid', 'shipped', 'completed', 'cancelled'):
        order_statuses[s] = Order.query.filter_by(status=s).count()

    # --- Item status distribution ---
    item_statuses = {}
    for s in ('active', 'draft', 'pending', 'rejected', 'removed'):
        item_statuses[s] = Item.query.filter_by(status=s).count()

    # --- Review status distribution ---
    review_statuses = {}
    for s in ('pending', 'approved', 'rejected', 'hidden'):
        review_statuses[s] = Review.query.filter_by(status=s).count()

    # --- 7d / 30d order stats ---
    def _period_order_stats(since):
        row = db.session.query(
            func.count(Order.id),
            func.coalesce(func.sum(Order.total_amount), 0)
        ).filter(
            Order.status.in_(['paid', 'shipped', 'completed']),
            Order.created_at >= since
        ).first()
        return int(row[0] or 0), float(row[1] or 0)

    orders_7d, sales_7d = _period_order_stats(seven_days_ago)
    orders_30d, sales_30d = _period_order_stats(thirty_days_ago)

    # --- User growth (7d) ---
    new_users_7d = User.query.filter(User.created_at >= seven_days_ago).count()
    new_merchants_7d = Merchant.query.filter(Merchant.created_at >= seven_days_ago).count()

    # --- Daily order trend (last 7 days) ---
    trend_rows = db.session.query(
        func.date(Order.created_at).label('d'),
        func.count(Order.id).label('cnt'),
        func.coalesce(func.sum(Order.total_amount), 0).label('amount')
    ).filter(
        Order.created_at >= seven_days_ago
    ).group_by(func.date(Order.created_at)).order_by(func.date(Order.created_at).asc()).all()

    trend_desc = []
    for row in trend_rows:
        trend_desc.append(f"  - {row.d}: {row.cnt} orders, ${float(row.amount):.2f}")

    # --- Top 5 merchants by GMV ---
    top_merchants = db.session.query(
        Merchant.username,
        func.coalesce(func.sum(OrderItem.subtotal), 0).label('gmv')
    ).join(Item, Item.merchant_id == Merchant.id).join(
        OrderItem, OrderItem.item_id == Item.item_id
    ).join(Order, Order.id == OrderItem.order_id).filter(
        Order.status.in_(['paid', 'shipped', 'completed'])
    ).group_by(Merchant.id, Merchant.username).order_by(
        func.sum(OrderItem.subtotal).desc()
    ).limit(5).all()

    top_merchants_desc = []
    for row in top_merchants:
        top_merchants_desc.append(f"  - {row.username}: ${float(row.gmv):.2f}")

    # --- Recent admin logs (last 20) ---
    recent_logs = AdminLog.query.order_by(AdminLog.created_at.desc()).limit(20).all()
    logs_desc = []
    for log in recent_logs:
        admin = Admin.query.get(log.admin_id)
        admin_name = admin.username if admin else f"admin#{log.admin_id}"
        ts = log.created_at.strftime('%Y-%m-%d %H:%M') if log.created_at else '?'
        detail_str = f" | {log.detail}" if log.detail else ""
        logs_desc.append(
            f"  - [{ts}] {admin_name}: {log.action} → {log.target_type or '-'}#{log.target_id or '-'}{detail_str}"
        )

    # --- Announcements summary ---
    active_announcements = Announcement.query.filter_by(status='published').count()
    draft_announcements = Announcement.query.filter_by(status='draft').count()

    # --- Build context string ---
    lines = [
        "[Platform Overview]",
        f"  Users: {total_users} (new 7d: {new_users_7d})",
        f"  Merchants: {total_merchants} (new 7d: {new_merchants_7d}) — pending: {merchant_statuses['pending']}, approved: {merchant_statuses['approved']}, rejected: {merchant_statuses['rejected']}, banned: {merchant_statuses['banned']}",
        f"  Products: {total_items} — active: {item_statuses['active']}, draft: {item_statuses['draft']}, pending: {item_statuses['pending']}, rejected: {item_statuses['rejected']}, removed: {item_statuses['removed']}",
        f"  Orders: {total_orders} — pending: {order_statuses['pending']}, paid: {order_statuses['paid']}, shipped: {order_statuses['shipped']}, completed: {order_statuses['completed']}, cancelled: {order_statuses['cancelled']}",
        f"  Reviews: {total_reviews} — pending: {review_statuses['pending']}, approved: {review_statuses['approved']}, rejected: {review_statuses['rejected']}, hidden: {review_statuses['hidden']}",
        f"  Total GMV (completed): ${total_gmv:.2f}",
        "",
        "[Recent Performance]",
        f"  Last 7 days: {orders_7d} orders, ${sales_7d:.2f} sales",
        f"  Last 30 days: {orders_30d} orders, ${sales_30d:.2f} sales",
        "",
        "[Daily Order Trend - Last 7 Days]",
        *(trend_desc if trend_desc else ["  No data"]),
        "",
        "[Top 5 Merchants by GMV]",
        *(top_merchants_desc if top_merchants_desc else ["  No data"]),
        "",
        "[Pending Actions]",
        f"  Merchants awaiting approval: {pending_merchants}",
        f"  Products awaiting review: {pending_items}",
        f"  Reviews awaiting moderation: {pending_reviews}",
        "",
        "[Announcements]",
        f"  Published: {active_announcements}, Drafts: {draft_announcements}",
        "",
        "[Recent Admin Activity Logs (last 20)]",
        *(logs_desc if logs_desc else ["  No recent logs"]),
    ]
    return "\n".join(lines)


@bp.route("/api/chat", methods=["POST"])
@admin_required
def admin_chat():
    """AI platform operations assistant chat endpoint for admins."""
    try:
        from openai import OpenAI

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid request body"}), 400
        messages = data.get("messages", [])

        if not messages or not isinstance(messages, list):
            return jsonify({"error": "Messages cannot be empty"}), 400

        api_key = os.environ.get("DEEPSEEK_API_KEY", "")
        base_url = os.environ.get("DEEPSEEK_API_BASE", "https://api.deepseek.com/v1")
        model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

        if not api_key or api_key == "your_deepseek_api_key_here":
            return jsonify({
                "error": "Please configure API Key",
                "message": "Please set DEEPSEEK_API_KEY in .env file"
            }), 500

        client = OpenAI(api_key=api_key, base_url=base_url)

        admin_context = _gather_admin_context()

        system_message = {
            "role": "system",
            "content": f"""You are a professional platform operations analyst assistant serving an administrator of an e-commerce platform.
Your job is to help the admin understand overall platform health, identify operational issues, and provide actionable recommendations.

{admin_context}

[Important Rules]
1. Analyze platform-wide data and provide clear, actionable insights
2. Prioritize urgent items: pending merchant approvals, pending product reviews, pending review moderation
3. When discussing trends, compare 7-day vs 30-day data and highlight significant changes
4. Monitor admin activity logs for unusual patterns or important recent actions
5. Provide specific, data-backed suggestions — avoid vague advice
6. Keep responses concise and professional
7. If asked about something not in the data, honestly say the information is not available
8. Respond in the same language the admin uses (Chinese or English)"""
        }

        full_messages = [system_message] + messages

        with _tracer.start_as_current_span(f"chat {model}") as _span:
            _span.set_attribute("gen_ai.operation.name", "chat")
            _span.set_attribute("gen_ai.system", "deepseek")
            _span.set_attribute("gen_ai.request.model", model)
            _span.set_attribute("gen_ai.request.temperature", 0.7)
            _span.set_attribute("gen_ai.request.max_tokens", 800)
            _span.set_attribute("recweb.ai.role", "admin")
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
            "success": True,
            "message": assistant_message,
            "usage": {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens
            }
        })

    except ImportError:
        return jsonify({
            "error": "Missing openai package",
            "message": "Please run: pip install openai"
        }), 500
    except Exception as e:
        logger.warning("[AdminChat] Error: %s", e)
        return jsonify({
            "error": "AI service call failed",
            "message": "AI service is temporarily unavailable, please try again later"
        }), 500
