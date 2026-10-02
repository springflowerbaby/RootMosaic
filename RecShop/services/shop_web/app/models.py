from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash
from app.extensions import db
import json
import time
import uuid

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_token = db.Column(db.String(100), unique=True, nullable=False)
    username = db.Column(db.String(100))
    email = db.Column(db.String(255))
    password_hash = db.Column(db.String(255))
    avatar_url = db.Column(db.String(500))
    status = db.Column(db.Enum('active', 'banned', name='user_status_enum'),
                       nullable=False, server_default='active')
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(), 
                          onupdate=db.func.current_timestamp())
    
    def check_password(self, password):
        if not self.password_hash or self.password_hash.strip() == '':
            return False
        return check_password_hash(self.password_hash, password)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def get_avatar(self):
        return self.avatar_url or 'https://placehold.co/40x40/cccccc/333333?text=User'

class Item(db.Model):
    __tablename__ = 'items'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    item_id = db.Column(db.String(100), unique=True, nullable=False)
    title = db.Column(db.String(500))
    category = db.Column(db.String(200))
    brand = db.Column(db.String(200))
    price = db.Column(db.Numeric(10, 2))
    image_url = db.Column(db.String(500))
    description = db.Column(db.Text)
    rating = db.Column(db.Numeric(3, 2))
    review_count = db.Column(db.Integer, default=0)
    merchant_id = db.Column(db.Integer, db.ForeignKey('merchants.id'), default=None)
    status = db.Column(db.Enum('draft', 'pending', 'active', 'rejected', 'removed',
                               name='item_status_enum'), nullable=False, server_default='active')
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(), 
                          onupdate=db.func.current_timestamp())
    
    merchant = db.relationship('Merchant', backref=db.backref('items', lazy='dynamic'))
    
    def to_dict(self):
        return {
            'id': self.id,
            'item_id': self.item_id,
            'name': self.title,
            'category': self.category or '商品',
            'brand': self.brand,
            'price': float(self.price) if self.price else 0.0,
            'image': self.image_url or 'https://placehold.co/400x400/f5f5f5/333333?text=No+Image',
            'description': self.description,
            'rating': float(self.rating) if self.rating else 0.0,
            'review_count': self.review_count or 0
        }

class Interaction(db.Model):
    __tablename__ = 'interactions'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_token = db.Column(db.String(100), nullable=False)
    item_id = db.Column(db.String(100), nullable=False)
    interaction_type = db.Column(db.Enum('view', 'click', 'purchase', 'rating', 'add_to_cart', 'remove_from_cart', name='interaction_type_enum'), default='view')
    rating = db.Column(db.Numeric(2, 1))
    duration = db.Column(db.Integer)
    quantity = db.Column(db.Integer, default=1)
    price = db.Column(db.Numeric(10, 2))
    source = db.Column(db.String(50))
    session_id = db.Column(db.String(100))
    timestamp = db.Column(db.BigInteger, nullable=False)
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    
    @staticmethod
    def create_click_interaction(user_token, item_id, session_id=None, source='direct'):
        interaction = Interaction(
            user_token=user_token,
            item_id=item_id,
            interaction_type='click',
            timestamp=int(time.time() * 1000),
            session_id=session_id,
            source=source
        )
        return interaction

class CartItem(db.Model):
    __tablename__ = 'cart_items'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_token = db.Column(db.String(100), nullable=False)
    item_id = db.Column(db.String(100), nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=1)
    added_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(), 
                          onupdate=db.func.current_timestamp())
    
    def to_dict(self, item=None):
        result = {
            'id': self.id,
            'user_token': self.user_token,
            'item_id': self.item_id,
            'quantity': self.quantity,
            'added_at': self.added_at.isoformat() if self.added_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        if item:
            result['item'] = item.to_dict()
            result['subtotal'] = float(item.price) * self.quantity if item.price else 0.0
        return result

class UserAddress(db.Model):
    __tablename__ = 'user_addresses'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_token = db.Column(db.String(100), nullable=False)
    receiver_name = db.Column(db.String(100), nullable=False)
    receiver_phone = db.Column(db.String(20), nullable=False)
    province = db.Column(db.String(50))
    city = db.Column(db.String(50))
    district = db.Column(db.String(50))
    address = db.Column(db.String(500), nullable=False)
    is_default = db.Column(db.SmallInteger, default=0)
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())
    
    def full_address(self):
        parts = [p for p in [self.province, self.city, self.district, self.address] if p]
        return ' '.join(parts)
    
    def to_dict(self):
        return {
            'id': self.id,
            'receiver_name': self.receiver_name,
            'receiver_phone': self.receiver_phone,
            'province': self.province or '',
            'city': self.city or '',
            'district': self.district or '',
            'address': self.address,
            'full_address': self.full_address(),
            'is_default': self.is_default
        }

class Order(db.Model):
    __tablename__ = 'orders'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    order_no = db.Column(db.String(50), unique=True, nullable=False)
    user_token = db.Column(db.String(100), nullable=False)
    address_id = db.Column(db.BigInteger, nullable=False)
    snapshot_receiver_name = db.Column(db.String(100), nullable=False)
    snapshot_receiver_phone = db.Column(db.String(20), nullable=False)
    snapshot_address = db.Column(db.String(500), nullable=False)
    total_amount = db.Column(db.Numeric(10, 2), nullable=False)
    item_count = db.Column(db.Integer, default=0)
    status = db.Column(db.Enum('pending', 'paid', 'shipped', 'completed', 'cancelled',
                               name='order_status_enum'), default='pending')
    remark = db.Column(db.Text)
    cancel_reason = db.Column(db.String(200))
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    paid_at = db.Column(db.TIMESTAMP)
    shipped_at = db.Column(db.TIMESTAMP)
    completed_at = db.Column(db.TIMESTAMP)
    cancelled_at = db.Column(db.TIMESTAMP)
    
    items = db.relationship('OrderItem', backref='order', cascade='all, delete-orphan',
                           lazy='joined')
    
    def to_dict(self):
        return {
            'id': self.id,
            'order_no': self.order_no,
            'user_token': self.user_token,
            'snapshot_receiver_name': self.snapshot_receiver_name,
            'snapshot_receiver_phone': self.snapshot_receiver_phone,
            'snapshot_address': self.snapshot_address,
            'total_amount': float(self.total_amount),
            'item_count': self.item_count,
            'status': self.status,
            'status_text': {
                'pending': 'Pending Payment',
                'paid': 'Paid',
                'shipped': 'Shipped',
                'completed': 'Completed',
                'cancelled': 'Cancelled'
            }.get(self.status, self.status),
            'remark': self.remark,
            'cancel_reason': self.cancel_reason,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'paid_at': self.paid_at.strftime('%Y-%m-%d %H:%M:%S') if self.paid_at else None,
            'shipped_at': self.shipped_at.strftime('%Y-%m-%d %H:%M:%S') if self.shipped_at else None,
            'completed_at': self.completed_at.strftime('%Y-%m-%d %H:%M:%S') if self.completed_at else None,
            'cancelled_at': self.cancelled_at.strftime('%Y-%m-%d %H:%M:%S') if self.cancelled_at else None,
            'items': [item.to_dict() for item in self.items]
        }

class OrderItem(db.Model):
    __tablename__ = 'order_items'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    order_id = db.Column(db.BigInteger, db.ForeignKey('orders.id'), nullable=False)
    item_id = db.Column(db.String(100), nullable=False)
    item_title = db.Column(db.String(500), nullable=False)
    item_image = db.Column(db.String(500))
    item_price = db.Column(db.Numeric(10, 2), nullable=False)
    quantity = db.Column(db.Integer, default=1)
    subtotal = db.Column(db.Numeric(10, 2), nullable=False)
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    
    def to_dict(self):
        return {
            'id': self.id,
            'item_id': self.item_id,
            'item_title': self.item_title,
            'item_image': self.item_image or 'https://placehold.co/400x400/f5f5f5/333333?text=No+Image',
            'item_price': float(self.item_price),
            'quantity': self.quantity,
            'subtotal': float(self.subtotal)
        }

# ============================================================
# 商家平台模型
# ============================================================

class Merchant(UserMixin, db.Model):
    __tablename__ = 'merchants'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    merchant_token = db.Column(db.String(100), unique=True, nullable=False)
    username = db.Column(db.String(100))
    email = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    phone = db.Column(db.String(20))
    status = db.Column(db.Enum('pending', 'approved', 'rejected', 'banned',
                               name='merchant_status_enum'), nullable=False, server_default='pending')
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())
    
    shop = db.relationship('Shop', backref='merchant', uselist=False, lazy='joined')
    
    def check_password(self, password):
        if not self.password_hash or self.password_hash.strip() == '':
            return False
        return check_password_hash(self.password_hash, password)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def get_avatar(self):
        return "https://placehold.co/40x40/f59e0b/ffffff?text=M"

    @property
    def user_token(self):
        return None
    
    def to_dict(self):
        return {
            'id': self.id,
            'merchant_token': self.merchant_token,
            'username': self.username,
            'email': self.email,
            'phone': self.phone,
            'status': self.status,
            'shop_name': self.shop.name if self.shop else None,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None
        }


class Shop(db.Model):
    __tablename__ = 'shops'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    merchant_id = db.Column(db.Integer, db.ForeignKey('merchants.id'), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text)
    logo_url = db.Column(db.String(500))
    status = db.Column(db.Enum('active', 'closed', name='shop_status_enum'),
                       nullable=False, server_default='active')
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())
    
    def to_dict(self):
        return {
            'id': self.id,
            'merchant_id': self.merchant_id,
            'name': self.name,
            'description': self.description,
            'logo_url': self.logo_url,
            'status': self.status
        }


# ============================================================
# 管理员平台模型
# ============================================================

class Admin(UserMixin, db.Model):
    __tablename__ = 'admins'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    email = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.Enum('super_admin', 'operation', 'finance', 'customer_service',
                             name='admin_role_enum'), nullable=False, server_default='operation')
    status = db.Column(db.Enum('active', 'disabled', name='admin_status_enum'),
                       nullable=False, server_default='active')
    last_login_at = db.Column(db.TIMESTAMP)
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())
    
    def check_password(self, password):
        if not self.password_hash or self.password_hash.strip() == '':
            return False
        return check_password_hash(self.password_hash, password)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def get_avatar(self):
        return "https://placehold.co/40x40/4f46e5/ffffff?text=A"

    @property
    def user_token(self):
        return None
    
    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role': self.role,
            'status': self.status,
            'last_login_at': self.last_login_at.strftime('%Y-%m-%d %H:%M:%S') if self.last_login_at else None,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None
        }


class AdminLog(db.Model):
    __tablename__ = 'admin_logs'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    admin_id = db.Column(db.Integer, db.ForeignKey('admins.id'), nullable=False)
    action = db.Column(db.String(100), nullable=False)
    target_type = db.Column(db.String(50))
    target_id = db.Column(db.String(100))
    detail = db.Column(db.Text)
    ip_address = db.Column(db.String(45))
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    
    admin = db.relationship('Admin', backref=db.backref('logs', lazy='dynamic'))
    
    @classmethod
    def log(cls, action, target_type=None, target_id=None, detail=None):
        """便捷记录管理员操作（需在请求上下文中调用）。

        Phase: 审计写路径已代理到 admin_audit_service(独立微服务,拥有 admin_logs 表)。
        鉴权/归属校验仍在各 admin 路由内(本方法只在校验通过后被调用)。

        - 优先 HTTP 代理到 admin_audit_service(由其单事务 INSERT admin_logs);
          代理成功直接返回(不再走本地 ORM,审计已由远端持久化)。
        - 代理失败(服务不可用/超时/非 2xx)时优雅降级:回退到原本地 ORM 写,
          沿用调用方后续的 db.session.commit(),保证审计不丢、调用方不崩。
        """
        import logging as _logging
        from flask import request
        from flask_login import current_user

        admin_id = current_user.id
        target_id_str = str(target_id) if target_id else None

        # ---- 优先代理 admin_audit_service(下游不可用则优雅降级到本地 ORM)----
        try:
            import requests as _requests
            from app.service_discovery import get_admin_audit_service_url
            resp = _requests.post(
                f"{get_admin_audit_service_url()}/api/audit",
                json={
                    "action": action,
                    "target_type": target_type,
                    "target_id": target_id_str,
                    "admin_id": admin_id,
                    "detail": detail,
                    "ip_address": request.remote_addr,
                },
                timeout=5,
            )
            if resp.status_code == 200 and (resp.json() or {}).get("success"):
                return None  # 审计已由 admin_audit_service 持久化,无需本地 ORM
            _logging.getLogger(__name__).warning(
                "[AdminAudit] audit service returned %s, falling back to local ORM",
                resp.status_code,
            )
        except Exception as e:
            _logging.getLogger(__name__).warning(
                "[AdminAudit] audit service unavailable (%s), falling back to local ORM", e
            )

        # ---- 降级:本地 ORM 写(由调用方后续 db.session.commit() 落库)----
        log_entry = cls(
            admin_id=admin_id,
            action=action,
            target_type=target_type,
            target_id=target_id_str,
            detail=json.dumps(detail, ensure_ascii=False) if detail else None,
            ip_address=request.remote_addr
        )
        db.session.add(log_entry)
        return log_entry


class Announcement(db.Model):
    __tablename__ = 'announcements'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text)
    type = db.Column(db.Enum('notice', 'banner', 'popup', name='announcement_type_enum'),
                     nullable=False, server_default='notice')
    image_url = db.Column(db.String(500))
    link_url = db.Column(db.String(500))
    sort_order = db.Column(db.Integer, default=0)
    status = db.Column(db.Enum('draft', 'published', 'archived', name='announcement_status_enum'),
                       nullable=False, server_default='draft')
    published_at = db.Column(db.TIMESTAMP)
    created_by = db.Column(db.Integer, db.ForeignKey('admins.id'))
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())
    
    creator = db.relationship('Admin', backref=db.backref('announcements', lazy='dynamic'))


# ============================================================
# 评论系统模型
# ============================================================

class Review(db.Model):
    __tablename__ = 'reviews'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    order_item_id = db.Column(db.BigInteger, db.ForeignKey('order_items.id'), unique=True, nullable=False)
    user_token = db.Column(db.String(100), db.ForeignKey('users.user_token'), nullable=False)
    item_id = db.Column(db.String(100), db.ForeignKey('items.item_id'), nullable=False)
    rating = db.Column(db.SmallInteger, nullable=False)
    content = db.Column(db.Text)
    images = db.Column(db.JSON)
    is_anonymous = db.Column(db.SmallInteger, default=0)
    status = db.Column(db.Enum('pending', 'approved', 'rejected', 'hidden',
                                name='review_status_enum'), nullable=False, server_default='approved')
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())

    order_item = db.relationship('OrderItem', backref=db.backref('review', uselist=False))
    user = db.relationship('User', backref=db.backref('reviews', lazy='dynamic'))
    item = db.relationship('Item', backref=db.backref('reviews', lazy='dynamic'))
    reply = db.relationship('ReviewReply', backref='review', uselist=False, cascade='all, delete-orphan')

    def to_dict(self):
        user = User.query.filter_by(user_token=self.user_token).first()
        return {
            'id': self.id,
            'username': 'Anonymous' if self.is_anonymous else (user.username if user and user.username else 'User'),
            'avatar': 'https://placehold.co/40x40/cccccc/333333?text=A' if self.is_anonymous
                      else (user.get_avatar() if user else 'https://placehold.co/40x40/cccccc/333333?text=U'),
            'rating': self.rating,
            'content': self.content or '',
            'images': self.images or [],
            'date': self.created_at.strftime('%Y-%m-%d') if self.created_at else '',
            'is_anonymous': self.is_anonymous,
            'reply': self.reply.to_dict() if self.reply else None,
        }


class ReviewReply(db.Model):
    __tablename__ = 'review_replies'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    review_id = db.Column(db.BigInteger, db.ForeignKey('reviews.id'), unique=True, nullable=False)
    merchant_id = db.Column(db.Integer, db.ForeignKey('merchants.id'), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())

    merchant = db.relationship('Merchant', backref=db.backref('review_replies', lazy='dynamic'))

    def to_dict(self):
        return {
            'id': self.id,
            'content': self.content,
            'date': self.created_at.strftime('%Y-%m-%d') if self.created_at else '',
        }


def refresh_item_rating(item_id):
    """Recalculate item's average rating and review count after review changes."""
    from sqlalchemy import func as sa_func
    result = db.session.query(
        sa_func.avg(Review.rating),
        sa_func.count(Review.id)
    ).filter(Review.item_id == item_id, Review.status == 'approved').first()

    item = Item.query.filter_by(item_id=item_id).first()
    if item:
        item.rating = round(result[0], 2) if result[0] else None
        item.review_count = result[1] or 0


# ============================================================
# AI 记忆模型
# ============================================================

class AiMemory(db.Model):
    __tablename__ = 'ai_memories'
    
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_token = db.Column(db.String(100), nullable=False)
    memory_type = db.Column(db.Enum('preference', 'need', 'constraint', 'personality',
                                     name='memory_type_enum'), nullable=False)
    content = db.Column(db.String(500), nullable=False)
    source = db.Column(db.String(200))
    confidence = db.Column(db.SmallInteger, default=5)
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp(),
                          onupdate=db.func.current_timestamp())
    
    def to_dict(self):
        return {
            'id': self.id,
            'user_token': self.user_token,
            'memory_type': self.memory_type,
            'content': self.content,
            'source': self.source,
            'confidence': self.confidence,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None
        }
