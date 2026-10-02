from flask import render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from app.extensions import db
from app.models import Item, User, Interaction, CartItem, UserAddress, Order, OrderItem, AiMemory, Review, refresh_item_rating
import random
import json as json_lib
from sqlalchemy.sql.expression import func
import uuid
import requests
import os
import re
import time

from app.buyer import buyer_bp as bp
from app.auth import is_buyer
from app.service_discovery import get_backend_api_url, get_rerank_service_url, get_review_service_url, get_review_query_service_url, get_catalog_service_url, get_cart_service_url, get_user_service_url, get_address_service_url, get_ai_memory_service_url, get_order_service_url, get_checkout_service_url, get_promotion_service_url, get_payment_service_url, get_inventory_service_url, get_search_service_url, get_interaction_service_url, get_notification_service_url
from opentelemetry import trace as _otel_trace_api
import logging

logger = logging.getLogger(__name__)
_tracer = _otel_trace_api.get_tracer(__name__)


@bp.route('/')
def index():
    # Hot products (random 6)
    items = Item.query.filter(
        Item.status == 'active',
        Item.title.isnot(None),
        db.not_(Item.title.like('Product\\_%'))
    ).order_by(func.rand()).limit(6).all()
    products = [item.to_dict() for item in items]
    
    # Guess You Like - use SASRec recommendations for logged-in users
    recommendations = []
    homepage_rec_limit = 3
    homepage_rec_fetch_k = 50
    
    if is_buyer():
        try:
            response = requests.post(
                f'{get_backend_api_url()}/api/recommend',
                json={'user_token': current_user.user_token, 'top_k': homepage_rec_fetch_k},
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                rec_items = result.get('recommendations', [])
                
                # Convert to format expected by template
                for rec_item in rec_items:
                    if rec_item.get('status') != 'active':
                        continue
                    title = rec_item.get('title') or ''
                    if title.startswith('Product_') or not title:
                        continue
                    recommendations.append({
                        'id': rec_item.get('id'),
                        'item_id': rec_item.get('item_id'),
                        'name': rec_item.get('title'),
                        'category': rec_item.get('category', 'Recommended'),
                        'price': float(rec_item.get('price')) if rec_item.get('price') else 0.0,
                        'image': rec_item.get('image_url') or 'https://placehold.co/400x400/f5f5f5/333333?text=No+Image'
                    })
                    if len(recommendations) >= homepage_rec_limit:
                        break
        except Exception as e:
            logger.warning('Error fetching recommendations: %s', e)
    
    # Fallback: use random popular items if no recommendations or not logged in
    if len(recommendations) < homepage_rec_limit:
        existing_item_ids = {item['item_id'] for item in recommendations}
        fallback_items = Item.query.filter(
            Item.status == 'active',
            Item.title.isnot(None),
            db.not_(Item.title.like('Product\\_%')),
            db.not_(Item.item_id.in_(existing_item_ids)) if existing_item_ids else db.true()
        ).order_by(func.rand()).limit(homepage_rec_limit - len(recommendations)).all()
        recommendations.extend(item.to_dict() for item in fallback_items)
    
    return render_template('buyer/index.html', products=products, recommendations=recommendations)

@bp.route('/about')
def about():
    return render_template('buyer/about.html')


@bp.route('/ai-picks')
@login_required
def ai_picks():
    """AI 精选页面：SASRec 候选 + LLM Rerank 重排序。"""
    error_msg = None
    ai_pick = None
    ai_reason = None
    ai_source = None
    candidates = []

    try:
        # 1. 获取 SASRec 推荐候选 (top 50, Product_ 占位商品交互量大会占据前排)
        rec_resp = requests.post(
            f'{get_backend_api_url()}/api/recommend',
            json={'user_token': current_user.user_token, 'top_k': 50},
            timeout=10
        )
        if rec_resp.status_code != 200:
            error_msg = '无法获取推荐候选。'
            return render_template('buyer/ai_picks.html', error=error_msg,
                                   ai_pick=None, candidates=[], ai_reason=None)

        rec_data = rec_resp.json()
        rec_items = rec_data.get('recommendations', [])
        if not rec_items:
            error_msg = '暂无推荐结果，请先浏览一些商品。'
            return render_template('buyer/ai_picks.html', error=error_msg,
                                   ai_pick=None, candidates=[], ai_reason=None)

        # 2. 构建用户历史和候选列表
        user_interactions = Interaction.query.filter(
            Interaction.user_token == current_user.user_token
        ).order_by(Interaction.timestamp.desc()).limit(20).all()

        seen_ids = set()
        user_history = []
        for inter in user_interactions:
            if inter.item_id not in seen_ids:
                seen_ids.add(inter.item_id)
                item = Item.query.filter_by(item_id=inter.item_id).first()
                if item:
                    user_history.append({
                        'item_id': item.item_id,
                        'title': item.title or f'Product_{item.item_id}'
                    })
        user_history = user_history[:10]

        # 候选商品列表 (给 rerank 服务)
        rerank_candidates = []
        for ri in rec_items:
            rerank_candidates.append({
                'item_id': ri.get('item_id'),
                'title': ri.get('title') or f"Product_{ri.get('item_id')}",
                'score': ri.get('recommendation_score', 0),
            })

        # 候选商品列表 (给前端展示)
        candidates = []
        for ri in rec_items:
            candidates.append({
                'id': ri.get('id'),
                'item_id': ri.get('item_id'),
                'name': ri.get('title') or f"Product_{ri.get('item_id')}",
                'category': ri.get('category') or 'Electronics',
                'brand': ri.get('brand', ''),
                'price': float(ri['price']) if ri.get('price') else 0.0,
                'image': ri.get('image_url') or 'https://placehold.co/400x400/f5f5f5/333333?text=No+Image',
                'score': ri.get('recommendation_score', 0),
                'rank': ri.get('recommendation_rank', 0),
            })

        # 3. 调用 LLM Rerank 服务
        try:
            rerank_resp = requests.post(
                f'{get_rerank_service_url()}/rerank',
                json={
                    'user_history': user_history,
                    'candidates': rerank_candidates,
                },
                timeout=30
            )
            rerank_data = rerank_resp.json()

            if rerank_data.get('success') and rerank_data.get('result'):
                result = rerank_data['result']
                selected_id = str(result.get('selected_item_id', '')).strip()
                ai_reason = result.get('reason', '')
                ai_source = result.get('source', 'llm')

                # 找到 AI 选中的商品
                for c in candidates:
                    if str(c['item_id']).strip() == selected_id:
                        ai_pick = c
                        break

                # OTel: 给当前 server span(GET /ai-picks)补 rerank 成功路径业务字段
                _picks_span = _otel_trace_api.get_current_span()
                _picks_span.set_attribute("recweb.ai_picks.source", ai_source)
                _picks_span.set_attribute("recweb.ai_picks.selected_item_id", selected_id)
                _picks_span.set_attribute("recweb.ai_picks.candidates_count", len(candidates))
        except Exception as e:
            logger.warning('LLM Rerank 服务调用失败: %s', e)
            _otel_trace_api.get_current_span().add_event(
                "ai_picks_rerank_call_failed", {"recweb.reason": str(e)[:200]})

        # Fallback: 如果 rerank 失败，用 SASRec top-1
        if not ai_pick and candidates:
            ai_pick = candidates[0]
            ai_reason = 'Based on your browsing history, our recommendation model selected this item for you.'
            ai_source = 'model'
            # OTel: fallback 是成功降级，用 event 记触发时刻 + source attribute 标状态(供 Jaeger 筛)
            _otel_trace_api.get_current_span().add_event(
                "ai_picks_fallback_to_sasrec_top1",
                {"reason": "rerank service unavailable or returned no pick"})
            _otel_trace_api.get_current_span().set_attribute("recweb.ai_picks.source", "model")

    except Exception as e:
        logger.warning('AI Picks 页面错误: %s', e)
        error_msg = '获取 AI 推荐时出错，请稍后再试。'

    return render_template('buyer/ai_picks.html',
                           ai_pick=ai_pick,
                           ai_reason=ai_reason,
                           ai_source=ai_source,
                           candidates=candidates,
                           error=error_msg)

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('buyer.index'))
    
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        remember = request.form.get('remember', False)
        
        user = User.query.filter_by(email=email).first()
        
        if user and user.check_password(password):
            if user.status == 'banned':
                flash('Your account has been banned. Please contact support.', 'error')
                return render_template('buyer/login.html')
            login_user(user, remember=bool(remember))
            session['_user_role'] = 'buyer'
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('buyer.index'))
        else:
            flash('Invalid email or password', 'error')
    
    return render_template('buyer/login.html')

@bp.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('buyer.index'))
    
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        
        if not email or not password or not confirm_password:
            flash('All fields are required', 'error')
            return render_template('buyer/register.html')
        
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            flash('Invalid email format', 'error')
            return render_template('buyer/register.html')
        
        if password != confirm_password:
            flash('Passwords do not match', 'error')
            return render_template('buyer/register.html')
        
        if len(password) < 8:
            flash('Password must be at least 8 characters long', 'error')
            return render_template('buyer/register.html')
        
        if not re.search(r'[A-Z]', password):
            flash('Password must contain at least one uppercase letter', 'error')
            return render_template('buyer/register.html')
        
        if not re.search(r'[a-z]', password):
            flash('Password must contain at least one lowercase letter', 'error')
            return render_template('buyer/register.html')
        
        if not re.search(r'[0-9]', password):
            flash('Password must contain at least one number', 'error')
            return render_template('buyer/register.html')
        
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            flash('Email already registered', 'error')
            return render_template('buyer/register.html')
        
        try:
            new_user = User()
            new_user.user_token = str(uuid.uuid4())
            new_user.email = email
            new_user.username = email.split('@')[0]
            new_user.set_password(password)
            
            db.session.add(new_user)
            db.session.commit()
            
            flash('Registration successful! Please login.', 'success')
            return redirect(url_for('buyer.login'))
        except Exception as e:
            db.session.rollback()
            logger.warning('Error creating user: %s', e)
            flash('Registration failed. Please try again.', 'error')
            return render_template('buyer/register.html')
    
    return render_template('buyer/register.html')

@bp.route('/logout')
@login_required
def logout():
    logout_user()
    session.pop('_user_role', None)
    return redirect(url_for('buyer.index'))


@bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    if not is_buyer():
        return redirect(url_for('buyer.index'))

    if request.method == 'POST':
        action = request.form.get('action', '').strip()
        if action == 'profile':
            username = request.form.get('username', '').strip()
            avatar_url = request.form.get('avatar_url', '').strip()
            # 鉴权由 @login_required + is_buyer() 保证;资料写入交给 user_service
            # (它拥有 users 资料,负责 UPDATE)。仅提交本次实际改动的字段(部分更新):
            # username 为空时保持原值不动,avatar_url 始终覆盖(空串→NULL,与原行为一致)。
            payload = {'avatar_url': avatar_url}
            if username:
                payload['username'] = username
            try:
                resp = requests.post(
                    f'{get_user_service_url()}/api/users/{current_user.user_token}/profile',
                    json=payload,
                    timeout=10,
                )
                if resp.status_code == 200 and resp.json().get('success'):
                    flash('Profile updated successfully', 'success')
                else:
                    flash('Profile update failed', 'error')
            except requests.exceptions.RequestException as e:
                logger.warning('[User] user_service unavailable: %s', e)
                flash('User service unavailable, please try again later', 'error')
            return redirect(url_for('buyer.profile'))

        if action == 'password':
            old_password = request.form.get('old_password', '')
            new_password = request.form.get('new_password', '')
            confirm_password = request.form.get('confirm_password', '')
            if not current_user.check_password(old_password):
                flash('Current password is incorrect', 'error')
                return redirect(url_for('buyer.profile'))
            if len(new_password) < 8:
                flash('New password must be at least 8 characters', 'error')
                return redirect(url_for('buyer.profile'))
            if new_password != confirm_password:
                flash('Passwords do not match', 'error')
                return redirect(url_for('buyer.profile'))
            current_user.set_password(new_password)
            db.session.commit()
            flash('Password updated successfully', 'success')
            return redirect(url_for('buyer.profile'))

    addresses = UserAddress.query.filter_by(
        user_token=current_user.user_token
    ).order_by(UserAddress.is_default.desc(), UserAddress.created_at.desc()).all()
    return render_template('buyer/profile.html', addresses=addresses)


@bp.route('/account')
@login_required
def account():
    return redirect(url_for('buyer.profile'))


@bp.route('/user')
@login_required
def user_center():
    return redirect(url_for('buyer.profile'))

def _fetch_reviews_via_service(item_id):
    """商品页首屏评论经 review_query_service 读取(thin-slice 代理)。

    调 GET /api/reviews(首屏前 10 条 approved) + GET /api/reviews/summary(总数/均分),
    两个都成功时返回 (reviews, review_stats) 元组;任何异常/非 success 返回 None,
    调用方落回原 ORM 路径——降级绝不阻断商品页渲染。
    """
    try:
        base = get_review_query_service_url()
        list_resp = requests.get(
            f'{base}/api/reviews',
            params={'item_id': item_id, 'status': 'approved',
                    'page': 1, 'per_page': 10},
            timeout=8,
        )
        summary_resp = requests.get(
            f'{base}/api/reviews/summary',
            params={'item_id': item_id},
            timeout=8,
        )
        if list_resp.status_code != 200 or summary_resp.status_code != 200:
            return None
        list_body = list_resp.json() or {}
        summary_body = summary_resp.json() or {}
        if not list_body.get('success') or not summary_body.get('success'):
            return None

        reviews = list_body.get('reviews') or []
        for review in reviews:
            # images 防御兜底(双保险): 服务端已 json.loads 反序列化,
            # 这里再防 str 透传——Jinja 首屏 {% for img in review.images %}
            # 拿到 str 会被逐字符迭代渲染出碎 <img>。
            images = review.get('images')
            if isinstance(images, str):
                try:
                    images = json_lib.loads(images)
                except (ValueError, TypeError):
                    images = []
            if not isinstance(images, list):
                images = []
            review['images'] = images
            # rating 防御性强转 int(模板 range(review.rating) 需 int)
            try:
                review['rating'] = int(review.get('rating') or 0)
            except (TypeError, ValueError):
                review['rating'] = 0

        review_stats = {
            'total': summary_body.get('total') or 0,
            'avg_rating': summary_body.get('avg_rating') or 0.0,
        }
        return reviews, review_stats
    except requests.exceptions.RequestException as e:
        logger.warning('[reviews] review_query_service unavailable for first screen, fallback to ORM: %s', e)
        return None
    except Exception as e:
        logger.warning('[reviews] first-screen review proxy failed, fallback to ORM: %s', e)
        return None


def _record_interaction(action, item_id, source, session_id=None, quantity=None, price=None):
    """用户行为打点(thin-slice 写代理 → interaction_service :5020)。

    先 POST /api/interactions(user_token 服务端取 current_user,不信前端);
    非 200/success 或请求异常时降级为本地 ORM 写——interactions 是推荐管线
    数据源(backend_api 取最近 50 条喂 SASRec),打点失败必须回退本地写而非丢弃。
    外层 try/except 吞掉一切异常仅 warning:打点永不阻断主流程。
    """
    try:
        fallback_reason = None
        try:
            resp = requests.post(
                f'{get_interaction_service_url()}/api/interactions',
                json={
                    'user_token': current_user.user_token,
                    'item_id': item_id,
                    'action': action,
                    'session_id': session_id,
                    'source': source,
                    'quantity': quantity,
                    'price': price,
                },
                timeout=5,
            )
            body = {}
            if resp.status_code == 200:
                try:
                    body = resp.json() or {}
                except ValueError:
                    body = {}
            if body.get('success'):
                return
            fallback_reason = f'HTTP {resp.status_code} non-success'
        except requests.exceptions.RequestException as e:
            fallback_reason = str(e)
        logger.warning('[interaction] interaction_service write failed (%s), fallback to local ORM write',
                       fallback_reason)

        # 降级:本地 ORM 写(与原打点同表同字段同 timestamp 语义)。
        # quantity/price 仅在提供时显式赋值,保留 click 打点的列默认值(quantity=1)。
        try:
            kwargs = {
                'user_token': current_user.user_token,
                'item_id': item_id,
                'interaction_type': action,
                'session_id': session_id,
                'source': source,
                'timestamp': int(time.time() * 1000),
            }
            if quantity is not None:
                kwargs['quantity'] = quantity
            if price is not None:
                kwargs['price'] = price
            db.session.add(Interaction(**kwargs))
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.warning('Error saving interaction: %s', e)
    except Exception as e:
        logger.warning('[interaction] record failed (non-blocking): %s', e)


@bp.route('/product/<int:product_id>')
def product_detail(product_id):
    product = Item.query.get_or_404(product_id)
    
    # Get or create session ID
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    
    # Save click interaction (only for buyer users) — proxied to interaction_service,
    # falls back to local ORM write; never blocks the page.
    if is_buyer():
        _record_interaction('click', product.item_id, 'direct', session['session_id'])
    
    # 首屏评论优先经 review_query_service 读取(失败回 ORM, 不阻断渲染)
    fetched = _fetch_reviews_via_service(product.item_id)
    if fetched is not None:
        reviews, review_stats = fetched
    else:
        # Fetch real reviews (ORM fallback)
        review_objs = Review.query.filter_by(
            item_id=product.item_id, status='approved'
        ).order_by(Review.created_at.desc()).limit(10).all()
        reviews = [r.to_dict() for r in review_objs]

        # Review stats
        from sqlalchemy import func as sa_func
        stats = db.session.query(
            sa_func.count(Review.id),
            sa_func.avg(Review.rating)
        ).filter(Review.item_id == product.item_id, Review.status == 'approved').first()
        review_stats = {
            'total': stats[0] or 0,
            'avg_rating': round(float(stats[1]), 1) if stats[1] else 0.0,
        }

    return render_template('buyer/product_detail.html', product=product.to_dict(),
                           reviews=reviews, review_stats=review_stats)

@bp.route('/recommended/<int:product_id>')
def recommended_product_detail(product_id):
    product = Item.query.get_or_404(product_id)
    
    # Get or create session ID
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    
    # Save click interaction with 'recommendation' source (only for buyer users) —
    # proxied to interaction_service, falls back to local ORM write; never blocks the page.
    if is_buyer():
        _record_interaction('click', product.item_id, 'recommendation', session['session_id'])
    
    # Fetch user interaction history for AI context (only for buyer users, excluding current product)
    user_interactions = []
    if is_buyer():
        user_interactions = Interaction.query.filter(
            Interaction.user_token == current_user.user_token,
            Interaction.item_id != product.item_id
        ).order_by(Interaction.timestamp.desc()).limit(50).all()
    
    # Process interactions: count duplicates and filter
    interaction_counts = {}
    for inter in user_interactions:
        if inter.item_id not in interaction_counts:
            interaction_counts[inter.item_id] = {
                'count': 0,
                'type': inter.interaction_type
            }
        interaction_counts[inter.item_id]['count'] += 1
    
    # Build processed history for AI
    processed_history = []
    for item_id, data in interaction_counts.items():
        # Get item info
        item = Item.query.filter_by(item_id=item_id).first()
        if item:
            if data['count'] >= 5:
                processed_history.append({
                    'name': item.title,
                    'category': item.category,
                    'note': f"User viewed this product multiple times ({data['count']} times)"
                })
            else:
                processed_history.append({
                    'name': item.title,
                    'category': item.category,
                    'note': None
                })
    
    # Limit to recent 10 unique items
    processed_history = processed_history[:10]
    
    # 首屏评论优先经 review_query_service 读取(失败回 ORM, 不阻断渲染)
    fetched = _fetch_reviews_via_service(product.item_id)
    if fetched is not None:
        reviews, review_stats = fetched
    else:
        # Fetch real reviews (ORM fallback)
        review_objs = Review.query.filter_by(
            item_id=product.item_id, status='approved'
        ).order_by(Review.created_at.desc()).limit(10).all()
        reviews = [r.to_dict() for r in review_objs]

        # Review stats
        from sqlalchemy import func as sa_func
        stats = db.session.query(
            sa_func.count(Review.id),
            sa_func.avg(Review.rating)
        ).filter(Review.item_id == product.item_id, Review.status == 'approved').first()
        review_stats = {
            'total': stats[0] or 0,
            'avg_rating': round(float(stats[1]), 1) if stats[1] else 0.0,
        }

    return render_template('buyer/recommended_product_detail.html',
                           product=product.to_dict(), 
                           reviews=reviews,
                           review_stats=review_stats,
                           interaction_history=json_lib.dumps(processed_history, ensure_ascii=False))

@bp.route('/api/chat', methods=['POST'])
@login_required
def chat_assistant():
    """AI assistant chat endpoint - supports multi-turn conversation"""
    try:
        from openai import OpenAI
        
        data = request.get_json(silent=True) or {}
        messages = data.get('messages', [])
        product_context = data.get('product_context', {})

        if not messages or not isinstance(messages, list):
            return {'error': 'Messages cannot be empty'}, 400
        
        # 直接从环境变量读取 DeepSeek 配置（load_dotenv 已在启动时加载 .env）
        api_key = os.environ.get('DEEPSEEK_API_KEY', '')
        base_url = os.environ.get('DEEPSEEK_API_BASE', 'https://api.deepseek.com/v1')
        model = os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat')
        temperature = 0.7
        max_tokens = 500
        
        if not api_key or api_key == 'your_deepseek_api_key_here':
            return {
                'error': 'Please configure API Key',
                'message': 'Please set DEEPSEEK_API_KEY in .env file'
            }, 500
        
        # Initialize DeepSeek client (OpenAI compatible)
        client = OpenAI(
            api_key=api_key,
            base_url=base_url
        )
        
        # Build system prompt with product context and user behavior history
        interaction_history = data.get('interaction_history', [])
        
        # Build history description
        history_desc = ""
        if interaction_history:
            history_items = []
            for item in interaction_history:
                if item.get('note'):
                    history_items.append(f"- {item['name']} ({item.get('category', 'Unknown category')}) - {item['note']}")
                else:
                    history_items.append(f"- {item['name']} ({item.get('category', 'Unknown category')})")
            history_desc = "\n".join(history_items)
        
        # Load persistent AI memories for this user
        memory_desc = ""
        try:
            memories = AiMemory.query.filter_by(
                user_token=current_user.user_token
            ).order_by(AiMemory.confidence.desc()).limit(15).all()
            if memories:
                memory_items = [f"- [{m.memory_type}] {m.content}" for m in memories]
                memory_desc = "\n".join(memory_items)
        except Exception as e:
            logger.warning('[AiMemory] Failed to load memories: %s', e)
        
        system_message = {
            "role": "system",
            "content": f"""You are a professional product recommendation assistant. Your role is to explain why the system recommended the current product based on the user's browsing behavior patterns.

[Current Recommended Product]
Product Name: {product_context.get('name', 'Unknown product')}
Category: {product_context.get('category', 'Unknown')}
Brand: {product_context.get('brand', 'Unknown')}
Price: {product_context.get('price', 'Price on Request')}
Description: {product_context.get('description', 'No description')}

[User Profile Memory]
{memory_desc if memory_desc else 'New user, no prior memory'}

[User Browsing History]
{history_desc if history_desc else 'No browsing history'}

[Important Rules]
1. Leverage the user's profile memory to provide personalized responses; refer to known preferences naturally
2. Explain why this product was recommended based on the user's browsing behavior
3. Do not directly say "You browsed XXX before", instead use natural phrases like "Based on your interest in XX category" or "You seem to be interested in XX"
4. If the user viewed certain product categories multiple times, emphasize this interest
5. Keep responses concise and friendly, like a professional shopping consultant
6. If the user asks about product-related questions, provide professional answers based on the product information"""
        }
        
        # Build complete message list
        full_messages = [system_message] + messages
        
        # Call DeepSeek API（新建 child span 承载 GenAI 语义；与 5b request_llm 同款写法）
        with _tracer.start_as_current_span(f"chat {model}") as _span:
            _span.set_attribute("gen_ai.operation.name", "chat")
            _span.set_attribute("gen_ai.system", "deepseek")
            _span.set_attribute("gen_ai.request.model", model)
            _span.set_attribute("gen_ai.request.temperature", temperature)
            _span.set_attribute("gen_ai.request.max_tokens", max_tokens)
            _span.set_attribute("recweb.ai.role", "buyer")
            _span.set_attribute("recweb.ai.purpose", "chat")
            response = client.chat.completions.create(
                model=model,
                messages=full_messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            if response.usage:
                _span.set_attribute("gen_ai.response.model", response.model)
                _span.set_attribute("gen_ai.response.id", response.id)
                _span.set_attribute("gen_ai.usage.input_tokens", response.usage.prompt_tokens)
                _span.set_attribute("gen_ai.usage.output_tokens", response.usage.completion_tokens)

        # Extract AI response
        assistant_message = response.choices[0].message.content
        
        return {
            'success': True,
            'message': assistant_message,
            'usage': {
                'prompt_tokens': response.usage.prompt_tokens,
                'completion_tokens': response.usage.completion_tokens,
                'total_tokens': response.usage.total_tokens
            }
        }
        
    except ImportError:
        return {
            'error': 'Missing openai package',
            'message': 'Please run: pip install openai'
        }, 500
    except Exception as e:
        logger.warning('[BuyerChat] Error: %s', e)
        return {
            'error': 'AI service call failed',
            'message': 'AI service is temporarily unavailable, please try again later'
        }, 500

@bp.route('/api/chat/extract-memory', methods=['POST'])
@login_required
def extract_memory():
    """Extract user insights from conversation and persist to ai_memories table"""
    if not is_buyer():
        return jsonify({'success': False, 'message': 'Buyer only'}), 403
    try:
        from openai import OpenAI
        
        # 畸形/缺体 → get_json(silent=True) 返 None → 落 isinstance 守卫返 400(不再 `or {}` 当空对话吞成 200)
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({'success': False, 'message': 'Invalid request body'}), 400
        conversation = data.get('conversation', [])
        product_context = data.get('product_context', {})

        if not conversation or len(conversation) < 2:
            return jsonify({'success': True, 'extracted': 0, 'message': 'Conversation too short'})
        
        api_key = os.environ.get('DEEPSEEK_API_KEY', '')
        base_url = os.environ.get('DEEPSEEK_API_BASE', 'https://api.deepseek.com/v1')
        model = os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat')
        
        if not api_key or api_key == 'your_deepseek_api_key_here':
            return jsonify({'success': False, 'message': 'API Key not configured'}), 500
        
        client = OpenAI(api_key=api_key, base_url=base_url)
        
        # Build conversation text for extraction
        conv_text = "\n".join([f"{m['role']}: {m['content']}" for m in conversation])
        product_name = product_context.get('name', 'Unknown')
        
        extract_prompt = f"""从以下购物助手对话中提取用户的关键偏好信息。

返回 JSON 数组，每条记录包含：
- "type": 类型，必须是 "preference" | "need" | "constraint" | "personality" 之一
- "content": 用用户使用的语言（中文对话就用中文）简洁描述该条信息（最多80字）
- "confidence": 1-10 置信度

类型说明：
- preference: 用户明确表达的喜好（品牌、品类、功能、生活偏好等）。例："喜欢索尼耳机"、"喜欢吃苹果"
- need: 当前购物目标或需求。例："正在找家庭影院系统"
- constraint: 预算或硬性限制。例："预算 200 美元以内"
- personality: 沟通风格或决策偏好。例："注重性价比"、"喜欢看详细参数"

核心规则：
1. 如果用户明确要求"记住XX"，必须**直接记录该信息本身**，而非对用户行为的元描述
   - 正确 ✓："喜欢吃的水果是苹果"
   - 错误 ✗："Volunteers personal non-shopping preferences"
2. 记录用户**实际表达的偏好内容**，不要做间接概括
3. 不要提取显而易见的事实（如"用户在购物"、"用户要求说中文"）
4. 如果没有有价值的信息，返回空数组 []
5. 每次最多提取 3 条
6. 只返回 JSON 数组，不要有其他文字

讨论的商品：{product_name}

对话内容：
{conv_text}"""
        
        with _tracer.start_as_current_span(f"chat {model}") as _span:
            _span.set_attribute("gen_ai.operation.name", "chat")
            _span.set_attribute("gen_ai.system", "deepseek")
            _span.set_attribute("gen_ai.request.model", model)
            _span.set_attribute("gen_ai.request.temperature", 0.3)
            _span.set_attribute("gen_ai.request.max_tokens", 300)
            _span.set_attribute("recweb.ai.role", "buyer")
            _span.set_attribute("recweb.ai.purpose", "extract_memory")
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": extract_prompt}],
                temperature=0.3,
                max_tokens=300
            )
            # W2 原代码未提取 usage，5c 补取（判空：错误响应 usage 可能为 None）
            if response.usage:
                _span.set_attribute("gen_ai.response.model", response.model)
                _span.set_attribute("gen_ai.response.id", response.id)
                _span.set_attribute("gen_ai.usage.input_tokens", response.usage.prompt_tokens)
                _span.set_attribute("gen_ai.usage.output_tokens", response.usage.completion_tokens)

        raw_response = response.choices[0].message.content.strip()
        
        # Parse JSON from response — handle markdown blocks, surrounding text, etc.
        json_str = raw_response
        if '```' in json_str:
            json_str = json_str.split('```')[1]
            if json_str.startswith('json'):
                json_str = json_str[4:]
            json_str = json_str.strip()
        
        # Fallback: find the first JSON array in the response
        if not json_str.startswith('['):
            start = json_str.find('[')
            end = json_str.rfind(']')
            if start != -1 and end != -1 and end > start:
                json_str = json_str[start:end + 1]
        
        extracted = json_lib.loads(json_str)
        
        if not isinstance(extracted, list):
            return jsonify({'success': True, 'extracted': 0, 'message': 'No memories extracted'})
        
        # Upsert memories: update if similar content exists, otherwise insert
        saved_count = 0
        valid_types = {'preference', 'need', 'constraint', 'personality'}
        
        for item in extracted[:3]:
            mem_type = item.get('type', '')
            content = item.get('content', '').strip()
            confidence = item.get('confidence', 5)
            
            if mem_type not in valid_types or not content:
                continue
            
            confidence = max(1, min(10, int(confidence)))
            
            # Check for duplicate/similar memory (same type, same user)
            existing = AiMemory.query.filter_by(
                user_token=current_user.user_token,
                memory_type=mem_type
            ).all()
            
            # Simple dedup: skip if very similar content already exists
            is_duplicate = False
            for mem in existing:
                if _memory_similarity(mem.content.lower(), content.lower()) >= 0.6:
                    # Update confidence if new is higher
                    if confidence > mem.confidence:
                        mem.confidence = confidence
                        mem.content = content
                        mem.source = f"chat about {product_name}"
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                new_memory = AiMemory(
                    user_token=current_user.user_token,
                    memory_type=mem_type,
                    content=content[:500],
                    source=f"chat about {product_name}",
                    confidence=confidence
                )
                db.session.add(new_memory)
                saved_count += 1
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'extracted': saved_count,
            'message': f'Extracted {saved_count} new memories'
        })
        
    except (json_lib.JSONDecodeError, ValueError) as e:
        logger.warning('[AiMemory] JSON parse error: %s', e)
        return jsonify({'success': True, 'extracted': 0, 'message': 'Parse error, no memories saved'})
    except ImportError:
        return jsonify({'success': False, 'message': 'Missing openai package'}), 500
    except Exception as e:
        db.session.rollback()
        logger.warning('[AiMemory] Extract error: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


def _memory_similarity(a: str, b: str) -> float:
    """Text similarity using character bigrams (works for CJK and English)."""
    def _bigrams(s):
        tokens = s.split()
        if len(tokens) <= 1:
            # Character-level bigrams for CJK / single-token text
            return {s[i:i+2] for i in range(len(s) - 1)} if len(s) > 1 else {s}
        return {f'{tokens[i]} {tokens[i+1]}' for i in range(len(tokens) - 1)}
    bg_a = _bigrams(a)
    bg_b = _bigrams(b)
    if not bg_a or not bg_b:
        return 0.0
    return len(bg_a & bg_b) / len(bg_a | bg_b)


@bp.route('/api/memories', methods=['GET'])
@login_required
def get_memories():
    """Get all AI memories for the current user"""
    # 鉴权由 @login_required 保证;读列表代理给 ai_memory_service(它拥有 ai_memories,
    # 带 user_token 做权威过滤)。
    try:
        resp = requests.get(
            f'{get_ai_memory_service_url()}/api/memories',
            params={'user_token': current_user.user_token},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[AiMemory] ai_memory_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'AI memory service unavailable'}), 503


@bp.route('/api/memories/<int:memory_id>', methods=['DELETE'])
@login_required
def delete_memory(memory_id):
    """Delete a specific AI memory"""
    # 鉴权由 @login_required 保证;归属校验与删除交给 ai_memory_service(带 user_token 做权威校验)。
    try:
        resp = requests.delete(
            f'{get_ai_memory_service_url()}/api/memories/{memory_id}',
            params={'user_token': current_user.user_token},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[AiMemory] ai_memory_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'AI memory service unavailable'}), 503


@bp.route('/api/notifications', methods=['GET'])
@login_required
def get_notifications():
    """Get notifications for the current user (Profile 页通知卡片)"""
    # 鉴权由 @login_required 保证;user_token 服务端取 current_user,不收前端参数,
    # 只能读自己的通知。page/per_page 透传(per_page 钳制不越服务上限 50)。
    # notifications 是新表、不在 shop_web models.py,无 ORM 可兜底:
    # notification_service 不可用时降级返回空列表 + degraded 标记,保 Profile 页不崩。
    try:
        page = int(request.args.get('page', 1))
    except (TypeError, ValueError):
        page = 1
    try:
        per_page = int(request.args.get('per_page', 20))
    except (TypeError, ValueError):
        per_page = 20
    page = max(page, 1)
    per_page = min(max(per_page, 1), 50)
    try:
        resp = requests.get(
            f'{get_notification_service_url()}/api/notifications',
            params={
                'user_token': current_user.user_token,
                'page': page,
                'per_page': per_page,
            },
            timeout=8,
        )
        return jsonify(resp.json()), resp.status_code
    except (requests.exceptions.RequestException, ValueError) as e:
        logger.warning('[Notification] notification_service unavailable: %s', e)
        return jsonify({'success': True, 'notifications': [], 'degraded': True})


@bp.route('/cart')
@login_required
def cart():
    cart_items = CartItem.query.filter_by(user_token=current_user.user_token).all()
    
    cart_data = []
    total_amount = 0.0
    
    for cart_item in cart_items:
        item = Item.query.filter_by(item_id=cart_item.item_id).first()
        if item:
            item_dict = cart_item.to_dict(item)
            cart_data.append(item_dict)
            total_amount += item_dict['subtotal']
    
    return render_template('buyer/cart.html', cart_items=cart_data, total_amount=total_amount)

@bp.route('/api/cart/add', methods=['POST'])
@login_required
def add_to_cart():
    # 鉴权由 @login_required 保证;参数轻校验后,写操作代理给 cart_service。
    data = request.json or {}
    item_id = data.get('item_id')
    try:
        quantity = int(data.get('quantity', 1))
    except (TypeError, ValueError):
        quantity = 0

    if not item_id or quantity <= 0:
        return jsonify({'success': False, 'message': 'Invalid parameters'}), 400

    # UPSERT + 行为日志 + 计数交给 cart_service(它经 catalog_service 校验商品/取价)
    try:
        resp = requests.post(
            f'{get_cart_service_url()}/api/cart/add',
            json={
                'user_token': current_user.user_token,
                'item_id': item_id,
                'quantity': quantity,
            },
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Cart] cart_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Cart service unavailable'}), 503

@bp.route('/api/cart/update/<int:cart_id>', methods=['POST'])
@login_required
def update_cart_item(cart_id):
    # 鉴权由 @login_required 保证;归属校验与改数量交给 cart_service(带 user_token 做权威校验)。
    data = request.json or {}
    try:
        quantity = int(data.get('quantity', 1))
    except (TypeError, ValueError):
        quantity = 0

    if quantity <= 0:
        return jsonify({'success': False, 'message': 'Quantity must be greater than 0'}), 400

    try:
        resp = requests.post(
            f'{get_cart_service_url()}/api/cart/update/{cart_id}',
            json={
                'user_token': current_user.user_token,
                'quantity': quantity,
            },
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Cart] cart_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Cart service unavailable'}), 503

@bp.route('/api/cart/remove/<int:cart_id>', methods=['POST'])
@login_required
def remove_from_cart(cart_id):
    # 鉴权由 @login_required 保证;归属校验、行为日志与删除交给 cart_service(带 user_token)。
    try:
        resp = requests.post(
            f'{get_cart_service_url()}/api/cart/remove/{cart_id}',
            json={'user_token': current_user.user_token},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Cart] cart_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Cart service unavailable'}), 503

@bp.route('/api/cart/count')
@login_required
def get_cart_count():
    # 鉴权由 @login_required 保证;计数读取交给 cart_service(带 user_token)。
    try:
        resp = requests.get(
            f'{get_cart_service_url()}/api/cart/count',
            params={'user_token': current_user.user_token},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Cart] cart_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Cart service unavailable'}), 503


# ============================================================
# Search Routes
# ============================================================

@bp.route('/search')
def search():
    q = request.args.get('q', '').strip()
    category = request.args.get('category', '').strip()
    min_price = request.args.get('min_price', type=float)
    max_price = request.args.get('max_price', type=float)
    sort_by = request.args.get('sort_by', 'relevance')
    page = request.args.get('page', 1, type=int)
    per_page = 12

    # 条件代理 → search_service:仅覆盖"无过滤 + 默认排序"的最高频形态
    # (导航栏搜索框 / 搜索页默认提交)。带 category/价格区间/非默认排序的检索
    # 走下方 ORM 全路径(search_service 契约不支持这些参数)。
    # 代理失败/非 200/success 为假 → 静默降级 ORM 兜底,绝不阻断检索页。
    if not category and min_price is None and max_price is None and sort_by == 'relevance':
        try:
            resp = requests.get(
                f'{get_search_service_url()}/api/search',
                params={'q': q, 'page': page, 'per_page': per_page},
                timeout=8,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('success'):
                    # 分类侧栏统计保留 ORM 直查(轻量聚合,不在 search_service 契约内)
                    all_cats = db.session.query(Item.category).filter(
                        Item.category.isnot(None),
                        Item.category != '',
                        Item.status == 'active'
                    ).all()
                    top_counts = {}
                    for (raw,) in all_cats:
                        top = raw.split(' > ')[0].strip()
                        if top:
                            top_counts[top] = top_counts.get(top, 0) + 1
                    category_list = sorted(top_counts.items(), key=lambda x: x[1], reverse=True)

                    return render_template('buyer/search.html',
                        products=data.get('items') or [],
                        query=q,
                        category=category,
                        categories=category_list,
                        min_price=min_price,
                        max_price=max_price,
                        sort_by=sort_by,
                        page=page,
                        total_pages=data.get('total_pages', 0),
                        total_results=data.get('total', 0)
                    )
        except (requests.exceptions.RequestException, ValueError) as e:
            logger.warning('[Search] search_service unavailable for /search page, fallback to local ORM: %s', e)

    query = Item.query.filter(
        Item.price.isnot(None),
        Item.status == 'active'
    )

    # Keyword search — use LIKE for reliable CJK support
    # (MySQL FULLTEXT default parser cannot tokenize Chinese text properly)
    if q:
        # 转义 LIKE 通配符(! % _, 先 !), 用 ! 作 escape 符避免反斜杠双重转义陷阱。
        like_q = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like_pattern = f'%{like_q}%'
        query = query.filter(
            db.or_(
                Item.title.ilike(like_pattern, escape='!'),
                Item.brand.ilike(like_pattern, escape='!'),
                Item.category.ilike(like_pattern, escape='!')
            )
        )

    # Category filter (top-level prefix match)
    if category:
        query = query.filter(
            db.or_(
                Item.category == category,
                Item.category.like(category + ' > %')
            )
        )

    # Price range filter
    if min_price is not None:
        query = query.filter(Item.price >= min_price)
    if max_price is not None:
        query = query.filter(Item.price <= max_price)

    # Sorting
    if sort_by == 'price_asc':
        query = query.order_by(Item.price.asc())
    elif sort_by == 'price_desc':
        query = query.order_by(Item.price.desc())
    elif sort_by == 'rating_desc':
        query = query.order_by(Item.rating.is_(None), Item.rating.desc())
    elif sort_by == 'newest':
        query = query.order_by(Item.created_at.desc())
    else:
        if q:
            query = query.order_by(Item.id)
        else:
            query = query.order_by(func.rand())

    # Pagination
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    products = [item.to_dict() for item in pagination.items]

    # Get top-level categories with counts for filter sidebar
    all_cats = db.session.query(Item.category).filter(
        Item.category.isnot(None),
        Item.category != '',
        Item.status == 'active'
    ).all()
    top_counts = {}
    for (raw,) in all_cats:
        top = raw.split(' > ')[0].strip()
        if top:
            top_counts[top] = top_counts.get(top, 0) + 1
    category_list = sorted(top_counts.items(), key=lambda x: x[1], reverse=True)

    return render_template('buyer/search.html',
        products=products,
        query=q,
        category=category,
        categories=category_list,
        min_price=min_price,
        max_price=max_price,
        sort_by=sort_by,
        page=page,
        total_pages=pagination.pages,
        total_results=pagination.total
    )


@bp.route('/api/search')
def api_search():
    """商品检索 JSON 接口 —— 代理到 search_service(读路径,无鉴权,公开)。

    新增"搜索经服务"这条边(shop_web → search_service)。search_service 不可用时
    优雅降级到本地 ORM 直查 items(LIKE title/brand/category),保证检索可用不阻断。
    """
    q = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 12, type=int)
    try:
        resp = requests.get(
            f'{get_search_service_url()}/api/search',
            params={'q': q, 'page': page, 'per_page': per_page},
            timeout=8,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Search] search_service unavailable, fallback to local ORM: %s', e)

    # 降级:本地 ORM 直查(与 search_service 同语义,CJK 用 LIKE)
    per_page = min(max(per_page or 12, 1), 50)
    page = max(page or 1, 1)
    query = Item.query.filter(Item.price.isnot(None), Item.status == 'active')
    if q:
        like_q = q.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        like_pattern = f'%{like_q}%'
        query = query.filter(
            db.or_(
                Item.title.ilike(like_pattern, escape='!'),
                Item.brand.ilike(like_pattern, escape='!'),
                Item.category.ilike(like_pattern, escape='!'),
            )
        )
    query = query.order_by(Item.id)
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    products = [item.to_dict() for item in pagination.items]
    return jsonify({
        'success': True,
        'query': q,
        'items': products,
        'page': page,
        'per_page': per_page,
        'total': pagination.total,
        'total_pages': pagination.pages,
        'degraded': True,
    })


@bp.route('/api/categories')
def get_categories():
    """商品分类列表 —— 代理到 catalog_service(读路径,无鉴权,公开)。"""
    try:
        resp = requests.get(f'{get_catalog_service_url()}/api/categories', timeout=10)
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Catalog] catalog_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Catalog service unavailable'}), 503


# ============================================================
# Order System Routes
# ============================================================

@bp.route('/checkout')
@login_required
def checkout():
    """Checkout page - confirm cart items and select/add address"""
    cart_items_db = CartItem.query.filter_by(user_token=current_user.user_token).all()
    if not cart_items_db:
        flash('Your cart is empty', 'error')
        return redirect(url_for('buyer.cart'))
    
    cart_items = []
    total_amount = 0
    for ci in cart_items_db:
        item = Item.query.filter_by(item_id=ci.item_id).first()
        if item and item.price:
            subtotal = float(item.price) * ci.quantity
            cart_items.append({
                'id': ci.id,
                'item': item.to_dict(),
                'quantity': ci.quantity,
                'subtotal': subtotal
            })
            total_amount += subtotal
    
    addresses = UserAddress.query.filter_by(
        user_token=current_user.user_token
    ).order_by(UserAddress.is_default.desc(), UserAddress.created_at.desc()).all()
    
    return render_template('buyer/checkout.html',
        cart_items=cart_items,
        total_amount=total_amount,
        item_count=sum(ci['quantity'] for ci in cart_items),
        addresses=addresses
    )


@bp.route('/api/checkout/preview', methods=['GET'])
@login_required
def checkout_preview():
    """结账预览(JSON)——代理到 checkout_service 的三路扇出。

    鉴权由 @login_required 保证;user_token 取自当前登录用户(权威,不信任入参),
    数据源/算价/查库存全部交给 checkout_service(checkout→cart/pricing/inventory 三路 fan-out)。
    渲染页面 /checkout 不动,这里只补一个走代理的只读 JSON 预览接口。
    下游不可用 → 503 降级,不影响页面渲染。
    """
    try:
        resp = requests.get(
            f'{get_checkout_service_url()}/api/checkout/preview',
            params={'user_token': current_user.user_token},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Checkout] checkout_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Checkout service unavailable'}), 503


@bp.route('/api/promotions', methods=['GET'])
def list_promotions():
    """当前有效优惠列表 —— 代理到 promotion_service(读路径,无鉴权,公开)。"""
    try:
        resp = requests.get(f'{get_promotion_service_url()}/api/promotions', timeout=10)
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Promotion] promotion_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Promotion service unavailable'}), 503


@bp.route('/api/promotions/validate', methods=['POST'])
@login_required
def validate_promotion():
    """校验券码 —— 代理到 promotion_service(契约返回 {valid, discount})。

    鉴权由 @login_required 保证(本项目无 CSRF,沿用 session);从 body 取 code 转发。
    下游不可用 → 503 降级。
    """
    data = request.get_json() or {}
    code = (data.get('code') or '').strip()
    if not code:
        return jsonify({'success': False, 'message': 'code is required'}), 400

    try:
        resp = requests.post(
            f'{get_promotion_service_url()}/api/promotions/validate',
            json={'code': code},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Promotion] promotion_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Promotion service unavailable'}), 503


@bp.route('/api/address/save', methods=['POST'])
@login_required
def save_address():
    """Save or update a shipping address.

    鉴权由 @login_required 保证;输入校验保留在 shop_web,
    is_default 互斥/首条强制默认/归属校验与 INSERT/UPDATE 交给 address_service
    (带 user_token 做权威归属校验)。
    """
    data = request.get_json() or {}
    receiver_name = (data.get('receiver_name') or '').strip()
    receiver_phone = (data.get('receiver_phone') or '').strip()
    address = (data.get('address') or '').strip()

    if not receiver_name or not receiver_phone or not address:
        return jsonify({'success': False, 'message': 'Name, phone and address are required'}), 400

    try:
        resp = requests.post(
            f'{get_address_service_url()}/api/address/save',
            json={
                'user_token': current_user.user_token,
                'receiver_name': receiver_name,
                'receiver_phone': receiver_phone,
                'province': (data.get('province') or '').strip(),
                'city': (data.get('city') or '').strip(),
                'district': (data.get('district') or '').strip(),
                'address': address,
                'is_default': data.get('is_default', 0),
                'address_id': data.get('address_id'),
            },
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Address] address_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Address service unavailable'}), 503


@bp.route('/api/address/delete', methods=['POST'])
@login_required
def delete_address():
    """Delete a shipping address.

    鉴权由 @login_required 保证;归属校验与删除交给 address_service(带 user_token)。
    """
    data = request.get_json() or {}
    address_id = data.get('address_id')
    if not address_id:
        return jsonify({'success': False, 'message': 'address_id is required'}), 400

    try:
        resp = requests.post(
            f'{get_address_service_url()}/api/address/delete',
            json={'user_token': current_user.user_token, 'address_id': address_id},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Address] address_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Address service unavailable'}), 503


@bp.route('/api/address/default', methods=['POST'])
@login_required
def set_default_address():
    """Set an address as default for current user.

    鉴权由 @login_required 保证;归属校验与 is_default 互斥更新交给 address_service(带 user_token)。
    """
    data = request.get_json() or {}
    address_id = data.get('address_id')
    if not address_id:
        return jsonify({'success': False, 'message': 'address_id is required'}), 400

    try:
        resp = requests.post(
            f'{get_address_service_url()}/api/address/default',
            json={'user_token': current_user.user_token, 'address_id': address_id},
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Address] address_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Address service unavailable'}), 503


@bp.route('/api/order/create', methods=['POST'])
@login_required
def create_order():
    """Create an order from cart items"""
    # Z-OBS-01: 在 try 之前初始化,保证 except 分支(无论异常发生在哪一步)总能引用,
    # 不会因 reserved_ok 未绑定而二次抛 NameError。
    reserved_ok = []
    try:
        data = request.get_json(silent=True) or {}
        address_id = data.get('address_id')
        remark = data.get('remark', '').strip()
        
        if not address_id:
            return jsonify({'success': False, 'message': 'Please select a shipping address'}), 400
        
        addr = UserAddress.query.filter_by(
            id=address_id, user_token=current_user.user_token
        ).first()
        if not addr:
            return jsonify({'success': False, 'message': 'Address not found'}), 404
        
        cart_items = CartItem.query.filter_by(user_token=current_user.user_token).all()
        if not cart_items:
            return jsonify({'success': False, 'message': 'Cart is empty'}), 400
        
        order_no = f"ORD{int(time.time() * 1000)}{random.randint(1000, 9999)}"
        
        total_amount = 0
        item_count = 0
        order_items = []
        # reserved_ok 已在 try 之前初始化(跟踪本次实际成功预留 inventory.reserved 的项,
        # 供下单失败回滚时精确释放)。

        for ci in cart_items:
            item = Item.query.filter_by(item_id=ci.item_id).first()
            if not item or not item.price:
                continue
            subtotal = float(item.price) * ci.quantity
            total_amount += subtotal
            item_count += ci.quantity
            order_items.append(OrderItem(
                item_id=ci.item_id,
                item_title=item.title or 'Unknown Product',
                item_image=item.image_url,
                item_price=item.price,
                quantity=ci.quantity,
                subtotal=subtotal
            ))

            # 软校验 / 遥测用:对每件计入订单的商品最佳努力调用 inventory_service 预留库存,
            # 顺带点亮 shop_web→inventory_service(→catalog_service)依赖边。
            # 必须非阻塞:inventory 表基本为空(stock=0),无论成功、库存不足(409)还是服务
            # 不可用,都只 logger.warning 然后继续建单——绝不因此拒单/rollback/改变下单结果。
            try:
                _inv_resp = requests.post(
                    f'{get_inventory_service_url()}/api/inventory/{ci.item_id}/reserve',
                    json={'quantity': ci.quantity},
                    timeout=8,
                )
                # 仅在确认 inventory 侧真扣了 reserved(2xx + body.success)时记入,
                # 以便失败回滚时精确释放。库存不足 409 等情况 inventory 侧未改 reserved,
                # 不可记入(否则 release 会错误扣减他人预留)。
                if _inv_resp.status_code == 200:
                    try:
                        if (_inv_resp.json() or {}).get('success'):
                            reserved_ok.append((ci.item_id, ci.quantity))
                    except ValueError:
                        pass  # 非 JSON 响应,保守不记入
            except Exception as inv_err:
                logger.warning('[Order] inventory reserve (soft, non-blocking) failed for item %s: %s', ci.item_id, inv_err)

        if not order_items:
            return jsonify({'success': False, 'message': 'No valid items in cart'}), 400
        
        order = Order(
            order_no=order_no,
            user_token=current_user.user_token,
            address_id=addr.id,
            snapshot_receiver_name=addr.receiver_name,
            snapshot_receiver_phone=addr.receiver_phone,
            snapshot_address=addr.full_address(),
            total_amount=total_amount,
            item_count=item_count,
            remark=remark
        )
        db.session.add(order)
        db.session.flush()
        
        for oi in order_items:
            oi.order_id = order.id
            db.session.add(oi)
        
        CartItem.query.filter_by(user_token=current_user.user_token).delete()
        
        db.session.commit()

        # 闭合预留生命周期: 下单成功后释放本次 best-effort 预留的 inventory.reserved。
        # 本系统下单不依赖/不真扣 stock, 成功提交后预留即可归还, 避免 reserved 单调累积。
        # 释放纯 best-effort, 任何失败仅 warning, 绝不改变已成功的下单响应。
        for _rel_item_id, _rel_qty in reserved_ok:
            try:
                requests.post(
                    f'{get_inventory_service_url()}/api/inventory/{_rel_item_id}/release',
                    json={'quantity': _rel_qty},
                    timeout=8,
                )
            except Exception as rel_err:
                logger.warning('[Order] inventory release (post-commit) failed for item %s: %s', _rel_item_id, rel_err)

        return jsonify({
            'success': True,
            'order_no': order_no,
            'order_id': order.id,
            'total_amount': total_amount
        })
    except Exception as e:
        db.session.rollback()
        logger.warning('[Order] Error creating order: %s', e)
        # Z-OBS-01: 下单失败,shop_web 侧 order/order_item 已 rollback,但循环中已 best-effort
        # 预留的 inventory.reserved 已落库(inventory 各自 commit),db.session.rollback() 不会
        # 撤销它们 → 这里 best-effort 把已成功预留的 reserved 还回去,消除失败下单的库存泄漏。
        # 释放本身也是 best-effort:任何失败仅 logger.warning,绝不二次抛错改变 500 响应。
        for _rel_item_id, _rel_qty in reserved_ok:
            try:
                requests.post(
                    f'{get_inventory_service_url()}/api/inventory/{_rel_item_id}/release',
                    json={'quantity': _rel_qty},
                    timeout=8,
                )
            except Exception as rel_err:
                logger.warning('[Order] inventory release (rollback compensation) failed for item %s: %s', _rel_item_id, rel_err)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


@bp.route('/orders')
@login_required
def orders():
    """Order list page with status filter"""
    status_filter = request.args.get('status', '')
    page = request.args.get('page', 1, type=int)
    per_page = 10
    
    query = Order.query.filter_by(user_token=current_user.user_token)
    if status_filter and status_filter in ('pending', 'paid', 'shipped', 'completed', 'cancelled'):
        query = query.filter_by(status=status_filter)
    
    query = query.order_by(Order.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    status_counts = {}
    all_orders = Order.query.filter_by(user_token=current_user.user_token)
    status_counts['all'] = all_orders.count()
    for s in ('pending', 'paid', 'shipped', 'completed', 'cancelled'):
        status_counts[s] = all_orders.filter_by(status=s).count()
    
    # For completed orders, build a set of order_item_ids that have been reviewed
    reviewed_oi_ids = set()
    completed_orders = [o for o in pagination.items if o.status == 'completed']
    if completed_orders:
        all_oi_ids = []
        for o in completed_orders:
            all_oi_ids.extend([oi.id for oi in o.items])
        if all_oi_ids:
            reviewed = Review.query.filter(Review.order_item_id.in_(all_oi_ids)).all()
            reviewed_oi_ids = {r.order_item_id for r in reviewed}

    return render_template('buyer/orders.html',
        orders=pagination.items,
        status_filter=status_filter,
        status_counts=status_counts,
        page=page,
        total_pages=pagination.pages,
        total_results=pagination.total,
        reviewed_oi_ids=reviewed_oi_ids
    )


@bp.route('/order/<order_no>')
@login_required
def order_detail(order_no):
    """Order detail page"""
    order = Order.query.filter_by(
        order_no=order_no, user_token=current_user.user_token
    ).first_or_404()

    # For completed orders, find which items have been reviewed
    reviewed_item_ids = set()
    if order.status == 'completed':
        order_item_ids = [oi.id for oi in order.items]
        if order_item_ids:
            reviewed = Review.query.filter(
                Review.order_item_id.in_(order_item_ids)
            ).all()
            reviewed_item_ids = {r.order_item_id for r in reviewed}

    return render_template('buyer/order_detail.html', order=order,
                           reviewed_item_ids=reviewed_item_ids)


@bp.route('/api/orders')
@login_required
def get_orders_json():
    """订单列表(JSON)—— 读路径代理到 order_service。

    /orders、/order/<no> 是服务端渲染页面,难以纯代理,保持其 ORM 渲染不动;
    这里另加 JSON 读接口走代理:鉴权由 @login_required 保证,带 user_token 让
    order_service 做权威归属过滤(读为主,不写)。
    """
    try:
        resp = requests.get(
            f'{get_order_service_url()}/api/orders',
            params={
                'user_token': current_user.user_token,
                'status': request.args.get('status', ''),
                'page': request.args.get('page', 1),
                'per_page': request.args.get('per_page', 10),
            },
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Order] order_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Order service unavailable'}), 503


@bp.route('/api/order/<order_no>')
@login_required
def get_order_json(order_no):
    """订单详情(JSON)—— 读路径代理到 order_service(带 user_token 做归属校验)。

    enrich 透传给 order_service:为 1 时触发深度链 order→catalog 补当前商品信息。
    """
    try:
        resp = requests.get(
            f'{get_order_service_url()}/api/orders/{order_no}',
            params={
                'user_token': current_user.user_token,
                'enrich': request.args.get('enrich', ''),
            },
            timeout=10,
        )
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.RequestException as e:
        logger.warning('[Order] order_service unavailable: %s', e)
        return jsonify({'success': False, 'message': 'Order service unavailable'}), 503


@bp.route('/api/order/<order_no>/pay', methods=['POST'])
@login_required
def pay_order(order_no):
    """Simulate payment"""
    try:
        # Lock the row (FOR UPDATE) before check-then-set so concurrent
        # pay/cancel/complete on the same order serialize: a second tx blocks
        # until commit, then reads status='paid' and is rejected by the guard.
        order = Order.query.filter_by(
            order_no=order_no, user_token=current_user.user_token
        ).with_for_update().first()
        if not order:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        if order.status != 'pending':
            return jsonify({'success': False, 'message': 'Order cannot be paid'}), 400
        
        # Commit payment state first so non-critical logging failures
        # never roll back a successful payment transition.
        order.status = 'paid'
        order.paid_at = db.func.current_timestamp()
        db.session.commit()

        if 'session_id' not in session:
            session['session_id'] = str(uuid.uuid4())
        # Purchase interactions — proxied to interaction_service (with quantity/price),
        # falls back to local ORM write per item; never affects the committed payment.
        for oi in order.items:
            _record_interaction('purchase', oi.item_id, 'order', session['session_id'],
                                quantity=oi.quantity, price=float(oi.item_price))

        # Best-effort: record the payment with payment_service for topology
        # observability. amount comes from the authoritative backend
        # order.total_amount (never trusted from the frontend). Any failure is
        # logged as a warning only — it never rolls back nor affects the
        # already-committed "paid" state / response.
        try:
            requests.post(
                f'{get_payment_service_url()}/api/payments',
                json={
                    'order_no': order.order_no,
                    'amount': float(order.total_amount),
                },
                timeout=8,
            )
        except Exception as e:
            logger.warning('[Order] payment_service record failed after payment: %s', e)

        return jsonify({'success': True, 'message': 'Payment successful'})
    except Exception as e:
        db.session.rollback()
        logger.warning('[Order] Error processing payment: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


@bp.route('/api/order/<order_no>/cancel', methods=['POST'])
@login_required
def cancel_order(order_no):
    """Cancel an order (only pending orders)"""
    try:
        data = request.get_json() or {}
        # Lock the row (FOR UPDATE) before check-then-set so concurrent
        # pay/cancel/complete on the same order serialize (see pay_order).
        order = Order.query.filter_by(
            order_no=order_no, user_token=current_user.user_token
        ).with_for_update().first()
        if not order:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        if order.status != 'pending':
            return jsonify({'success': False, 'message': 'Only pending orders can be cancelled'}), 400
        
        order.status = 'cancelled'
        order.cancelled_at = db.func.current_timestamp()
        order.cancel_reason = data.get('cancel_reason', '')
        
        db.session.commit()
        return jsonify({'success': True, 'message': 'Order cancelled'})
    except Exception as e:
        db.session.rollback()
        logger.warning('[Order] Error cancelling order: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


@bp.route('/api/review', methods=['POST'])
@login_required
def submit_review():
    """Submit a review for a purchased item (order must be completed)."""
    try:
        data = request.get_json() or {}
        order_item_id = data.get('order_item_id')
        rating = data.get('rating')
        content = data.get('content', '')
        images = data.get('images', [])
        is_anonymous = 1 if data.get('is_anonymous') else 0

        if not order_item_id or not rating:
            return jsonify({'success': False, 'message': 'order_item_id and rating are required'}), 400
        # bool is a subclass of int in Python (isinstance(True, int) is True),
        # so exclude bool first to stop rating:true from passing as 1.
        if isinstance(rating, bool) or not isinstance(rating, int) or rating < 1 or rating > 5:
            return jsonify({'success': False, 'message': 'Rating must be an integer between 1 and 5'}), 400

        # Verify order_item belongs to current user and order is completed
        order_item = OrderItem.query.get(order_item_id)
        if not order_item:
            return jsonify({'success': False, 'message': 'Order item not found'}), 404

        order = Order.query.get(order_item.order_id)
        if not order or order.user_token != current_user.user_token:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        if order.status != 'completed':
            return jsonify({'success': False, 'message': 'Only completed orders can be reviewed'}), 400

        # Check if already reviewed
        existing = Review.query.filter_by(order_item_id=order_item_id).first()
        if existing:
            return jsonify({'success': False, 'message': 'This item has already been reviewed'}), 400

        # 写入交给 review_service(共享库,负责 INSERT + 重算 items 评分)
        try:
            resp = requests.post(
                f'{get_review_service_url()}/api/reviews',
                json={
                    'order_item_id': order_item_id,
                    'user_token': current_user.user_token,
                    'item_id': order_item.item_id,
                    'rating': rating,
                    'content': content,
                    'images': images,
                    'is_anonymous': is_anonymous,
                },
                timeout=10,
            )
            return jsonify(resp.json()), resp.status_code
        except requests.exceptions.RequestException as e:
            logger.warning('[Review] review_service unavailable: %s', e)
            return jsonify({'success': False, 'message': 'Review service unavailable'}), 503
    except Exception as e:
        db.session.rollback()
        logger.warning('[Review] Error submitting review: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500


@bp.route('/api/reviews/<item_id>')
def get_reviews(item_id):
    """Get paginated reviews for an item (public).

    读路径代理: 优先经 HTTP 调 review_query_service(只读评论微服务);
    下游不可用/异常时优雅降级回本地 ORM 直查(行为与拆分前一致,不中断商品页)。
    """
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    per_page = min(per_page, 50)

    # ---- 优先代理 review_query_service ----
    try:
        resp = requests.get(
            f'{get_review_query_service_url()}/api/reviews',
            params={'item_id': item_id, 'status': 'approved',
                    'page': page, 'per_page': per_page},
            timeout=8,
        )
        if resp.status_code == 200:
            body = resp.json() or {}
            if body.get('success'):
                return jsonify(body)
    except requests.exceptions.RequestException as e:
        logger.warning('[reviews] review_query_service unavailable, fallback to ORM: %s', e)
    except Exception as e:
        logger.warning('[reviews] review_query_service proxy failed, fallback to ORM: %s', e)

    # ---- 降级: 本地 ORM 直查 ----
    query = Review.query.filter_by(item_id=item_id, status='approved').order_by(Review.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        'success': True,
        'reviews': [r.to_dict() for r in pagination.items],
        'page': page,
        'per_page': per_page,
        'total': pagination.total,
        'total_pages': pagination.pages,
    })


@bp.route('/api/order/<order_no>/complete', methods=['POST'])
@login_required
def complete_order(order_no):
    """Confirm receipt (only shipped orders)"""
    try:
        # Lock the row (FOR UPDATE) before check-then-set so concurrent
        # pay/cancel/complete on the same order serialize (see pay_order).
        order = Order.query.filter_by(
            order_no=order_no, user_token=current_user.user_token
        ).with_for_update().first()
        if not order:
            return jsonify({'success': False, 'message': 'Order not found'}), 404
        if order.status != 'shipped':
            return jsonify({'success': False, 'message': 'Only shipped orders can be completed'}), 400
        
        order.status = 'completed'
        order.completed_at = db.func.current_timestamp()
        
        db.session.commit()
        return jsonify({'success': True, 'message': 'Order completed'})
    except Exception as e:
        db.session.rollback()
        logger.warning('[Order] Error completing order: %s', e)
        return jsonify({'success': False, 'message': 'Internal server error'}), 500
